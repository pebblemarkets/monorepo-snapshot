use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rmp_serde::{from_slice, to_vec};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::error::FluidError;
use crate::wal::{
    local_wal_tip, wal_command_crc32c, WalCommand, WalOffset, WalRecord, WalSegmentReader,
    WalWriterHandle,
};

pub const REPLICATION_MAGIC: [u8; 4] = *b"PBLR";
pub const REPLICATION_PROTOCOL_VERSION: u8 = 1;
pub const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_FOLLOWER_QUEUE_CAPACITY: usize = 4_096;

/// Handshake: follower -> leader.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationHandshake {
    pub magic: [u8; 4],
    pub protocol_version: u8,
    pub node_id: String,
    pub engine_version: String,
    pub after_offset: WalOffset,
    pub after_offset_crc32c: u32,
    pub auth_token: Vec<u8>,
}

/// Handshake response: leader -> follower.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HandshakeResponse {
    pub magic: [u8; 4],
    pub protocol_version: u8,
    pub leader_epoch: u64,
    pub status: HandshakeStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum HandshakeStatus {
    Ok,
    OffsetNotAvailable,
    DivergentLog { truncate_to: WalOffset },
    VersionMismatch,
    AuthFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReplicationMessage {
    WalBatch {
        leader_epoch: u64,
        records: Vec<WalRecordPayload>,
    },
    Heartbeat {
        leader_epoch: u64,
        tip_offset: WalOffset,
        committed_offset: WalOffset,
    },
    Shutdown {
        final_offset: WalOffset,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalRecordPayload {
    pub offset: WalOffset,
    pub command_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReplicationAck {
    DurableApplied { up_to: WalOffset },
}

fn replication_error(message: impl Into<String>) -> FluidError {
    FluidError::Replication(message.into())
}

pub fn validate_leader_epoch(
    known_epoch: u64,
    incoming_epoch: u64,
    context: &str,
) -> Result<u64, FluidError> {
    if incoming_epoch < known_epoch {
        return Err(replication_error(format!(
            "stale leader epoch in {context}: incoming={incoming_epoch}, known={known_epoch}"
        )));
    }
    Ok(known_epoch.max(incoming_epoch))
}

pub async fn write_framed<T, W>(writer: &mut W, value: &T) -> Result<(), FluidError>
where
    T: Serialize,
    W: AsyncWrite + Unpin,
{
    let payload = to_vec(value)
        .map_err(|err| replication_error(format!("failed to encode replication frame: {err}")))?;
    if payload.len() > DEFAULT_MAX_FRAME_BYTES {
        return Err(replication_error(format!(
            "replication frame exceeds max size: {} > {}",
            payload.len(),
            DEFAULT_MAX_FRAME_BYTES
        )));
    }
    let frame_len = u32::try_from(payload.len())
        .map_err(|_| replication_error("replication frame length overflow"))?;
    writer
        .write_all(&frame_len.to_le_bytes())
        .await
        .map_err(|err| replication_error(format!("failed to write frame length: {err}")))?;
    writer
        .write_all(&payload)
        .await
        .map_err(|err| replication_error(format!("failed to write frame payload: {err}")))?;
    writer
        .flush()
        .await
        .map_err(|err| replication_error(format!("failed to flush frame payload: {err}")))?;
    Ok(())
}

pub async fn read_framed<T, R>(reader: &mut R) -> Result<T, FluidError>
where
    T: DeserializeOwned,
    R: AsyncRead + Unpin,
{
    let mut len_buf = [0_u8; 4];
    reader
        .read_exact(&mut len_buf)
        .await
        .map_err(|err| replication_io_to_error("failed to read frame length", err))?;
    let frame_len = usize::try_from(u32::from_le_bytes(len_buf))
        .map_err(|_| replication_error("replication frame length conversion overflow"))?;
    if frame_len > DEFAULT_MAX_FRAME_BYTES {
        return Err(replication_error(format!(
            "replication frame exceeds max size: {frame_len} > {DEFAULT_MAX_FRAME_BYTES}"
        )));
    }
    let mut payload = vec![0_u8; frame_len];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(|err| replication_io_to_error("failed to read frame payload", err))?;
    let decoded = from_slice::<T>(&payload)
        .map_err(|err| replication_error(format!("failed to decode replication frame: {err}")))?;
    Ok(decoded)
}

fn replication_io_to_error(context: &str, err: io::Error) -> FluidError {
    replication_error(format!("{context}: {err}"))
}

pub fn validate_handshake(
    handshake: &ReplicationHandshake,
    expected_engine_version: &str,
    expected_auth_token: &[u8],
    leader_tip: WalOffset,
    leader_crc_at_after_offset: Option<u32>,
) -> HandshakeStatus {
    if handshake.magic != REPLICATION_MAGIC
        || handshake.protocol_version != REPLICATION_PROTOCOL_VERSION
        || handshake.engine_version != expected_engine_version
    {
        return HandshakeStatus::VersionMismatch;
    }
    if handshake.auth_token.as_slice() != expected_auth_token {
        return HandshakeStatus::AuthFailed;
    }
    if handshake.after_offset.0 == 0 {
        return HandshakeStatus::Ok;
    }
    if handshake.after_offset > leader_tip {
        return HandshakeStatus::DivergentLog {
            truncate_to: leader_tip,
        };
    }
    let Some(leader_crc) = leader_crc_at_after_offset else {
        return HandshakeStatus::OffsetNotAvailable;
    };
    if leader_crc != handshake.after_offset_crc32c {
        let truncate_to = WalOffset(handshake.after_offset.0.saturating_sub(1));
        return HandshakeStatus::DivergentLog { truncate_to };
    }
    HandshakeStatus::Ok
}

#[derive(Debug)]
pub struct FollowerRegistration {
    pub captured_tip: WalOffset,
    pub rx: mpsc::Receiver<Arc<Vec<WalRecordPayload>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BroadcastOutcome {
    pub dropped_followers: Vec<String>,
    pub tip_offset: WalOffset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowerDurabilitySnapshot {
    pub node_id: String,
    pub durable_applied: WalOffset,
}

struct FollowerState {
    tx: mpsc::Sender<Arc<Vec<WalRecordPayload>>>,
    durable_applied: WalOffset,
}

struct BroadcasterState {
    current_tip: WalOffset,
    followers: HashMap<String, FollowerState>,
}

#[derive(Clone)]
pub struct ReplicationBroadcaster {
    inner: Arc<Mutex<BroadcasterState>>,
    queue_capacity: usize,
}

impl ReplicationBroadcaster {
    pub fn new(initial_tip: WalOffset, queue_capacity: usize) -> Result<Self, FluidError> {
        if queue_capacity == 0 {
            return Err(replication_error(
                "replication queue_capacity must be greater than zero",
            ));
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(BroadcasterState {
                current_tip: initial_tip,
                followers: HashMap::new(),
            })),
            queue_capacity,
        })
    }

    pub fn with_default_capacity(initial_tip: WalOffset) -> Result<Self, FluidError> {
        Self::new(initial_tip, DEFAULT_FOLLOWER_QUEUE_CAPACITY)
    }

    pub fn register_follower(&self, node_id: String) -> Result<FollowerRegistration, FluidError> {
        if node_id.trim().is_empty() {
            return Err(replication_error("replication node_id must not be empty"));
        }
        let (tx, rx) = mpsc::channel(self.queue_capacity);
        let captured_tip = {
            let mut state = self
                .inner
                .lock()
                .map_err(|_| replication_error("replication broadcaster mutex poisoned"))?;
            if state.followers.contains_key(&node_id) {
                return Err(replication_error(format!(
                    "duplicate replication node_id: {node_id}"
                )));
            }
            let tip = state.current_tip;
            state.followers.insert(
                node_id,
                FollowerState {
                    tx,
                    durable_applied: WalOffset(0),
                },
            );
            tip
        };
        Ok(FollowerRegistration { captured_tip, rx })
    }

    pub fn unregister_follower(&self, node_id: &str) -> Result<(), FluidError> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| replication_error("replication broadcaster mutex poisoned"))?;
        let _ = state.followers.remove(node_id);
        Ok(())
    }

    pub fn broadcast_batch(
        &self,
        batch: Arc<Vec<WalRecordPayload>>,
    ) -> Result<BroadcastOutcome, FluidError> {
        let mut dropped = Vec::new();
        let tip_offset = {
            let mut state = self
                .inner
                .lock()
                .map_err(|_| replication_error("replication broadcaster mutex poisoned"))?;
            if let Some(last) = batch.last() {
                state.current_tip = last.offset;
            }
            for (node_id, follower) in &state.followers {
                if follower.tx.try_send(Arc::clone(&batch)).is_err() {
                    dropped.push(node_id.clone());
                }
            }
            state.current_tip
        };

        for node_id in &dropped {
            let _ = self.unregister_follower(node_id);
        }

        Ok(BroadcastOutcome {
            dropped_followers: dropped,
            tip_offset,
        })
    }

    pub fn update_durable_applied(
        &self,
        node_id: &str,
        up_to: WalOffset,
    ) -> Result<(), FluidError> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| replication_error("replication broadcaster mutex poisoned"))?;
        if let Some(follower) = state.followers.get_mut(node_id) {
            if up_to > follower.durable_applied {
                follower.durable_applied = up_to;
            }
        }
        Ok(())
    }

    pub fn follower_durable_applied(&self, node_id: &str) -> Result<Option<WalOffset>, FluidError> {
        let state = self
            .inner
            .lock()
            .map_err(|_| replication_error("replication broadcaster mutex poisoned"))?;
        Ok(state
            .followers
            .get(node_id)
            .map(|follower| follower.durable_applied))
    }

    pub fn tip_offset(&self) -> Result<WalOffset, FluidError> {
        let state = self
            .inner
            .lock()
            .map_err(|_| replication_error("replication broadcaster mutex poisoned"))?;
        Ok(state.current_tip)
    }

    pub fn connected_followers(&self) -> Result<usize, FluidError> {
        let state = self
            .inner
            .lock()
            .map_err(|_| replication_error("replication broadcaster mutex poisoned"))?;
        Ok(state.followers.len())
    }

    pub fn durable_ack_count_at_or_above(&self, offset: WalOffset) -> Result<usize, FluidError> {
        let state = self
            .inner
            .lock()
            .map_err(|_| replication_error("replication broadcaster mutex poisoned"))?;
        Ok(state
            .followers
            .values()
            .filter(|follower| follower.durable_applied >= offset)
            .count())
    }

    pub fn follower_durability_snapshot(
        &self,
    ) -> Result<Vec<FollowerDurabilitySnapshot>, FluidError> {
        let state = self
            .inner
            .lock()
            .map_err(|_| replication_error("replication broadcaster mutex poisoned"))?;
        let mut snapshots = state
            .followers
            .iter()
            .map(|(node_id, follower)| FollowerDurabilitySnapshot {
                node_id: node_id.clone(),
                durable_applied: follower.durable_applied,
            })
            .collect::<Vec<_>>();
        snapshots.sort_by(|lhs, rhs| lhs.node_id.cmp(&rhs.node_id));
        Ok(snapshots)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FollowerWriteResult {
    Appended { offset: WalOffset },
    Duplicate { offset: WalOffset },
}

pub struct FollowerWalWriter {
    wal_writer: WalWriterHandle,
    wal_dir: PathBuf,
    durable_applied: WalOffset,
}

impl FollowerWalWriter {
    pub fn new(wal_writer: WalWriterHandle, wal_dir: PathBuf) -> Result<Self, FluidError> {
        let durable_applied = local_wal_tip(&wal_dir)?;
        Ok(Self {
            wal_writer,
            wal_dir,
            durable_applied,
        })
    }

    pub fn durable_applied(&self) -> WalOffset {
        self.durable_applied
    }

    pub async fn write_payload(
        &mut self,
        payload: &WalRecordPayload,
    ) -> Result<FollowerWriteResult, FluidError> {
        if payload.offset.0 > self.durable_applied.0.saturating_add(1) {
            return Err(replication_error(format!(
                "replication offset gap: expected {}, got {}",
                self.durable_applied.0.saturating_add(1),
                payload.offset.0
            )));
        }

        if payload.offset <= self.durable_applied {
            let local_crc =
                wal_command_crc32c(&self.wal_dir, payload.offset)?.ok_or_else(|| {
                    replication_error(format!(
                        "duplicate offset {} missing in local WAL",
                        payload.offset
                    ))
                })?;
            let incoming_crc = crc32c::crc32c(&payload.command_bytes);
            if local_crc == incoming_crc {
                return Ok(FollowerWriteResult::Duplicate {
                    offset: payload.offset,
                });
            }
            return Err(replication_error(format!(
                "replication payload conflict at offset {}",
                payload.offset
            )));
        }

        let command: WalCommand = from_slice(&payload.command_bytes).map_err(|err| {
            replication_error(format!(
                "failed to decode replicated command at offset {}: {err}",
                payload.offset
            ))
        })?;
        let assigned_offset = self.wal_writer.append(command).await?;
        if assigned_offset != payload.offset {
            return Err(replication_error(format!(
                "replicated WAL offset mismatch: expected {}, wrote {}",
                payload.offset, assigned_offset
            )));
        }
        self.durable_applied = assigned_offset;
        Ok(FollowerWriteResult::Appended {
            offset: assigned_offset,
        })
    }
}

pub fn wal_record_to_payload(record: &WalRecord) -> Result<WalRecordPayload, FluidError> {
    let command_bytes = to_vec(&record.command).map_err(|err| {
        replication_error(format!(
            "failed to serialize WAL record at offset {} for replication: {err}",
            record.offset
        ))
    })?;
    Ok(WalRecordPayload {
        offset: record.offset,
        command_bytes,
    })
}

pub fn read_payloads_between_offsets(
    wal_dir: &Path,
    after_offset: WalOffset,
    up_to_inclusive: WalOffset,
) -> Result<Vec<WalRecordPayload>, FluidError> {
    if up_to_inclusive < after_offset {
        return Ok(Vec::new());
    }
    let mut reader = WalSegmentReader::open(wal_dir, after_offset, false)?;
    let mut payloads = Vec::new();
    loop {
        let batch = reader.read_batch(256)?;
        if batch.is_empty() {
            break;
        }
        for record in batch {
            if record.offset <= after_offset {
                continue;
            }
            if record.offset > up_to_inclusive {
                return Ok(payloads);
            }
            payloads.push(wal_record_to_payload(&record)?);
        }
    }
    Ok(payloads)
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use pebble_trading::{IncomingOrder, MarketId, OrderId, OrderNonce, OrderSide, OrderType};
    use tokio::io::{duplex, sink};

    use crate::wal::{WalConfig, WalOffset, WalSegmentReader, WalWriterHandle};

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> PathBuf {
        let mut dir = env::temp_dir();
        let process_id = std::process::id();
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be >= unix epoch")
            .as_nanos();
        let seq = TEST_DIR_COUNTER.fetch_add(1, Ordering::SeqCst);
        dir.push(format!("pebble-fluid-repl-{process_id}-{elapsed}-{seq}"));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn drop_dir(path: &Path) {
        let _ = fs::remove_dir_all(path);
    }

    fn make_order(order_id: &str, nonce: i64) -> WalCommand {
        WalCommand::PlaceOrder(IncomingOrder {
            order_id: OrderId(order_id.to_string()),
            account_id: "alice".to_string(),
            market_id: MarketId("m1".to_string()),
            outcome: "YES".to_string(),
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            tif: pebble_trading::TimeInForce::Gtc,
            nonce: OrderNonce(nonce),
            price_ticks: 100,
            quantity_minor: 1,
            submitted_at_micros: 1,
        })
    }

    #[test]
    fn test_validate_handshake_rejects_engine_version_mismatch() {
        let handshake = ReplicationHandshake {
            magic: REPLICATION_MAGIC,
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            node_id: "node-1".to_string(),
            engine_version: "engine-v2".to_string(),
            after_offset: WalOffset(0),
            after_offset_crc32c: 0,
            auth_token: b"token".to_vec(),
        };
        let status = validate_handshake(&handshake, "engine-v1", b"token", WalOffset(0), None);
        assert_eq!(status, HandshakeStatus::VersionMismatch);
    }

    #[test]
    fn test_validate_handshake_rejects_wrong_auth_token() {
        let handshake = ReplicationHandshake {
            magic: REPLICATION_MAGIC,
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            node_id: "node-1".to_string(),
            engine_version: "engine-v1".to_string(),
            after_offset: WalOffset(0),
            after_offset_crc32c: 0,
            auth_token: b"bad-token".to_vec(),
        };
        let status = validate_handshake(&handshake, "engine-v1", b"token", WalOffset(0), None);
        assert_eq!(status, HandshakeStatus::AuthFailed);
    }

    #[test]
    fn test_validate_handshake_crc_mismatch_reports_divergent() {
        let handshake = ReplicationHandshake {
            magic: REPLICATION_MAGIC,
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            node_id: "node-1".to_string(),
            engine_version: "engine-v1".to_string(),
            after_offset: WalOffset(10),
            after_offset_crc32c: 10,
            auth_token: b"token".to_vec(),
        };
        let status = validate_handshake(&handshake, "engine-v1", b"token", WalOffset(20), Some(11));
        assert_eq!(
            status,
            HandshakeStatus::DivergentLog {
                truncate_to: WalOffset(9)
            }
        );
    }

    #[test]
    fn test_validate_handshake_ahead_offset_reports_divergent_to_tip() {
        let handshake = ReplicationHandshake {
            magic: REPLICATION_MAGIC,
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            node_id: "node-1".to_string(),
            engine_version: "engine-v1".to_string(),
            after_offset: WalOffset(30),
            after_offset_crc32c: 0,
            auth_token: b"token".to_vec(),
        };
        let status = validate_handshake(&handshake, "engine-v1", b"token", WalOffset(20), None);
        assert_eq!(
            status,
            HandshakeStatus::DivergentLog {
                truncate_to: WalOffset(20)
            }
        );
    }

    #[test]
    fn test_validate_handshake_missing_crc_reports_offset_not_available() {
        let handshake = ReplicationHandshake {
            magic: REPLICATION_MAGIC,
            protocol_version: REPLICATION_PROTOCOL_VERSION,
            node_id: "node-1".to_string(),
            engine_version: "engine-v1".to_string(),
            after_offset: WalOffset(3),
            after_offset_crc32c: 0,
            auth_token: b"token".to_vec(),
        };
        let status = validate_handshake(&handshake, "engine-v1", b"token", WalOffset(10), None);
        assert_eq!(status, HandshakeStatus::OffsetNotAvailable);
    }

    #[tokio::test]
    async fn test_frame_roundtrip_success() {
        let (mut write_side, mut read_side) = duplex(4096);
        let expected = ReplicationMessage::Heartbeat {
            leader_epoch: 7,
            tip_offset: WalOffset(11),
            committed_offset: WalOffset(9),
        };

        write_framed(&mut write_side, &expected)
            .await
            .expect("write frame");
        let actual: ReplicationMessage = read_framed(&mut read_side).await.expect("read frame");
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_validate_leader_epoch_rejects_stale_epoch() {
        let result = validate_leader_epoch(9, 8, "heartbeat");
        assert!(matches!(result, Err(FluidError::Replication(_))));
    }

    #[test]
    fn test_validate_leader_epoch_accepts_equal_epoch() {
        let updated = validate_leader_epoch(9, 9, "wal_batch").expect("equal epoch accepted");
        assert_eq!(updated, 9);
    }

    #[test]
    fn test_validate_leader_epoch_advances_on_newer_epoch() {
        let updated = validate_leader_epoch(9, 10, "wal_batch").expect("new epoch accepted");
        assert_eq!(updated, 10);
    }

    #[tokio::test]
    async fn test_max_frame_size_enforced() {
        let (mut write_side, mut read_side) = duplex(32);
        let len = u32::try_from(DEFAULT_MAX_FRAME_BYTES + 1).expect("frame size fits u32");
        write_side
            .write_all(&len.to_le_bytes())
            .await
            .expect("write len");
        let result = read_framed::<ReplicationMessage, _>(&mut read_side).await;
        assert!(matches!(result, Err(FluidError::Replication(_))));
    }

    #[tokio::test]
    async fn test_write_framed_rejects_oversized_payload() {
        let mut writer = sink();
        let oversized_payload = vec![0_u8; DEFAULT_MAX_FRAME_BYTES + 1];
        let message = ReplicationMessage::WalBatch {
            leader_epoch: 1,
            records: vec![WalRecordPayload {
                offset: WalOffset(1),
                command_bytes: oversized_payload,
            }],
        };
        let result = write_framed(&mut writer, &message).await;
        assert!(matches!(result, Err(FluidError::Replication(_))));
    }

    #[tokio::test]
    async fn test_follower_writer_duplicate_and_gap_validation() {
        let dir = temp_dir();
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 16,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 1024 * 1024,
        })
        .expect("spawn writer");
        let first = make_order("o1", 0);
        let first_bytes = to_vec(&first).expect("serialize order");
        let first_offset = writer.append(first).await.expect("append first");
        assert_eq!(first_offset, WalOffset(1));

        let mut follower = FollowerWalWriter::new(writer.clone(), dir.clone()).expect("follower");
        let duplicate = follower
            .write_payload(&WalRecordPayload {
                offset: WalOffset(1),
                command_bytes: first_bytes,
            })
            .await
            .expect("duplicate should be accepted");
        assert_eq!(
            duplicate,
            FollowerWriteResult::Duplicate {
                offset: WalOffset(1)
            }
        );

        let gap = follower
            .write_payload(&WalRecordPayload {
                offset: WalOffset(3),
                command_bytes: to_vec(&make_order("o2", 1)).expect("serialize"),
            })
            .await;
        assert!(matches!(gap, Err(FluidError::Replication(_))));
        drop_dir(&dir);
    }

    #[tokio::test]
    async fn test_follower_writer_conflicting_duplicate_is_rejected() {
        let dir = temp_dir();
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 16,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 1024 * 1024,
        })
        .expect("spawn writer");

        let first = make_order("o1", 0);
        let first_offset = writer.append(first).await.expect("append first");
        assert_eq!(first_offset, WalOffset(1));

        let mut follower = FollowerWalWriter::new(writer.clone(), dir.clone()).expect("follower");
        let conflict = follower
            .write_payload(&WalRecordPayload {
                offset: WalOffset(1),
                command_bytes: to_vec(&make_order("o2", 1)).expect("serialize second"),
            })
            .await;
        assert!(matches!(conflict, Err(FluidError::Replication(_))));
        drop_dir(&dir);
    }

    #[tokio::test]
    async fn test_follower_writer_append_advances_durable_applied() {
        let dir = temp_dir();
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 16,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 1024 * 1024,
        })
        .expect("spawn writer");
        let mut follower = FollowerWalWriter::new(writer.clone(), dir.clone()).expect("follower");
        assert_eq!(follower.durable_applied(), WalOffset(0));

        let payload = WalRecordPayload {
            offset: WalOffset(1),
            command_bytes: to_vec(&make_order("o1", 0)).expect("serialize"),
        };
        let appended = follower
            .write_payload(&payload)
            .await
            .expect("append payload");
        assert_eq!(
            appended,
            FollowerWriteResult::Appended {
                offset: WalOffset(1)
            }
        );
        assert_eq!(follower.durable_applied(), WalOffset(1));
        assert_eq!(local_wal_tip(&dir).expect("local tip"), WalOffset(1));
        drop_dir(&dir);
    }

    #[tokio::test]
    async fn test_broadcaster_register_and_drop_lagging_follower() {
        let broadcaster = ReplicationBroadcaster::new(WalOffset(5), 1).expect("create broadcaster");
        let registration = broadcaster
            .register_follower("node-a".to_string())
            .expect("register");
        assert_eq!(registration.captured_tip, WalOffset(5));

        let duplicate = broadcaster.register_follower("node-a".to_string());
        assert!(matches!(duplicate, Err(FluidError::Replication(_))));

        let batch1 = Arc::new(vec![WalRecordPayload {
            offset: WalOffset(6),
            command_bytes: vec![1],
        }]);
        let batch2 = Arc::new(vec![WalRecordPayload {
            offset: WalOffset(7),
            command_bytes: vec![2],
        }]);

        let first_outcome = broadcaster
            .broadcast_batch(batch1)
            .expect("first broadcast");
        assert!(first_outcome.dropped_followers.is_empty());
        let second_outcome = broadcaster
            .broadcast_batch(batch2)
            .expect("second broadcast");
        assert_eq!(second_outcome.tip_offset, WalOffset(7));
        assert_eq!(second_outcome.dropped_followers, vec!["node-a".to_string()]);
        assert_eq!(
            broadcaster
                .connected_followers()
                .expect("connected followers"),
            0
        );
    }

    #[test]
    fn test_broadcaster_durable_applied_is_monotonic() {
        let broadcaster =
            ReplicationBroadcaster::with_default_capacity(WalOffset(3)).expect("broadcaster");
        let registration = broadcaster
            .register_follower("node-a".to_string())
            .expect("register");
        drop(registration);

        broadcaster
            .update_durable_applied("node-a", WalOffset(9))
            .expect("update high");
        broadcaster
            .update_durable_applied("node-a", WalOffset(4))
            .expect("update low");
        let durable = broadcaster
            .follower_durable_applied("node-a")
            .expect("query")
            .expect("exists");
        assert_eq!(durable, WalOffset(9));
    }

    #[test]
    fn test_durable_ack_count_at_or_above_filters_followers() {
        let broadcaster =
            ReplicationBroadcaster::with_default_capacity(WalOffset(3)).expect("broadcaster");
        let registration_a = broadcaster
            .register_follower("node-a".to_string())
            .expect("register a");
        let registration_b = broadcaster
            .register_follower("node-b".to_string())
            .expect("register b");
        drop(registration_a);
        drop(registration_b);

        broadcaster
            .update_durable_applied("node-a", WalOffset(11))
            .expect("update a");
        broadcaster
            .update_durable_applied("node-b", WalOffset(7))
            .expect("update b");

        let acks_at_7 = broadcaster
            .durable_ack_count_at_or_above(WalOffset(7))
            .expect("count 7");
        let acks_at_10 = broadcaster
            .durable_ack_count_at_or_above(WalOffset(10))
            .expect("count 10");
        assert_eq!(acks_at_7, 2);
        assert_eq!(acks_at_10, 1);
    }

    #[test]
    fn test_follower_durability_snapshot_lists_followers_sorted() {
        let broadcaster =
            ReplicationBroadcaster::with_default_capacity(WalOffset(3)).expect("broadcaster");
        let registration_b = broadcaster
            .register_follower("node-b".to_string())
            .expect("register b");
        let registration_a = broadcaster
            .register_follower("node-a".to_string())
            .expect("register a");
        drop(registration_b);
        drop(registration_a);

        broadcaster
            .update_durable_applied("node-a", WalOffset(9))
            .expect("update a");
        broadcaster
            .update_durable_applied("node-b", WalOffset(7))
            .expect("update b");

        let snapshot = broadcaster
            .follower_durability_snapshot()
            .expect("snapshot");
        assert_eq!(
            snapshot,
            vec![
                FollowerDurabilitySnapshot {
                    node_id: "node-a".to_string(),
                    durable_applied: WalOffset(9),
                },
                FollowerDurabilitySnapshot {
                    node_id: "node-b".to_string(),
                    durable_applied: WalOffset(7),
                },
            ]
        );
    }

    #[tokio::test]
    async fn test_read_payloads_between_offsets_respects_bounds() {
        let dir = temp_dir();
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 16,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 1024 * 1024,
        })
        .expect("spawn writer");
        writer.append(make_order("o1", 0)).await.expect("append 1");
        writer.append(make_order("o2", 1)).await.expect("append 2");
        writer.append(make_order("o3", 2)).await.expect("append 3");
        writer.flush().await.expect("flush");

        let payloads =
            read_payloads_between_offsets(&dir, WalOffset(1), WalOffset(2)).expect("read payloads");
        let offsets: Vec<u64> = payloads
            .into_iter()
            .map(|payload| payload.offset.0)
            .collect();
        assert_eq!(offsets, vec![2]);

        let empty = read_payloads_between_offsets(&dir, WalOffset(3), WalOffset(2))
            .expect("read empty range");
        assert!(empty.is_empty());
        drop_dir(&dir);
    }

    #[tokio::test]
    async fn test_wal_record_to_payload_roundtrip() {
        let dir = temp_dir();
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 16,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 1024 * 1024,
        })
        .expect("spawn writer");
        writer.append(make_order("o1", 0)).await.expect("append");
        writer.flush().await.expect("flush");

        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), true).expect("open reader");
        let batch = reader.read_batch(8).expect("read batch");
        assert_eq!(batch.len(), 1);
        let payload = wal_record_to_payload(&batch[0]).expect("convert payload");
        let decoded: WalCommand = from_slice(&payload.command_bytes).expect("decode payload");
        assert_eq!(decoded, make_order("o1", 0));
        assert_eq!(payload.offset, WalOffset(1));
        drop_dir(&dir);
    }
}
