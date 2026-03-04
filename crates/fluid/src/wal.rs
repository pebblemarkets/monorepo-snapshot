use std::collections::HashSet;
use std::fmt::{self, Display};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{
    mpsc::{self, Receiver, Sender},
    Arc, Mutex,
};
use std::thread;
use std::time::{Duration, Instant};

use pebble_trading::{IncomingOrder, MarketId, OrderId};
use rmp_serde::{from_slice, to_vec};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::replication::WalRecordPayload;
use crate::{engine::FluidState, error::FluidError};

const MAGIC: [u8; 4] = *b"PBLW";
const FORMAT_VERSION: u16 = 1;
const MIN_HEADER_BYTES: usize = 4 + 2 + 4;
const DEFAULT_BATCH_SIZE: usize = 1000;
const DEFAULT_BATCH_WAIT: Duration = Duration::from_millis(1);
const DEFAULT_SEGMENT_SIZE_BYTES: u64 = 64 * 1024 * 1024;
const COMMITTED_OFFSET_HINT_FILE: &str = "committed_offset.hint";
type ReplicationBatch = Arc<Vec<WalRecordPayload>>;
type ReplicationSender = Sender<ReplicationBatch>;
type ReplicationTxSlot = Arc<Mutex<Option<ReplicationSender>>>;

/// Monotonic WAL offset; increments across all markets.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WalOffset(pub u64);

impl WalOffset {}

impl Display for WalOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub fn committed_offset_hint_path(wal_dir: &Path) -> PathBuf {
    wal_dir.join(COMMITTED_OFFSET_HINT_FILE)
}

pub fn read_committed_offset_hint(wal_dir: &Path) -> Result<Option<WalOffset>, FluidError> {
    let path = committed_offset_hint_path(wal_dir);
    if !path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(&path).map_err(|err| {
        FluidError::Wal(format!(
            "failed to read committed offset hint at {}: {err}",
            path.display()
        ))
    })?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let parsed = trimmed.parse::<u64>().map_err(|err| {
        FluidError::Wal(format!(
            "invalid committed offset hint at {}: {err}",
            path.display()
        ))
    })?;
    Ok(Some(WalOffset(parsed)))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WalCommand {
    PlaceOrder(IncomingOrder),
    CancelOrder {
        order_id: OrderId,
        market_id: MarketId,
        account_id: String,
    },
}

#[derive(Debug, Clone)]
pub struct WalConfig {
    pub wal_dir: PathBuf,
    pub max_batch_size: usize,
    pub max_batch_wait: Duration,
    pub segment_size_bytes: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            wal_dir: PathBuf::from("./wal"),
            max_batch_size: DEFAULT_BATCH_SIZE,
            max_batch_wait: DEFAULT_BATCH_WAIT,
            segment_size_bytes: DEFAULT_SEGMENT_SIZE_BYTES,
        }
    }
}

#[derive(Clone)]
pub struct WalWriterHandle {
    tx: Sender<WriterMsg>,
    stats: Arc<WalWriterStatsAtomic>,
    replication_tx: ReplicationTxSlot,
    #[cfg(test)]
    fsync_count: Arc<AtomicUsize>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct WalWriterStats {
    pub batches: u64,
    pub entries: u64,
    pub fsyncs: u64,
    pub last_batch_size: u64,
    pub last_fsync_latency_micros: u64,
    pub max_fsync_latency_micros: u64,
    pub total_fsync_latency_micros: u64,
}

#[derive(Default)]
struct WalWriterStatsAtomic {
    batches: std::sync::atomic::AtomicU64,
    entries: std::sync::atomic::AtomicU64,
    fsyncs: std::sync::atomic::AtomicU64,
    last_batch_size: std::sync::atomic::AtomicU64,
    last_fsync_latency_micros: std::sync::atomic::AtomicU64,
    max_fsync_latency_micros: std::sync::atomic::AtomicU64,
    total_fsync_latency_micros: std::sync::atomic::AtomicU64,
}

impl WalWriterStatsAtomic {
    fn update_after_batch(&self, entry_count: usize, fsync_latency_micros: u64) {
        let entry_count_u64 = u64::try_from(entry_count).unwrap_or(u64::MAX);
        self.batches.fetch_add(1, Ordering::Relaxed);
        self.entries.fetch_add(entry_count_u64, Ordering::Relaxed);
        self.fsyncs.fetch_add(1, Ordering::Relaxed);
        self.last_batch_size
            .store(entry_count_u64, Ordering::Relaxed);
        self.last_fsync_latency_micros
            .store(fsync_latency_micros, Ordering::Relaxed);
        self.total_fsync_latency_micros
            .fetch_add(fsync_latency_micros, Ordering::Relaxed);

        let mut current_max = self.max_fsync_latency_micros.load(Ordering::Relaxed);
        while fsync_latency_micros > current_max {
            match self.max_fsync_latency_micros.compare_exchange_weak(
                current_max,
                fsync_latency_micros,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current_max = observed,
            }
        }
    }

    fn snapshot(&self) -> WalWriterStats {
        WalWriterStats {
            batches: self.batches.load(Ordering::Relaxed),
            entries: self.entries.load(Ordering::Relaxed),
            fsyncs: self.fsyncs.load(Ordering::Relaxed),
            last_batch_size: self.last_batch_size.load(Ordering::Relaxed),
            last_fsync_latency_micros: self.last_fsync_latency_micros.load(Ordering::Relaxed),
            max_fsync_latency_micros: self.max_fsync_latency_micros.load(Ordering::Relaxed),
            total_fsync_latency_micros: self.total_fsync_latency_micros.load(Ordering::Relaxed),
        }
    }
}

enum WriterMsg {
    Append {
        command: WalCommand,
        reply: oneshot::Sender<Result<WalOffset, FluidError>>,
    },
    Flush {
        reply: oneshot::Sender<Result<(), FluidError>>,
    },
}

struct AppendMsg {
    command: WalCommand,
    reply: oneshot::Sender<Result<WalOffset, FluidError>>,
}

impl WalWriterHandle {
    pub fn spawn(config: WalConfig) -> Result<Self, FluidError> {
        Self::spawn_with_engine_version(config, default_engine_version())
    }

    pub fn spawn_with_engine_version(
        config: WalConfig,
        engine_version: String,
    ) -> Result<Self, FluidError> {
        Self::spawn_with_replication(config, engine_version, None)
    }

    pub fn spawn_with_replication(
        config: WalConfig,
        engine_version: String,
        replication_tx: Option<ReplicationSender>,
    ) -> Result<Self, FluidError> {
        if engine_version.is_empty() {
            return Err(FluidError::Wal(
                "engine_version cannot be empty".to_string(),
            ));
        }
        validate_wal_config(&config)?;
        init_wal_dir(&config.wal_dir)?;

        let fsync_count = Arc::new(AtomicUsize::new(0));
        let stats = Arc::new(WalWriterStatsAtomic::default());
        let replication_tx = Arc::new(Mutex::new(replication_tx));
        let (tx, rx) = mpsc::channel();
        let (segments, next_offset) = scan_existing_wal(&config.wal_dir)?;
        let segment_start = if let Some(tail_segment) = segments.last() {
            if let Some(tail_engine_version) = try_read_segment_engine_version(&tail_segment.path)?
            {
                if tail_engine_version == engine_version {
                    tail_segment.first_offset
                } else {
                    next_offset
                }
            } else {
                tail_segment.first_offset
            }
        } else {
            WalOffset(0)
        };

        let writer = WalWriter::new(
            config.clone(),
            segment_start,
            next_offset,
            engine_version,
            Arc::clone(&fsync_count),
            Arc::clone(&stats),
            Arc::clone(&replication_tx),
        )?;

        thread::spawn(move || {
            writer.run(rx);
        });

        Ok(Self {
            tx,
            stats,
            replication_tx,
            #[cfg(test)]
            fsync_count,
        })
    }

    pub async fn append(&self, command: WalCommand) -> Result<WalOffset, FluidError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let msg = WriterMsg::Append {
            command,
            reply: reply_tx,
        };
        self.tx
            .send(msg)
            .map_err(|_| FluidError::Wal("wal writer channel closed".to_string()))?;
        reply_rx
            .await
            .map_err(|_| FluidError::Wal("wal writer dropped append reply channel".to_string()))?
    }

    pub async fn flush(&self) -> Result<(), FluidError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let msg = WriterMsg::Flush { reply: reply_tx };
        self.tx
            .send(msg)
            .map_err(|_| FluidError::Wal("wal writer channel closed".to_string()))?;
        reply_rx
            .await
            .map_err(|_| FluidError::Wal("wal writer dropped flush reply channel".to_string()))?
    }

    pub fn stats(&self) -> WalWriterStats {
        self.stats.snapshot()
    }

    pub fn set_replication_tx(
        &self,
        replication_tx: Option<ReplicationSender>,
    ) -> Result<(), FluidError> {
        let mut guard = self
            .replication_tx
            .lock()
            .map_err(|_| FluidError::Wal("wal replication mutex poisoned".to_string()))?;
        *guard = replication_tx;
        Ok(())
    }

    #[cfg(test)]
    fn fsync_count(&self) -> usize {
        self.fsync_count.load(Ordering::Relaxed)
    }
}

pub struct WalSegmentReader {
    in_recovery: bool,
    segments: Vec<WalSegmentInfo>,
    segment_index: usize,
    next_offset: WalOffset,
    current_file: Option<File>,
    current_header: Option<SegmentHeader>,
}

#[derive(Clone, Debug)]
pub struct WalRecord {
    pub offset: WalOffset,
    pub command: WalCommand,
    pub engine_version: String,
}

#[derive(Debug, Clone)]
struct SegmentHeader {
    engine_version: String,
}

#[derive(Clone)]
struct WalSegmentInfo {
    first_offset: WalOffset,
    path: PathBuf,
}

impl WalSegmentInfo {
    fn path(wal_dir: &Path, first_offset: WalOffset) -> PathBuf {
        wal_dir.join(format!("segment-{first_offset:020}.wal"))
    }

    fn parse_name(name: &str) -> Option<WalOffset> {
        if !name.starts_with("segment-") || !name.ends_with(".wal") {
            return None;
        }
        let first = &name[8..name.len() - 4];
        first.parse().ok().map(WalOffset)
    }
}

impl WalSegmentReader {
    pub fn open<P: AsRef<Path>>(
        wal_dir: P,
        after_offset: WalOffset,
        in_recovery: bool,
    ) -> Result<Self, FluidError> {
        let wal_dir = wal_dir.as_ref().to_path_buf();
        let segments = discover_segments(&wal_dir)?;
        let mut segment_index = 0usize;
        if !segments.is_empty() && after_offset.0 > segments[0].first_offset.0 {
            for (idx, segment) in segments.iter().enumerate() {
                if segment.first_offset <= after_offset {
                    segment_index = idx;
                } else {
                    break;
                }
            }
        }

        Ok(Self {
            in_recovery,
            segments,
            segment_index,
            next_offset: after_offset,
            current_file: None,
            current_header: None,
        })
    }

    pub fn read_batch(&mut self, max_entries: usize) -> Result<Vec<WalRecord>, FluidError> {
        let mut batch = Vec::new();
        while batch.len() < max_entries {
            let record = self.next_record()?;
            match record {
                Some(record) => batch.push(record),
                None => break,
            }
        }
        Ok(batch)
    }

    fn next_record(&mut self) -> Result<Option<WalRecord>, FluidError> {
        while self.segment_index < self.segments.len() {
            self.ensure_open_segment()?;
            let file = if let Some(file) = self.current_file.as_mut() {
                file
            } else {
                self.segment_index = self.segment_index.saturating_add(1);
                continue;
            };

            let Some(header) = self.current_header.clone() else {
                return Ok(None);
            };

            let is_live_tail_segment =
                !self.in_recovery && self.segment_index + 1 == self.segments.len();
            if let Some((offset, command)) =
                read_next_record(file, self.in_recovery, is_live_tail_segment)?
            {
                if offset <= self.next_offset {
                    continue;
                }
                self.next_offset = offset;
                return Ok(Some(WalRecord {
                    offset,
                    command,
                    engine_version: header.engine_version,
                }));
            }
            self.current_file = None;
            self.current_header = None;
            self.segment_index = self.segment_index.saturating_add(1);
        }
        Ok(None)
    }

    fn ensure_open_segment(&mut self) -> Result<(), FluidError> {
        if self.current_file.is_some() {
            return Ok(());
        }

        while self.segment_index < self.segments.len() {
            let info = self.segments[self.segment_index].clone();
            let mut file = OpenOptions::new()
                .read(true)
                .open(&info.path)
                .map_err(|e| {
                    FluidError::Wal(format!(
                        "failed to open WAL segment {}: {e}",
                        info.path.display()
                    ))
                })?;

            let header = match read_segment_header(&mut file, self.in_recovery) {
                Ok(Some(header)) => header,
                Ok(None) => {
                    self.segment_index = self.segment_index.saturating_add(1);
                    continue;
                }
                Err(err) => {
                    if self.in_recovery {
                        self.segment_index = self.segment_index.saturating_add(1);
                        continue;
                    }
                    return Err(err);
                }
            };

            self.current_file = Some(file);
            self.current_header = Some(header);
            return Ok(());
        }

        Ok(())
    }
}

struct WalWriter {
    tx_config: WalConfig,
    fsync_count: Arc<AtomicUsize>,
    stats: Arc<WalWriterStatsAtomic>,
    next_offset: WalOffset,
    active: ActiveSegmentWriter,
    replication_tx: ReplicationTxSlot,
}

impl WalWriter {
    fn new(
        tx_config: WalConfig,
        segment_start: WalOffset,
        next_offset: WalOffset,
        engine_version: String,
        fsync_count: Arc<AtomicUsize>,
        stats: Arc<WalWriterStatsAtomic>,
        replication_tx: ReplicationTxSlot,
    ) -> Result<Self, FluidError> {
        let active = ActiveSegmentWriter::new(
            &tx_config.wal_dir,
            segment_start,
            &engine_version,
            tx_config.segment_size_bytes,
        )?;
        Ok(Self {
            tx_config,
            fsync_count,
            stats,
            next_offset,
            active,
            replication_tx,
        })
    }

    fn run(mut self, rx: Receiver<WriterMsg>) {
        loop {
            let mut appends: Vec<AppendMsg> = Vec::new();
            let mut flushes: Vec<oneshot::Sender<Result<(), FluidError>>> = Vec::new();

            let first_msg = match rx.recv() {
                Ok(msg) => msg,
                Err(_) => {
                    return;
                }
            };
            enqueue_message(first_msg, &mut appends, &mut flushes);

            let mut should_exit = false;
            while appends.len() < self.tx_config.max_batch_size {
                match rx.recv_timeout(self.tx_config.max_batch_wait) {
                    Ok(msg) => {
                        enqueue_message(msg, &mut appends, &mut flushes);

                        if !flushes.is_empty() {
                            while let Ok(msg) = rx.try_recv() {
                                enqueue_message(msg, &mut appends, &mut flushes);
                            }
                            break;
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => break,
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        should_exit = true;
                        break;
                    }
                }
            }

            let batch_size = appends.len() + flushes.len();
            if batch_size == 0 {
                if should_exit {
                    return;
                }
                continue;
            }

            let result = self.apply_batch(&mut appends);

            match result {
                Ok(()) => {
                    for flush in flushes {
                        let _ = flush.send(Ok(()));
                    }
                }
                Err(err) => {
                    for append in appends {
                        let _ = append.reply.send(Err(err.clone()));
                    }
                    for flush in flushes {
                        let _ = flush.send(Err(err.clone()));
                    }
                }
            }

            if should_exit {
                return;
            }
        }
    }

    fn apply_batch(&mut self, appends: &mut Vec<AppendMsg>) -> Result<(), FluidError> {
        if appends.is_empty() {
            return Ok(());
        }

        let mut offset = self.next_offset.0;
        let mut pending = Vec::with_capacity(appends.len());
        let mut replicated_payloads = Vec::with_capacity(appends.len());
        for append in appends.iter() {
            let payload = to_vec(&append.command)
                .map_err(|e| FluidError::Wal(format!("failed to serialize WAL command: {e}")))?;

            let next_offset = WalOffset(offset);
            self.active.append_record(next_offset, &payload)?;

            offset = offset
                .checked_add(1)
                .ok_or_else(|| FluidError::Wal("WAL sequence overflow".to_string()))?;
            pending.push(next_offset);
            replicated_payloads.push(WalRecordPayload {
                offset: next_offset,
                command_bytes: payload,
            });
        }

        let sync_started = Instant::now();
        self.active.sync()?;
        let fsync_latency_micros =
            u64::try_from(sync_started.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.fsync_count.fetch_add(1, Ordering::Relaxed);
        self.stats
            .update_after_batch(appends.len(), fsync_latency_micros);
        let replication_tx = self
            .replication_tx
            .lock()
            .map_err(|_| FluidError::Wal("wal replication mutex poisoned".to_string()))?
            .as_ref()
            .cloned();
        if let Some(replication_tx) = replication_tx {
            let _ = replication_tx.send(Arc::new(replicated_payloads));
        }

        for (append, assigned_offset) in appends.drain(..).zip(pending) {
            let _ = append.reply.send(Ok(assigned_offset));
        }
        self.next_offset = WalOffset(offset);
        let committed_offset = WalOffset(offset.saturating_sub(1));
        if let Err(err) = persist_committed_offset_hint(&self.tx_config.wal_dir, committed_offset) {
            tracing::warn!(error = ?err, "wal: failed to persist committed offset hint");
        }
        Ok(())
    }
}

fn persist_committed_offset_hint(
    wal_dir: &Path,
    committed_offset: WalOffset,
) -> Result<(), FluidError> {
    let path = committed_offset_hint_path(wal_dir);
    let hint = format!("{}\n", committed_offset.0);
    fs::write(&path, hint).map_err(|err| {
        FluidError::Wal(format!(
            "failed to write committed offset hint at {}: {err}",
            path.display()
        ))
    })?;
    Ok(())
}

fn enqueue_message(
    msg: WriterMsg,
    appends: &mut Vec<AppendMsg>,
    flushes: &mut Vec<oneshot::Sender<Result<(), FluidError>>>,
) {
    match msg {
        WriterMsg::Append { command, reply } => {
            appends.push(AppendMsg { command, reply });
        }
        WriterMsg::Flush { reply } => {
            flushes.push(reply);
        }
    }
}

struct ActiveSegmentWriter {
    dir: PathBuf,
    engine_version: String,
    segment: File,
    segment_start: WalOffset,
    segment_size_bytes: u64,
    current_size: u64,
}

impl ActiveSegmentWriter {
    fn new(
        wal_dir: &Path,
        segment_start: WalOffset,
        engine_version: &str,
        max_segment_size: u64,
    ) -> Result<Self, FluidError> {
        init_wal_dir(wal_dir)?;
        let (segment, size) = open_or_create_segment(wal_dir, segment_start, engine_version, true)?;
        Ok(Self {
            dir: wal_dir.to_path_buf(),
            engine_version: engine_version.to_string(),
            segment,
            segment_start,
            segment_size_bytes: max_segment_size,
            current_size: size,
        })
    }

    fn append_record(&mut self, offset: WalOffset, payload: &[u8]) -> Result<(), FluidError> {
        let record = build_record_bytes(offset.0, payload)?;
        let record_len = u64::try_from(record.len())
            .map_err(|_| FluidError::Wal("record size overflow".to_string()))?;

        if record_len > self.segment_size_bytes {
            return Err(FluidError::Wal(
                "WAL record larger than segment capacity".to_string(),
            ));
        }

        if self
            .current_size
            .checked_add(record_len)
            .is_none_or(|v| v > self.segment_size_bytes)
        {
            self.rotate_segment(offset)?;
        }

        self.segment
            .write_all(&record)
            .map_err(|e| FluidError::Wal(format!("failed to write WAL entry: {e}")))?;
        self.current_size = self
            .current_size
            .checked_add(record_len)
            .ok_or_else(|| FluidError::Wal("WAL segment size overflow".to_string()))?;
        Ok(())
    }

    fn sync(&mut self) -> Result<(), FluidError> {
        self.segment
            .sync_data()
            .map_err(|e| FluidError::Wal(format!("failed to fsync WAL segment: {e}")))?;
        Ok(())
    }

    fn rotate_segment(&mut self, next_offset: WalOffset) -> Result<(), FluidError> {
        self.segment.sync_data().map_err(|e| {
            FluidError::Wal(format!("failed to sync WAL segment before rotation: {e}"))
        })?;

        let (segment, size) =
            open_or_create_segment(&self.dir, next_offset, &self.engine_version, true)?;
        self.segment = segment;
        self.segment_start = next_offset;
        self.current_size = size;
        Ok(())
    }
}

fn build_record_bytes(offset: u64, payload: &[u8]) -> Result<Vec<u8>, FluidError> {
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| FluidError::Wal("WAL payload exceeds supported size".to_string()))?;
    let crc = crc32c::crc32c(payload);

    let mut record = Vec::with_capacity(
        4usize
            .checked_add(4usize)
            .and_then(|v| v.checked_add(8usize))
            .and_then(|v| v.checked_add(usize::try_from(payload_len).ok()?))
            .ok_or_else(|| FluidError::Wal("WAL record size overflow".to_string()))?,
    );
    record.extend_from_slice(&payload_len.to_le_bytes());
    record.extend_from_slice(&crc.to_le_bytes());
    record.extend_from_slice(&offset.to_le_bytes());
    record.extend_from_slice(payload);
    Ok(record)
}

fn validate_wal_config(config: &WalConfig) -> Result<(), FluidError> {
    if config.wal_dir.as_os_str().is_empty() {
        return Err(FluidError::Wal("wal_dir cannot be empty".to_string()));
    }
    if config.max_batch_size == 0 {
        return Err(FluidError::Wal(
            "max_batch_size must be greater than zero".to_string(),
        ));
    }
    if config.max_batch_wait.is_zero() {
        return Err(FluidError::Wal(
            "max_batch_wait must be greater than zero".to_string(),
        ));
    }
    if config.segment_size_bytes < MIN_HEADER_BYTES as u64 {
        return Err(FluidError::Wal(
            "segment_size_bytes must be larger than header".to_string(),
        ));
    }
    Ok(())
}

fn init_wal_dir(path: &Path) -> Result<(), FluidError> {
    fs::create_dir_all(path)
        .map_err(|e| FluidError::Wal(format!("failed to create WAL directory: {e}")))
}

pub fn wal_disk_usage_bytes(path: &Path) -> Result<u64, FluidError> {
    let mut total = 0u64;
    for segment in discover_segments(path)? {
        let metadata = fs::metadata(segment.path).map_err(|e| {
            FluidError::Wal(format!("failed to stat WAL segment for disk usage: {e}"))
        })?;
        total = total
            .checked_add(metadata.len())
            .ok_or_else(|| FluidError::Wal("wal disk usage overflow".to_string()))?;
    }
    Ok(total)
}

pub fn latest_wal_offset(path: &Path) -> Result<Option<WalOffset>, FluidError> {
    let mut reader = WalSegmentReader::open(path, WalOffset(0), false)?;
    let mut latest = None;
    loop {
        let batch = reader.read_batch(256)?;
        if batch.is_empty() {
            break;
        }
        if let Some(record) = batch.last() {
            latest = Some(record.offset);
        }
    }
    Ok(latest)
}

pub fn local_wal_tip(path: &Path) -> Result<WalOffset, FluidError> {
    Ok(latest_wal_offset(path)?.unwrap_or(WalOffset(0)))
}

pub fn wal_command_crc32c(path: &Path, offset: WalOffset) -> Result<Option<u32>, FluidError> {
    if offset.0 == 0 {
        return Ok(Some(0));
    }
    let start_offset = WalOffset(offset.0.saturating_sub(1));
    let mut reader = WalSegmentReader::open(path, start_offset, true)?;
    loop {
        let batch = reader.read_batch(256)?;
        if batch.is_empty() {
            break;
        }
        for record in batch {
            if record.offset == offset {
                let payload = to_vec(&record.command).map_err(|err| {
                    FluidError::Wal(format!(
                        "failed to serialize WAL command for CRC at offset {offset}: {err}"
                    ))
                })?;
                return Ok(Some(crc32c::crc32c(&payload)));
            }
        }
    }
    Ok(None)
}

pub fn truncate_fully_projected_segments(
    path: &Path,
    projected_offset: WalOffset,
    keep_last_segments: usize,
) -> Result<usize, FluidError> {
    if keep_last_segments == 0 {
        return Err(FluidError::Wal(
            "keep_last_segments must be greater than zero".to_string(),
        ));
    }

    let segments = discover_segments(path)?;
    if segments.len() <= keep_last_segments {
        return Ok(0);
    }

    let mut removed = 0usize;
    let truncate_cutoff = segments.len() - keep_last_segments;
    for index in 0..truncate_cutoff {
        let segment = &segments[index];
        let next_segment = &segments[index + 1];
        let segment_end_offset = next_segment.first_offset.0.saturating_sub(1);
        if projected_offset.0 >= segment_end_offset {
            fs::remove_file(&segment.path).map_err(|e| {
                FluidError::Wal(format!(
                    "failed to remove WAL segment {}: {e}",
                    segment.path.display()
                ))
            })?;
            removed = removed.saturating_add(1);
        }
    }

    Ok(removed)
}

fn discover_segments(wal_dir: &Path) -> Result<Vec<WalSegmentInfo>, FluidError> {
    if !wal_dir.exists() {
        return Ok(Vec::new());
    }

    let mut offsets = HashSet::new();
    let mut segments = Vec::new();

    for entry in fs::read_dir(wal_dir).map_err(|e| {
        FluidError::Wal(format!(
            "failed to list WAL directory {}: {e}",
            wal_dir.display()
        ))
    })? {
        let entry = entry
            .map_err(|e| FluidError::Wal(format!("failed to read WAL directory entry: {e}")))?;
        let path = entry.path();
        if path
            .metadata()
            .map_err(|e| {
                FluidError::Wal(format!("failed to stat WAL file {}: {e}", path.display()))
            })?
            .is_file()
        {
            let Some(file_name) = path.file_name().and_then(|v| v.to_str()) else {
                continue;
            };
            if let Some(first_offset) = WalSegmentInfo::parse_name(file_name) {
                if offsets.insert(first_offset.0) {
                    segments.push(WalSegmentInfo {
                        first_offset,
                        path: WalSegmentInfo::path(wal_dir, first_offset),
                    });
                }
            }
        }
    }

    segments.sort_unstable_by_key(|s| s.first_offset);
    Ok(segments)
}

fn try_read_segment_engine_version(path: &Path) -> Result<Option<String>, FluidError> {
    let mut file = OpenOptions::new().read(true).open(path).map_err(|e| {
        FluidError::Wal(format!(
            "failed to open WAL segment {} for header check: {e}",
            path.display()
        ))
    })?;

    match read_segment_header(&mut file, false) {
        Ok(Some(header)) => Ok(Some(header.engine_version)),
        Ok(None) | Err(_) => Ok(None),
    }
}

fn scan_existing_wal(wal_dir: &Path) -> Result<(Vec<WalSegmentInfo>, WalOffset), FluidError> {
    let segments = discover_segments(wal_dir)?;
    let mut reader = WalSegmentReader::open(wal_dir, WalOffset(0), true)?;
    let mut last_offset = None;
    loop {
        let batch = reader.read_batch(128)?;
        if batch.is_empty() {
            break;
        }
        if let Some(record) = batch.last() {
            last_offset = Some(record.offset);
        }
    }
    let next_offset = last_offset
        .and_then(|offset| offset.0.checked_add(1))
        .unwrap_or(1);
    Ok((segments, WalOffset(next_offset)))
}

fn read_segment_header(
    file: &mut File,
    in_recovery: bool,
) -> Result<Option<SegmentHeader>, FluidError> {
    let start = file
        .stream_position()
        .map_err(|e| FluidError::Wal(format!("failed to read file cursor: {e}")))?;

    let mut magic = [0u8; 4];
    if !read_segment_field(file, &mut magic, in_recovery, start)? {
        return Ok(None);
    }

    if magic != MAGIC {
        return Err(FluidError::Wal("invalid WAL segment magic".to_string()));
    }

    let mut version = [0u8; 2];
    if !read_segment_field(file, &mut version, in_recovery, start)? {
        return Ok(None);
    }
    if u16::from_le_bytes(version) != FORMAT_VERSION {
        return Err(FluidError::Wal(
            "unsupported WAL segment format version".to_string(),
        ));
    }

    let mut engine_version_len = [0u8; 4];
    if !read_segment_field(file, &mut engine_version_len, in_recovery, start)? {
        return Ok(None);
    }

    let engine_len = u32::from_le_bytes(engine_version_len);
    if engine_len == 0 {
        if in_recovery {
            file.set_len(start).map_err(|e| {
                FluidError::Wal(format!("failed to truncate WAL segment header: {e}"))
            })?;
            return Ok(None);
        }
        return Err(FluidError::Wal("empty WAL engine version".to_string()));
    }
    let engine_len = usize::try_from(engine_len)
        .map_err(|_| FluidError::Wal("engine version length conversion overflow".to_string()))?;

    let mut engine_version = vec![0u8; engine_len];
    if !read_segment_field(file, &mut engine_version, in_recovery, start)? {
        return Ok(None);
    }

    let engine_version = String::from_utf8(engine_version)
        .map_err(|_| FluidError::Wal("WAL engine version field is not valid UTF-8".to_string()))?;

    Ok(Some(SegmentHeader { engine_version }))
}

fn read_segment_field(
    file: &mut File,
    buf: &mut [u8],
    in_recovery: bool,
    start: u64,
) -> Result<bool, FluidError> {
    match file.read_exact(buf) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
            if in_recovery {
                let _ = file.set_len(start);
                return Ok(false);
            }
            Err(FluidError::Wal(format!(
                "failed to read WAL segment header: {err}"
            )))
        }
        Err(err) => Err(FluidError::Wal(format!(
            "failed to read WAL segment header: {err}"
        ))),
    }
}

fn open_or_create_segment(
    wal_dir: &Path,
    first_offset: WalOffset,
    engine_version: &str,
    repair: bool,
) -> Result<(File, u64), FluidError> {
    fn segment_header_len(engine_version: &str) -> Result<u64, FluidError> {
        u64::try_from(4 + 2 + 4 + engine_version.len())
            .map_err(|_| FluidError::Wal("segment header length overflow".to_string()))
    }

    let path = WalSegmentInfo::path(wal_dir, first_offset);
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(&path)
        .map_err(|e| {
            FluidError::Wal(format!(
                "failed to open WAL segment {}: {e}",
                path.display()
            ))
        })?;

    let len = file.metadata().map_err(|e| {
        FluidError::Wal(format!(
            "failed to stat WAL segment {}: {e}",
            path.display()
        ))
    })?;
    if len.len() == 0 {
        write_segment_header(&mut file, engine_version)?;
        return Ok((file, segment_header_len(engine_version)?));
    }

    match read_segment_header(&mut file, repair) {
        Ok(Some(header)) => {
            if header.engine_version != engine_version {
                let metadata = file.metadata().map_err(|e| {
                    FluidError::Wal(format!(
                        "failed to stat WAL segment {}: {e}",
                        path.display()
                    ))
                })?;
                let cursor = file.stream_position().map_err(|e| {
                    FluidError::Wal(format!(
                        "failed to read WAL segment cursor {}: {e}",
                        path.display()
                    ))
                })?;

                // Stale rotated segments can exist with only a header and no records.
                // In that case, it's safe to rewrite the header to the current engine version.
                if cursor == metadata.len() {
                    file.set_len(0).map_err(|e| {
                        FluidError::Wal(format!(
                            "failed to reset stale WAL segment {}: {e}",
                            path.display()
                        ))
                    })?;
                    write_segment_header(&mut file, engine_version)?;
                    return Ok((file, segment_header_len(engine_version)?));
                }

                return Err(FluidError::Wal(format!(
                    "WAL segment {} engine version mismatch: expected {}, found {}",
                    path.display(),
                    engine_version,
                    header.engine_version
                )));
            }
            let size = file.metadata().map_err(|e| {
                FluidError::Wal(format!(
                    "failed to stat WAL segment {}: {e}",
                    path.display()
                ))
            })?;
            file.seek(SeekFrom::End(0)).map_err(|e| {
                FluidError::Wal(format!(
                    "failed to seek to end of WAL segment {}: {e}",
                    path.display()
                ))
            })?;
            Ok((file, size.len()))
        }
        Ok(None) => {
            file.set_len(0).map_err(|e| {
                FluidError::Wal(format!(
                    "failed to reset partial WAL segment {}: {e}",
                    path.display()
                ))
            })?;
            write_segment_header(&mut file, engine_version)?;
            Ok((file, segment_header_len(engine_version)?))
        }
        Err(err) => Err(err),
    }
}

fn write_segment_header(file: &mut File, engine_version: &str) -> Result<(), FluidError> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&MAGIC);
    payload.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    let version = engine_version.as_bytes();
    let version_len = u32::try_from(version.len())
        .map_err(|_| FluidError::Wal("engine version too long".to_string()))?;
    payload.extend_from_slice(&version_len.to_le_bytes());
    payload.extend_from_slice(version);

    file.seek(SeekFrom::Start(0))
        .map_err(|e| FluidError::Wal(format!("failed to seek WAL segment header start: {e}")))?;
    file.set_len(0).map_err(|e| {
        FluidError::Wal(format!(
            "failed to truncate WAL segment for header rewrite: {e}"
        ))
    })?;
    file.write_all(&payload)
        .map_err(|e| FluidError::Wal(format!("failed to write WAL segment header: {e}")))?;
    file.sync_data()
        .map_err(|e| FluidError::Wal(format!("failed to fsync WAL segment header: {e}")))?;
    file.seek(SeekFrom::End(0))
        .map_err(|e| FluidError::Wal(format!("failed to seek WAL segment end: {e}")))?;
    Ok(())
}

fn read_next_record(
    file: &mut File,
    in_recovery: bool,
    is_live_tail_segment: bool,
) -> Result<Option<(WalOffset, WalCommand)>, FluidError> {
    let record_start = file
        .stream_position()
        .map_err(|e| FluidError::Wal(format!("failed to read file cursor: {e}")))?;

    let mut payload_len = [0u8; 4];
    match read_exact_or_recover(
        file,
        &mut payload_len,
        in_recovery,
        is_live_tail_segment,
        record_start,
    )? {
        Some(()) => {}
        None => return Ok(None),
    }

    let payload_len = u32::from_le_bytes(payload_len);
    let payload_len = usize::try_from(payload_len)
        .map_err(|_| FluidError::Wal("record payload length overflow".to_string()))?;

    let mut crc = [0u8; 4];
    if read_exact_or_recover(
        file,
        &mut crc,
        in_recovery,
        is_live_tail_segment,
        record_start,
    )?
    .is_none()
    {
        return Ok(None);
    }
    let expected_crc = u32::from_le_bytes(crc);

    let mut offset = [0u8; 8];
    if read_exact_or_recover(
        file,
        &mut offset,
        in_recovery,
        is_live_tail_segment,
        record_start,
    )?
    .is_none()
    {
        return Ok(None);
    }
    let offset = WalOffset(u64::from_le_bytes(offset));

    let mut payload = vec![0u8; payload_len];
    if read_exact_or_recover(
        file,
        &mut payload,
        in_recovery,
        is_live_tail_segment,
        record_start,
    )?
    .is_none()
    {
        return Ok(None);
    }

    let crc = crc32c::crc32c(&payload);
    if crc != expected_crc {
        return Err(FluidError::Wal(format!(
            "CRC32C mismatch at offset {offset}"
        )));
    }

    let command = from_slice(&payload).map_err(|err| {
        FluidError::Wal(format!(
            "failed to deserialize WAL record at offset {offset}: {err}"
        ))
    })?;
    Ok(Some((offset, command)))
}

fn read_exact_or_recover(
    file: &mut File,
    buf: &mut [u8],
    in_recovery: bool,
    is_live_tail_segment: bool,
    record_start: u64,
) -> Result<Option<()>, FluidError> {
    let mut total = 0usize;
    while total < buf.len() {
        match file.read(&mut buf[total..]) {
            Ok(0) => {
                if total == 0 {
                    return Ok(None);
                }
                if in_recovery {
                    let _ = file.set_len(record_start);
                    return Ok(None);
                }
                if is_live_tail_segment {
                    return Ok(None);
                }
                return Err(FluidError::Wal(
                    "unexpected EOF while reading WAL entry".to_string(),
                ));
            }
            Ok(v) => total += v,
            Err(err) => {
                return Err(FluidError::Wal(format!(
                    "failed to read WAL record bytes: {err}"
                )));
            }
        }
    }
    Ok(Some(()))
}

fn default_engine_version() -> String {
    if let Ok(version) = std::env::var("PEBBLE_FLUID_ENGINE_VERSION") {
        version
    } else {
        option_env!("CARGO_PKG_VERSION")
            .unwrap_or("0.0.0")
            .to_string()
    }
}

/// Replay WAL entries onto an in-memory `FluidState`.
pub fn replay_wal(
    state: &mut FluidState,
    reader: &mut WalSegmentReader,
    after_offset: WalOffset,
    engine_version: &str,
) -> Result<Option<WalOffset>, FluidError> {
    let mut last_offset = None;
    let mut cursor = after_offset;

    loop {
        let mut batch = reader.read_batch(128)?;
        if batch.is_empty() {
            return Ok(last_offset);
        }

        for record in batch.drain(..) {
            if record.offset <= cursor {
                continue;
            }
            if record.engine_version != engine_version {
                return Err(FluidError::ReplayDivergence(format!(
                    "WAL engine version mismatch at offset {}: expected {engine_version}, got {}",
                    record.offset, record.engine_version
                )));
            }

            match record.command {
                WalCommand::PlaceOrder(incoming) => {
                    let transition =
                        state.compute_place_order_replay(&incoming).map_err(|err| {
                            FluidError::ReplayDivergence(format!(
                                "place replay compute failed for order {}: {err}",
                                incoming.order_id.0
                            ))
                        })?;
                    state.apply_place_order(transition).map_err(|err| {
                        FluidError::ReplayDivergence(format!(
                            "place replay apply failed for order {}: {err}",
                            incoming.order_id.0
                        ))
                    })?;
                }
                WalCommand::CancelOrder {
                    order_id,
                    market_id,
                    account_id,
                } => {
                    let transition = state
                        .compute_cancel_order(&market_id, &order_id, &account_id)
                        .map_err(|err| {
                            FluidError::ReplayDivergence(format!(
                                "cancel replay compute failed for order {}: {err}",
                                order_id.0
                            ))
                        })?;
                    state.apply_cancel_order(transition).map_err(|err| {
                        FluidError::ReplayDivergence(format!(
                            "cancel replay apply failed for order {}: {err}",
                            order_id.0
                        ))
                    })?;
                }
            }

            cursor = record.offset;
            last_offset = Some(record.offset);
        }
    }
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_arguments,
    clippy::explicit_iter_loop,
    clippy::let_unit_value,
    clippy::ignored_unit_patterns
)]
mod tests {
    use super::*;
    use crate::engine::{BinaryConfig, FluidState, MarketMeta};
    use crate::ids::StringInterner;
    use crate::risk::RiskEngine;
    use crate::{book::MarketBook, book::RestingEntry};
    use pebble_trading::{
        IncomingOrder, InternalAccountId, InternalOrderId, OrderNonce, OrderSide, OrderType,
        TimeInForce,
    };
    use std::collections::HashMap;
    use std::env;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::time::{sleep, Duration as TokioDuration};

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[derive(Default)]
    struct TestIds {
        order_interner: StringInterner<InternalOrderId>,
        account_interner: StringInterner<InternalAccountId>,
    }

    fn temp_dir() -> PathBuf {
        let mut dir = env::temp_dir();
        let process_id = process_id();
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX_EPOCH")
            .as_nanos();
        let seq = TEST_DIR_COUNTER.fetch_add(1, AtomicOrdering::SeqCst);
        dir.push(format!("pebble-fluid-wal-{process_id}-{elapsed}-{seq}"));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn process_id() -> u32 {
        std::process::id()
    }

    fn make_account(cleared: i64) -> crate::risk::AccountState {
        crate::risk::AccountState {
            cleared_cash_minor: cleared,
            delta_pending_trades_minor: 0,
            locked_open_orders_minor: 0,
            pending_withdrawals_reserved_minor: 0,
            pending_nonce: None,
            pending_lock_minor: 0,
            last_nonce: None,
            status: "Active".to_string(),
            active: true,
            instrument_admin: "admin".to_string(),
            instrument_id: "USD".to_string(),
        }
    }

    fn build_state(
        accounts: &[(&str, i64)],
        books: HashMap<MarketId, MarketBook>,
        sequences: HashMap<MarketId, crate::engine::SequenceState>,
    ) -> FluidState {
        build_state_with_interners(
            accounts,
            books,
            sequences,
            StringInterner::default(),
            StringInterner::default(),
        )
    }

    fn build_state_with_interners(
        accounts: &[(&str, i64)],
        books: HashMap<MarketId, MarketBook>,
        sequences: HashMap<MarketId, crate::engine::SequenceState>,
        order_interner: StringInterner<InternalOrderId>,
        account_interner: StringInterner<InternalAccountId>,
    ) -> FluidState {
        let mut risk = RiskEngine::default();
        for (account_id, balance) in accounts.iter() {
            risk.upsert_account(account_id, make_account(*balance));
        }

        let market_meta = HashMap::from([(MarketId("m1".to_string()), test_market_meta())]);
        let mut books = books;
        for (market_id, meta) in &market_meta {
            books.entry(market_id.clone()).or_insert_with(|| {
                if let Some(binary_config) = &meta.binary_config {
                    MarketBook::new_binary(binary_config.clone())
                } else {
                    MarketBook::new_multi_outcome()
                }
            });
        }
        let mut sequences = sequences;
        for market_id in market_meta.keys() {
            sequences.entry(market_id.clone()).or_default();
        }
        FluidState::new(
            books,
            risk,
            sequences,
            market_meta,
            order_interner,
            account_interner,
            default_engine_version(),
            false,
        )
    }

    fn test_market_meta() -> MarketMeta {
        MarketMeta {
            status: "Open".to_string(),
            outcomes: vec!["YES".to_string(), "NO".to_string()],
            binary_config: Some(BinaryConfig {
                yes_outcome: "YES".to_string(),
                no_outcome: "NO".to_string(),
                max_price_ticks: 10_000,
            }),
            instrument_admin: "admin".to_string(),
            instrument_id: "USD".to_string(),
        }
    }

    fn make_incoming(
        id: &str,
        account: &str,
        market_id: &str,
        side: OrderSide,
        order_type: OrderType,
        price: i64,
        qty: i64,
        nonce: i64,
    ) -> IncomingOrder {
        make_incoming_with_outcome(
            id, account, market_id, "YES", side, order_type, price, qty, nonce,
        )
    }

    fn make_incoming_with_outcome(
        id: &str,
        account: &str,
        market_id: &str,
        outcome: &str,
        side: OrderSide,
        order_type: OrderType,
        price: i64,
        qty: i64,
        nonce: i64,
    ) -> IncomingOrder {
        IncomingOrder {
            order_id: OrderId(id.to_string()),
            account_id: account.to_string(),
            market_id: MarketId(market_id.to_string()),
            outcome: outcome.to_string(),
            side,
            order_type,
            tif: match order_type {
                OrderType::Limit => TimeInForce::Gtc,
                OrderType::Market => TimeInForce::Ioc,
            },
            nonce: OrderNonce(nonce),
            price_ticks: price,
            quantity_minor: qty,
            submitted_at_micros: 123_456,
        }
    }

    fn make_resting(
        ids: &mut TestIds,
        id: &str,
        account: &str,
        side: OrderSide,
        price: i64,
        remaining: i64,
    ) -> RestingEntry {
        RestingEntry {
            order_id: ids.order_interner.intern(id),
            account_id: ids.account_interner.intern(account),
            outcome: "YES".to_string(),
            side,
            price_ticks: price,
            remaining_minor: remaining,
            locked_minor: remaining,
            submitted_at_micros: 1,
        }
    }

    fn drop_dir(dir: &Path) {
        let _ = fs::remove_dir_all(dir);
    }

    fn take_all_commands(dir: &Path) -> Vec<WalCommand> {
        let mut reader = WalSegmentReader::open(dir, WalOffset(0), true).expect("open reader");
        let mut commands = Vec::new();
        loop {
            let batch = reader.read_batch(128).expect("read all");
            if batch.is_empty() {
                break;
            }
            for record in batch {
                commands.push(record.command);
            }
        }
        commands
    }

    #[tokio::test]
    async fn test_write_read_roundtrip() {
        let dir = temp_dir();
        let handle = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 16,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 8 * 1024 * 1024,
        })
        .expect("spawn");

        let place = WalCommand::PlaceOrder(make_incoming(
            "o1",
            "alice",
            "m1",
            OrderSide::Buy,
            OrderType::Limit,
            100,
            10,
            0,
        ));
        let cancel = WalCommand::CancelOrder {
            order_id: OrderId("o1".to_string()),
            market_id: MarketId("m1".to_string()),
            account_id: "alice".to_string(),
        };
        let expected = vec![place.clone(), cancel.clone()];

        for command in &expected {
            let _ = handle.append(command.clone()).await.expect("append");
        }

        let actual = take_all_commands(&dir);
        assert_eq!(actual, expected);
        drop_dir(&dir);
    }

    #[tokio::test]
    async fn test_group_commit_batching() {
        let dir = temp_dir();
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 64,
            max_batch_wait: Duration::from_millis(250),
            segment_size_bytes: 8 * 1024 * 1024,
        })
        .expect("spawn");

        let mut tasks = Vec::new();
        for idx in 0..32u64 {
            let writer = writer.clone();
            tasks.push(tokio::spawn(async move {
                let cmd = WalCommand::PlaceOrder(make_incoming(
                    &format!("o{idx}"),
                    "alice",
                    "m1",
                    if idx % 2 == 0 {
                        OrderSide::Buy
                    } else {
                        OrderSide::Sell
                    },
                    OrderType::Limit,
                    100,
                    1,
                    i64::try_from(idx).expect("idx fits i64"),
                ));
                writer.append(cmd).await.expect("append in batch");
            }));
        }

        for task in tasks {
            task.await.expect("task join");
        }
        sleep(TokioDuration::from_millis(10)).await;
        assert_eq!(writer.fsync_count(), 1);
        drop_dir(&dir);
    }

    #[test]
    fn test_crc_corruption_detected() {
        let dir = temp_dir();
        let order = make_incoming(
            "o1",
            "alice",
            "m1",
            OrderSide::Buy,
            OrderType::Limit,
            100,
            10,
            0,
        );
        let payload = to_vec(&WalCommand::PlaceOrder(order)).expect("serialize");
        let record = build_record_bytes(0, &payload).expect("record bytes");

        let mut header = Vec::new();
        header.extend_from_slice(&MAGIC);
        header.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        let version = default_engine_version();
        let version_bytes = version.as_bytes();
        header.extend_from_slice(
            &(u32::try_from(version_bytes.len()).expect("version len fits u32")).to_le_bytes(),
        );
        header.extend_from_slice(version_bytes);

        let segment = WalSegmentInfo::path(&dir, WalOffset(0));
        fs::create_dir_all(&dir).expect("make dir");
        let mut file = File::create(&segment).expect("create segment");
        file.write_all(&header).expect("write header");
        file.write_all(&record).expect("write record");
        file.sync_data().expect("fsync");

        let mut data = fs::read(&segment).expect("read segment");
        let len = data.len();
        let bad = usize::checked_sub(len, 1).unwrap_or(len);
        data[bad] = data[bad].wrapping_add(1);
        fs::write(&segment, &data).expect("write corrupt");

        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), true).expect("open reader");
        let err = reader.read_batch(10).unwrap_err();
        assert!(matches!(err, FluidError::Wal(_)));
        drop_dir(&dir);
    }

    #[test]
    fn test_torn_write_truncated() {
        let dir = temp_dir();
        let order = make_incoming(
            "o1",
            "alice",
            "m1",
            OrderSide::Buy,
            OrderType::Limit,
            100,
            10,
            0,
        );
        let payload = to_vec(&WalCommand::PlaceOrder(order)).expect("serialize");
        let entry = build_record_bytes(1, &payload).expect("record bytes");

        let mut header = Vec::new();
        header.extend_from_slice(&MAGIC);
        header.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        let version = default_engine_version();
        let version_bytes = version.as_bytes();
        header.extend_from_slice(
            &(u32::try_from(version_bytes.len()).expect("version len fits u32")).to_le_bytes(),
        );
        header.extend_from_slice(version_bytes);

        let segment = WalSegmentInfo::path(&dir, WalOffset(0));
        fs::create_dir_all(&dir).expect("make dir");
        let mut file = File::create(&segment).expect("create segment");
        file.write_all(&header).expect("write header");
        file.write_all(&entry).expect("write entry");
        file.write_all(&[1, 2, 3]).expect("torn bytes");
        file.sync_data().expect("fsync");

        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), true).expect("open reader");
        let batch = reader.read_batch(10).expect("read batch");
        assert_eq!(batch.len(), 1);
        drop_dir(&dir);
    }

    #[test]
    fn test_live_tail_partial_entry_returns_empty_batch() {
        let dir = temp_dir();
        let mut header = Vec::new();
        header.extend_from_slice(&MAGIC);
        header.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        let version = default_engine_version();
        let version_bytes = version.as_bytes();
        header.extend_from_slice(
            &(u32::try_from(version_bytes.len()).expect("version len fits u32")).to_le_bytes(),
        );
        header.extend_from_slice(version_bytes);

        let segment = WalSegmentInfo::path(&dir, WalOffset(0));
        fs::create_dir_all(&dir).expect("make dir");
        let mut file = File::create(&segment).expect("create segment");
        file.write_all(&header).expect("write header");
        file.write_all(&[1, 2, 3]).expect("partial entry bytes");
        file.sync_data().expect("fsync");

        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), false).expect("open reader");
        let batch = reader.read_batch(10).expect("read batch");
        assert!(batch.is_empty());
        drop_dir(&dir);
    }

    #[test]
    fn test_non_tail_partial_entry_errors_in_live_mode() {
        let dir = temp_dir();
        let version = default_engine_version();
        let version_bytes = version.as_bytes();

        let mut header = Vec::new();
        header.extend_from_slice(&MAGIC);
        header.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        header.extend_from_slice(
            &(u32::try_from(version_bytes.len()).expect("version len fits u32")).to_le_bytes(),
        );
        header.extend_from_slice(version_bytes);

        // Older segment contains a torn record.
        let first = WalSegmentInfo::path(&dir, WalOffset(0));
        fs::create_dir_all(&dir).expect("make dir");
        let mut first_file = File::create(&first).expect("create first segment");
        first_file.write_all(&header).expect("write header");
        first_file
            .write_all(&[1, 2, 3, 4, 5])
            .expect("write torn bytes");
        first_file.sync_data().expect("fsync");

        // Newer segment exists, so first is not live tail.
        let second = WalSegmentInfo::path(&dir, WalOffset(10));
        let mut second_file = File::create(&second).expect("create second segment");
        second_file.write_all(&header).expect("write header");
        second_file.sync_data().expect("fsync");

        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), false).expect("open reader");
        let err = reader
            .read_batch(10)
            .expect_err("non-tail partial must error");
        assert!(matches!(err, FluidError::Wal(_)));
        drop_dir(&dir);
    }

    #[tokio::test]
    async fn test_segment_rotation() {
        let dir = temp_dir();
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 8,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 512,
        })
        .expect("spawn");

        for idx in 0..64 {
            writer
                .append(WalCommand::PlaceOrder(make_incoming(
                    &format!("o{idx}"),
                    "alice",
                    "m1",
                    OrderSide::Buy,
                    OrderType::Limit,
                    1_000,
                    1,
                    i64::from(idx),
                )))
                .await
                .expect("append");
        }

        let segments = discover_segments(&dir).expect("discover");
        assert!(segments.len() >= 2);
        drop_dir(&dir);
    }

    #[test]
    fn test_replay_place_order() {
        let dir = temp_dir();
        let mut live_state = build_state(&[("alice", 10_000)], HashMap::new(), HashMap::new());
        let order = make_incoming(
            "o1",
            "alice",
            "m1",
            OrderSide::Buy,
            OrderType::Limit,
            100,
            10,
            0,
        );
        let transition = live_state
            .compute_place_order(&order)
            .expect("compute place");
        live_state
            .apply_place_order(transition)
            .expect("apply place");

        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 8,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 8 * 1024 * 1024,
        })
        .expect("spawn");
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            writer
                .append(WalCommand::PlaceOrder(order.clone()))
                .await
                .expect("append");
        });

        let replay_state = {
            let mut state = build_state(&[("alice", 10_000)], HashMap::new(), HashMap::new());
            let mut reader = WalSegmentReader::open(&dir, WalOffset(0), true).expect("reader");
            replay_wal(
                &mut state,
                &mut reader,
                WalOffset(0),
                &default_engine_version(),
            )
            .expect("replay");
            state
        };

        let book = replay_state
            .books()
            .get(&MarketId("m1".to_string()))
            .expect("book exists");
        assert_eq!(book.order_count(), 1);
        let entry = book
            .get_entry_by_external_order_id(
                &OrderId("o1".to_string()),
                replay_state.order_interner(),
            )
            .expect("entry");
        assert_eq!(entry.remaining_minor, 10);
        assert_eq!(entry.locked_minor, 1000);
        assert_eq!(
            replay_state
                .risk()
                .get_account("alice")
                .expect("account")
                .locked_open_orders_minor,
            1000
        );
        drop_dir(&dir);
        drop(live_state);
    }

    #[test]
    fn test_replay_cancel_order() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        book.insert(make_resting(
            &mut ids,
            "o1",
            "alice",
            OrderSide::Sell,
            100,
            10,
        ));

        let mut replay_state = build_state_with_interners(
            &[("alice", 10_000)],
            HashMap::from([(MarketId("m1".to_string()), book)]),
            HashMap::new(),
            ids.order_interner,
            ids.account_interner,
        );
        let mut alice = make_account(10_000);
        alice.locked_open_orders_minor = 10;
        replay_state.risk_mut().upsert_account("alice", alice);
        replay_state
            .risk_mut()
            .apply_cancel_risk_delta("alice", 0)
            .expect("release baseline");

        let dir = temp_dir();
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 8,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 8 * 1024 * 1024,
        })
        .expect("spawn");
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            writer
                .append(WalCommand::CancelOrder {
                    order_id: OrderId("o1".to_string()),
                    market_id: MarketId("m1".to_string()),
                    account_id: "alice".to_string(),
                })
                .await
                .expect("append");
        });

        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), true).expect("reader");
        replay_wal(
            &mut replay_state,
            &mut reader,
            WalOffset(0),
            &default_engine_version(),
        )
        .expect("replay");

        let book = replay_state
            .books()
            .get(&MarketId("m1".to_string()))
            .expect("book");
        assert_eq!(book.order_count(), 0);
        drop_dir(&dir);
    }

    #[test]
    fn test_wal_replay_cancel_release_ordering() {
        let dir = temp_dir();
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 8,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 8 * 1024 * 1024,
        })
        .expect("spawn");

        let place_a = make_incoming(
            "order-a",
            "alice",
            "m1",
            OrderSide::Buy,
            OrderType::Limit,
            100,
            1,
            0,
        );
        let place_b = make_incoming(
            "order-b",
            "alice",
            "m1",
            OrderSide::Buy,
            OrderType::Limit,
            100,
            1,
            1,
        );
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            writer
                .append(WalCommand::PlaceOrder(place_a))
                .await
                .expect("append place a");
            writer
                .append(WalCommand::CancelOrder {
                    order_id: OrderId("order-a".to_string()),
                    market_id: MarketId("m1".to_string()),
                    account_id: "alice".to_string(),
                })
                .await
                .expect("append cancel a");
            writer
                .append(WalCommand::PlaceOrder(place_b))
                .await
                .expect("append place b");
        });

        let mut replay_state = build_state(&[("alice", 10_000)], HashMap::new(), HashMap::new());
        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), true).expect("reader");
        replay_wal(
            &mut replay_state,
            &mut reader,
            WalOffset(0),
            &default_engine_version(),
        )
        .expect("replay");

        let market_id = MarketId("m1".to_string());
        let book = replay_state
            .books()
            .get(&market_id)
            .expect("book exists after replay");
        assert_eq!(book.order_count(), 1);
        assert!(book
            .get_entry_by_external_order_id(
                &OrderId("order-a".to_string()),
                replay_state.order_interner()
            )
            .is_none());
        assert!(replay_state.order_interner().lookup("order-a").is_none());

        let order_b_internal = replay_state
            .order_interner()
            .lookup("order-b")
            .expect("order-b should be interned");
        assert_eq!(order_b_internal, InternalOrderId(0));
        assert_eq!(
            replay_state.order_interner().to_external(order_b_internal),
            Some("order-b")
        );
        assert!(book.get_entry(order_b_internal).is_some());

        let cancel_b = replay_state
            .compute_cancel_order(&market_id, &OrderId("order-b".to_string()), "alice")
            .expect("cancel routing should find order-b");
        assert_eq!(cancel_b.order_id.0, "order-b");
        drop_dir(&dir);
    }

    #[test]
    fn test_replay_cancel_not_found_errors() {
        let mut replay_state = build_state(&[("alice", 10_000)], HashMap::new(), HashMap::new());
        let dir = temp_dir();
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 8,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 8 * 1024 * 1024,
        })
        .expect("spawn");
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            writer
                .append(WalCommand::CancelOrder {
                    order_id: OrderId("missing".to_string()),
                    market_id: MarketId("m1".to_string()),
                    account_id: "alice".to_string(),
                })
                .await
                .expect("append");
        });

        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), true).expect("reader");
        let err = replay_wal(
            &mut replay_state,
            &mut reader,
            WalOffset(0),
            &default_engine_version(),
        )
        .unwrap_err();
        assert!(matches!(err, FluidError::ReplayDivergence(_)));
        drop_dir(&dir);
    }

    #[test]
    fn test_replay_cancel_wrong_owner_errors() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        book.insert(make_resting(
            &mut ids,
            "o1",
            "alice",
            OrderSide::Sell,
            100,
            10,
        ));
        let mut replay_state = build_state_with_interners(
            &[("alice", 10_000)],
            HashMap::from([(MarketId("m1".to_string()), book)]),
            HashMap::new(),
            ids.order_interner,
            ids.account_interner,
        );
        let dir = temp_dir();
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 8,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 8 * 1024 * 1024,
        })
        .expect("spawn");
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            writer
                .append(WalCommand::CancelOrder {
                    order_id: OrderId("o1".to_string()),
                    market_id: MarketId("m1".to_string()),
                    account_id: "bob".to_string(),
                })
                .await
                .expect("append");
        });

        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), true).expect("reader");
        let err = replay_wal(
            &mut replay_state,
            &mut reader,
            WalOffset(0),
            &default_engine_version(),
        )
        .unwrap_err();
        assert!(matches!(err, FluidError::ReplayDivergence(_)));
        drop_dir(&dir);
    }

    #[test]
    fn test_replay_mode_skips_balance_check() {
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        book.insert(make_resting(
            &mut ids,
            "m1",
            "maker",
            OrderSide::Sell,
            100,
            100,
        ));
        let mut replay_state = build_state_with_interners(
            &[("buyer", 10), ("maker", 1_000)],
            HashMap::from([(MarketId("m1".to_string()), book)]),
            HashMap::new(),
            ids.order_interner,
            ids.account_interner,
        );
        let mut maker_account = make_account(1_000);
        maker_account.locked_open_orders_minor = 100;
        replay_state
            .risk_mut()
            .upsert_account("maker", maker_account);

        let dir = temp_dir();
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 8,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 8 * 1024 * 1024,
        })
        .expect("spawn");
        let buy = make_incoming(
            "buy",
            "buyer",
            "m1",
            OrderSide::Buy,
            OrderType::Market,
            0,
            20,
            0,
        );

        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            writer
                .append(WalCommand::PlaceOrder(buy.clone()))
                .await
                .expect("append");
        });

        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), true).expect("reader");
        let result = replay_wal(
            &mut replay_state,
            &mut reader,
            WalOffset(0),
            &default_engine_version(),
        );
        assert!(result.is_ok());
        drop_dir(&dir);
    }

    #[tokio::test]
    async fn test_writer_flush_drains_pending_appends_and_is_idempotent() {
        let dir = temp_dir();
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 1024,
            max_batch_wait: Duration::from_secs(1),
            segment_size_bytes: 8 * 1024 * 1024,
        })
        .expect("spawn");

        let writer_for_append = writer.clone();
        let append = tokio::spawn(async move {
            writer_for_append
                .append(WalCommand::PlaceOrder(make_incoming(
                    "flush-order",
                    "alice",
                    "m1",
                    OrderSide::Buy,
                    OrderType::Limit,
                    100,
                    1,
                    0,
                )))
                .await
        });

        sleep(TokioDuration::from_millis(20)).await;
        assert_eq!(writer.fsync_count(), 0);

        let before_fsync = writer.fsync_count();
        writer.flush().await.expect("flush");
        assert_eq!(writer.fsync_count(), before_fsync + 1);

        let offset = append.await.expect("join append").expect("append");
        assert_eq!(offset, WalOffset(1));
        assert_eq!(writer.fsync_count(), before_fsync + 1);

        let before_idle_flush = writer.fsync_count();
        writer.flush().await.expect("flush");
        assert_eq!(writer.fsync_count(), before_idle_flush);
        drop_dir(&dir);
    }

    #[tokio::test]
    async fn test_writer_stats_updated_after_append_and_flush() {
        let dir = temp_dir();
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 64,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 8 * 1024 * 1024,
        })
        .expect("spawn");

        writer
            .append(WalCommand::PlaceOrder(make_incoming(
                "stats-order",
                "alice",
                "m1",
                OrderSide::Buy,
                OrderType::Limit,
                100,
                1,
                0,
            )))
            .await
            .expect("append");
        writer.flush().await.expect("flush");

        let stats = writer.stats();
        assert!(stats.entries >= 1);
        assert!(stats.fsyncs >= 1);
        assert!(stats.batches >= 1);
        assert!(stats.last_batch_size >= 1);
        drop_dir(&dir);
    }

    #[tokio::test]
    async fn test_set_replication_tx_enables_runtime_fanout() {
        let dir = temp_dir();
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 16,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 1024 * 1024,
        })
        .expect("spawn");
        let (tx, rx) = mpsc::channel::<Arc<Vec<WalRecordPayload>>>();
        writer
            .set_replication_tx(Some(tx))
            .expect("attach replication tx");

        writer
            .append(WalCommand::PlaceOrder(make_incoming(
                "fanout-o1",
                "alice",
                "m1",
                OrderSide::Buy,
                OrderType::Limit,
                100,
                1,
                0,
            )))
            .await
            .expect("append");
        writer.flush().await.expect("flush");

        let batch = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("replication batch");
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].offset, WalOffset(1));
        drop_dir(&dir);
    }

    #[tokio::test]
    async fn test_set_replication_tx_none_disables_runtime_fanout() {
        let dir = temp_dir();
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 16,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 1024 * 1024,
        })
        .expect("spawn");
        let (tx, rx) = mpsc::channel::<Arc<Vec<WalRecordPayload>>>();
        writer
            .set_replication_tx(Some(tx))
            .expect("attach replication tx");
        writer
            .set_replication_tx(None)
            .expect("detach replication tx");

        writer
            .append(WalCommand::PlaceOrder(make_incoming(
                "fanout-o2",
                "alice",
                "m1",
                OrderSide::Buy,
                OrderType::Limit,
                100,
                1,
                0,
            )))
            .await
            .expect("append");
        writer.flush().await.expect("flush");

        let recv = rx.recv_timeout(Duration::from_millis(100));
        assert!(recv.is_err());
        drop_dir(&dir);
    }

    #[tokio::test]
    async fn test_latest_offset_and_segment_truncation() {
        let dir = temp_dir();
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 8,
            max_batch_wait: Duration::from_millis(1),
            // Force frequent segment rotations.
            segment_size_bytes: 256,
        })
        .expect("spawn");

        for idx in 0..40 {
            writer
                .append(WalCommand::PlaceOrder(make_incoming(
                    &format!("truncate-{idx}"),
                    "alice",
                    "m1",
                    OrderSide::Buy,
                    OrderType::Limit,
                    100,
                    1,
                    i64::from(idx),
                )))
                .await
                .expect("append");
        }
        writer.flush().await.expect("flush");

        let latest = latest_wal_offset(&dir).expect("latest offset");
        assert!(latest.is_some());
        let latest = latest.expect("latest should exist");

        let before = discover_segments(&dir).expect("discover before");
        assert!(before.len() >= 3);
        let removed = truncate_fully_projected_segments(&dir, latest, 2).expect("truncate");
        assert!(removed >= 1);
        let after = discover_segments(&dir).expect("discover after");
        assert!(after.len() >= 2);
        assert_eq!(before.len(), after.len() + removed);
        drop_dir(&dir);
    }

    #[test]
    fn test_validate_wal_config_rejects_invalid_values() {
        let dir = temp_dir();
        let mut wal_config = WalConfig {
            wal_dir: PathBuf::from(""),
            max_batch_size: 0,
            max_batch_wait: Duration::from_secs(1),
            segment_size_bytes: 8 * 1024 * 1024,
        };

        assert!(validate_wal_config(&wal_config).is_err());

        wal_config = WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 0,
            max_batch_wait: Duration::from_secs(1),
            segment_size_bytes: 8 * 1024 * 1024,
        };
        assert!(validate_wal_config(&wal_config).is_err());

        wal_config = WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 1,
            max_batch_wait: Duration::ZERO,
            segment_size_bytes: 8 * 1024 * 1024,
        };
        assert!(validate_wal_config(&wal_config).is_err());

        wal_config = WalConfig {
            wal_dir: dir,
            max_batch_size: 1,
            max_batch_wait: Duration::from_secs(1),
            segment_size_bytes: (MIN_HEADER_BYTES - 1) as u64,
        };
        assert!(validate_wal_config(&wal_config).is_err());
    }

    #[test]
    fn test_reader_recovery_skips_invalid_segment_magic() {
        let dir = temp_dir();
        let path = WalSegmentInfo::path(&dir, WalOffset(1));
        let mut segment = File::create(&path).expect("create segment");
        segment.write_all(b"NOTPBLW").expect("write bad magic");
        segment.sync_data().expect("sync segment");

        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), true).expect("open reader");
        let batch = reader.read_batch(16).expect("read");
        assert!(batch.is_empty());
        drop_dir(&dir);
    }

    #[test]
    fn test_reader_rejects_invalid_segment_magic() {
        let dir = temp_dir();
        let path = WalSegmentInfo::path(&dir, WalOffset(1));
        let mut segment = File::create(&path).expect("create segment");
        segment.write_all(b"NOTPBLW").expect("write bad magic");
        segment.sync_data().expect("sync segment");

        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), false).expect("open reader");
        let err = reader.read_batch(16).unwrap_err();
        assert!(matches!(err, FluidError::Wal(_)));
        drop_dir(&dir);
    }

    #[test]
    fn test_reader_rejects_unsupported_segment_version() {
        let dir = temp_dir();
        let path = WalSegmentInfo::path(&dir, WalOffset(1));
        let mut segment = File::create(&path).expect("create segment");

        let mut header = Vec::new();
        header.extend_from_slice(&MAGIC);
        header.extend_from_slice(&(2u16).to_le_bytes());
        let version = default_engine_version();
        let version_bytes = version.as_bytes();
        header.extend_from_slice(
            &(u32::try_from(version_bytes.len()).expect("version len")).to_le_bytes(),
        );
        header.extend_from_slice(version_bytes);
        segment.write_all(&header).expect("write header");
        segment.sync_data().expect("sync segment");

        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), false).expect("open reader");
        let err = reader.read_batch(16).unwrap_err();
        assert!(matches!(err, FluidError::Wal(_)));
        drop_dir(&dir);
    }

    #[test]
    fn test_recovery_skips_zero_length_engine_version() {
        let dir = temp_dir();
        let path = WalSegmentInfo::path(&dir, WalOffset(1));
        let mut segment = File::create(&path).expect("create segment");

        let mut header = Vec::new();
        header.extend_from_slice(&MAGIC);
        header.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        header.extend_from_slice(&0u32.to_le_bytes());
        segment.write_all(&header).expect("write header");
        segment.sync_data().expect("sync");

        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), true).expect("open reader");
        let batch = reader.read_batch(16).expect("read");
        assert!(batch.is_empty());
        drop_dir(&dir);
    }

    #[test]
    fn test_latest_wal_offset_uses_live_mode_on_header_errors() {
        let dir = temp_dir();
        let path = WalSegmentInfo::path(&dir, WalOffset(1));
        let mut segment = File::create(&path).expect("create segment");

        let mut header = Vec::new();
        header.extend_from_slice(&MAGIC);
        header.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        header.extend_from_slice(&0u32.to_le_bytes());
        segment.write_all(&header).expect("write header");
        segment.sync_data().expect("sync");

        let err = latest_wal_offset(&dir).expect_err("latest must fail in live mode");
        assert!(matches!(
            err,
            FluidError::Wal(message) if message.contains("empty WAL engine version")
        ));
        drop_dir(&dir);
    }

    #[test]
    fn test_replay_rejects_engine_version_mismatch() {
        let dir = temp_dir();
        let mut state = build_state(&[("alice", 10_000)], HashMap::new(), HashMap::new());
        let order = make_incoming(
            "o1",
            "alice",
            "m1",
            OrderSide::Buy,
            OrderType::Limit,
            100,
            1,
            0,
        );
        let transition = state.compute_place_order(&order).expect("compute place");
        state.apply_place_order(transition).expect("apply place");

        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 8,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 8 * 1024 * 1024,
        })
        .expect("spawn");
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            writer
                .append(WalCommand::PlaceOrder(order))
                .await
                .expect("append");
        });

        let mut replay = build_state(&[("alice", 10_000)], HashMap::new(), HashMap::new());
        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), true).expect("reader");
        let err =
            replay_wal(&mut replay, &mut reader, WalOffset(0), "mismatched-version").unwrap_err();
        assert!(matches!(err, FluidError::ReplayDivergence(_)));
        drop_dir(&dir);
    }

    #[test]
    fn test_spawn_with_engine_version_writes_custom_header_version() {
        let dir = temp_dir();
        let writer = WalWriterHandle::spawn_with_engine_version(
            WalConfig {
                wal_dir: dir.clone(),
                max_batch_size: 8,
                max_batch_wait: Duration::from_millis(1),
                segment_size_bytes: 8 * 1024 * 1024,
            },
            "engine-custom-version".to_string(),
        )
        .expect("spawn");

        let order = make_incoming(
            "custom-version-order",
            "alice",
            "m1",
            OrderSide::Buy,
            OrderType::Limit,
            100,
            1,
            0,
        );

        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            writer
                .append(WalCommand::PlaceOrder(order))
                .await
                .expect("append");
        });

        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), true).expect("reader");
        let batch = reader.read_batch(8).expect("read");
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].engine_version, "engine-custom-version");
        drop_dir(&dir);
    }

    #[test]
    fn test_spawn_with_engine_version_rejects_empty_version() {
        let dir = temp_dir();
        let result = WalWriterHandle::spawn_with_engine_version(
            WalConfig {
                wal_dir: dir.clone(),
                max_batch_size: 8,
                max_batch_wait: Duration::from_millis(1),
                segment_size_bytes: 8 * 1024 * 1024,
            },
            String::new(),
        );
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(matches!(err, FluidError::Wal(_)));
        }
        drop_dir(&dir);
    }

    #[test]
    fn test_replay_place_order_missing_maker_account_rejects() {
        let dir = temp_dir();
        let mut ids = TestIds::default();
        let mut book = MarketBook::new_multi_outcome();
        book.insert(make_resting(
            &mut ids,
            "maker-order",
            "maker",
            OrderSide::Sell,
            100,
            1,
        ));

        let mut replay_state = build_state_with_interners(
            &[("alice", 10_000)],
            HashMap::from([(MarketId("m1".to_string()), book)]),
            HashMap::new(),
            ids.order_interner,
            ids.account_interner,
        );

        let order = make_incoming(
            "taker",
            "alice",
            "m1",
            OrderSide::Buy,
            OrderType::Limit,
            100,
            1,
            0,
        );
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 8,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 8 * 1024 * 1024,
        })
        .expect("spawn");
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        rt.block_on(async {
            writer
                .append(WalCommand::PlaceOrder(order))
                .await
                .expect("append");
        });

        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), true).expect("reader");
        let err = replay_wal(
            &mut replay_state,
            &mut reader,
            WalOffset(0),
            &default_engine_version(),
        )
        .unwrap_err();
        assert!(matches!(err, FluidError::ReplayDivergence(_)));
        drop_dir(&dir);
    }

    #[tokio::test]
    async fn test_replay_respects_after_offset_cursor() {
        let dir = temp_dir();
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 64,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 8 * 1024 * 1024,
        })
        .expect("spawn");

        for idx in 0..3 {
            let order = make_incoming(
                &format!("after-offset-{idx}"),
                "alice",
                "m1",
                OrderSide::Buy,
                OrderType::Limit,
                100,
                1,
                i64::from(idx),
            );
            writer
                .append(WalCommand::PlaceOrder(order))
                .await
                .expect("append");
        }

        let mut replay_state = build_state(&[("alice", 10_000)], HashMap::new(), HashMap::new());
        let mut reader = WalSegmentReader::open(&dir, WalOffset(2), true).expect("reader");
        replay_wal(
            &mut replay_state,
            &mut reader,
            WalOffset(2),
            &default_engine_version(),
        )
        .expect("replay");

        let book = replay_state
            .books()
            .get(&MarketId("m1".to_string()))
            .expect("book");
        assert_eq!(book.order_count(), 1);
        assert!(book
            .get_entry_by_external_order_id(
                &OrderId("after-offset-0".to_string()),
                replay_state.order_interner(),
            )
            .is_none());
        assert!(book
            .get_entry_by_external_order_id(
                &OrderId("after-offset-1".to_string()),
                replay_state.order_interner(),
            )
            .is_none());
        assert!(book
            .get_entry_by_external_order_id(
                &OrderId("after-offset-2".to_string()),
                replay_state.order_interner(),
            )
            .is_some());
        drop_dir(&dir);
    }

    #[tokio::test]
    async fn test_writer_resumes_from_true_tail_offset_after_restart() {
        let dir = temp_dir();
        let config = WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 64,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 8 * 1024 * 1024,
        };

        let writer = WalWriterHandle::spawn(config.clone()).expect("spawn");
        for idx in 0..200 {
            let order = make_incoming(
                &format!("restart-{idx}"),
                "alice",
                "m1",
                OrderSide::Buy,
                OrderType::Limit,
                100,
                1,
                i64::from(idx),
            );
            writer
                .append(WalCommand::PlaceOrder(order))
                .await
                .expect("append");
        }
        writer.flush().await.expect("flush");
        drop(writer);

        sleep(TokioDuration::from_millis(20)).await;

        let resumed = WalWriterHandle::spawn(config).expect("spawn resumed");
        let offset = resumed
            .append(WalCommand::PlaceOrder(make_incoming(
                "restart-tail",
                "alice",
                "m1",
                OrderSide::Buy,
                OrderType::Limit,
                100,
                1,
                200,
            )))
            .await
            .expect("append resumed");

        assert_eq!(offset, WalOffset(201));
        drop_dir(&dir);
    }

    #[tokio::test]
    async fn test_crash_recovery_recovers_fsyncd_entries_under_load() {
        let dir = temp_dir();
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 64,
            max_batch_wait: Duration::from_millis(10),
            segment_size_bytes: 8 * 1024 * 1024,
        })
        .expect("spawn");

        let mut tasks = Vec::new();
        for idx in 0..128u64 {
            let writer = writer.clone();
            tasks.push(tokio::spawn(async move {
                writer
                    .append(WalCommand::PlaceOrder(make_incoming(
                        &format!("load-{idx}"),
                        "alice",
                        "m1",
                        OrderSide::Buy,
                        OrderType::Limit,
                        100,
                        1,
                        i64::try_from(idx).expect("nonce fits i64"),
                    )))
                    .await
                    .expect("append")
            }));
        }

        let mut offsets = Vec::new();
        for task in tasks {
            offsets.push(task.await.expect("join"));
        }
        offsets.sort_unstable();
        assert_eq!(offsets.len(), 128);
        assert_eq!(offsets.first(), Some(&WalOffset(1)));
        assert_eq!(offsets.last(), Some(&WalOffset(128)));

        // Simulate abrupt process stop after acknowledged appends.
        drop(writer);
        sleep(TokioDuration::from_millis(20)).await;

        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), true).expect("open reader");
        let mut total = 0usize;
        loop {
            let batch = reader.read_batch(64).expect("read batch");
            if batch.is_empty() {
                break;
            }
            total = total.saturating_add(batch.len());
        }
        assert_eq!(total, 128);
        drop_dir(&dir);
    }

    #[tokio::test]
    async fn test_writer_rotates_segment_when_engine_version_changes_on_restart() {
        let dir = temp_dir();
        let config = WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 64,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 8 * 1024 * 1024,
        };

        let v1 =
            WalWriterHandle::spawn_with_engine_version(config.clone(), "engine-v1".to_string())
                .expect("spawn v1");
        let first_offset = v1
            .append(WalCommand::PlaceOrder(make_incoming(
                "engine-v1-order",
                "alice",
                "m1",
                OrderSide::Buy,
                OrderType::Limit,
                100,
                1,
                0,
            )))
            .await
            .expect("append v1");
        assert_eq!(first_offset, WalOffset(1));
        v1.flush().await.expect("flush v1");
        drop(v1);

        sleep(TokioDuration::from_millis(20)).await;

        let v2 = WalWriterHandle::spawn_with_engine_version(config, "engine-v2".to_string())
            .expect("spawn v2");
        let second_offset = v2
            .append(WalCommand::PlaceOrder(make_incoming(
                "engine-v2-order",
                "alice",
                "m1",
                OrderSide::Buy,
                OrderType::Limit,
                100,
                1,
                1,
            )))
            .await
            .expect("append v2");
        assert_eq!(second_offset, WalOffset(2));
        v2.flush().await.expect("flush v2");

        let segments = discover_segments(&dir).expect("discover");
        assert!(segments.len() >= 2);
        assert!(segments
            .iter()
            .any(|segment| segment.first_offset == WalOffset(2)));

        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), true).expect("open reader");
        let records = reader.read_batch(16).expect("read records");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].engine_version, "engine-v1");
        assert_eq!(records[1].engine_version, "engine-v2");
        drop_dir(&dir);
    }

    #[tokio::test]
    async fn test_writer_rewrites_stale_empty_segment_header_on_version_change() {
        let dir = temp_dir();
        let config = WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 64,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 8 * 1024 * 1024,
        };

        let v1 =
            WalWriterHandle::spawn_with_engine_version(config.clone(), "engine-v1".to_string())
                .expect("spawn v1");
        let first_offset = v1
            .append(WalCommand::PlaceOrder(make_incoming(
                "v1-order",
                "alice",
                "m1",
                OrderSide::Buy,
                OrderType::Limit,
                100,
                1,
                0,
            )))
            .await
            .expect("append v1");
        assert_eq!(first_offset, WalOffset(1));
        v1.flush().await.expect("flush v1");
        drop(v1);

        // Simulate a stale pre-created next segment with the old engine header only.
        let stale = open_or_create_segment(&dir, WalOffset(2), "engine-v1", true)
            .expect("create stale segment");
        drop(stale);

        let v2 = WalWriterHandle::spawn_with_engine_version(config, "engine-v2".to_string())
            .expect("spawn v2");
        let second_offset = v2
            .append(WalCommand::PlaceOrder(make_incoming(
                "v2-order",
                "alice",
                "m1",
                OrderSide::Buy,
                OrderType::Limit,
                100,
                1,
                1,
            )))
            .await
            .expect("append v2");
        assert_eq!(second_offset, WalOffset(2));
        v2.flush().await.expect("flush v2");

        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), true).expect("open reader");
        let records = reader.read_batch(16).expect("read");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].engine_version, "engine-v1");
        assert_eq!(records[1].engine_version, "engine-v2");
        drop_dir(&dir);
    }

    #[tokio::test]
    async fn test_open_or_create_segment_rejects_non_empty_engine_version_mismatch() {
        let dir = temp_dir();
        let config = WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 64,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 8 * 1024 * 1024,
        };

        let v1 = WalWriterHandle::spawn_with_engine_version(config, "engine-v1".to_string())
            .expect("spawn v1");
        v1.append(WalCommand::PlaceOrder(make_incoming(
            "mismatch-order",
            "alice",
            "m1",
            OrderSide::Buy,
            OrderType::Limit,
            100,
            1,
            0,
        )))
        .await
        .expect("append");
        v1.flush().await.expect("flush");
        drop(v1);

        let err = open_or_create_segment(&dir, WalOffset(0), "engine-v2", true)
            .expect_err("non-empty mismatch must fail");
        assert!(
            matches!(err, FluidError::Wal(message) if message.contains("engine version mismatch"))
        );
        drop_dir(&dir);
    }

    #[tokio::test]
    async fn test_replay_determinism_property() {
        let dir = temp_dir();
        let mut live = build_state(
            &[("alice", 100_000), ("bob", 100_000), ("carol", 100_000)],
            HashMap::new(),
            HashMap::new(),
        );
        let mut replay = build_state(
            &[("alice", 100_000), ("bob", 100_000), ("carol", 100_000)],
            HashMap::new(),
            HashMap::new(),
        );

        let mut nonces = HashMap::from([
            ("alice".to_string(), 0_i64),
            ("bob".to_string(), 0_i64),
            ("carol".to_string(), 0_i64),
        ]);
        let mut active_orders: Vec<OrderId> = Vec::new();
        let book_ids = [MarketId("m1".to_string())];
        let mut accepted = Vec::new();

        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 64,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 8 * 1024 * 1024,
        })
        .expect("spawn");

        let mut seed = 0x1234_u64;
        fn next_rand(seed: &mut u64) -> u64 {
            *seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            *seed
        }

        for _ in 0..200 {
            let choose_cancel = next_rand(&mut seed) % 10;
            if choose_cancel < 7 {
                let accounts = ["alice", "bob", "carol"];
                let idx = usize::try_from(next_rand(&mut seed) % 3).expect("idx fits");
                let account = accounts[idx];
                let nonce = *nonces.get(account).expect("account nonce");
                let side = if next_rand(&mut seed).is_multiple_of(2) {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                };
                let order = make_incoming(
                    &format!("o{}", next_rand(&mut seed) % 10_000),
                    account,
                    "m1",
                    side,
                    OrderType::Limit,
                    100 + i64::try_from(next_rand(&mut seed) % 50).expect("price"),
                    1 + i64::try_from(next_rand(&mut seed) % 10).expect("qty"),
                    nonce,
                );
                if let Ok(transition) = live.compute_place_order(&order) {
                    let seq = live.apply_place_order(transition).expect("apply place");
                    let _ = seq;
                    *nonces.get_mut(account).expect("nonce entry") = nonce + 1;
                    active_orders.push(order.order_id.clone());
                    accepted.push(WalCommand::PlaceOrder(order));
                }
            } else if !active_orders.is_empty() {
                let idx = usize::try_from(
                    next_rand(&mut seed) % u64::try_from(active_orders.len()).expect("len"),
                )
                .expect("idx");
                let order_id = active_orders.remove(idx);
                let market = &book_ids[0];
                let entry = live.books().get(market).and_then(|book| {
                    book.get_entry_by_external_order_id(&order_id, live.order_interner())
                });
                if let Some(entry) = entry {
                    let account_id = live
                        .account_interner()
                        .to_external(entry.account_id)
                        .expect("entry account mapping must exist")
                        .to_string();
                    if let Ok(transition) =
                        live.compute_cancel_order(market, &order_id, &account_id)
                    {
                        live.apply_cancel_order(transition).expect("cancel apply");
                        accepted.push(WalCommand::CancelOrder {
                            order_id,
                            market_id: market.clone(),
                            account_id,
                        });
                    }
                }
            }
        }

        for command in accepted.iter() {
            writer.append(command.clone()).await.expect("append");
        }

        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), true).expect("reader");
        replay_wal(
            &mut replay,
            &mut reader,
            WalOffset(0),
            &default_engine_version(),
        )
        .expect("replay");

        // Book equality (deterministic replay path).
        let live_book = live
            .books()
            .get(&MarketId("m1".to_string()))
            .expect("live market");
        let replay_book = replay
            .books()
            .get(&MarketId("m1".to_string()))
            .expect("replay market");
        assert_eq!(live_book.order_count(), replay_book.order_count());
        for order_id in live_book.order_ids() {
            let left = live_book
                .get_entry(order_id)
                .expect("live order should exist");
            let right = replay_book
                .get_entry(order_id)
                .expect("replay order should exist");
            assert_eq!(left, right);
        }
        assert_eq!(
            live.risk()
                .get_account("alice")
                .expect("alice")
                .locked_open_orders_minor,
            replay
                .risk()
                .get_account("alice")
                .expect("alice")
                .locked_open_orders_minor
        );
        assert_eq!(
            live.risk()
                .get_account("bob")
                .expect("bob")
                .locked_open_orders_minor,
            replay
                .risk()
                .get_account("bob")
                .expect("bob")
                .locked_open_orders_minor
        );
        assert_eq!(
            live.risk()
                .get_account("carol")
                .expect("carol")
                .locked_open_orders_minor,
            replay
                .risk()
                .get_account("carol")
                .expect("carol")
                .locked_open_orders_minor
        );

        let live_seq = live
            .sequences()
            .get(&MarketId("m1".to_string()))
            .expect("live seq");
        let replay_seq = replay
            .sequences()
            .get(&MarketId("m1".to_string()))
            .expect("replay seq");
        assert_eq!(live_seq.next_fill_sequence, replay_seq.next_fill_sequence);
        assert_eq!(live_seq.next_event_sequence, replay_seq.next_event_sequence);
        drop_dir(&dir);
    }

    #[tokio::test]
    async fn test_replay_determinism_unified_cross_outcome() {
        let dir = temp_dir();
        let mut live = build_state(
            &[
                ("alice", 100_000),
                ("bob", 100_000),
                ("carol", 100_000),
                ("dave", 100_000),
            ],
            HashMap::new(),
            HashMap::new(),
        );
        let mut replay = build_state(
            &[
                ("alice", 100_000),
                ("bob", 100_000),
                ("carol", 100_000),
                ("dave", 100_000),
            ],
            HashMap::new(),
            HashMap::new(),
        );

        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 64,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 8 * 1024 * 1024,
        })
        .expect("spawn");

        let commands = vec![
            WalCommand::PlaceOrder(make_incoming_with_outcome(
                "alice-buy-no",
                "alice",
                "m1",
                "NO",
                OrderSide::Buy,
                OrderType::Limit,
                4_000,
                5,
                0,
            )),
            WalCommand::PlaceOrder(make_incoming_with_outcome(
                "bob-buy-yes",
                "bob",
                "m1",
                "YES",
                OrderSide::Buy,
                OrderType::Limit,
                6_000,
                3,
                0,
            )),
            WalCommand::PlaceOrder(make_incoming_with_outcome(
                "carol-sell-no",
                "carol",
                "m1",
                "NO",
                OrderSide::Sell,
                OrderType::Limit,
                6_200,
                4,
                0,
            )),
            WalCommand::PlaceOrder(make_incoming_with_outcome(
                "dave-sell-yes",
                "dave",
                "m1",
                "YES",
                OrderSide::Sell,
                OrderType::Limit,
                3_800,
                2,
                0,
            )),
            WalCommand::CancelOrder {
                order_id: OrderId("alice-buy-no".to_string()),
                market_id: MarketId("m1".to_string()),
                account_id: "alice".to_string(),
            },
        ];

        for command in &commands {
            match command {
                WalCommand::PlaceOrder(incoming) => {
                    let transition = live.compute_place_order(incoming).expect("compute place");
                    let _ = live.apply_place_order(transition).expect("apply place");
                }
                WalCommand::CancelOrder {
                    order_id,
                    market_id,
                    account_id,
                } => {
                    let transition = live
                        .compute_cancel_order(market_id, order_id, account_id)
                        .expect("compute cancel");
                    live.apply_cancel_order(transition).expect("apply cancel");
                }
            }
            writer.append(command.clone()).await.expect("append");
        }
        writer.flush().await.expect("flush");

        let mut reader = WalSegmentReader::open(&dir, WalOffset(0), true).expect("reader");
        replay_wal(
            &mut replay,
            &mut reader,
            WalOffset(0),
            &default_engine_version(),
        )
        .expect("replay");

        let market_id = MarketId("m1".to_string());
        let live_book = live.books().get(&market_id).expect("live market");
        let replay_book = replay.books().get(&market_id).expect("replay market");
        assert_eq!(live_book.order_count(), replay_book.order_count());
        for order_id in live_book.order_ids() {
            let left = live_book
                .get_entry(order_id)
                .expect("live order should exist");
            let right = replay_book
                .get_entry(order_id)
                .expect("replay order should exist");
            assert_eq!(left, right);
        }

        for account_id in ["alice", "bob", "carol", "dave"] {
            let live_account = live.risk().get_account(account_id).expect("live account");
            let replay_account = replay
                .risk()
                .get_account(account_id)
                .expect("replay account");
            assert_eq!(
                live_account.delta_pending_trades_minor,
                replay_account.delta_pending_trades_minor
            );
            assert_eq!(
                live_account.locked_open_orders_minor,
                replay_account.locked_open_orders_minor
            );
            assert_eq!(live_account.last_nonce, replay_account.last_nonce);
        }

        let live_seq = live.sequences().get(&market_id).expect("live sequence");
        let replay_seq = replay.sequences().get(&market_id).expect("replay sequence");
        assert_eq!(live_seq.next_fill_sequence, replay_seq.next_fill_sequence);
        assert_eq!(live_seq.next_event_sequence, replay_seq.next_event_sequence);
        assert_eq!(live.order_interner().len(), replay.order_interner().len());
        drop_dir(&dir);
    }

    #[tokio::test]
    async fn test_committed_offset_hint_persists_after_append() {
        let dir = temp_dir();
        let writer = WalWriterHandle::spawn(WalConfig {
            wal_dir: dir.clone(),
            max_batch_size: 16,
            max_batch_wait: Duration::from_millis(1),
            segment_size_bytes: 1024 * 1024,
        })
        .expect("spawn");

        let _ = writer
            .append(WalCommand::PlaceOrder(make_incoming(
                "o1",
                "alice",
                "m1",
                OrderSide::Buy,
                OrderType::Limit,
                100,
                1,
                0,
            )))
            .await
            .expect("append");
        writer.flush().await.expect("flush");

        let committed = read_committed_offset_hint(&dir)
            .expect("read committed offset hint")
            .expect("hint exists");
        assert_eq!(committed, WalOffset(1));
        drop_dir(&dir);
    }

    #[test]
    fn test_read_committed_offset_hint_returns_none_when_missing() {
        let dir = temp_dir();
        assert_eq!(
            read_committed_offset_hint(&dir).expect("read missing hint"),
            None
        );
        drop_dir(&dir);
    }

    #[test]
    fn test_read_committed_offset_hint_rejects_invalid_payload() {
        let dir = temp_dir();
        let path = committed_offset_hint_path(&dir);
        fs::write(path, "not-an-offset\n").expect("write invalid hint");
        let err = read_committed_offset_hint(&dir).expect_err("must reject invalid payload");
        assert!(matches!(err, FluidError::Wal(_)));
        drop_dir(&dir);
    }
}
