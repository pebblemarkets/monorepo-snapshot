use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use pebble_fluid::engine::FluidState;
use pebble_fluid::error::FluidError;
use pebble_fluid::transition::{CancelOrderTransition, PlaceOrderTransition};
use pebble_fluid::wal::{
    latest_wal_offset, truncate_fully_projected_segments, wal_disk_usage_bytes, WalCommand,
    WalOffset, WalRecord, WalSegmentReader,
};
use pebble_trading::IncomingOrder;
use serde::Serialize;
use sqlx::{Postgres, Transaction};
use tokio::sync::{mpsc, Notify};

use crate::{m3_orders, AppState};

const PROJECTOR_BATCH_SIZE: usize = 256;
const PROJECTOR_IDLE_POLL: Duration = Duration::from_millis(25);
const PROJECTOR_RETRY_BACKOFF: Duration = Duration::from_millis(250);
const PROJECTOR_METRICS_POLL: Duration = Duration::from_secs(1);
const PROJECTOR_TRUNCATE_KEEP_SEGMENTS: usize = 2;
const PROJECTOR_MAX_PENDING_TRANSITIONS: usize = 10_000;

#[derive(Clone, Debug)]
pub(crate) struct WalProjectorConfig {
    pub(crate) projection_lag_unhealthy_threshold: Duration,
}

impl Default for WalProjectorConfig {
    fn default() -> Self {
        Self {
            projection_lag_unhealthy_threshold: Duration::from_secs(60),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct WalProjectorMetrics {
    pub(crate) projected_offset: u64,
    pub(crate) lag_entries: u64,
    pub(crate) wal_disk_usage_bytes: u64,
    pub(crate) projected_records: u64,
}

#[derive(Default)]
struct WalProjectorMetricsAtomic {
    lag_entries: AtomicU64,
    wal_disk_usage_bytes: AtomicU64,
    projected_records: AtomicU64,
}

#[derive(Clone, Debug)]
pub(crate) enum ProjectionData {
    PlaceOrder {
        incoming: IncomingOrder,
        transition: Box<PlaceOrderTransition>,
        instrument_admin: String,
        instrument_id: String,
    },
    CancelOrder {
        transition: CancelOrderTransition,
        instrument_admin: String,
        instrument_id: String,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct ProjectionPayload {
    pub(crate) wal_offset: WalOffset,
    pub(crate) command_bytes: Option<Vec<u8>>,
    pub(crate) data: ProjectionData,
}

#[derive(Clone)]
pub(crate) struct WalProjectorHandle {
    projected_offset: Arc<AtomicU64>,
    metrics: Arc<WalProjectorMetricsAtomic>,
    notify: Arc<Notify>,
}

impl WalProjectorHandle {
    fn new(initial_offset: WalOffset) -> Self {
        Self {
            projected_offset: Arc::new(AtomicU64::new(initial_offset.0)),
            metrics: Arc::new(WalProjectorMetricsAtomic::default()),
            notify: Arc::new(Notify::new()),
        }
    }

    fn set_projected_offset(&self, offset: WalOffset) {
        self.projected_offset.store(offset.0, Ordering::SeqCst);
        self.metrics
            .projected_records
            .fetch_add(1, Ordering::Relaxed);
        self.notify.notify_waiters();
    }

    fn set_lag_entries(&self, lag_entries: u64) {
        self.metrics
            .lag_entries
            .store(lag_entries, Ordering::Relaxed);
    }

    fn set_wal_disk_usage_bytes(&self, wal_disk_usage_bytes: u64) {
        self.metrics
            .wal_disk_usage_bytes
            .store(wal_disk_usage_bytes, Ordering::Relaxed);
    }

    pub(crate) fn projected_offset(&self) -> WalOffset {
        WalOffset(self.projected_offset.load(Ordering::SeqCst))
    }

    pub(crate) fn metrics(&self) -> WalProjectorMetrics {
        WalProjectorMetrics {
            projected_offset: self.projected_offset().0,
            lag_entries: self.metrics.lag_entries.load(Ordering::Relaxed),
            wal_disk_usage_bytes: self.metrics.wal_disk_usage_bytes.load(Ordering::Relaxed),
            projected_records: self.metrics.projected_records.load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_projection(
        &self,
        target_offset: WalOffset,
        timeout: Duration,
    ) -> Result<(), FluidError> {
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| FluidError::Wal("projection wait timeout overflow".to_string()))?;

        loop {
            if self.projected_offset() >= target_offset {
                return Ok(());
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(FluidError::Wal(format!(
                    "projection wait timed out at offset {}, target {}",
                    self.projected_offset().0,
                    target_offset.0
                )));
            }

            tokio::time::timeout(
                deadline.saturating_duration_since(now),
                self.notify.notified(),
            )
            .await
            .map_err(|_| {
                FluidError::Wal(format!(
                    "projection wait timed out at offset {}, target {}",
                    self.projected_offset().0,
                    target_offset.0
                ))
            })?;
        }
    }
}

pub(crate) fn spawn_wal_projector(
    state: AppState,
    mut shadow_state: FluidState,
    config: WalProjectorConfig,
    wal_dir: PathBuf,
    after_offset: WalOffset,
    mut projection_rx: mpsc::Receiver<ProjectionPayload>,
) -> WalProjectorHandle {
    let handle = WalProjectorHandle::new(after_offset);
    let task_handle = handle.clone();
    tokio::spawn(async move {
        let mut cursor = after_offset;
        let mut payload_cache = BTreeMap::<u64, ProjectionPayload>::new();
        let mut lag_started_at: Option<tokio::time::Instant> = None;
        let mut lag_progress_cursor = after_offset;
        let mut last_metrics_poll = tokio::time::Instant::now();
        loop {
            drain_projection_payloads(&mut projection_rx, &mut payload_cache);
            prune_payload_cache(&mut payload_cache, cursor);
            let batch = match read_wal_batch(&wal_dir, cursor) {
                Ok(records) => records,
                Err(err) => {
                    tracing::error!(error = ?err, "projector: failed to read WAL batch");
                    tokio::time::sleep(PROJECTOR_RETRY_BACKOFF).await;
                    continue;
                }
            };

            if batch.is_empty() {
                evaluate_lag_health(
                    &state,
                    &task_handle,
                    &wal_dir,
                    &mut last_metrics_poll,
                    &mut lag_started_at,
                    &mut lag_progress_cursor,
                    config.projection_lag_unhealthy_threshold,
                );
                tokio::time::sleep(PROJECTOR_IDLE_POLL).await;
                continue;
            }

            let mut retry = false;
            let mut progressed = false;
            match project_batch(&state, &mut shadow_state, &mut payload_cache, &batch).await {
                Ok(applied_offsets) => {
                    if !applied_offsets.is_empty() {
                        progressed = true;
                    }
                    for offset in applied_offsets {
                        cursor = offset;
                        task_handle.set_projected_offset(cursor);
                    }
                }
                Err(err) => {
                    tracing::error!(error = ?err, "projector: failed to project WAL batch");
                    if let Some(fluid) = state.fluid.as_ref() {
                        fluid.mark_unhealthy("projector failed to project WAL batch");
                    }
                    retry = true;
                }
            }
            if retry {
                tokio::time::sleep(PROJECTOR_RETRY_BACKOFF).await;
            }
            prune_payload_cache(&mut payload_cache, cursor);
            if progressed {
                match truncate_fully_projected_segments(
                    &wal_dir,
                    cursor,
                    PROJECTOR_TRUNCATE_KEEP_SEGMENTS,
                ) {
                    Ok(removed) => {
                        if removed > 0 {
                            tracing::info!(removed, "projector: truncated projected wal segments");
                        }
                    }
                    Err(err) => {
                        tracing::warn!(error = ?err, "projector: wal truncation failed");
                    }
                }
            }
            evaluate_lag_health(
                &state,
                &task_handle,
                &wal_dir,
                &mut last_metrics_poll,
                &mut lag_started_at,
                &mut lag_progress_cursor,
                config.projection_lag_unhealthy_threshold,
            );
        }
    });
    handle
}

fn poll_projector_metrics(
    handle: &WalProjectorHandle,
    wal_dir: &Path,
    last_metrics_poll: &mut tokio::time::Instant,
) -> Option<u64> {
    let now = tokio::time::Instant::now();
    if now.duration_since(*last_metrics_poll) < PROJECTOR_METRICS_POLL {
        return None;
    }
    *last_metrics_poll = now;
    let lag_entries = match latest_wal_offset(wal_dir) {
        Ok(tail) => {
            let lag = lag_entries_from_offsets(handle.projected_offset(), tail);
            handle.set_lag_entries(lag);
            lag
        }
        Err(err) => {
            tracing::warn!(error = ?err, "projector: failed to read wal tail offset");
            handle.set_lag_entries(0);
            0
        }
    };
    match wal_disk_usage_bytes(wal_dir) {
        Ok(bytes) => handle.set_wal_disk_usage_bytes(bytes),
        Err(err) => {
            tracing::warn!(error = ?err, "projector: failed to read wal disk usage");
        }
    }
    Some(lag_entries)
}

fn evaluate_lag_health(
    state: &AppState,
    handle: &WalProjectorHandle,
    wal_dir: &Path,
    last_metrics_poll: &mut tokio::time::Instant,
    lag_started_at: &mut Option<tokio::time::Instant>,
    lag_progress_cursor: &mut WalOffset,
    threshold: Duration,
) {
    let Some(lag_entries) = poll_projector_metrics(handle, wal_dir, last_metrics_poll) else {
        return;
    };
    let now = tokio::time::Instant::now();
    let projected = handle.projected_offset();
    if lag_entries == 0 {
        *lag_started_at = None;
        *lag_progress_cursor = projected;
        return;
    }

    if projected > *lag_progress_cursor {
        *lag_started_at = None;
        *lag_progress_cursor = projected;
        return;
    }

    if lag_started_at.is_none() {
        *lag_started_at = Some(now);
    }
    if projection_lag_exceeded(now, *lag_started_at, threshold) {
        if let Some(fluid) = state.fluid.as_ref() {
            fluid.mark_unhealthy("projector lag exceeded threshold");
        }
    }
}

fn lag_entries_from_offsets(projected_offset: WalOffset, wal_tail: Option<WalOffset>) -> u64 {
    match wal_tail {
        Some(tail) if tail.0 > projected_offset.0 => tail.0.saturating_sub(projected_offset.0),
        _ => 0,
    }
}

fn projection_lag_exceeded(
    now: tokio::time::Instant,
    lag_started_at: Option<tokio::time::Instant>,
    threshold: Duration,
) -> bool {
    lag_started_at.is_some_and(|started| now.duration_since(started) >= threshold)
}

fn drain_projection_payloads(
    projection_rx: &mut mpsc::Receiver<ProjectionPayload>,
    payload_cache: &mut BTreeMap<u64, ProjectionPayload>,
) {
    loop {
        match projection_rx.try_recv() {
            Ok(payload) => {
                payload_cache.insert(payload.wal_offset.0, payload);
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
        }
    }
}

fn prune_payload_cache(payload_cache: &mut BTreeMap<u64, ProjectionPayload>, cursor: WalOffset) {
    payload_cache.retain(|offset, _| *offset > cursor.0);
    while payload_cache.len() > PROJECTOR_MAX_PENDING_TRANSITIONS {
        let _ = payload_cache.pop_first();
    }
}

fn read_wal_batch(wal_dir: &Path, after_offset: WalOffset) -> Result<Vec<WalRecord>, FluidError> {
    let mut reader = WalSegmentReader::open(wal_dir, after_offset, false)?;
    reader.read_batch(PROJECTOR_BATCH_SIZE)
}

fn payload_matches_record(payload: &ProjectionPayload, record: &WalRecord) -> bool {
    if payload.wal_offset != record.offset {
        return false;
    }

    match &payload.command_bytes {
        Some(cached_bytes) => match rmp_serde::to_vec(&record.command) {
            Ok(wal_bytes) => cached_bytes == &wal_bytes,
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    wal_offset = record.offset.0,
                    "projector: failed to serialize WAL command for cache validation"
                );
                false
            }
        },
        None => false,
    }
}

fn build_projection_data_from_command(
    shadow_state: &FluidState,
    command: &WalCommand,
) -> Result<ProjectionData, FluidError> {
    match command {
        WalCommand::PlaceOrder(incoming) => {
            let transition = shadow_state.compute_place_order_replay(incoming)?;
            let account = shadow_state.risk().get_account(&incoming.account_id)?;
            Ok(ProjectionData::PlaceOrder {
                incoming: incoming.clone(),
                transition: Box::new(transition),
                instrument_admin: account.instrument_admin.clone(),
                instrument_id: account.instrument_id.clone(),
            })
        }
        WalCommand::CancelOrder {
            order_id,
            market_id,
            account_id,
        } => {
            let transition = shadow_state.compute_cancel_order(market_id, order_id, account_id)?;
            let account = shadow_state.risk().get_account(account_id)?;
            Ok(ProjectionData::CancelOrder {
                transition,
                instrument_admin: account.instrument_admin.clone(),
                instrument_id: account.instrument_id.clone(),
            })
        }
    }
}

fn projection_data_for_record(
    expected_engine_version: &str,
    shadow_state: &FluidState,
    payload_cache: &mut BTreeMap<u64, ProjectionPayload>,
    record: &WalRecord,
) -> Result<ProjectionData> {
    if record.engine_version != expected_engine_version {
        return Err(anyhow::anyhow!(
            "projector: WAL engine version mismatch at offset {} expected={} got={}",
            record.offset.0,
            expected_engine_version,
            record.engine_version
        ));
    }

    let data = match payload_cache.remove(&record.offset.0) {
        Some(payload) if payload_matches_record(&payload, record) => payload.data,
        _ => build_projection_data_from_command(shadow_state, &record.command)
            .context("projector: failed to re-build projection data from WAL command")?,
    };

    Ok(data)
}

async fn persist_projection_data_in_tx(
    state: &AppState,
    tx: &mut Transaction<'_, Postgres>,
    wal_offset: WalOffset,
    data: &ProjectionData,
) -> Result<()> {
    match data {
        ProjectionData::PlaceOrder {
            incoming,
            transition,
            instrument_admin,
            instrument_id,
        } => {
            m3_orders::persist_place_order_fluid_in_tx(
                state,
                tx,
                incoming,
                transition,
                wal_offset,
                instrument_admin,
                instrument_id,
            )
            .await
            .map_err(|err| anyhow::anyhow!("projector place persist failed: {err:?}"))?;
        }
        ProjectionData::CancelOrder {
            transition,
            instrument_admin,
            instrument_id,
        } => {
            m3_orders::persist_cancel_order_fluid_in_tx(
                state,
                tx,
                transition,
                wal_offset,
                instrument_admin,
                instrument_id,
            )
            .await
            .map_err(|err| anyhow::anyhow!("projector cancel persist failed: {err:?}"))?;
        }
    }

    Ok(())
}

fn apply_projection_data_to_shadow(
    shadow_state: &mut FluidState,
    data: ProjectionData,
) -> Result<()> {
    match data {
        ProjectionData::PlaceOrder { transition, .. } => shadow_state
            .apply_place_order(*transition)
            .context("projector: failed to apply place transition to shadow state")?,
        ProjectionData::CancelOrder { transition, .. } => shadow_state
            .apply_cancel_order(transition)
            .context("projector: failed to apply cancel transition to shadow state")?,
    }
    Ok(())
}

async fn project_batch(
    state: &AppState,
    shadow_state: &mut FluidState,
    payload_cache: &mut BTreeMap<u64, ProjectionPayload>,
    batch: &[WalRecord],
) -> Result<Vec<WalOffset>> {
    let mut tx = state
        .db
        .begin()
        .await
        .context("projector: begin batch tx")?;
    let expected_epoch = state.leader_epoch.load(Ordering::Relaxed);
    ensure_projector_epoch_in_tx(&mut tx, expected_epoch).await?;
    let mut working_shadow = shadow_state.clone();
    let mut applied_offsets = Vec::with_capacity(batch.len());

    for record in batch {
        let data = projection_data_for_record(
            &state.m3_cfg.engine_version,
            &working_shadow,
            payload_cache,
            record,
        )?;
        persist_projection_data_in_tx(state, &mut tx, record.offset, &data).await?;
        apply_projection_data_to_shadow(&mut working_shadow, data)?;
        applied_offsets.push(record.offset);
    }

    ensure_projector_epoch_in_tx(&mut tx, expected_epoch).await?;
    tx.commit().await.context("projector: commit batch tx")?;
    *shadow_state = working_shadow;
    Ok(applied_offsets)
}

async fn ensure_projector_epoch_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    expected_epoch: u64,
) -> Result<()> {
    let observed_epoch: Option<i64> = sqlx::query_scalar(
        "SELECT leader_epoch FROM fluid_leader_state WHERE singleton_key = TRUE",
    )
    .fetch_optional(&mut **tx)
    .await
    .context("projector: load leader epoch in tx")?;
    validate_projector_epoch(expected_epoch, observed_epoch.unwrap_or(0))
}

fn validate_projector_epoch(expected_epoch: u64, observed_epoch: i64) -> Result<()> {
    if observed_epoch < 0 {
        anyhow::bail!("projector: observed negative leader epoch {observed_epoch}");
    }
    let observed_epoch =
        u64::try_from(observed_epoch).context("projector: convert observed leader epoch to u64")?;
    if observed_epoch != expected_epoch {
        anyhow::bail!(
            "projector: leader epoch superseded (expected={expected_epoch}, observed={observed_epoch})"
        );
    }
    Ok(())
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use pebble_fluid::book::MarketBook;
    use pebble_fluid::engine::{BinaryConfig, MarketMeta, SequenceState};
    use pebble_fluid::ids::StringInterner;
    use pebble_fluid::risk::{AccountState, RiskEngine};
    use pebble_fluid::wal::{WalConfig, WalWriterHandle};
    use pebble_trading::{MarketId, OrderId, OrderNonce, OrderSide, OrderType, TimeInForce};
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> PathBuf {
        let mut dir = env::temp_dir();
        let process_id = std::process::id();
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        let seq = TEST_DIR_COUNTER.fetch_add(1, AtomicOrdering::SeqCst);
        dir.push(format!("pebble-wal-projector-{process_id}-{elapsed}-{seq}"));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn drop_dir(path: &Path) {
        let _ = fs::remove_dir_all(path);
    }

    fn make_account(cleared_cash_minor: i64) -> AccountState {
        AccountState {
            cleared_cash_minor,
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

    fn build_shadow_state() -> FluidState {
        let market_id = MarketId("m1".to_string());
        let mut books = HashMap::new();
        books.insert(
            market_id.clone(),
            MarketBook::new_binary(BinaryConfig {
                yes_outcome: "YES".to_string(),
                no_outcome: "NO".to_string(),
                max_price_ticks: 10_000,
            }),
        );

        let mut risk = RiskEngine::default();
        risk.upsert_account("alice", make_account(100_000));
        risk.upsert_account("bob", make_account(100_000));

        let mut sequences = HashMap::new();
        sequences.insert(
            market_id.clone(),
            SequenceState {
                next_fill_sequence: 0,
                next_event_sequence: 1,
            },
        );

        let mut market_meta = HashMap::new();
        market_meta.insert(
            market_id,
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
            },
        );

        FluidState::new(
            books,
            risk,
            sequences,
            market_meta,
            StringInterner::default(),
            StringInterner::default(),
            "v1".to_string(),
            false,
        )
    }

    fn build_empty_shadow_state() -> FluidState {
        FluidState::new(
            HashMap::new(),
            RiskEngine::default(),
            HashMap::new(),
            HashMap::new(),
            StringInterner::default(),
            StringInterner::default(),
            "v1".to_string(),
            false,
        )
    }

    fn make_incoming(order_id: &str) -> IncomingOrder {
        make_incoming_for(order_id, "alice", "YES", OrderSide::Buy, 10, 1, 0)
    }

    fn make_incoming_for(
        order_id: &str,
        account_id: &str,
        outcome: &str,
        side: OrderSide,
        price_ticks: i64,
        quantity_minor: i64,
        nonce: i64,
    ) -> IncomingOrder {
        IncomingOrder {
            order_id: OrderId(order_id.to_string()),
            account_id: account_id.to_string(),
            market_id: MarketId("m1".to_string()),
            outcome: outcome.to_string(),
            side,
            order_type: OrderType::Limit,
            tif: TimeInForce::Gtc,
            nonce: OrderNonce(nonce),
            price_ticks,
            quantity_minor,
            submitted_at_micros: 1,
        }
    }

    fn payload_for_offset(offset: u64) -> ProjectionPayload {
        let order_id = OrderId(format!("o-{offset}"));
        let market_id = MarketId("m1".to_string());
        let account_id = "alice".to_string();
        let command = WalCommand::CancelOrder {
            order_id: order_id.clone(),
            market_id: market_id.clone(),
            account_id: account_id.clone(),
        };
        ProjectionPayload {
            wal_offset: WalOffset(offset),
            command_bytes: Some(rmp_serde::to_vec(&command).expect("serialize command")),
            data: ProjectionData::CancelOrder {
                transition: CancelOrderTransition {
                    market_id,
                    order_id,
                    account_id,
                    released_locked_minor: 0,
                },
                instrument_admin: "admin".to_string(),
                instrument_id: "USD".to_string(),
            },
        }
    }

    #[tokio::test]
    async fn test_projector_wal_tail_driven() {
        let wal_dir = temp_dir();
        let writer = WalWriterHandle::spawn_with_engine_version(
            WalConfig {
                wal_dir: wal_dir.clone(),
                max_batch_size: 64,
                max_batch_wait: StdDuration::from_millis(1),
                segment_size_bytes: 8 * 1024 * 1024,
            },
            "v1".to_string(),
        )
        .expect("spawn writer");
        writer
            .append(WalCommand::PlaceOrder(make_incoming("tail-1")))
            .await
            .expect("append");
        writer.flush().await.expect("flush");

        let batch = read_wal_batch(&wal_dir, WalOffset(0)).expect("read batch");
        assert_eq!(batch.len(), 1);

        let mut payload_cache = BTreeMap::new();
        let shadow_state = build_shadow_state();
        let data = projection_data_for_record("v1", &shadow_state, &mut payload_cache, &batch[0])
            .expect("derive projection data");
        assert!(matches!(data, ProjectionData::PlaceOrder { .. }));
        drop_dir(&wal_dir);
    }

    #[test]
    fn test_projector_channel_optimization() {
        let shadow_state = build_empty_shadow_state();
        let command = WalCommand::CancelOrder {
            order_id: OrderId("missing".to_string()),
            market_id: MarketId("m1".to_string()),
            account_id: "alice".to_string(),
        };
        let record = WalRecord {
            offset: WalOffset(11),
            command: command.clone(),
            engine_version: "v1".to_string(),
        };

        let mut payload_cache = BTreeMap::new();
        payload_cache.insert(
            11,
            ProjectionPayload {
                wal_offset: WalOffset(11),
                command_bytes: Some(rmp_serde::to_vec(&command).expect("serialize command")),
                data: ProjectionData::CancelOrder {
                    transition: CancelOrderTransition {
                        market_id: MarketId("m1".to_string()),
                        order_id: OrderId("missing".to_string()),
                        account_id: "alice".to_string(),
                        released_locked_minor: 0,
                    },
                    instrument_admin: "admin".to_string(),
                    instrument_id: "USD".to_string(),
                },
            },
        );

        let data = projection_data_for_record("v1", &shadow_state, &mut payload_cache, &record)
            .expect("must use cached payload");
        assert!(matches!(data, ProjectionData::CancelOrder { .. }));
        assert!(!payload_cache.contains_key(&11));
    }

    #[test]
    fn test_projector_handles_channel_drops() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut payload_cache = BTreeMap::new();
        payload_cache.insert(99, payload_for_offset(99));
        drop(tx);

        drain_projection_payloads(&mut rx, &mut payload_cache);
        assert_eq!(payload_cache.len(), 1);
        assert!(payload_cache.contains_key(&99));
    }

    #[tokio::test]
    async fn test_persistence_fence() {
        let handle = WalProjectorHandle::new(WalOffset(5));
        let waiter = {
            let handle = handle.clone();
            tokio::spawn(async move {
                handle
                    .wait_for_projection(WalOffset(7), Duration::from_secs(1))
                    .await
            })
        };

        tokio::time::sleep(Duration::from_millis(10)).await;
        handle.set_projected_offset(WalOffset(7));
        waiter.await.expect("join").expect("wait succeeds");
    }

    #[tokio::test]
    async fn test_end_to_end_projection_latency_smoke() {
        let handle = WalProjectorHandle::new(WalOffset(0));
        let notifier = handle.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(25)).await;
            notifier.set_projected_offset(WalOffset(1));
        });

        let started = tokio::time::Instant::now();
        handle
            .wait_for_projection(WalOffset(1), Duration::from_secs(1))
            .await
            .expect("projection wait");
        let elapsed = started.elapsed();
        assert!(elapsed < Duration::from_secs(1));
    }

    #[test]
    fn test_projection_matches_direct_persist() {
        let shadow_state = build_shadow_state();
        let incoming = make_incoming("direct-match");
        let direct = shadow_state
            .compute_place_order_replay(&incoming)
            .expect("direct replay transition");
        let record = WalRecord {
            offset: WalOffset(1),
            command: WalCommand::PlaceOrder(incoming.clone()),
            engine_version: "v1".to_string(),
        };

        let mut payload_cache = BTreeMap::new();
        let from_wal = projection_data_for_record("v1", &shadow_state, &mut payload_cache, &record)
            .expect("projection from wal");
        match from_wal {
            ProjectionData::PlaceOrder { transition, .. } => {
                assert_eq!(transition.fills, direct.fills);
                assert_eq!(transition.taker_remaining, direct.taker_remaining);
                assert_eq!(transition.taker_locked, direct.taker_locked);
                assert_eq!(transition.next_fill_sequence, direct.next_fill_sequence);
                assert_eq!(transition.next_event_sequence, direct.next_event_sequence);
            }
            _ => panic!("expected place projection data"),
        }
    }

    #[test]
    fn test_projector_cross_outcome_fill() {
        let mut shadow_state = build_shadow_state();
        let maker_incoming =
            make_incoming_for("maker-buy-no", "bob", "NO", OrderSide::Buy, 4_000, 3, 0);
        let maker_transition = shadow_state
            .compute_place_order_replay(&maker_incoming)
            .expect("maker replay transition");
        shadow_state
            .apply_place_order(maker_transition)
            .expect("apply maker transition");

        let taker_incoming =
            make_incoming_for("taker-buy-yes", "alice", "YES", OrderSide::Buy, 6_000, 2, 0);
        let record = WalRecord {
            offset: WalOffset(1),
            command: WalCommand::PlaceOrder(taker_incoming.clone()),
            engine_version: "v1".to_string(),
        };

        let mut payload_cache = BTreeMap::new();
        let data =
            projection_data_for_record("v1", &shadow_state, &mut payload_cache, &record).unwrap();
        match &data {
            ProjectionData::PlaceOrder { transition, .. } => {
                assert_eq!(transition.fills.len(), 1);
                assert_eq!(transition.fills[0].maker_order_id.0, "maker-buy-no");
                assert_eq!(transition.fills[0].maker_outcome, "NO");
                assert_eq!(transition.fills[0].maker_side, OrderSide::Buy);
                assert_eq!(transition.fills[0].maker_price_ticks, 4_000);
                assert_eq!(transition.fills[0].taker_outcome, "YES");
                assert_eq!(transition.fills[0].taker_side, OrderSide::Buy);
                assert_eq!(transition.fills[0].taker_price_ticks, 6_000);
                assert_eq!(transition.fills[0].quantity_minor, 2);
            }
            _ => panic!("expected place projection data"),
        }

        apply_projection_data_to_shadow(&mut shadow_state, data).expect("apply to shadow");

        let market_id = MarketId("m1".to_string());
        let remaining_entry = shadow_state
            .books()
            .get(&market_id)
            .and_then(|book| {
                book.get_entry_by_external_order_id(
                    &OrderId("maker-buy-no".to_string()),
                    shadow_state.order_interner(),
                )
            })
            .expect("maker order should remain");
        assert_eq!(remaining_entry.outcome, "NO");
        assert_eq!(remaining_entry.side, OrderSide::Buy);
        assert_eq!(remaining_entry.remaining_minor, 1);
        assert_eq!(remaining_entry.locked_minor, 4_000);
    }

    #[test]
    fn test_projection_failure_retries() {
        let mut shadow_state = build_shadow_state();
        let record = WalRecord {
            offset: WalOffset(3),
            command: WalCommand::CancelOrder {
                order_id: OrderId("retry-order".to_string()),
                market_id: MarketId("m1".to_string()),
                account_id: "alice".to_string(),
            },
            engine_version: "v1".to_string(),
        };

        let mut payload_cache = BTreeMap::new();
        let first_try =
            projection_data_for_record("v1", &shadow_state, &mut payload_cache, &record);
        assert!(first_try.is_err());

        let market_id = MarketId("m1".to_string());
        let transition = shadow_state
            .compute_place_order_replay(&make_incoming("retry-order"))
            .expect("compute retry-order replay transition");
        shadow_state
            .apply_place_order(transition)
            .expect("apply retry-order replay transition");
        assert_eq!(
            shadow_state
                .books()
                .get(&market_id)
                .expect("market")
                .order_count(),
            1
        );

        let second_try =
            projection_data_for_record("v1", &shadow_state, &mut payload_cache, &record);
        assert!(second_try.is_ok());
    }

    #[tokio::test]
    async fn test_wal_truncation() {
        let wal_dir = temp_dir();
        let writer = WalWriterHandle::spawn_with_engine_version(
            WalConfig {
                wal_dir: wal_dir.clone(),
                max_batch_size: 16,
                max_batch_wait: StdDuration::from_millis(1),
                segment_size_bytes: 256,
            },
            "v1".to_string(),
        )
        .expect("spawn writer");
        for idx in 0..40 {
            writer
                .append(WalCommand::PlaceOrder(make_incoming(&format!(
                    "truncate-{idx}"
                ))))
                .await
                .expect("append");
        }
        writer.flush().await.expect("flush");

        let before = wal_disk_usage_bytes(&wal_dir).expect("disk usage before");
        let tail = latest_wal_offset(&wal_dir)
            .expect("latest offset")
            .expect("tail exists");
        let removed =
            truncate_fully_projected_segments(&wal_dir, tail, 2).expect("truncate projected");
        let after = wal_disk_usage_bytes(&wal_dir).expect("disk usage after");

        assert!(removed > 0);
        assert!(after < before);
        drop_dir(&wal_dir);
    }

    #[test]
    fn payload_match_requires_same_offset_and_bytes() {
        let command = WalCommand::PlaceOrder(make_incoming("o-1"));
        let payload = ProjectionPayload {
            wal_offset: WalOffset(7),
            command_bytes: Some(rmp_serde::to_vec(&command).expect("serialize command")),
            data: ProjectionData::PlaceOrder {
                incoming: make_incoming("o-1"),
                transition: Box::new(PlaceOrderTransition {
                    market_id: MarketId("m1".to_string()),
                    book_transition: pebble_fluid::transition::BookTransition::default(),
                    risk_transition: pebble_fluid::transition::RiskTransition::default(),
                    fills: Vec::new(),
                    taker_status: pebble_fluid::events::OrderStatus::Open,
                    taker_remaining: 1,
                    taker_locked: 10,
                    available_minor_after: 0,
                    maker_updates: Vec::new(),
                    next_fill_sequence: 0,
                    next_event_sequence: 1,
                }),
                instrument_admin: "admin".to_string(),
                instrument_id: "USD".to_string(),
            },
        };
        let record = WalRecord {
            offset: WalOffset(7),
            command: command.clone(),
            engine_version: "v1".to_string(),
        };
        assert!(payload_matches_record(&payload, &record));

        let different_offset = WalRecord {
            offset: WalOffset(8),
            command: command.clone(),
            engine_version: "v1".to_string(),
        };
        assert!(!payload_matches_record(&payload, &different_offset));

        let different_command = WalRecord {
            offset: WalOffset(7),
            command: WalCommand::PlaceOrder(make_incoming("o-2")),
            engine_version: "v1".to_string(),
        };
        assert!(!payload_matches_record(&payload, &different_command));

        let no_bytes_payload = ProjectionPayload {
            wal_offset: WalOffset(7),
            command_bytes: None,
            data: payload.data.clone(),
        };
        assert!(!payload_matches_record(&no_bytes_payload, &record));
    }

    #[tokio::test]
    async fn wait_for_projection_returns_when_target_reached() {
        let handle = WalProjectorHandle::new(WalOffset(3));
        let cloned = handle.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            cloned.set_projected_offset(WalOffset(5));
        });
        handle
            .wait_for_projection(WalOffset(5), Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn wait_for_projection_times_out() {
        let handle = WalProjectorHandle::new(WalOffset(1));
        let err = handle
            .wait_for_projection(WalOffset(2), Duration::from_millis(20))
            .await
            .unwrap_err();
        assert!(matches!(err, FluidError::Wal(_)));
    }

    #[test]
    fn projection_lag_exceeded_obeys_threshold() {
        let now = tokio::time::Instant::now();
        let lag_start = now.checked_sub(Duration::from_secs(5)).unwrap_or(now);
        assert!(projection_lag_exceeded(
            now,
            Some(lag_start),
            Duration::from_secs(1)
        ));
        assert!(!projection_lag_exceeded(
            now,
            Some(lag_start),
            Duration::from_secs(10)
        ));
        assert!(!projection_lag_exceeded(now, None, Duration::from_secs(1)));
    }

    #[test]
    fn validate_projector_epoch_accepts_equal_values() {
        validate_projector_epoch(5, 5).expect("equal epoch must be accepted");
    }

    #[test]
    fn validate_projector_epoch_rejects_negative_observed() {
        let err = validate_projector_epoch(5, -1).expect_err("negative epoch must be rejected");
        assert!(err.to_string().contains("negative leader epoch"));
    }

    #[test]
    fn validate_projector_epoch_rejects_mismatch() {
        let err = validate_projector_epoch(5, 6).expect_err("mismatch must be rejected");
        assert!(err.to_string().contains("superseded"));
    }

    #[test]
    fn lag_entries_from_offsets_tracks_true_backlog() {
        assert_eq!(
            lag_entries_from_offsets(WalOffset(10), Some(WalOffset(10))),
            0
        );
        assert_eq!(
            lag_entries_from_offsets(WalOffset(10), Some(WalOffset(13))),
            3
        );
        assert_eq!(
            lag_entries_from_offsets(WalOffset(10), Some(WalOffset(9))),
            0
        );
        assert_eq!(lag_entries_from_offsets(WalOffset(10), None), 0);
    }

    #[test]
    fn prune_payload_cache_drops_entries_at_or_below_cursor() {
        let mut payload_cache = BTreeMap::new();
        payload_cache.insert(1, payload_for_offset(1));
        payload_cache.insert(2, payload_for_offset(2));
        payload_cache.insert(3, payload_for_offset(3));

        prune_payload_cache(&mut payload_cache, WalOffset(2));

        assert_eq!(payload_cache.len(), 1);
        assert!(payload_cache.contains_key(&3));
    }

    #[test]
    fn prune_payload_cache_enforces_max_pending_bound() {
        let mut payload_cache = BTreeMap::new();
        let total = PROJECTOR_MAX_PENDING_TRANSITIONS + 32;
        for offset in 1..=total {
            let offset = u64::try_from(offset).unwrap_or(u64::MAX);
            payload_cache.insert(offset, payload_for_offset(offset));
        }

        prune_payload_cache(&mut payload_cache, WalOffset(0));

        assert_eq!(payload_cache.len(), PROJECTOR_MAX_PENDING_TRANSITIONS);
        assert!(payload_cache
            .first_key_value()
            .is_some_and(|(k, _)| *k == 33));
    }
}
