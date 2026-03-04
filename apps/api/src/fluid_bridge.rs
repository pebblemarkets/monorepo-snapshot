/// Fluid engine initialization, state sync, leader watchdog, and error bridging.
///
/// This module handles the lifecycle of the in-memory matching engine:
/// - Startup: acquire leader lock, load snapshot, initialize FluidState
/// - Sync: background task polls for external state changes
/// - Watchdog: heartbeat on dedicated connection, fail-closed on loss
/// - Error bridging: FluidError → ApiError conversion
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use pebble_fluid::actor::RiskActorHandle;
use pebble_fluid::book::{MarketBook, RestingEntry};
use pebble_fluid::engine::FluidState;
use pebble_fluid::engine::{BinaryConfig, ExchangeEngine, MarketMeta, SequenceState};
use pebble_fluid::error::FluidError;
use pebble_fluid::ids::StringInterner;
use pebble_fluid::replication::{
    read_framed, read_payloads_between_offsets, validate_handshake, validate_leader_epoch,
    write_framed, FollowerWalWriter, HandshakeResponse, HandshakeStatus, ReplicationAck,
    ReplicationBroadcaster, ReplicationHandshake, ReplicationMessage, WalRecordPayload,
    REPLICATION_MAGIC, REPLICATION_PROTOCOL_VERSION,
};
use pebble_fluid::risk::{AccountState, RiskEngine};
use pebble_fluid::wal::{
    local_wal_tip, replay_wal, wal_command_crc32c, WalCommand, WalConfig, WalOffset,
    WalSegmentReader, WalWriterHandle,
};
use pebble_trading::{InternalAccountId, InternalOrderId, MarketId, OrderSide};
use rmp_serde::from_slice;
use sqlx::postgres::PgConnection;
use sqlx::{Connection, PgPool};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpListener, TcpStream};

use crate::ApiError;

/// Advisory lock namespace reserved for Fluid leader election.
const FLUID_LOCK_NAMESPACE: i64 = 99;
const FLUID_LOCK_KEY: i64 = 0;

/// How often the leader watchdog pings the dedicated connection.
const LEADER_WATCHDOG_INTERVAL: Duration = Duration::from_secs(1);

/// How often the state sync task polls for changes.
const STATE_SYNC_INTERVAL: Duration = Duration::from_secs(5);
const RISK_ACTOR_CHANNEL_CAPACITY: usize = 16;

/// Consecutive probe failures before a legacy instance stops writes (fail-closed).
const LEGACY_PROBE_FAIL_THRESHOLD: u32 = 3;

// ── WriteMode ──

/// Determines the write behavior of this API instance. Immutable after startup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteMode {
    /// Existing per-order DB round-trips.
    Legacy,
    /// In-memory compute-persist-apply path (this node holds the Fluid leader lock).
    FluidLeader,
    /// Read-only query node that continuously replicates WAL from the leader.
    FluidFollower,
    /// Fatal runtime state (e.g. local WAL fsync failure). Reject all traffic.
    Fatal,
}

impl WriteMode {
    fn as_u8(self) -> u8 {
        match self {
            Self::Legacy => 0,
            Self::FluidLeader => 1,
            Self::FluidFollower => 2,
            Self::Fatal => 3,
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::FluidLeader,
            2 => Self::FluidFollower,
            3 => Self::Fatal,
            _ => Self::Legacy,
        }
    }
}

pub(crate) fn new_write_mode_state(initial: WriteMode) -> Arc<AtomicU8> {
    Arc::new(AtomicU8::new(initial.as_u8()))
}

pub(crate) fn load_write_mode(state: &Arc<AtomicU8>) -> WriteMode {
    WriteMode::from_u8(state.load(Ordering::Relaxed))
}

pub(crate) fn store_write_mode(state: &Arc<AtomicU8>, mode: WriteMode) {
    state.store(mode.as_u8(), Ordering::Relaxed);
}

// ── FluidError → ApiError ──

pub(crate) fn fluid_error_to_api_error(err: &FluidError) -> ApiError {
    match err {
        FluidError::InsufficientBalance {
            available,
            required,
        } => ApiError::bad_request(format!(
            "insufficient available balance: available={available}, required_lock={required}"
        )),
        FluidError::NoncePending {
            account_id,
            pending_nonce,
        } => ApiError::conflict_code(
            format!("nonce {pending_nonce} is in-flight for account {account_id}"),
            "nonce_pending",
        ),
        FluidError::AccountNotFound(id) => ApiError::not_found(format!("account not found: {id}")),
        FluidError::AccountNotActive(id) => {
            ApiError::bad_request(format!("account must be Active: {id}"))
        }
        FluidError::MarketNotFound(id) => ApiError::not_found(format!("market not found: {id}")),
        FluidError::MarketNotOpen(id) => ApiError::bad_request(format!("market not open: {id}")),
        FluidError::OrderNotFound(id) => ApiError::not_found(format!("order not found: {id}")),
        FluidError::Unauthorized => ApiError::bad_request("not authorized to cancel this order"),
        FluidError::NonceMismatch { expected, got } => {
            ApiError::bad_request(format!("nonce must be {expected}, got {got}"))
        }
        FluidError::MatchFailed(msg) => ApiError::bad_request(format!("match failed: {msg}")),
        FluidError::InstrumentMismatch => {
            ApiError::bad_request("account instrument does not match market instrument")
        }
        FluidError::Overflow(msg) => ApiError::internal(format!("arithmetic overflow: {msg}")),
        FluidError::EngineUnhealthy(msg) => ApiError::service_unavailable_code(
            format!("engine unhealthy: {msg}"),
            "engine_unhealthy",
        ),
        FluidError::Wal(msg) => ApiError::internal(format!("wal error: {msg}")),
        FluidError::Replication(msg) => {
            ApiError::service_unavailable_code(format!("replication error: {msg}"), "replication")
        }
        FluidError::ReplayDivergence(msg) => {
            ApiError::internal(format!("wal replay divergence: {msg}"))
        }
    }
}

// ── Initialization ──

/// Result of Fluid initialization.
pub(crate) struct FluidInit {
    pub(crate) fluid: Arc<ExchangeEngine>,
    /// Dedicated PG connection holding the session-scoped leader lock.
    pub(crate) leader_conn: PgConnection,
    /// WAL append handle for dual-write persistence.
    pub(crate) wal_handle: WalWriterHandle,
    /// Shadow in-memory state used by the projector for deterministic re-match.
    pub(crate) shadow_state: FluidState,
    /// Watermarks for the sync task.
    pub(crate) sync_watermarks: SyncWatermarks,
    /// Monotonic fencing epoch for this leader session.
    pub(crate) leader_epoch: u64,
    /// Authoritative committed offset from PG at leader-entry time.
    pub(crate) committed_offset: WalOffset,
}

pub(crate) struct FluidFollowerInit {
    pub(crate) fluid: Arc<ExchangeEngine>,
    pub(crate) wal_handle: WalWriterHandle,
    pub(crate) sync_watermarks: SyncWatermarks,
}

/// Initial watermarks recorded at snapshot load time for the sync task.
#[derive(Debug, Clone, Default)]
pub(crate) struct SyncWatermarks {
    pub(crate) account_state_offset: i64,
    pub(crate) account_ref_offset: i64,
    pub(crate) market_offset: i64,
    pub(crate) wal_projection_cursor: i64,
}

/// Error from `initialize_fluid` — distinguishes lock contention from hard failures.
pub(crate) enum FluidInitError {
    /// Another instance holds the leader lock — safe to run as follower.
    LockNotAcquired,
    /// Local WAL is behind committed offset; node must catch up as follower first.
    IneligibleForLeader {
        local_wal_tip: WalOffset,
        committed_offset: WalOffset,
    },
    /// Hard failure (connection error, snapshot load error, etc.) — should not silently degrade.
    Hard(anyhow::Error),
}

pub(crate) struct LeaderIdentity<'a> {
    pub(crate) node_id: &'a str,
    pub(crate) repl_addr: &'a str,
}

/// Attempt to acquire the Fluid leader lock. Returns `true` if acquired.
/// On success, the lock is session-scoped and held for the lifetime of `conn`.
async fn try_acquire_leader_lock(conn: &mut PgConnection) -> Result<bool> {
    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1, $2)")
        .bind(FLUID_LOCK_NAMESPACE)
        .bind(FLUID_LOCK_KEY)
        .fetch_one(&mut *conn)
        .await
        .context("fluid: try_advisory_lock")?;
    Ok(acquired)
}

/// Probe whether a Fluid leader currently holds the lock.
/// Opens a temporary connection, attempts the lock, releases if acquired.
/// Returns `true` if a leader is detected (lock NOT acquired).
pub(crate) async fn probe_fluid_leader(db_url: &str) -> Result<bool> {
    let mut conn = PgConnection::connect(db_url)
        .await
        .context("fluid probe: connect")?;
    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1, $2)")
        .bind(FLUID_LOCK_NAMESPACE)
        .bind(FLUID_LOCK_KEY)
        .fetch_one(&mut conn)
        .await
        .context("fluid probe: try_advisory_lock")?;
    if acquired {
        // No leader — release and close
        let _: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1, $2)")
            .bind(FLUID_LOCK_NAMESPACE)
            .bind(FLUID_LOCK_KEY)
            .fetch_one(&mut conn)
            .await
            .context("fluid probe: advisory_unlock")?;
    }
    Ok(!acquired)
}

/// Initialize the Fluid engine: acquire leader lock, load snapshot.
pub(crate) async fn initialize_fluid(
    db: &PgPool,
    db_url: &str,
    engine_version: &str,
    order_available_includes_withdrawal_reserves: bool,
    wal_config: WalConfig,
    leader_identity: LeaderIdentity<'_>,
    replication_tx: Option<Sender<Arc<Vec<WalRecordPayload>>>>,
) -> Result<FluidInit, FluidInitError> {
    // 1. Open dedicated PG connection (outside the pool)
    let mut conn = PgConnection::connect(db_url)
        .await
        .context("fluid: open dedicated connection")
        .map_err(FluidInitError::Hard)?;

    // 2. Acquire session-scoped advisory lock
    let acquired = try_acquire_leader_lock(&mut conn)
        .await
        .map_err(FluidInitError::Hard)?;
    if !acquired {
        return Err(FluidInitError::LockNotAcquired);
    }
    tracing::info!(
        namespace = FLUID_LOCK_NAMESPACE,
        key = FLUID_LOCK_KEY,
        "fluid: acquired leader lock"
    );

    let committed_offset = load_committed_offset(&mut conn)
        .await
        .map_err(FluidInitError::Hard)?;
    let local_tip = local_wal_tip(&wal_config.wal_dir)
        .map_err(|err| FluidInitError::Hard(anyhow::anyhow!("fluid: load local WAL tip: {err}")))?;
    if !startup_eligible_for_leader(local_tip, committed_offset) {
        if let Err(err) = release_leader_lock(&mut conn).await {
            tracing::warn!(error = ?err, "fluid: failed to release lock after eligibility check");
        }
        return Err(FluidInitError::IneligibleForLeader {
            local_wal_tip: local_tip,
            committed_offset,
        });
    }
    let leader_epoch = publish_leader_state(
        &mut conn,
        leader_identity.node_id,
        leader_identity.repl_addr,
    )
    .await
    .map_err(FluidInitError::Hard)?;

    // 3. Load snapshot in REPEATABLE READ and initialize replay cursor
    let (books, risk, sequences, market_meta, order_interner, account_interner, watermarks) =
        load_snapshot(db).await.map_err(FluidInitError::Hard)?;
    // Shadow state must remain aligned with the persisted DB cursor (pre-replay).
    // The projector starts from `wal_projection_cursor`, so bootstrapping shadow at WAL tip
    // would double-apply historical records.
    let shadow_state = FluidState::new(
        books.clone(),
        risk.clone(),
        sequences.clone(),
        market_meta.clone(),
        order_interner.clone(),
        account_interner.clone(),
        engine_version.to_string(),
        order_available_includes_withdrawal_reserves,
    );
    let after_offset = wal_projection_cursor_to_offset(watermarks.wal_projection_cursor)
        .map_err(FluidInitError::Hard)?;
    let (books, risk, sequences, market_meta, order_interner, account_interner) =
        apply_wal_replay_from_cursor(
            books,
            risk,
            sequences,
            market_meta,
            order_interner,
            account_interner,
            WalReplayFromCursor {
                engine_version,
                order_available_includes_withdrawal_reserves,
                wal_dir: wal_config.wal_dir.clone(),
                after_offset,
            },
        )
        .await
        .map_err(FluidInitError::Hard)?;

    let wal_handle = WalWriterHandle::spawn_with_replication(
        wal_config,
        engine_version.to_string(),
        replication_tx,
    )
    .map_err(|err| FluidInitError::Hard(anyhow::Error::msg(err.to_string())))?;
    let account_count = risk.account_count();
    let market_count = market_meta.len();
    let book_order_count: usize = books.values().map(|b| b.order_count()).sum();
    let risk_handle = RiskActorHandle::spawn(risk, RISK_ACTOR_CHANNEL_CAPACITY);
    let fluid_engine = ExchangeEngine::new(
        books,
        risk_handle,
        sequences,
        market_meta,
        order_interner,
        account_interner,
        engine_version.to_string(),
        order_available_includes_withdrawal_reserves,
    );

    tracing::info!(
        accounts = account_count,
        markets = market_count,
        resting_orders = book_order_count,
        "fluid: snapshot loaded"
    );

    Ok(FluidInit {
        fluid: Arc::new(fluid_engine),
        leader_conn: conn,
        wal_handle,
        shadow_state,
        sync_watermarks: watermarks,
        leader_epoch,
        committed_offset,
    })
}

async fn publish_leader_state(
    conn: &mut PgConnection,
    leader_node_id: &str,
    leader_repl_addr: &str,
) -> Result<u64> {
    if leader_node_id.trim().is_empty() {
        anyhow::bail!("fluid: leader node id must not be empty");
    }
    if leader_repl_addr.trim().is_empty() {
        anyhow::bail!("fluid: leader replication address must not be empty");
    }
    let leader_epoch: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO fluid_leader_state (
            singleton_key,
            leader_epoch,
            leader_node_id,
            leader_repl_addr,
            elected_at,
            last_heartbeat_at
        )
        VALUES (TRUE, 1, $1, $2, NOW(), NOW())
        ON CONFLICT (singleton_key) DO UPDATE SET
            leader_epoch = fluid_leader_state.leader_epoch + 1,
            leader_node_id = EXCLUDED.leader_node_id,
            leader_repl_addr = EXCLUDED.leader_repl_addr,
            elected_at = NOW(),
            last_heartbeat_at = NOW()
        RETURNING leader_epoch
        "#,
    )
    .bind(leader_node_id)
    .bind(leader_repl_addr)
    .fetch_one(conn)
    .await
    .context("fluid: publish leader state")?;
    u64::try_from(leader_epoch).context("fluid: leader_epoch must be non-negative")
}

async fn load_committed_offset(conn: &mut PgConnection) -> Result<WalOffset> {
    let committed_offset: Option<i64> = sqlx::query_scalar(
        "SELECT committed_offset FROM fluid_leader_state WHERE singleton_key = TRUE",
    )
    .fetch_optional(conn)
    .await
    .context("fluid: load committed offset")?;
    let raw = committed_offset.unwrap_or(0);
    if raw < 0 {
        anyhow::bail!("fluid: committed_offset must be non-negative, got {raw}");
    }
    let offset = u64::try_from(raw).context("fluid: committed_offset conversion to u64 failed")?;
    Ok(WalOffset(offset))
}

async fn release_leader_lock(conn: &mut PgConnection) -> Result<()> {
    let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1, $2)")
        .bind(FLUID_LOCK_NAMESPACE)
        .bind(FLUID_LOCK_KEY)
        .fetch_one(conn)
        .await
        .context("fluid: advisory_unlock")?;
    if !released {
        anyhow::bail!("fluid: advisory lock release returned false");
    }
    Ok(())
}

fn startup_eligible_for_leader(local_wal_tip: WalOffset, committed_offset: WalOffset) -> bool {
    local_wal_tip >= committed_offset
}

pub(crate) async fn load_leader_replication_addr(db: &PgPool) -> Result<String> {
    let leader_addr: Option<String> = sqlx::query_scalar(
        "SELECT leader_repl_addr FROM fluid_leader_state WHERE singleton_key = TRUE",
    )
    .fetch_optional(db)
    .await
    .context("fluid: load leader replication address")?;
    let leader_addr = leader_addr.unwrap_or_default();
    if leader_addr.trim().is_empty() {
        anyhow::bail!("fluid: leader replication address is not published yet");
    }
    Ok(leader_addr)
}

pub(crate) async fn initialize_fluid_follower(
    db: &PgPool,
    engine_version: &str,
    order_available_includes_withdrawal_reserves: bool,
    wal_config: WalConfig,
) -> Result<FluidFollowerInit> {
    let (books, risk, sequences, market_meta, order_interner, account_interner, watermarks) =
        load_snapshot(db).await?;

    let after_offset = wal_projection_cursor_to_offset(watermarks.wal_projection_cursor)?;
    let (books, risk, sequences, market_meta, order_interner, account_interner) =
        apply_wal_replay_from_cursor(
            books,
            risk,
            sequences,
            market_meta,
            order_interner,
            account_interner,
            WalReplayFromCursor {
                engine_version,
                order_available_includes_withdrawal_reserves,
                wal_dir: wal_config.wal_dir.clone(),
                after_offset,
            },
        )
        .await?;

    let wal_handle = WalWriterHandle::spawn_with_engine_version(wal_config, engine_version.into())
        .map_err(|err| anyhow::Error::msg(err.to_string()))?;
    let risk_handle = RiskActorHandle::spawn(risk, RISK_ACTOR_CHANNEL_CAPACITY);
    let fluid_engine = ExchangeEngine::new(
        books,
        risk_handle,
        sequences,
        market_meta,
        order_interner,
        account_interner,
        engine_version.to_string(),
        order_available_includes_withdrawal_reserves,
    );

    Ok(FluidFollowerInit {
        fluid: Arc::new(fluid_engine),
        wal_handle,
        sync_watermarks: watermarks,
    })
}

fn wal_projection_cursor_to_offset(cursor: i64) -> Result<WalOffset> {
    let offset =
        u64::try_from(cursor).context("fluid: wal_projection_cursor must be non-negative")?;
    Ok(WalOffset(offset))
}

struct WalReplayFromCursor<'a> {
    engine_version: &'a str,
    order_available_includes_withdrawal_reserves: bool,
    wal_dir: PathBuf,
    after_offset: WalOffset,
}

async fn apply_wal_replay_from_cursor(
    books: HashMap<MarketId, MarketBook>,
    risk: RiskEngine,
    sequences: HashMap<MarketId, SequenceState>,
    market_meta: HashMap<MarketId, MarketMeta>,
    order_interner: StringInterner<InternalOrderId>,
    account_interner: StringInterner<InternalAccountId>,
    config: WalReplayFromCursor<'_>,
) -> Result<(
    HashMap<MarketId, MarketBook>,
    RiskEngine,
    HashMap<MarketId, SequenceState>,
    HashMap<MarketId, MarketMeta>,
    StringInterner<InternalOrderId>,
    StringInterner<InternalAccountId>,
)> {
    let mut replay_state = FluidState::new(
        books,
        risk,
        sequences,
        market_meta,
        order_interner,
        account_interner,
        config.engine_version.to_string(),
        config.order_available_includes_withdrawal_reserves,
    );
    let mut reader = WalSegmentReader::open(&config.wal_dir, config.after_offset, true)?;
    let _ = replay_wal(
        &mut replay_state,
        &mut reader,
        config.after_offset,
        config.engine_version,
    )?;

    Ok((
        replay_state.books().clone(),
        replay_state.risk().clone(),
        replay_state.sequences().clone(),
        replay_state.market_meta().clone(),
        replay_state.order_interner().clone(),
        replay_state.account_interner().clone(),
    ))
}

// ── Snapshot Load ──

async fn load_snapshot(
    db: &PgPool,
) -> Result<(
    HashMap<MarketId, MarketBook>,
    RiskEngine,
    HashMap<MarketId, SequenceState>,
    HashMap<MarketId, MarketMeta>,
    StringInterner<InternalOrderId>,
    StringInterner<InternalAccountId>,
    SyncWatermarks,
)> {
    let mut tx = db.begin().await.context("fluid: begin snapshot tx")?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await
        .context("fluid: set repeatable read")?;

    let (market_meta_with_offsets, market_meta) = load_market_meta(&mut tx).await?;
    let (books, order_interner, account_interner) = load_books(&mut tx, &market_meta).await?;
    let (risk, max_account_state_offset, max_account_ref_offset) = load_risk_state(&mut tx).await?;
    let sequences = load_sequences(&mut tx).await?;

    let max_market_offset = market_meta_with_offsets
        .values()
        .copied()
        .max()
        .unwrap_or(0);
    let wal_projection_cursor = load_wal_projection_cursor(&mut tx).await?;

    tx.commit().await.context("fluid: commit snapshot tx")?;

    let watermarks = SyncWatermarks {
        account_state_offset: max_account_state_offset,
        account_ref_offset: max_account_ref_offset,
        market_offset: max_market_offset,
        wal_projection_cursor,
    };

    Ok((
        books,
        risk,
        sequences,
        market_meta,
        order_interner,
        account_interner,
        watermarks,
    ))
}

/// Load market metadata. Returns (offsets_by_market, meta_by_market) for separate use.
async fn load_market_meta(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(HashMap<MarketId, i64>, HashMap<MarketId, MarketMeta>)> {
    let rows: Vec<MarketMetaRow> = sqlx::query_as(
        r#"
        SELECT DISTINCT ON (market_id)
            market_id, status, outcomes,
            instrument_admin, instrument_id, max_price_ticks, last_offset
        FROM markets
        WHERE active = TRUE
        ORDER BY market_id, last_offset DESC
        "#,
    )
    .fetch_all(&mut **tx)
    .await
    .context("fluid: load market meta")?;

    let mut offsets = HashMap::new();
    let mut meta = HashMap::new();
    for row in rows {
        let mid = MarketId(row.market_id);
        let outcomes = row.outcomes.0;
        offsets.insert(mid.clone(), row.last_offset);
        meta.insert(
            mid,
            MarketMeta {
                status: row.status,
                binary_config: binary_config_for_outcomes(&outcomes, row.max_price_ticks),
                outcomes,
                instrument_admin: row.instrument_admin,
                instrument_id: row.instrument_id,
            },
        );
    }
    Ok((offsets, meta))
}

#[derive(sqlx::FromRow)]
struct MarketMetaRow {
    market_id: String,
    status: String,
    outcomes: sqlx::types::Json<Vec<String>>,
    instrument_admin: String,
    instrument_id: String,
    max_price_ticks: i64,
    last_offset: i64,
}

fn binary_config_for_outcomes(outcomes: &[String], max_price_ticks: i64) -> Option<BinaryConfig> {
    if outcomes.len() != 2 {
        return None;
    }
    Some(BinaryConfig {
        yes_outcome: outcomes[0].clone(),
        no_outcome: outcomes[1].clone(),
        max_price_ticks,
    })
}

/// Load all resting orders into per-market books.
/// Orders are loaded in market-wide submission order to preserve FIFO
/// when binary canonical levels merge YES/NO resting liquidity.
async fn load_books(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    market_meta: &HashMap<MarketId, MarketMeta>,
) -> Result<(
    HashMap<MarketId, MarketBook>,
    StringInterner<InternalOrderId>,
    StringInterner<InternalAccountId>,
)> {
    let rows: Vec<BookOrderRow> = sqlx::query_as(
        r#"
        SELECT
            order_id, market_id, account_id, outcome, side,
            price_ticks, remaining_minor, locked_minor,
            (EXTRACT(EPOCH FROM submitted_at) * 1000000)::BIGINT AS submitted_at_micros
        FROM orders
        WHERE status IN ('Open', 'PartiallyFilled')
          AND remaining_minor > 0
        ORDER BY market_id, submitted_at, order_id
        "#,
    )
    .fetch_all(&mut **tx)
    .await
    .context("fluid: load resting orders")?;

    let mut books: HashMap<MarketId, MarketBook> = HashMap::new();
    for (market_id, meta) in market_meta {
        let book = if let Some(binary_config) = &meta.binary_config {
            MarketBook::new_binary(binary_config.clone())
        } else {
            MarketBook::new_multi_outcome()
        };
        books.insert(market_id.clone(), book);
    }
    let mut order_interner = StringInterner::<InternalOrderId>::default();
    let mut account_interner = StringInterner::<InternalAccountId>::default();
    for row in rows {
        let side = match row.side.as_str() {
            "Buy" => OrderSide::Buy,
            "Sell" => OrderSide::Sell,
            other => anyhow::bail!("fluid: unexpected order side: {other}"),
        };
        let market_id = MarketId(row.market_id);
        let Some(book) = books.get_mut(&market_id) else {
            anyhow::bail!(
                "fluid: found open order for market '{}' missing from active market metadata",
                market_id.0
            );
        };
        book.insert(RestingEntry {
            order_id: order_interner.intern(&row.order_id),
            account_id: account_interner.intern(&row.account_id),
            outcome: row.outcome,
            side,
            price_ticks: row.price_ticks,
            remaining_minor: row.remaining_minor,
            locked_minor: row.locked_minor,
            submitted_at_micros: row.submitted_at_micros,
        });
    }
    Ok((books, order_interner, account_interner))
}

#[derive(sqlx::FromRow)]
struct BookOrderRow {
    order_id: String,
    market_id: String,
    account_id: String,
    outcome: String,
    side: String,
    price_ticks: i64,
    remaining_minor: i64,
    locked_minor: i64,
    submitted_at_micros: i64,
}

/// Load account risk state for all active accounts.
/// Returns (RiskEngine, max_account_state_offset, max_account_ref_offset).
async fn load_risk_state(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(RiskEngine, i64, i64)> {
    let rows: Vec<RiskAccountRow> = sqlx::query_as(
        r#"
        SELECT
            arl.account_id,
            ar.status AS account_status,
            ar.active AS account_active,
            ar.instrument_admin,
            ar.instrument_id,
            ast.cleared_cash_minor,
            COALESCE(rs.delta_pending_trades_minor, 0) AS delta_pending_trades_minor,
            COALESCE(rs.locked_open_orders_minor, 0) AS locked_open_orders_minor,
            COALESCE(ls.pending_withdrawals_reserved_minor, 0) AS pending_withdrawals_reserved_minor,
            an.last_nonce,
            asl.last_offset AS state_offset,
            arl.last_offset AS ref_offset
        FROM account_ref_latest arl
        JOIN account_refs ar ON ar.contract_id = arl.contract_id
        JOIN account_state_latest asl ON asl.account_id = arl.account_id
        JOIN account_states ast ON ast.contract_id = asl.contract_id
        LEFT JOIN account_risk_state rs ON rs.account_id = arl.account_id
        LEFT JOIN account_liquidity_state ls ON ls.account_id = arl.account_id
        LEFT JOIN account_nonces an ON an.account_id = arl.account_id
        WHERE ar.active = TRUE AND ast.active = TRUE
        "#,
    )
    .fetch_all(&mut **tx)
    .await
    .context("fluid: load risk state")?;

    let mut risk = RiskEngine::default();
    let mut max_state_offset: i64 = 0;
    let mut max_ref_offset: i64 = 0;

    for row in &rows {
        risk.upsert_account(
            &row.account_id,
            AccountState {
                cleared_cash_minor: row.cleared_cash_minor,
                delta_pending_trades_minor: row.delta_pending_trades_minor,
                locked_open_orders_minor: row.locked_open_orders_minor,
                pending_withdrawals_reserved_minor: row.pending_withdrawals_reserved_minor,
                last_nonce: row.last_nonce,
                status: row.account_status.clone(),
                active: row.account_active,
                pending_nonce: None,
                pending_lock_minor: 0,
                instrument_admin: row.instrument_admin.clone(),
                instrument_id: row.instrument_id.clone(),
            },
        );
        if row.state_offset > max_state_offset {
            max_state_offset = row.state_offset;
        }
        if row.ref_offset > max_ref_offset {
            max_ref_offset = row.ref_offset;
        }
    }

    Ok((risk, max_state_offset, max_ref_offset))
}

#[derive(sqlx::FromRow)]
struct RiskAccountRow {
    account_id: String,
    account_status: String,
    account_active: bool,
    instrument_admin: String,
    instrument_id: String,
    cleared_cash_minor: i64,
    delta_pending_trades_minor: i64,
    locked_open_orders_minor: i64,
    pending_withdrawals_reserved_minor: i64,
    last_nonce: Option<i64>,
    state_offset: i64,
    ref_offset: i64,
}

/// Load per-market sequence counters.
async fn load_sequences(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<HashMap<MarketId, SequenceState>> {
    let rows: Vec<(String, i64, i64)> = sqlx::query_as(
        r#"
        SELECT market_id, next_fill_sequence, next_event_sequence
        FROM market_sequences
        "#,
    )
    .fetch_all(&mut **tx)
    .await
    .context("fluid: load market sequences")?;

    let mut sequences = HashMap::new();
    for (market_id, next_fill, next_event) in rows {
        sequences.insert(
            MarketId(market_id),
            SequenceState {
                next_fill_sequence: next_fill,
                next_event_sequence: next_event,
            },
        );
    }
    Ok(sequences)
}

// ── Full Resync ──

/// Reload all in-memory state from a fresh DB snapshot.
/// Called by the sync task when the engine is unhealthy.
async fn run_resync_impl(
    db: &PgPool,
    fluid: &Arc<ExchangeEngine>,
    wal_handle: &WalWriterHandle,
    wal_dir: &Path,
) -> Result<Option<SyncWatermarks>> {
    let Some(guard) = fluid.begin_resync().await else {
        return Ok(None);
    };
    wal_handle.flush().await?;

    let (books, risk, sequences, market_meta, order_interner, account_interner, watermarks) =
        load_snapshot(db).await?;
    let (books, risk, sequences, market_meta, order_interner, account_interner) =
        apply_wal_replay_from_cursor(
            books,
            risk,
            sequences,
            market_meta,
            order_interner,
            account_interner,
            WalReplayFromCursor {
                engine_version: fluid.engine_version(),
                order_available_includes_withdrawal_reserves: fluid
                    .order_available_includes_withdrawal_reserves(),
                wal_dir: wal_dir.to_path_buf(),
                after_offset: wal_projection_cursor_to_offset(watermarks.wal_projection_cursor)?,
            },
        )
        .await?;

    fluid
        .complete_resync(
            guard,
            books,
            risk,
            sequences,
            market_meta,
            order_interner,
            account_interner,
        )
        .await?;

    tracing::info!("fluid: resync completed");
    Ok(Some(watermarks))
}

// ── State Sync Background Task ──

pub(crate) fn spawn_state_sync_task(
    db: PgPool,
    fluid: Arc<ExchangeEngine>,
    initial_watermarks: SyncWatermarks,
    wal_handle: WalWriterHandle,
    wal_dir: PathBuf,
) {
    tokio::spawn(async move {
        let mut watermarks = initial_watermarks;
        loop {
            tokio::time::sleep(STATE_SYNC_INTERVAL).await;
            if let Err(err) =
                run_sync_poll(&db, &fluid, &wal_handle, wal_dir.as_path(), &mut watermarks).await
            {
                tracing::error!(error = ?err, "fluid sync poll failed");
            }
        }
    });
}

async fn run_sync_poll(
    db: &PgPool,
    fluid: &Arc<ExchangeEngine>,
    wal_handle: &WalWriterHandle,
    wal_dir: &Path,
    watermarks: &mut SyncWatermarks,
) -> Result<()> {
    // Check health first — if unhealthy, do full resync instead of incremental.
    if !fluid.is_healthy() {
        tracing::warn!("fluid sync: engine unhealthy, triggering full resync");
        if let Some(new_watermarks) = run_resync_impl(db, fluid, wal_handle, wal_dir).await? {
            *watermarks = new_watermarks;
        }
        return Ok(());
    }

    // 1. Poll cleared cash changes (account_state_latest offset)
    let cash_rows: Vec<(String, i64, i64)> = sqlx::query_as(
        r#"
        SELECT asl.account_id, ast.cleared_cash_minor, asl.last_offset
        FROM account_state_latest asl
        JOIN account_states ast ON ast.contract_id = asl.contract_id
        WHERE asl.last_offset > $1
        "#,
    )
    .bind(watermarks.account_state_offset)
    .fetch_all(db)
    .await
    .context("fluid sync: poll cleared cash")?;

    if !cash_rows.is_empty() {
        for (account_id, cleared_cash, offset) in &cash_rows {
            fluid
                .risk_handle()
                .sync_cleared_cash(account_id, *cleared_cash)
                .await?;
            if *offset > watermarks.account_state_offset {
                watermarks.account_state_offset = *offset;
            }
        }
    }

    // 2. Poll account status changes (account_ref_latest offset)
    let status_rows: Vec<(String, String, bool, i64)> = sqlx::query_as(
        r#"
        SELECT arl.account_id, ar.status, ar.active, arl.last_offset
        FROM account_ref_latest arl
        JOIN account_refs ar ON ar.contract_id = arl.contract_id
        WHERE arl.last_offset > $1
        "#,
    )
    .bind(watermarks.account_ref_offset)
    .fetch_all(db)
    .await
    .context("fluid sync: poll account status")?;

    if !status_rows.is_empty() {
        for (account_id, status, active, offset) in &status_rows {
            fluid
                .risk_handle()
                .sync_account_status(account_id, status, *active)
                .await?;
            if *offset > watermarks.account_ref_offset {
                watermarks.account_ref_offset = *offset;
            }
        }
    }

    // 3. Poll withdrawal reserves (full table scan — no global watermark)
    let reserve_rows: Vec<(String, i64)> = sqlx::query_as(
        r#"
        SELECT account_id, pending_withdrawals_reserved_minor
        FROM account_liquidity_state
        "#,
    )
    .fetch_all(db)
    .await
    .context("fluid sync: poll withdrawal reserves")?;

    if !reserve_rows.is_empty() {
        for (account_id, reserves) in &reserve_rows {
            fluid
                .risk_handle()
                .sync_withdrawal_reserves(account_id, *reserves)
                .await?;
        }
    }

    // 4. Poll market status changes (markets offset)
    let market_rows: Vec<(String, String, i64)> = sqlx::query_as(
        r#"
        SELECT DISTINCT ON (market_id) market_id, status, last_offset
        FROM markets
        WHERE last_offset > $1
          AND active = TRUE
        ORDER BY market_id, last_offset DESC
        "#,
    )
    .bind(watermarks.market_offset)
    .fetch_all(db)
    .await
    .context("fluid sync: poll market status")?;

    if !market_rows.is_empty() {
        for (market_id, status, offset) in &market_rows {
            fluid
                .update_market_status(&MarketId(market_id.clone()), status.clone())
                .await;
            if *offset > watermarks.market_offset {
                watermarks.market_offset = *offset;
            }
        }
    }

    Ok(())
}

#[derive(Clone)]
pub(crate) struct LeaderReplicationConfig {
    pub(crate) bind_addr: String,
    pub(crate) engine_version: String,
    pub(crate) auth_token: String,
    pub(crate) heartbeat_interval: Duration,
    pub(crate) leader_epoch: Arc<AtomicU64>,
    pub(crate) committed_offset: Arc<AtomicU64>,
}

pub(crate) fn spawn_replication_fanout_task(
    replication_rx: Receiver<Arc<Vec<WalRecordPayload>>>,
    broadcaster: ReplicationBroadcaster,
    committed_offset: Arc<AtomicU64>,
    advance_committed_with_tip: bool,
) {
    std::thread::spawn(move || {
        while let Ok(batch) = replication_rx.recv() {
            match broadcaster.broadcast_batch(batch) {
                Ok(outcome) => {
                    if advance_committed_with_tip {
                        committed_offset.store(outcome.tip_offset.0, Ordering::Relaxed);
                    }
                }
                Err(err) => {
                    tracing::warn!(error = ?err, "fluid replication: fanout failed");
                }
            }
        }
    });
}

pub(crate) fn spawn_leader_replication_listener(
    wal_dir: PathBuf,
    broadcaster: ReplicationBroadcaster,
    config: LeaderReplicationConfig,
) {
    tokio::spawn(async move {
        let listener = match TcpListener::bind(&config.bind_addr).await {
            Ok(listener) => listener,
            Err(err) => {
                tracing::error!(
                    error = ?err,
                    bind_addr = %config.bind_addr,
                    "fluid replication: failed to bind listener"
                );
                return;
            }
        };

        tracing::info!(
            bind_addr = %config.bind_addr,
            "fluid replication: leader listener started"
        );

        loop {
            let accepted = listener.accept().await;
            let (stream, peer_addr) = match accepted {
                Ok(accepted) => accepted,
                Err(err) => {
                    tracing::warn!(error = ?err, "fluid replication: accept failed");
                    continue;
                }
            };
            let wal_dir = wal_dir.clone();
            let broadcaster = broadcaster.clone();
            let connection_config = config.clone();
            tokio::spawn(async move {
                if let Err(err) =
                    handle_replication_connection(stream, &wal_dir, &broadcaster, connection_config)
                        .await
                {
                    tracing::warn!(
                        error = ?err,
                        peer = %peer_addr,
                        "fluid replication: connection ended"
                    );
                }
            });
        }
    });
}

async fn handle_replication_connection(
    mut stream: TcpStream,
    wal_dir: &Path,
    broadcaster: &ReplicationBroadcaster,
    config: LeaderReplicationConfig,
) -> Result<()> {
    let handshake: ReplicationHandshake = read_framed(&mut stream).await?;
    let leader_tip = broadcaster.tip_offset()?;
    let leader_crc = wal_command_crc32c(wal_dir, handshake.after_offset)?;
    let mut status = validate_handshake(
        &handshake,
        &config.engine_version,
        config.auth_token.as_bytes(),
        leader_tip,
        leader_crc,
    );
    let registration = if matches!(status, HandshakeStatus::Ok) {
        match broadcaster.register_follower(handshake.node_id.clone()) {
            Ok(registration) => Some(registration),
            Err(_) => {
                status = HandshakeStatus::AuthFailed;
                None
            }
        }
    } else {
        None
    };
    let response = HandshakeResponse {
        magic: REPLICATION_MAGIC,
        protocol_version: REPLICATION_PROTOCOL_VERSION,
        leader_epoch: config.leader_epoch.load(Ordering::Relaxed),
        status: status.clone(),
    };
    write_framed(&mut stream, &response).await?;
    if !matches!(status, HandshakeStatus::Ok) {
        return Ok(());
    }

    let Some(mut registration) = registration else {
        return Ok(());
    };
    let node_id = handshake.node_id.clone();
    let catchup =
        read_payloads_between_offsets(wal_dir, handshake.after_offset, registration.captured_tip)?;

    let (read_half, mut write_half) = stream.into_split();
    let ack_broadcaster = broadcaster.clone();
    let ack_node_id = node_id.clone();
    let ack_task = tokio::spawn(async move {
        run_replication_ack_reader(read_half, ack_broadcaster, ack_node_id).await
    });

    for chunk in catchup.chunks(256) {
        let records = chunk.to_vec();
        let message = ReplicationMessage::WalBatch {
            leader_epoch: config.leader_epoch.load(Ordering::Relaxed),
            records,
        };
        write_framed(&mut write_half, &message).await?;
    }

    let mut heartbeat = tokio::time::interval(config.heartbeat_interval);
    loop {
        tokio::select! {
            maybe_batch = registration.rx.recv() => {
                let Some(batch) = maybe_batch else {
                    break;
                };
                let message = ReplicationMessage::WalBatch {
                    leader_epoch: config.leader_epoch.load(Ordering::Relaxed),
                    records: (*batch).clone(),
                };
                write_framed(&mut write_half, &message).await?;
            }
            _ = heartbeat.tick() => {
                let tip_offset = broadcaster.tip_offset()?;
                let committed_offset = WalOffset(config.committed_offset.load(Ordering::Relaxed));
                let message = ReplicationMessage::Heartbeat {
                    leader_epoch: config.leader_epoch.load(Ordering::Relaxed),
                    tip_offset,
                    committed_offset,
                };
                write_framed(&mut write_half, &message).await?;
            }
        }
    }

    ack_task.abort();
    broadcaster.unregister_follower(&node_id)?;
    Ok(())
}

async fn run_replication_ack_reader(
    mut read_half: OwnedReadHalf,
    broadcaster: ReplicationBroadcaster,
    node_id: String,
) -> Result<(), FluidError> {
    loop {
        let ack: ReplicationAck = read_framed(&mut read_half).await?;
        match ack {
            ReplicationAck::DurableApplied { up_to } => {
                broadcaster.update_durable_applied(&node_id, up_to)?;
            }
        }
    }
}

pub(crate) struct FollowerReplicationConfig {
    pub(crate) node_id: String,
    pub(crate) engine_version: String,
    pub(crate) auth_token: String,
    pub(crate) heartbeat_timeout: Duration,
}

pub(crate) struct FollowerReplicationRuntime {
    pub(crate) db: PgPool,
    pub(crate) fluid: Arc<ExchangeEngine>,
    pub(crate) wal_dir: PathBuf,
    pub(crate) wal_handle: WalWriterHandle,
    pub(crate) config: FollowerReplicationConfig,
    pub(crate) promotion: FollowerPromotionConfig,
    pub(crate) leader_epoch: Arc<AtomicU64>,
    pub(crate) committed_offset: Arc<AtomicU64>,
}

#[derive(Clone)]
pub(crate) struct FollowerPromotionConfig {
    pub(crate) allow_promotion: bool,
    pub(crate) db_url: String,
    pub(crate) node_id: String,
    pub(crate) advertise_addr: String,
    pub(crate) leader_bind_addr: String,
    pub(crate) engine_version: String,
    pub(crate) auth_token: String,
    pub(crate) heartbeat_interval: Duration,
    pub(crate) write_mode: Arc<AtomicU8>,
    pub(crate) leader_connection_healthy: Arc<AtomicBool>,
    pub(crate) advance_committed_with_tip: bool,
    pub(crate) publish_committed_in_watchdog: bool,
    pub(crate) failure_policy: crate::PostFsyncFailurePolicy,
    pub(crate) pg_unreachable_demote_after: Duration,
    pub(crate) pg_publish_unreachable_since: Arc<std::sync::Mutex<Option<Instant>>>,
}

#[derive(Clone)]
pub(crate) struct LeaderWatchdogConfig {
    pub(crate) leader_connection_healthy: Arc<AtomicBool>,
    pub(crate) leader_epoch: Arc<AtomicU64>,
    pub(crate) committed_offset: Arc<AtomicU64>,
    pub(crate) leader_node_id: String,
    pub(crate) db_url: String,
    pub(crate) fluid: Arc<ExchangeEngine>,
    pub(crate) publish_committed_in_watchdog: bool,
    pub(crate) failure_policy: crate::PostFsyncFailurePolicy,
    pub(crate) pg_unreachable_demote_after: Duration,
    pub(crate) pg_publish_unreachable_since: Arc<std::sync::Mutex<Option<Instant>>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FollowerSessionOutcome {
    Reconnect,
    HeartbeatTimeout,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PromotionAttemptOutcome {
    Promoted,
    LockHeld,
    Ineligible {
        local_wal_tip: WalOffset,
        committed_offset: WalOffset,
    },
}

pub(crate) fn spawn_follower_replication_task(runtime: FollowerReplicationRuntime) {
    let FollowerReplicationRuntime {
        db,
        fluid,
        wal_dir,
        wal_handle,
        config,
        promotion,
        leader_epoch,
        committed_offset,
    } = runtime;
    tokio::spawn(async move {
        loop {
            let leader_addr = match load_leader_replication_addr(&db).await {
                Ok(addr) => addr,
                Err(err) => {
                    tracing::warn!(error = ?err, "fluid follower: leader address unavailable");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
            };

            let session_result = run_follower_replication_session(
                &leader_addr,
                fluid.clone(),
                wal_dir.as_path(),
                &wal_handle,
                &config,
                leader_epoch.clone(),
                committed_offset.clone(),
            )
            .await;
            let session_outcome = match session_result {
                Ok(outcome) => outcome,
                Err(err) => {
                    tracing::warn!(
                        error = ?err,
                        leader_addr = %leader_addr,
                        "fluid follower: replication session ended"
                    );
                    FollowerSessionOutcome::Reconnect
                }
            };

            if should_attempt_promotion(session_outcome, promotion.allow_promotion) {
                match try_promote_to_leader(
                    &fluid,
                    wal_dir.as_path(),
                    &wal_handle,
                    &promotion,
                    leader_epoch.clone(),
                    committed_offset.clone(),
                )
                .await
                {
                    Ok(PromotionAttemptOutcome::Promoted) => {
                        tracing::info!("fluid follower: promoted to leader");
                        return;
                    }
                    Ok(PromotionAttemptOutcome::LockHeld) => {
                        tracing::debug!("fluid follower: promotion skipped, lock held");
                    }
                    Ok(PromotionAttemptOutcome::Ineligible {
                        local_wal_tip,
                        committed_offset: required,
                    }) => {
                        tracing::warn!(
                            local_wal_tip = local_wal_tip.0,
                            committed_offset = required.0,
                            "fluid follower: promotion blocked by eligibility gate"
                        );
                    }
                    Err(err) => {
                        tracing::warn!(error = ?err, "fluid follower: promotion attempt failed");
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
}

async fn run_follower_replication_session(
    leader_addr: &str,
    fluid: Arc<ExchangeEngine>,
    wal_dir: &Path,
    wal_handle: &WalWriterHandle,
    config: &FollowerReplicationConfig,
    leader_epoch: Arc<AtomicU64>,
    committed_offset: Arc<AtomicU64>,
) -> Result<FollowerSessionOutcome> {
    let local_tip = local_wal_tip(wal_dir)?;
    let local_crc32c = wal_command_crc32c(wal_dir, local_tip)?.unwrap_or(0);
    let mut stream = TcpStream::connect(leader_addr)
        .await
        .with_context(|| format!("fluid follower: connect to leader {leader_addr}"))?;

    let handshake = ReplicationHandshake {
        magic: REPLICATION_MAGIC,
        protocol_version: REPLICATION_PROTOCOL_VERSION,
        node_id: config.node_id.clone(),
        engine_version: config.engine_version.clone(),
        after_offset: local_tip,
        after_offset_crc32c: local_crc32c,
        auth_token: config.auth_token.as_bytes().to_vec(),
    };
    write_framed(&mut stream, &handshake).await?;
    let response: HandshakeResponse = read_framed(&mut stream).await?;
    let _ = promote_known_leader_epoch(
        &leader_epoch,
        response.leader_epoch,
        "replication handshake",
    )?;

    match response.status {
        HandshakeStatus::Ok => {}
        HandshakeStatus::OffsetNotAvailable => {
            reset_local_wal_files(wal_dir)?;
            fluid.mark_unhealthy("follower offset not available, local WAL reset");
            return Ok(FollowerSessionOutcome::Reconnect);
        }
        HandshakeStatus::DivergentLog { truncate_to } => {
            reset_local_wal_files(wal_dir)?;
            fluid.mark_unhealthy(&format!(
                "follower divergent WAL detected at offset {truncate_to}, local WAL reset"
            ));
            return Ok(FollowerSessionOutcome::Reconnect);
        }
        HandshakeStatus::VersionMismatch => {
            anyhow::bail!("fluid follower: replication handshake version mismatch");
        }
        HandshakeStatus::AuthFailed => {
            anyhow::bail!("fluid follower: replication handshake auth failed");
        }
    }

    let mut follower_writer = FollowerWalWriter::new(wal_handle.clone(), wal_dir.to_path_buf())?;

    loop {
        let message: ReplicationMessage =
            match tokio::time::timeout(config.heartbeat_timeout, read_framed(&mut stream)).await {
                Ok(read) => read?,
                Err(_) => return Ok(FollowerSessionOutcome::HeartbeatTimeout),
            };
        if let Some(incoming_epoch) = replication_message_epoch(&message) {
            let _ =
                promote_known_leader_epoch(&leader_epoch, incoming_epoch, "replication stream")?;
        }
        match message {
            ReplicationMessage::WalBatch { records, .. } => {
                for payload in records {
                    let write_result = follower_writer.write_payload(&payload).await?;
                    let durable_applied = match &write_result {
                        pebble_fluid::replication::FollowerWriteResult::Appended { offset }
                        | pebble_fluid::replication::FollowerWriteResult::Duplicate { offset } => {
                            *offset
                        }
                    };
                    write_framed(
                        &mut stream,
                        &ReplicationAck::DurableApplied {
                            up_to: durable_applied,
                        },
                    )
                    .await?;
                    if matches!(
                        write_result,
                        pebble_fluid::replication::FollowerWriteResult::Appended { .. }
                    ) {
                        let command: WalCommand =
                            from_slice(&payload.command_bytes).map_err(|err| {
                                anyhow::anyhow!(
                                    "fluid follower: decode replicated command at offset {}: {err}",
                                    payload.offset
                                )
                            })?;
                        if let Err(err) = apply_replicated_command(&fluid, &command).await {
                            fluid.mark_unhealthy(&format!(
                                "follower apply failed at offset {}: {err}",
                                payload.offset
                            ));
                            return Err(err);
                        }
                    }
                }
            }
            ReplicationMessage::Heartbeat {
                committed_offset: heartbeat_committed,
                ..
            } => {
                committed_offset.store(heartbeat_committed.0, Ordering::Relaxed);
                write_framed(
                    &mut stream,
                    &ReplicationAck::DurableApplied {
                        up_to: follower_writer.durable_applied(),
                    },
                )
                .await?;
            }
            ReplicationMessage::Shutdown { .. } => return Ok(FollowerSessionOutcome::Reconnect),
        }
    }
}

fn should_attempt_promotion(
    session_outcome: FollowerSessionOutcome,
    allow_promotion: bool,
) -> bool {
    allow_promotion
        && matches!(
            session_outcome,
            FollowerSessionOutcome::HeartbeatTimeout | FollowerSessionOutcome::Reconnect
        )
}

async fn try_promote_to_leader(
    fluid: &Arc<ExchangeEngine>,
    wal_dir: &Path,
    wal_handle: &WalWriterHandle,
    promotion: &FollowerPromotionConfig,
    leader_epoch: Arc<AtomicU64>,
    committed_offset: Arc<AtomicU64>,
) -> Result<PromotionAttemptOutcome> {
    let mut leader_conn = PgConnection::connect(&promotion.db_url)
        .await
        .context("fluid promotion: connect leader session")?;
    if !try_acquire_leader_lock(&mut leader_conn).await? {
        return Ok(PromotionAttemptOutcome::LockHeld);
    }

    let prev_committed = load_committed_offset(&mut leader_conn)
        .await
        .context("fluid promotion: load committed offset")?;
    let local_tip = local_wal_tip(wal_dir)?;
    if !startup_eligible_for_leader(local_tip, prev_committed) {
        if let Err(err) = release_leader_lock(&mut leader_conn).await {
            tracing::warn!(
                error = ?err,
                "fluid promotion: failed to release ineligible leader lock"
            );
        }
        return Ok(PromotionAttemptOutcome::Ineligible {
            local_wal_tip: local_tip,
            committed_offset: prev_committed,
        });
    }

    let epoch = publish_leader_state(
        &mut leader_conn,
        &promotion.node_id,
        &promotion.advertise_addr,
    )
    .await
    .context("fluid promotion: publish leader state")?;
    leader_epoch.store(epoch, Ordering::Relaxed);
    committed_offset.store(prev_committed.0, Ordering::Relaxed);
    promotion
        .leader_connection_healthy
        .store(true, Ordering::Relaxed);

    let broadcaster = ReplicationBroadcaster::with_default_capacity(local_tip)
        .map_err(|err| anyhow::anyhow!("fluid promotion: broadcaster init failed: {err}"))?;
    let (replication_tx, replication_rx) = std::sync::mpsc::channel::<Arc<Vec<WalRecordPayload>>>();
    wal_handle
        .set_replication_tx(Some(replication_tx))
        .map_err(|err| anyhow::anyhow!("fluid promotion: attach wal replication failed: {err}"))?;
    spawn_replication_fanout_task(
        replication_rx,
        broadcaster.clone(),
        committed_offset.clone(),
        promotion.advance_committed_with_tip,
    );
    spawn_leader_replication_listener(
        wal_dir.to_path_buf(),
        broadcaster,
        LeaderReplicationConfig {
            bind_addr: promotion.leader_bind_addr.clone(),
            engine_version: promotion.engine_version.clone(),
            auth_token: promotion.auth_token.clone(),
            heartbeat_interval: promotion.heartbeat_interval,
            leader_epoch: leader_epoch.clone(),
            committed_offset: committed_offset.clone(),
        },
    );
    spawn_leader_watchdog(
        leader_conn,
        LeaderWatchdogConfig {
            leader_connection_healthy: promotion.leader_connection_healthy.clone(),
            leader_epoch,
            committed_offset,
            leader_node_id: promotion.node_id.clone(),
            db_url: promotion.db_url.clone(),
            fluid: fluid.clone(),
            publish_committed_in_watchdog: promotion.publish_committed_in_watchdog,
            failure_policy: promotion.failure_policy,
            pg_unreachable_demote_after: promotion.pg_unreachable_demote_after,
            pg_publish_unreachable_since: promotion.pg_publish_unreachable_since.clone(),
        },
    );

    store_write_mode(&promotion.write_mode, WriteMode::FluidLeader);
    Ok(PromotionAttemptOutcome::Promoted)
}

fn replication_message_epoch(message: &ReplicationMessage) -> Option<u64> {
    match message {
        ReplicationMessage::WalBatch { leader_epoch, .. }
        | ReplicationMessage::Heartbeat { leader_epoch, .. } => Some(*leader_epoch),
        ReplicationMessage::Shutdown { .. } => None,
    }
}

fn promote_known_leader_epoch(
    epoch_state: &Arc<AtomicU64>,
    incoming_epoch: u64,
    context: &str,
) -> Result<u64> {
    let known = epoch_state.load(Ordering::Relaxed);
    let updated = validate_leader_epoch(known, incoming_epoch, context)
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;
    epoch_state.store(updated, Ordering::Relaxed);
    Ok(updated)
}

fn reset_local_wal_files(wal_dir: &Path) -> Result<()> {
    if !wal_dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(wal_dir).with_context(|| {
        format!(
            "fluid follower: list WAL directory for reset: {}",
            wal_dir.display()
        )
    })? {
        let entry = entry.context("fluid follower: read WAL directory entry for reset")?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let file_name = match path.file_name().and_then(|name| name.to_str()) {
            Some(name) => name,
            None => continue,
        };
        if !file_name.starts_with("segment-") || !file_name.ends_with(".wal") {
            continue;
        }
        fs::remove_file(&path).with_context(|| {
            format!(
                "fluid follower: remove WAL segment during reset: {}",
                path.display()
            )
        })?;
    }
    Ok(())
}

async fn apply_replicated_command(fluid: &Arc<ExchangeEngine>, command: &WalCommand) -> Result<()> {
    match command {
        WalCommand::PlaceOrder(incoming) => apply_replicated_place(fluid, incoming).await,
        WalCommand::CancelOrder {
            order_id,
            market_id,
            account_id,
        } => apply_replicated_cancel(fluid, market_id, order_id, account_id).await,
    }
}

async fn apply_replicated_place(
    fluid: &Arc<ExchangeEngine>,
    incoming: &pebble_trading::IncomingOrder,
) -> Result<()> {
    let market = fluid
        .get_market(&incoming.market_id)
        .await
        .ok_or_else(|| anyhow::anyhow!("replication apply: market not found"))?;
    let mut market_guard = market.lock().await;

    let account = fluid
        .risk_handle()
        .get_account(&incoming.account_id)
        .await
        .map_err(|err| anyhow::Error::msg(err.to_string()))?;
    let required_lock = match incoming.order_type {
        pebble_trading::OrderType::Limit => replicated_lock_required_minor(
            incoming.side,
            incoming.price_ticks,
            incoming.quantity_minor,
        )?,
        pebble_trading::OrderType::Market => 0,
    };

    let reserve = fluid
        .risk_handle()
        .reserve(
            &incoming.account_id,
            required_lock,
            incoming.nonce.0,
            &account.instrument_admin,
            &account.instrument_id,
            fluid.order_available_includes_withdrawal_reserves(),
        )
        .await
        .map_err(|err| anyhow::Error::msg(err.to_string()))?;

    let order_interner_read = fluid.order_interner().read().await;
    let account_interner_read = fluid.account_interner().read().await;
    let transition = market_guard
        .compute_place_order(
            incoming,
            reserve.available_before,
            fluid.engine_version(),
            &order_interner_read,
            &account_interner_read,
        )
        .map_err(|err| anyhow::Error::msg(err.to_string()));
    drop(account_interner_read);
    drop(order_interner_read);
    let transition = match transition {
        Ok(transition) => transition,
        Err(err) => {
            let _ = fluid
                .risk_handle()
                .abort(&incoming.account_id, required_lock)
                .await;
            return Err(err);
        }
    };

    fluid
        .risk_handle()
        .commit(&incoming.account_id, transition.risk_transition.clone())
        .await
        .map_err(|err| anyhow::Error::msg(err.to_string()))?;

    let mut order_interner = fluid.order_interner().write().await;
    let mut account_interner = fluid.account_interner().write().await;
    market_guard
        .apply_place_order(&transition, &mut order_interner, &mut account_interner)
        .map_err(|err| anyhow::Error::msg(err.to_string()))?;
    drop(account_interner);
    drop(order_interner);
    fluid
        .apply_place_index_updates(&incoming.market_id, &transition)
        .await;
    Ok(())
}

async fn apply_replicated_cancel(
    fluid: &Arc<ExchangeEngine>,
    market_id: &MarketId,
    order_id: &pebble_trading::OrderId,
    account_id: &str,
) -> Result<()> {
    let market = fluid
        .get_market(market_id)
        .await
        .ok_or_else(|| anyhow::anyhow!("replication apply: market not found"))?;
    let mut market_guard = market.lock().await;

    let order_interner_read = fluid.order_interner().read().await;
    let account_interner_read = fluid.account_interner().read().await;
    let transition = market_guard
        .compute_cancel_order(
            market_id,
            order_id,
            account_id,
            &order_interner_read,
            &account_interner_read,
        )
        .map_err(|err| anyhow::Error::msg(err.to_string()))?;
    drop(account_interner_read);
    market_guard
        .apply_cancel_order(&transition, &order_interner_read)
        .map_err(|err| anyhow::Error::msg(err.to_string()))?;
    drop(order_interner_read);

    fluid.apply_cancel_index_update(order_id).await;
    if transition.released_locked_minor != 0 {
        let risk_transition = pebble_fluid::transition::RiskTransition {
            deltas: vec![pebble_fluid::transition::AccountRiskDelta {
                account_id: account_id.to_string(),
                delta_pending_minor: 0,
                locked_delta_minor: -transition.released_locked_minor,
            }],
            nonce_advance: None,
        };
        fluid
            .risk_handle()
            .apply_delta(risk_transition)
            .await
            .map_err(|err| anyhow::Error::msg(err.to_string()))?;
    }
    Ok(())
}

fn replicated_lock_required_minor(
    side: OrderSide,
    price_ticks: i64,
    quantity_minor: i64,
) -> Result<i64> {
    match side {
        OrderSide::Buy => price_ticks
            .checked_mul(quantity_minor)
            .ok_or_else(|| anyhow::anyhow!("replication apply: buy lock overflow")),
        OrderSide::Sell => Ok(quantity_minor),
    }
}

async fn load_wal_projection_cursor(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<i64> {
    let existing: Option<i64> =
        sqlx::query_scalar("SELECT last_projected_offset FROM wal_projection_cursor WHERE id = 1")
            .fetch_optional(&mut **tx)
            .await
            .context("fluid: load wal_projection_cursor")?;

    if let Some(cursor) = existing {
        if cursor < 0 {
            anyhow::bail!("wal_projection_cursor must be non-negative, got {cursor}");
        }
        return Ok(cursor);
    }

    // Defensive fallback for pre-migration instances:
    // initialize the cursor row and start from 0.
    sqlx::query(
        r#"
        INSERT INTO wal_projection_cursor (id, last_projected_offset)
        VALUES (1, 0)
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .execute(&mut **tx)
    .await
    .context("fluid: initialize wal_projection_cursor")?;

    Ok(0)
}

// ── Leader Watchdog ──

pub(crate) fn spawn_leader_watchdog(mut leader_conn: PgConnection, config: LeaderWatchdogConfig) {
    let LeaderWatchdogConfig {
        leader_connection_healthy,
        leader_epoch,
        committed_offset,
        leader_node_id,
        db_url,
        fluid,
        publish_committed_in_watchdog,
        failure_policy,
        pg_unreachable_demote_after,
        pg_publish_unreachable_since,
    } = config;
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(LEADER_WATCHDOG_INTERVAL).await;

            let heartbeat_epoch = leader_epoch.load(Ordering::Relaxed);
            let heartbeat_epoch = match i64::try_from(heartbeat_epoch) {
                Ok(epoch) => epoch,
                Err(err) => {
                    tracing::error!(error = ?err, "fluid watchdog: leader epoch overflows i64");
                    leader_connection_healthy.store(false, Ordering::Relaxed);
                    fluid.mark_unhealthy("leader epoch overflow");
                    return;
                }
            };
            let heartbeat_committed = committed_offset.load(Ordering::Relaxed);
            let heartbeat_committed = match i64::try_from(heartbeat_committed) {
                Ok(offset) => offset,
                Err(err) => {
                    tracing::error!(error = ?err, "fluid watchdog: committed offset overflows i64");
                    leader_connection_healthy.store(false, Ordering::Relaxed);
                    fluid.mark_unhealthy("committed offset overflow");
                    return;
                }
            };

            let ping_result = if publish_committed_in_watchdog {
                sqlx::query(
                    r#"
                    UPDATE fluid_leader_state
                    SET last_heartbeat_at = NOW(), committed_offset = $1
                    WHERE singleton_key = TRUE
                      AND leader_epoch = $2
                      AND leader_node_id = $3
                    "#,
                )
                .bind(heartbeat_committed)
                .bind(heartbeat_epoch)
                .bind(&leader_node_id)
                .execute(&mut leader_conn)
                .await
            } else {
                sqlx::query(
                    r#"
                    UPDATE fluid_leader_state
                    SET last_heartbeat_at = NOW()
                    WHERE singleton_key = TRUE
                      AND leader_epoch = $1
                      AND leader_node_id = $2
                    "#,
                )
                .bind(heartbeat_epoch)
                .bind(&leader_node_id)
                .execute(&mut leader_conn)
                .await
            };

            match ping_result {
                Ok(result) => {
                    if result.rows_affected() != 1 {
                        tracing::error!(
                            leader_epoch = heartbeat_epoch,
                            leader_node_id = %leader_node_id,
                            "fluid watchdog: leader epoch superseded"
                        );
                        leader_connection_healthy.store(false, Ordering::Relaxed);
                        fluid.mark_unhealthy("leader epoch superseded");
                        return;
                    }
                    clear_pg_unreachable_since(&pg_publish_unreachable_since);
                    leader_connection_healthy.store(true, Ordering::Relaxed);
                }
                Err(err) => {
                    if failure_policy == crate::PostFsyncFailurePolicy::DegradeToAsync {
                        let elapsed = note_pg_unreachable_since(&pg_publish_unreachable_since)
                            .unwrap_or(Duration::ZERO);
                        if elapsed < pg_unreachable_demote_after {
                            tracing::warn!(
                                error = ?err,
                                elapsed_secs = elapsed.as_secs_f64(),
                                demote_after_secs = pg_unreachable_demote_after.as_secs_f64(),
                                "fluid watchdog: PG unavailable in degrade mode, delaying demotion"
                            );
                            leader_connection_healthy.store(true, Ordering::Relaxed);
                            continue;
                        }
                        tracing::error!(
                            error = ?err,
                            elapsed_secs = elapsed.as_secs_f64(),
                            "fluid watchdog: PG unavailable beyond degrade threshold, demoting"
                        );
                        leader_connection_healthy.store(false, Ordering::Relaxed);
                        fluid.mark_unhealthy("leader PG unreachable beyond degrade threshold");
                        return;
                    }

                    tracing::error!(error = ?err, "fluid watchdog: leader connection lost");
                    leader_connection_healthy.store(false, Ordering::Relaxed);

                    // Attempt to re-acquire on a new connection
                    match try_reacquire_leader(&db_url).await {
                        Ok(new_conn) => {
                            tracing::info!(
                                "fluid watchdog: re-acquired leader lock on new connection"
                            );
                            leader_conn = new_conn;
                            leader_connection_healthy.store(true, Ordering::Relaxed);
                            clear_pg_unreachable_since(&pg_publish_unreachable_since);
                            // Trigger resync (sync task will detect unhealthy and reload)
                            fluid.mark_unhealthy("leader connection lost and re-acquired");
                        }
                        Err(reacquire_err) => {
                            tracing::error!(
                                error = ?reacquire_err,
                                "fluid watchdog: failed to re-acquire — staying unhealthy"
                            );
                            // leader_connection_healthy stays false permanently → 503 on all writes
                        }
                    }
                }
            }
        }
    });
}

fn clear_pg_unreachable_since(shared: &Arc<std::sync::Mutex<Option<Instant>>>) {
    match shared.lock() {
        Ok(mut guard) => {
            *guard = None;
        }
        Err(err) => {
            tracing::error!(error = %err, "fluid watchdog: PG unreachable state lock poisoned");
        }
    }
}

fn note_pg_unreachable_since(shared: &Arc<std::sync::Mutex<Option<Instant>>>) -> Option<Duration> {
    match shared.lock() {
        Ok(mut guard) => {
            let now = Instant::now();
            let started = guard.get_or_insert(now);
            Some(now.saturating_duration_since(*started))
        }
        Err(err) => {
            tracing::error!(error = %err, "fluid watchdog: PG unreachable state lock poisoned");
            None
        }
    }
}

async fn try_reacquire_leader(db_url: &str) -> Result<PgConnection> {
    let mut conn = PgConnection::connect(db_url)
        .await
        .context("fluid reacquire: connect")?;
    let acquired = try_acquire_leader_lock(&mut conn).await?;
    if acquired {
        Ok(conn)
    } else {
        Err(anyhow::anyhow!(
            "fluid reacquire: lock held by another instance"
        ))
    }
}

// ── Legacy Runtime Guard ──

/// Action returned by `LegacyProbeState::handle_probe_result`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LegacyProbeAction {
    /// No action needed — continue polling.
    Continue,
    /// Invariant violation — set flag and shut down.
    Shutdown,
}

/// Pure decision logic for the legacy probe task.
/// Extracted so it can be unit-tested without spawning tasks or calling `exit`.
struct LegacyProbeState {
    consecutive_failures: u32,
    threshold: u32,
}

impl LegacyProbeState {
    fn new(threshold: u32) -> Self {
        Self {
            consecutive_failures: 0,
            threshold,
        }
    }

    /// Process a single probe result and return the action to take.
    fn handle_probe_result(&mut self, result: &Result<bool>) -> LegacyProbeAction {
        match result {
            Ok(true) => {
                // Leader detected — invariant violation
                self.consecutive_failures = 0;
                LegacyProbeAction::Shutdown
            }
            Ok(false) => {
                // No leader — all clear
                self.consecutive_failures = 0;
                LegacyProbeAction::Continue
            }
            Err(_) => {
                self.consecutive_failures = self.consecutive_failures.saturating_add(1);
                if self.consecutive_failures >= self.threshold {
                    LegacyProbeAction::Shutdown
                } else {
                    LegacyProbeAction::Continue
                }
            }
        }
    }
}

pub(crate) fn spawn_legacy_probe_task(db_url: String, fluid_leader_detected: Arc<AtomicBool>) {
    tokio::spawn(async move {
        let mut probe_state = LegacyProbeState::new(LEGACY_PROBE_FAIL_THRESHOLD);
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;

            let result = probe_fluid_leader(&db_url).await;
            let action = probe_state.handle_probe_result(&result);
            if let Err(err) = &result {
                tracing::warn!(
                    error = ?err,
                    consecutive_failures = probe_state.consecutive_failures,
                    "fluid legacy guard: probe failed"
                );
            }
            match action {
                LegacyProbeAction::Continue => {}
                LegacyProbeAction::Shutdown => {
                    tracing::error!(
                        consecutive_failures = probe_state.consecutive_failures,
                        "fluid legacy guard: invariant violation, shutting down"
                    );
                    fluid_leader_detected.store(true, Ordering::Relaxed);
                    std::process::exit(1);
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use pebble_fluid::engine::FluidState;
    use pebble_fluid::ids::StringInterner;
    use pebble_fluid::risk::AccountState;
    use pebble_fluid::wal::{WalCommand, WalWriterHandle};
    use pebble_trading::{
        IncomingOrder, InternalAccountId, InternalOrderId, OrderId, OrderNonce, TimeInForce,
    };
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};
    use tokio::time::{sleep, Duration as TokioDuration};

    static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(0);
    const TEST_ENGINE_VERSION: &str = "test-engine-version";

    type TestComponents = (
        HashMap<MarketId, MarketBook>,
        RiskEngine,
        HashMap<MarketId, SequenceState>,
        HashMap<MarketId, MarketMeta>,
    );

    fn temp_dir() -> PathBuf {
        let mut dir = env::temp_dir();
        let process_id = std::process::id();
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        let seq = TEST_DIR_COUNTER.fetch_add(1, AtomicOrdering::SeqCst);
        dir.push(format!("pebble-fluid-bridge-{process_id}-{elapsed}-{seq}"));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn drop_dir(dir: &Path) {
        let _ = fs::remove_dir_all(dir);
    }

    fn make_account(cleared: i64) -> AccountState {
        AccountState {
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

    fn make_components() -> TestComponents {
        let market_id = MarketId("m1".to_string());

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
        let meta = MarketMeta {
            status: "Open".to_string(),
            outcomes: vec!["YES".to_string(), "NO".to_string()],
            binary_config: Some(BinaryConfig {
                yes_outcome: "YES".to_string(),
                no_outcome: "NO".to_string(),
                max_price_ticks: 10_000,
            }),
            instrument_admin: "admin".to_string(),
            instrument_id: "USD".to_string(),
        };
        let book = if let Some(binary_config) = &meta.binary_config {
            MarketBook::new_binary(binary_config.clone())
        } else {
            MarketBook::new_multi_outcome()
        };
        let mut books = HashMap::new();
        books.insert(market_id.clone(), book);
        market_meta.insert(market_id, meta);

        (books, risk, sequences, market_meta)
    }

    fn build_state(
        books: HashMap<MarketId, MarketBook>,
        risk: RiskEngine,
        sequences: HashMap<MarketId, SequenceState>,
        market_meta: HashMap<MarketId, MarketMeta>,
    ) -> FluidState {
        build_state_with_interners(
            books,
            risk,
            sequences,
            market_meta,
            StringInterner::default(),
            StringInterner::default(),
        )
    }

    fn build_state_with_interners(
        books: HashMap<MarketId, MarketBook>,
        risk: RiskEngine,
        sequences: HashMap<MarketId, SequenceState>,
        market_meta: HashMap<MarketId, MarketMeta>,
        order_interner: StringInterner<InternalOrderId>,
        account_interner: StringInterner<InternalAccountId>,
    ) -> FluidState {
        FluidState::new(
            books,
            risk,
            sequences,
            market_meta,
            order_interner,
            account_interner,
            TEST_ENGINE_VERSION.to_string(),
            false,
        )
    }

    fn make_incoming(
        order_id: &str,
        account_id: &str,
        side: OrderSide,
        price_ticks: i64,
        quantity_minor: i64,
        nonce: i64,
    ) -> IncomingOrder {
        make_incoming_with_outcome(
            order_id,
            account_id,
            "YES",
            side,
            price_ticks,
            quantity_minor,
            nonce,
        )
    }

    fn make_incoming_with_outcome(
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
            order_type: pebble_trading::OrderType::Limit,
            tif: TimeInForce::Gtc,
            nonce: OrderNonce(nonce),
            price_ticks,
            quantity_minor,
            submitted_at_micros: 1,
        }
    }

    fn assert_states_equal(expected: &FluidState, actual: &FluidState) {
        let market_id = MarketId("m1".to_string());
        let expected_book = expected.books().get(&market_id).expect("expected book");
        let actual_book = actual.books().get(&market_id).expect("actual book");
        assert_eq!(expected_book.order_count(), actual_book.order_count());

        for order_id in expected_book.order_ids() {
            let expected_entry = expected_book.get_entry(order_id).expect("expected entry");
            let actual_entry = actual_book.get_entry(order_id).expect("actual entry");
            assert_eq!(expected_entry, actual_entry);
        }

        for account_id in ["alice", "bob"] {
            let expected_account = expected
                .risk()
                .get_account(account_id)
                .expect("expected risk");
            let actual_account = actual.risk().get_account(account_id).expect("actual risk");
            assert_eq!(
                expected_account.delta_pending_trades_minor,
                actual_account.delta_pending_trades_minor
            );
            assert_eq!(
                expected_account.locked_open_orders_minor,
                actual_account.locked_open_orders_minor
            );
            assert_eq!(expected_account.last_nonce, actual_account.last_nonce);
        }

        let expected_seq = expected
            .sequences()
            .get(&market_id)
            .expect("expected sequence");
        let actual_seq = actual.sequences().get(&market_id).expect("actual sequence");
        assert_eq!(
            expected_seq.next_fill_sequence,
            actual_seq.next_fill_sequence
        );
        assert_eq!(
            expected_seq.next_event_sequence,
            actual_seq.next_event_sequence
        );
    }

    async fn append_place_and_apply(
        writer: &WalWriterHandle,
        state: &mut FluidState,
        incoming: IncomingOrder,
    ) -> WalOffset {
        let transition = state.compute_place_order(&incoming).expect("compute place");
        let offset = writer
            .append(WalCommand::PlaceOrder(incoming))
            .await
            .expect("append place");
        state.apply_place_order(transition).expect("apply place");
        offset
    }

    async fn append_cancel_and_apply(
        writer: &WalWriterHandle,
        state: &mut FluidState,
        order_id: &str,
        account_id: &str,
    ) -> WalOffset {
        let market_id = MarketId("m1".to_string());
        let oid = OrderId(order_id.to_string());
        let transition = state
            .compute_cancel_order(&market_id, &oid, account_id)
            .expect("compute cancel");
        let offset = writer
            .append(WalCommand::CancelOrder {
                order_id: oid,
                market_id,
                account_id: account_id.to_string(),
            })
            .await
            .expect("append cancel");
        state.apply_cancel_order(transition).expect("apply cancel");
        offset
    }

    // ── FluidInitError → WriteMode resolution ──

    #[test]
    fn test_lock_not_acquired_yields_follower_mode() {
        let err = FluidInitError::LockNotAcquired;
        let mode = match err {
            FluidInitError::LockNotAcquired => WriteMode::FluidFollower,
            FluidInitError::IneligibleForLeader { .. } => WriteMode::FluidFollower,
            FluidInitError::Hard(_) => WriteMode::Legacy, // should not reach
        };
        assert_eq!(mode, WriteMode::FluidFollower);
    }

    #[test]
    fn test_hard_error_is_not_lock_contention() {
        let err = FluidInitError::Hard(anyhow::anyhow!("connection refused"));
        assert!(matches!(err, FluidInitError::Hard(_)));
    }

    // ── LegacyProbeState decision logic ──

    #[test]
    fn test_probe_no_leader_continues() {
        let mut state = LegacyProbeState::new(3);
        let action = state.handle_probe_result(&Ok(false));
        assert_eq!(action, LegacyProbeAction::Continue);
        assert_eq!(state.consecutive_failures, 0);
    }

    #[test]
    fn test_probe_leader_detected_shuts_down() {
        let mut state = LegacyProbeState::new(3);
        let action = state.handle_probe_result(&Ok(true));
        assert_eq!(action, LegacyProbeAction::Shutdown);
    }

    #[test]
    fn test_probe_failure_below_threshold_continues() {
        let mut state = LegacyProbeState::new(3);
        let err: Result<bool> = Err(anyhow::anyhow!("connection timeout"));
        assert_eq!(state.handle_probe_result(&err), LegacyProbeAction::Continue);
        assert_eq!(state.consecutive_failures, 1);
        let err2: Result<bool> = Err(anyhow::anyhow!("connection timeout"));
        assert_eq!(
            state.handle_probe_result(&err2),
            LegacyProbeAction::Continue
        );
        assert_eq!(state.consecutive_failures, 2);
    }

    #[test]
    fn test_probe_failure_at_threshold_shuts_down() {
        let mut state = LegacyProbeState::new(3);
        for _ in 0..2 {
            let err: Result<bool> = Err(anyhow::anyhow!("timeout"));
            state.handle_probe_result(&err);
        }
        assert_eq!(state.consecutive_failures, 2);
        let err: Result<bool> = Err(anyhow::anyhow!("timeout"));
        assert_eq!(state.handle_probe_result(&err), LegacyProbeAction::Shutdown);
        assert_eq!(state.consecutive_failures, 3);
    }

    #[test]
    fn test_probe_success_resets_failure_counter() {
        let mut state = LegacyProbeState::new(3);
        // Two failures
        for _ in 0..2 {
            let err: Result<bool> = Err(anyhow::anyhow!("timeout"));
            state.handle_probe_result(&err);
        }
        assert_eq!(state.consecutive_failures, 2);
        // Success resets
        state.handle_probe_result(&Ok(false));
        assert_eq!(state.consecutive_failures, 0);
        // Need 3 fresh failures to trigger shutdown
        for _ in 0..2 {
            let err: Result<bool> = Err(anyhow::anyhow!("timeout"));
            assert_eq!(state.handle_probe_result(&err), LegacyProbeAction::Continue);
        }
        let err: Result<bool> = Err(anyhow::anyhow!("timeout"));
        assert_eq!(state.handle_probe_result(&err), LegacyProbeAction::Shutdown);
    }

    #[test]
    fn test_probe_leader_detected_also_resets_failure_counter() {
        let mut state = LegacyProbeState::new(3);
        let err: Result<bool> = Err(anyhow::anyhow!("timeout"));
        state.handle_probe_result(&err);
        assert_eq!(state.consecutive_failures, 1);
        // Leader detected — shutdown, and counter is reset
        assert_eq!(
            state.handle_probe_result(&Ok(true)),
            LegacyProbeAction::Shutdown
        );
        assert_eq!(state.consecutive_failures, 0);
    }

    #[test]
    fn test_wal_projection_cursor_to_offset_rejects_negative_cursor() {
        let err = wal_projection_cursor_to_offset(-1).unwrap_err();
        assert!(err.to_string().contains("must be non-negative"));
    }

    #[test]
    fn test_wal_projection_cursor_to_offset_accepts_zero() {
        let offset = wal_projection_cursor_to_offset(0).unwrap();
        assert_eq!(offset.0, 0);
    }

    #[test]
    fn test_startup_eligibility_auto_stale_wal() {
        let local_wal_tip = WalOffset(3);
        let committed_offset = WalOffset(7);
        assert!(!startup_eligible_for_leader(
            local_wal_tip,
            committed_offset
        ));
    }

    #[test]
    fn test_startup_eligibility_leader_stale_wal() {
        let local_wal_tip = WalOffset(0);
        let committed_offset = WalOffset(1);
        assert!(!startup_eligible_for_leader(
            local_wal_tip,
            committed_offset
        ));
    }

    #[test]
    fn test_startup_eligibility_accepts_caught_up_wal() {
        let local_wal_tip = WalOffset(12);
        let committed_offset = WalOffset(12);
        assert!(startup_eligible_for_leader(local_wal_tip, committed_offset));
    }

    #[test]
    fn test_follower_rejects_stale_epoch() {
        let known_epoch = Arc::new(AtomicU64::new(9));
        let result = promote_known_leader_epoch(&known_epoch, 8, "test");
        assert!(result.is_err());
        assert_eq!(known_epoch.load(Ordering::Relaxed), 9);
    }

    #[test]
    fn test_follower_accepts_new_epoch() {
        let known_epoch = Arc::new(AtomicU64::new(9));
        let updated = promote_known_leader_epoch(&known_epoch, 10, "test").expect("new epoch");
        assert_eq!(updated, 10);
        assert_eq!(known_epoch.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn test_should_attempt_promotion_only_on_timeout_and_allowed() {
        assert!(should_attempt_promotion(
            FollowerSessionOutcome::HeartbeatTimeout,
            true
        ));
        assert!(should_attempt_promotion(
            FollowerSessionOutcome::Reconnect,
            true
        ));
        assert!(!should_attempt_promotion(
            FollowerSessionOutcome::HeartbeatTimeout,
            false
        ));
    }

    #[test]
    fn test_write_mode_state_roundtrip() {
        let state = new_write_mode_state(WriteMode::FluidFollower);
        assert_eq!(load_write_mode(&state), WriteMode::FluidFollower);
        store_write_mode(&state, WriteMode::FluidLeader);
        assert_eq!(load_write_mode(&state), WriteMode::FluidLeader);
    }

    #[test]
    fn test_binary_config_for_outcomes_binary_market() {
        let outcomes = vec!["YES".to_string(), "NO".to_string()];
        let config = binary_config_for_outcomes(&outcomes, 10_000).expect("binary config");
        assert_eq!(config.yes_outcome, "YES");
        assert_eq!(config.no_outcome, "NO");
        assert_eq!(config.max_price_ticks, 10_000);
    }

    #[test]
    fn test_binary_config_for_outcomes_non_binary_market() {
        let outcomes = vec!["YES".to_string(), "NO".to_string(), "MAYBE".to_string()];
        assert!(binary_config_for_outcomes(&outcomes, 10_000).is_none());
    }

    #[tokio::test]
    async fn test_wal_replay_matches_live_state() {
        let wal_dir = temp_dir();
        let wal_writer = WalWriterHandle::spawn_with_engine_version(
            WalConfig {
                wal_dir: wal_dir.clone(),
                max_batch_size: 64,
                max_batch_wait: StdDuration::from_millis(1),
                segment_size_bytes: 8 * 1024 * 1024,
            },
            TEST_ENGINE_VERSION.to_string(),
        )
        .expect("spawn wal writer");

        let (books, risk, sequences, market_meta) = make_components();
        let mut live_state = build_state(
            books.clone(),
            risk.clone(),
            sequences.clone(),
            market_meta.clone(),
        );

        append_place_and_apply(
            &wal_writer,
            &mut live_state,
            make_incoming("o-alice-1", "alice", OrderSide::Buy, 60, 10, 0),
        )
        .await;
        append_place_and_apply(
            &wal_writer,
            &mut live_state,
            make_incoming("o-bob-1", "bob", OrderSide::Sell, 55, 4, 0),
        )
        .await;
        append_place_and_apply(
            &wal_writer,
            &mut live_state,
            make_incoming("o-bob-2", "bob", OrderSide::Sell, 75, 3, 1),
        )
        .await;
        append_cancel_and_apply(&wal_writer, &mut live_state, "o-bob-2", "bob").await;
        wal_writer.flush().await.expect("flush wal");

        let (
            replay_books,
            replay_risk,
            replay_sequences,
            replay_meta,
            replay_order_interner,
            replay_account_interner,
        ) = apply_wal_replay_from_cursor(
            books,
            risk,
            sequences,
            market_meta,
            StringInterner::default(),
            StringInterner::default(),
            WalReplayFromCursor {
                engine_version: TEST_ENGINE_VERSION,
                order_available_includes_withdrawal_reserves: false,
                wal_dir: wal_dir.clone(),
                after_offset: WalOffset(0),
            },
        )
        .await
        .expect("replay");
        let replay_state = build_state_with_interners(
            replay_books,
            replay_risk,
            replay_sequences,
            replay_meta,
            replay_order_interner,
            replay_account_interner,
        );

        assert_states_equal(&live_state, &replay_state);
        drop_dir(&wal_dir);
    }

    #[tokio::test]
    async fn test_cross_outcome_end_to_end() {
        let wal_dir = temp_dir();
        let wal_writer = WalWriterHandle::spawn_with_engine_version(
            WalConfig {
                wal_dir: wal_dir.clone(),
                max_batch_size: 64,
                max_batch_wait: StdDuration::from_millis(1),
                segment_size_bytes: 8 * 1024 * 1024,
            },
            TEST_ENGINE_VERSION.to_string(),
        )
        .expect("spawn wal writer");

        let (books, risk, sequences, market_meta) = make_components();
        let mut live_state = build_state(
            books.clone(),
            risk.clone(),
            sequences.clone(),
            market_meta.clone(),
        );

        append_place_and_apply(
            &wal_writer,
            &mut live_state,
            make_incoming_with_outcome("maker-buy-no", "bob", "NO", OrderSide::Buy, 4_000, 5, 0),
        )
        .await;
        append_place_and_apply(
            &wal_writer,
            &mut live_state,
            make_incoming_with_outcome(
                "taker-buy-yes",
                "alice",
                "YES",
                OrderSide::Buy,
                6_000,
                3,
                0,
            ),
        )
        .await;
        wal_writer.flush().await.expect("flush wal");

        let (
            replay_books,
            replay_risk,
            replay_sequences,
            replay_meta,
            replay_order_interner,
            replay_account_interner,
        ) = apply_wal_replay_from_cursor(
            books,
            risk,
            sequences,
            market_meta,
            StringInterner::default(),
            StringInterner::default(),
            WalReplayFromCursor {
                engine_version: TEST_ENGINE_VERSION,
                order_available_includes_withdrawal_reserves: false,
                wal_dir: wal_dir.clone(),
                after_offset: WalOffset(0),
            },
        )
        .await
        .expect("replay");
        let replay_state = build_state_with_interners(
            replay_books,
            replay_risk,
            replay_sequences,
            replay_meta,
            replay_order_interner,
            replay_account_interner,
        );

        assert_states_equal(&live_state, &replay_state);

        let bob = replay_state.risk().get_account("bob").expect("bob");
        let alice = replay_state.risk().get_account("alice").expect("alice");
        assert_eq!(bob.delta_pending_trades_minor, -12_000);
        assert_eq!(bob.locked_open_orders_minor, 8_000);
        assert_eq!(alice.delta_pending_trades_minor, -18_000);
        assert_eq!(alice.locked_open_orders_minor, 0);

        let maker_entry = replay_state
            .books()
            .get(&MarketId("m1".to_string()))
            .and_then(|book| {
                book.get_entry_by_external_order_id(
                    &OrderId("maker-buy-no".to_string()),
                    replay_state.order_interner(),
                )
            })
            .expect("maker should remain resting");
        assert_eq!(maker_entry.outcome, "NO");
        assert_eq!(maker_entry.side, OrderSide::Buy);
        assert_eq!(maker_entry.remaining_minor, 2);
        assert_eq!(maker_entry.locked_minor, 8_000);
        drop_dir(&wal_dir);
    }

    #[tokio::test]
    async fn test_startup_wal_replay() {
        let wal_dir = temp_dir();
        let wal_writer = WalWriterHandle::spawn_with_engine_version(
            WalConfig {
                wal_dir: wal_dir.clone(),
                max_batch_size: 64,
                max_batch_wait: StdDuration::from_millis(1),
                segment_size_bytes: 8 * 1024 * 1024,
            },
            TEST_ENGINE_VERSION.to_string(),
        )
        .expect("spawn wal writer");

        let (books, risk, sequences, market_meta) = make_components();
        let mut live_state = build_state(
            books.clone(),
            risk.clone(),
            sequences.clone(),
            market_meta.clone(),
        );
        append_place_and_apply(
            &wal_writer,
            &mut live_state,
            make_incoming("startup-order", "alice", OrderSide::Buy, 60, 5, 0),
        )
        .await;
        wal_writer.flush().await.expect("flush wal");
        drop(wal_writer);
        sleep(TokioDuration::from_millis(20)).await;

        let (
            replay_books,
            replay_risk,
            replay_sequences,
            replay_meta,
            replay_order_interner,
            replay_account_interner,
        ) = apply_wal_replay_from_cursor(
            books,
            risk,
            sequences,
            market_meta,
            StringInterner::default(),
            StringInterner::default(),
            WalReplayFromCursor {
                engine_version: TEST_ENGINE_VERSION,
                order_available_includes_withdrawal_reserves: false,
                wal_dir: wal_dir.clone(),
                after_offset: WalOffset(0),
            },
        )
        .await
        .expect("replay startup");
        let replay_state = build_state_with_interners(
            replay_books,
            replay_risk,
            replay_sequences,
            replay_meta,
            replay_order_interner,
            replay_account_interner,
        );

        assert_states_equal(&live_state, &replay_state);
        drop_dir(&wal_dir);
    }

    #[tokio::test]
    async fn test_no_double_apply_on_restart() {
        let wal_dir = temp_dir();
        let wal_writer = WalWriterHandle::spawn_with_engine_version(
            WalConfig {
                wal_dir: wal_dir.clone(),
                max_batch_size: 64,
                max_batch_wait: StdDuration::from_millis(1),
                segment_size_bytes: 8 * 1024 * 1024,
            },
            TEST_ENGINE_VERSION.to_string(),
        )
        .expect("spawn wal writer");

        let (books, risk, sequences, market_meta) = make_components();
        let mut live_state = build_state(books, risk, sequences, market_meta);

        let _ = append_place_and_apply(
            &wal_writer,
            &mut live_state,
            make_incoming("o1", "alice", OrderSide::Buy, 60, 5, 0),
        )
        .await;
        let last_offset = append_place_and_apply(
            &wal_writer,
            &mut live_state,
            make_incoming("o2", "bob", OrderSide::Sell, 70, 2, 0),
        )
        .await;
        wal_writer.flush().await.expect("flush wal");

        let expected_state = live_state.clone();
        let (
            books_after,
            risk_after,
            seq_after,
            meta_after,
            order_interner_after,
            account_interner_after,
        ) = apply_wal_replay_from_cursor(
            live_state.books().clone(),
            live_state.risk().clone(),
            live_state.sequences().clone(),
            live_state.market_meta().clone(),
            live_state.order_interner().clone(),
            live_state.account_interner().clone(),
            WalReplayFromCursor {
                engine_version: TEST_ENGINE_VERSION,
                order_available_includes_withdrawal_reserves: false,
                wal_dir: wal_dir.clone(),
                after_offset: last_offset,
            },
        )
        .await
        .expect("replay from persisted cursor");
        let after_restart_state = build_state_with_interners(
            books_after,
            risk_after,
            seq_after,
            meta_after,
            order_interner_after,
            account_interner_after,
        );

        assert_states_equal(&expected_state, &after_restart_state);
        drop_dir(&wal_dir);
    }

    #[tokio::test]
    async fn test_wal_migration_clean_start() {
        let wal_dir = temp_dir();
        let (books, risk, sequences, market_meta) = make_components();
        let mut live_state = build_state(
            books.clone(),
            risk.clone(),
            sequences.clone(),
            market_meta.clone(),
        );

        let old_writer = WalWriterHandle::spawn_with_engine_version(
            WalConfig {
                wal_dir: wal_dir.clone(),
                max_batch_size: 64,
                max_batch_wait: StdDuration::from_millis(1),
                segment_size_bytes: 8 * 1024 * 1024,
            },
            "old-stage-d-engine".to_string(),
        )
        .expect("spawn old wal writer");

        let old_offset = append_place_and_apply(
            &old_writer,
            &mut live_state,
            make_incoming("old-order", "alice", OrderSide::Buy, 60, 5, 0),
        )
        .await;
        old_writer.flush().await.expect("flush old wal");
        drop(old_writer);

        let snapshot_state = live_state.clone();

        let new_writer = WalWriterHandle::spawn_with_engine_version(
            WalConfig {
                wal_dir: wal_dir.clone(),
                max_batch_size: 64,
                max_batch_wait: StdDuration::from_millis(1),
                segment_size_bytes: 8 * 1024 * 1024,
            },
            TEST_ENGINE_VERSION.to_string(),
        )
        .expect("spawn new wal writer");

        append_place_and_apply(
            &new_writer,
            &mut live_state,
            make_incoming("new-order", "bob", OrderSide::Sell, 55, 2, 0),
        )
        .await;
        new_writer.flush().await.expect("flush new wal");

        let (
            replay_books,
            replay_risk,
            replay_sequences,
            replay_meta,
            replay_order_interner,
            replay_account_interner,
        ) = apply_wal_replay_from_cursor(
            snapshot_state.books().clone(),
            snapshot_state.risk().clone(),
            snapshot_state.sequences().clone(),
            snapshot_state.market_meta().clone(),
            snapshot_state.order_interner().clone(),
            snapshot_state.account_interner().clone(),
            WalReplayFromCursor {
                engine_version: TEST_ENGINE_VERSION,
                order_available_includes_withdrawal_reserves: false,
                wal_dir: wal_dir.clone(),
                after_offset: old_offset,
            },
        )
        .await
        .expect("replay clean-start after migration");
        let replay_state = build_state_with_interners(
            replay_books,
            replay_risk,
            replay_sequences,
            replay_meta,
            replay_order_interner,
            replay_account_interner,
        );

        assert_states_equal(&live_state, &replay_state);
        drop_dir(&wal_dir);
    }

    #[tokio::test]
    async fn test_wal_fsync_ok_postgres_fail_recovery() {
        let wal_dir = temp_dir();
        let wal_writer = WalWriterHandle::spawn_with_engine_version(
            WalConfig {
                wal_dir: wal_dir.clone(),
                max_batch_size: 64,
                max_batch_wait: StdDuration::from_millis(1),
                segment_size_bytes: 8 * 1024 * 1024,
            },
            TEST_ENGINE_VERSION.to_string(),
        )
        .expect("spawn wal writer");

        let (books, risk, sequences, market_meta) = make_components();
        let stale_state = build_state(
            books.clone(),
            risk.clone(),
            sequences.clone(),
            market_meta.clone(),
        );

        let first = make_incoming("fsync-fail-1", "alice", OrderSide::Buy, 60, 3, 0);
        wal_writer
            .append(WalCommand::PlaceOrder(first.clone()))
            .await
            .expect("append first");
        wal_writer.flush().await.expect("flush wal");

        let second = make_incoming("fsync-fail-2", "alice", OrderSide::Buy, 61, 2, 1);
        let stale_err = stale_state
            .compute_place_order(&second)
            .expect_err("stale state should reject nonce+1");
        assert!(matches!(stale_err, FluidError::NonceMismatch { .. }));

        let (
            replay_books,
            replay_risk,
            replay_sequences,
            replay_meta,
            replay_order_interner,
            replay_account_interner,
        ) = apply_wal_replay_from_cursor(
            books,
            risk,
            sequences,
            market_meta,
            StringInterner::default(),
            StringInterner::default(),
            WalReplayFromCursor {
                engine_version: TEST_ENGINE_VERSION,
                order_available_includes_withdrawal_reserves: false,
                wal_dir: wal_dir.clone(),
                after_offset: WalOffset(0),
            },
        )
        .await
        .expect("replay");
        let mut recovered_state = build_state_with_interners(
            replay_books,
            replay_risk,
            replay_sequences,
            replay_meta,
            replay_order_interner,
            replay_account_interner,
        );

        let second_transition = recovered_state
            .compute_place_order(&second)
            .expect("nonce+1 should succeed after recovery replay");
        recovered_state
            .apply_place_order(second_transition)
            .expect("apply recovered second order");
        drop_dir(&wal_dir);
    }

    #[tokio::test]
    async fn test_resync_wal_integration_replays_backlog_since_cursor() {
        let wal_dir = temp_dir();
        let wal_writer = WalWriterHandle::spawn_with_engine_version(
            WalConfig {
                wal_dir: wal_dir.clone(),
                max_batch_size: 64,
                max_batch_wait: StdDuration::from_millis(1),
                segment_size_bytes: 8 * 1024 * 1024,
            },
            TEST_ENGINE_VERSION.to_string(),
        )
        .expect("spawn wal writer");

        let (books, risk, sequences, market_meta) = make_components();
        let mut live_state = build_state(
            books.clone(),
            risk.clone(),
            sequences.clone(),
            market_meta.clone(),
        );
        let first_offset = append_place_and_apply(
            &wal_writer,
            &mut live_state,
            make_incoming("r1", "alice", OrderSide::Buy, 60, 4, 0),
        )
        .await;
        append_place_and_apply(
            &wal_writer,
            &mut live_state,
            make_incoming("r2", "bob", OrderSide::Sell, 55, 2, 0),
        )
        .await;
        append_place_and_apply(
            &wal_writer,
            &mut live_state,
            make_incoming("r3", "alice", OrderSide::Buy, 70, 1, 1),
        )
        .await;
        wal_writer.flush().await.expect("flush wal");

        // Simulate DB snapshot at cursor = first_offset, then replay backlog (offset > cursor).
        let mut snapshot_state = build_state(books, risk, sequences, market_meta);
        let first_transition = snapshot_state
            .compute_place_order(&make_incoming("r1", "alice", OrderSide::Buy, 60, 4, 0))
            .expect("compute first");
        snapshot_state
            .apply_place_order(first_transition)
            .expect("apply first");

        let (
            resynced_books,
            resynced_risk,
            resynced_sequences,
            resynced_meta,
            resynced_order_interner,
            resynced_account_interner,
        ) = apply_wal_replay_from_cursor(
            snapshot_state.books().clone(),
            snapshot_state.risk().clone(),
            snapshot_state.sequences().clone(),
            snapshot_state.market_meta().clone(),
            snapshot_state.order_interner().clone(),
            snapshot_state.account_interner().clone(),
            WalReplayFromCursor {
                engine_version: TEST_ENGINE_VERSION,
                order_available_includes_withdrawal_reserves: false,
                wal_dir: wal_dir.clone(),
                after_offset: first_offset,
            },
        )
        .await
        .expect("replay backlog");
        let resynced_state = build_state_with_interners(
            resynced_books,
            resynced_risk,
            resynced_sequences,
            resynced_meta,
            resynced_order_interner,
            resynced_account_interner,
        );

        assert_states_equal(&live_state, &resynced_state);
        drop_dir(&wal_dir);
    }
}
