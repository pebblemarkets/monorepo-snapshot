use std::{
    collections::{HashMap, HashSet},
    hash::{Hash as _, Hasher as _},
    net::SocketAddr,
    path::PathBuf,
    sync::{atomic::AtomicBool, atomic::AtomicU64, atomic::AtomicU8, Arc},
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context as _, Result};
use axum::{
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Redirect, Response},
    routing::{delete, get, post},
    Json, Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use pebble_daml_grpc::com::daml::ledger::api::v2 as lapi;
use pebble_ids::{parse_account_id, parse_deposit_id, parse_market_id, parse_withdrawal_id};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use sqlx::{postgres::PgPoolOptions, PgPool, Postgres, Transaction};
use tonic::{metadata::MetadataValue, transport::Endpoint, Request};
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    trace::TraceLayer,
};
use tracing::Instrument as _;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

mod auth_keys;
mod fluid_bridge;
mod m3_orders;
mod m4_views;
mod perf_metrics;
mod projection_sql;
mod public_cache;
mod wal_projector;

const MARKET_LOCK_NAMESPACE: i32 = 12;
const MAX_BOUNDED_COMMAND_ID_LEN: usize = 120;
const WITHDRAWAL_RECEIPT_STATUS_COMPLETED: &str = "Completed";
const OPERATOR_ACCEPT_MODE_WITHDRAWAL_AUTO_ACCEPT: &str = "WithdrawalAutoAccept";
const WITHDRAWAL_CLAIM_QUERY_DEFAULT_LIMIT: i64 = 50;
const WITHDRAWAL_CLAIM_QUERY_MAX_LIMIT: i64 = 200;
const WITHDRAWAL_CLAIM_QUERY_MAX_LIMIT_USIZE: usize = 200;
const MARKETS_QUERY_DEFAULT_LIMIT: i64 = 200;
const MARKETS_QUERY_MAX_LIMIT: i64 = 500;
const RATE_LIMIT_SHARD_COUNT: usize = 64;
const RATE_LIMIT_SHARD_COUNT_U64: u64 = 64;
const RATE_LIMIT_PRUNE_THRESHOLD_PER_SHARD: usize = 512;
const API_PERF_METRICS_DEFAULT_WINDOW_SIZE: usize = 50_000;
const API_PERF_METRICS_DEFAULT_MAX_SERIES: usize = 4_096;
const API_PERF_METRICS_DEFAULT_SUMMARY_SECONDS: u64 = 15;
const API_PERF_METRICS_DEFAULT_LOG_DIR: &str = "/tmp/pebble-api-perf-metrics";
const API_DB_STATEMENT_TIMEOUT_MS_DEFAULT: u64 = 5_000;
const API_DB_ACQUIRE_TIMEOUT_MS_DEFAULT: u64 = 2_000;
const API_PUBLIC_CACHE_MAX_ENTRIES_DEFAULT: usize = 2_048;
const API_ORDERBOOK_STREAM_POLL_MS_DEFAULT: u64 = 500;
const API_ORDERBOOK_STREAM_IDLE_TTL_MS_DEFAULT: u64 = 30_000;
const ADMIN_ASSET_UPLOAD_MAX_BYTES: usize = 32 * 1024 * 1024;
const REGISTRATION_PARTY_LIST_PAGE_SIZE: usize = 1_000;
const REGISTRATION_PARTY_LIST_MAX_PAGES: usize = 2_000;
const REGISTRATION_PARTY_RESOLVE_RETRY_COUNT: usize = 20;
const REGISTRATION_PARTY_RESOLVE_RETRY_DELAY_MS: u64 = 250;
const USER_API_KEY_LABEL_MAX_LEN: usize = 64;
const USER_API_KEY_ID_MAX_LEN: usize = 128;
const USER_API_KEY_CREATE_MAX_RETRIES: usize = 8;
const USER_SESSION_CREATE_MAX_RETRIES: usize = 8;
const USER_SESSION_COOKIE_NAME: &str = "pebble_session";
const GOOGLE_OAUTH_STATE_COOKIE_NAME: &str = "pebble_google_oauth_state";
const GOOGLE_OAUTH_SCOPE: &str = "openid email profile";
const GOOGLE_OAUTH_AUTH_SOURCE: &str = "google_oauth";
const WAL_PROJECTION_CHANNEL_CAPACITY: usize = 8_192;
const RECONCILE_TTL_SKEW_TOLERANCE_SECS: u64 = 5;

type HmacSha256 = Hmac<Sha256>;

struct ProjectorBootstrap {
    shadow_state: pebble_fluid::engine::FluidState,
    wal_dir: PathBuf,
    after_offset: pebble_fluid::wal::WalOffset,
    projector_config: wal_projector::WalProjectorConfig,
    projection_rx: tokio::sync::mpsc::Receiver<wal_projector::ProjectionPayload>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FluidRuntimeRole {
    Auto,
    Leader,
    Follower,
}

#[derive(Clone, Debug)]
struct FluidReplicationConfig {
    node_id: String,
    bind_addr: String,
    advertise_addr: String,
    auth_token: String,
    heartbeat_interval: Duration,
    follower_heartbeat_timeout: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FluidReplicationMode {
    Async,
    SemiSync,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PostFsyncFailurePolicy {
    Strict,
    DegradeToAsync,
}

#[derive(Clone, Debug)]
pub(crate) struct FluidDurabilityConfig {
    pub(crate) replication_mode: FluidReplicationMode,
    pub(crate) min_ack_count: usize,
    pub(crate) ack_timeout: Duration,
    pub(crate) failure_policy: PostFsyncFailurePolicy,
    pub(crate) ack_timeout_demote_count: u64,
    pub(crate) pg_unreachable_demote_after: Duration,
}

#[derive(Clone, Debug)]
pub(crate) struct ReconcileConfig {
    pub(crate) pending_ttl: Duration,
    pub(crate) hmac_key: Option<Vec<u8>>,
    pub(crate) hmac_key_previous: Option<Vec<u8>>,
    pub(crate) ttl_skew_tolerance: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReconcileTokenPayload {
    #[serde(rename = "a")]
    pub(crate) account_id: String,
    #[serde(rename = "n")]
    pub(crate) nonce: i64,
    #[serde(rename = "o")]
    pub(crate) wal_offset: u64,
    #[serde(rename = "t")]
    pub(crate) unknown_at_unix_ms: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct VerifiedReconcileToken {
    pub(crate) unknown_at: DateTime<Utc>,
}

#[derive(Clone)]
struct FluidRuntimeState {
    write_mode: Arc<AtomicU8>,
    leader_connection_healthy: Arc<AtomicBool>,
    leader_epoch: Arc<AtomicU64>,
    committed_offset: Arc<AtomicU64>,
    pg_publish_unreachable_since: Arc<std::sync::Mutex<Option<Instant>>>,
}

#[derive(Clone)]
struct FollowerBootstrapConfig {
    db_url: String,
    wal_config: pebble_fluid::wal::WalConfig,
    engine_version: String,
    order_available_includes_withdrawal_reserves: bool,
    replication_config: FluidReplicationConfig,
    allow_promotion: bool,
    advance_committed_with_tip: bool,
    publish_committed_in_watchdog: bool,
    failure_policy: PostFsyncFailurePolicy,
    pg_unreachable_demote_after: Duration,
}

type FluidStartupTuple = (
    fluid_bridge::WriteMode,
    Option<Arc<pebble_fluid::engine::ExchangeEngine>>,
    Option<pebble_fluid::wal::WalWriterHandle>,
    Option<tokio::sync::mpsc::Sender<wal_projector::ProjectionPayload>>,
    Option<ProjectorBootstrap>,
    Option<pebble_fluid::replication::ReplicationBroadcaster>,
    Option<PathBuf>,
);

async fn start_fluid_follower_mode(
    db: PgPool,
    bootstrap: FollowerBootstrapConfig,
    runtime_state: FluidRuntimeState,
) -> Result<FluidStartupTuple> {
    let wal_dir = bootstrap.wal_config.wal_dir.clone();
    let follower_init = fluid_bridge::initialize_fluid_follower(
        &db,
        &bootstrap.engine_version,
        bootstrap.order_available_includes_withdrawal_reserves,
        bootstrap.wal_config.clone(),
    )
    .await
    .context("fluid: initialize follower")?;

    fluid_bridge::spawn_state_sync_task(
        db.clone(),
        follower_init.fluid.clone(),
        follower_init.sync_watermarks,
        follower_init.wal_handle.clone(),
        wal_dir.clone(),
    );
    fluid_bridge::spawn_follower_replication_task(fluid_bridge::FollowerReplicationRuntime {
        db,
        fluid: follower_init.fluid.clone(),
        wal_dir: wal_dir.clone(),
        wal_handle: follower_init.wal_handle.clone(),
        config: fluid_bridge::FollowerReplicationConfig {
            node_id: bootstrap.replication_config.node_id.clone(),
            engine_version: bootstrap.engine_version.clone(),
            auth_token: bootstrap.replication_config.auth_token.clone(),
            heartbeat_timeout: bootstrap.replication_config.follower_heartbeat_timeout,
        },
        promotion: fluid_bridge::FollowerPromotionConfig {
            allow_promotion: bootstrap.allow_promotion,
            db_url: bootstrap.db_url.clone(),
            node_id: bootstrap.replication_config.node_id.clone(),
            advertise_addr: bootstrap.replication_config.advertise_addr.clone(),
            leader_bind_addr: bootstrap.replication_config.bind_addr.clone(),
            engine_version: bootstrap.engine_version.clone(),
            auth_token: bootstrap.replication_config.auth_token.clone(),
            heartbeat_interval: bootstrap.replication_config.heartbeat_interval,
            write_mode: runtime_state.write_mode,
            leader_connection_healthy: runtime_state.leader_connection_healthy,
            advance_committed_with_tip: bootstrap.advance_committed_with_tip,
            publish_committed_in_watchdog: bootstrap.publish_committed_in_watchdog,
            failure_policy: bootstrap.failure_policy,
            pg_unreachable_demote_after: bootstrap.pg_unreachable_demote_after,
            pg_publish_unreachable_since: runtime_state.pg_publish_unreachable_since,
        },
        leader_epoch: runtime_state.leader_epoch,
        committed_offset: runtime_state.committed_offset,
    });

    Ok((
        fluid_bridge::WriteMode::FluidFollower,
        Some(follower_init.fluid),
        Some(follower_init.wal_handle),
        None,
        None,
        None,
        Some(wal_dir),
    ))
}

#[tokio::main]
async fn main() -> Result<()> {
    // Local dev convenience; no-op if .env doesn't exist.
    let _ = dotenvy::dotenv();
    let (log_writer, _log_guard) = tracing_appender::non_blocking(std::io::stdout());

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(log_writer))
        .init();

    let addr: SocketAddr = std::env::var("PEBBLE_API_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:3030".to_string())
        .parse()
        .context("parse PEBBLE_API_ADDR")?;

    let db_url = std::env::var("PEBBLE_DB_URL")
        .unwrap_or_else(|_| "postgres://pebble:pebble@127.0.0.1:5432/pebble".to_string());
    let db_max_connections = std::env::var("PEBBLE_API_DB_MAX_CONNECTIONS")
        .unwrap_or_else(|_| "40".to_string())
        .parse::<u32>()
        .context("parse PEBBLE_API_DB_MAX_CONNECTIONS")?;
    if db_max_connections == 0 {
        return Err(anyhow!("PEBBLE_API_DB_MAX_CONNECTIONS must be > 0"));
    }
    let db_statement_timeout_ms = std::env::var("PEBBLE_API_DB_STATEMENT_TIMEOUT_MS")
        .unwrap_or_else(|_| API_DB_STATEMENT_TIMEOUT_MS_DEFAULT.to_string())
        .parse::<u64>()
        .context("parse PEBBLE_API_DB_STATEMENT_TIMEOUT_MS")?;
    if db_statement_timeout_ms == 0 {
        return Err(anyhow!("PEBBLE_API_DB_STATEMENT_TIMEOUT_MS must be > 0"));
    }
    let db_acquire_timeout_ms = std::env::var("PEBBLE_API_DB_ACQUIRE_TIMEOUT_MS")
        .unwrap_or_else(|_| API_DB_ACQUIRE_TIMEOUT_MS_DEFAULT.to_string())
        .parse::<u64>()
        .context("parse PEBBLE_API_DB_ACQUIRE_TIMEOUT_MS")?;
    if db_acquire_timeout_ms == 0 {
        return Err(anyhow!("PEBBLE_API_DB_ACQUIRE_TIMEOUT_MS must be > 0"));
    }
    let statement_timeout_sql = format!("SET statement_timeout = '{db_statement_timeout_ms}ms'");
    let db = PgPoolOptions::new()
        .max_connections(db_max_connections)
        .acquire_timeout(Duration::from_millis(db_acquire_timeout_ms))
        .after_connect(move |conn, _meta| {
            let statement_timeout_sql = statement_timeout_sql.clone();
            Box::pin(async move {
                sqlx::query(&statement_timeout_sql).execute(conn).await?;
                Ok(())
            })
        })
        .connect(&db_url)
        .await
        .context("connect to postgres")?;
    let public_cache_max_entries = std::env::var("PEBBLE_API_PUBLIC_CACHE_MAX_ENTRIES")
        .unwrap_or_else(|_| API_PUBLIC_CACHE_MAX_ENTRIES_DEFAULT.to_string())
        .parse::<usize>()
        .context("parse PEBBLE_API_PUBLIC_CACHE_MAX_ENTRIES")?;
    if public_cache_max_entries == 0 {
        return Err(anyhow!("PEBBLE_API_PUBLIC_CACHE_MAX_ENTRIES must be > 0"));
    }
    let orderbook_stream_poll_ms = std::env::var("PEBBLE_API_ORDERBOOK_STREAM_POLL_MS")
        .unwrap_or_else(|_| API_ORDERBOOK_STREAM_POLL_MS_DEFAULT.to_string())
        .parse::<u64>()
        .context("parse PEBBLE_API_ORDERBOOK_STREAM_POLL_MS")?;
    if orderbook_stream_poll_ms == 0 {
        return Err(anyhow!("PEBBLE_API_ORDERBOOK_STREAM_POLL_MS must be > 0"));
    }
    let orderbook_stream_idle_ttl_ms = std::env::var("PEBBLE_API_ORDERBOOK_STREAM_IDLE_TTL_MS")
        .unwrap_or_else(|_| API_ORDERBOOK_STREAM_IDLE_TTL_MS_DEFAULT.to_string())
        .parse::<u64>()
        .context("parse PEBBLE_API_ORDERBOOK_STREAM_IDLE_TTL_MS")?;
    if orderbook_stream_idle_ttl_ms == 0 {
        return Err(anyhow!(
            "PEBBLE_API_ORDERBOOK_STREAM_IDLE_TTL_MS must be > 0"
        ));
    }

    let perf_metrics_enabled = match std::env::var("PEBBLE_API_PERF_METRICS_ENABLED") {
        Ok(raw) => parse_env_bool("PEBBLE_API_PERF_METRICS_ENABLED", &raw)?,
        Err(_) => true,
    };
    let perf_metrics_window_size = std::env::var("PEBBLE_API_PERF_METRICS_WINDOW_SIZE")
        .unwrap_or_else(|_| API_PERF_METRICS_DEFAULT_WINDOW_SIZE.to_string())
        .parse::<usize>()
        .context("parse PEBBLE_API_PERF_METRICS_WINDOW_SIZE")?;
    if perf_metrics_window_size == 0 {
        return Err(anyhow!("PEBBLE_API_PERF_METRICS_WINDOW_SIZE must be > 0"));
    }
    let perf_metrics_max_series = std::env::var("PEBBLE_API_PERF_METRICS_MAX_SERIES")
        .unwrap_or_else(|_| API_PERF_METRICS_DEFAULT_MAX_SERIES.to_string())
        .parse::<usize>()
        .context("parse PEBBLE_API_PERF_METRICS_MAX_SERIES")?;
    if perf_metrics_max_series == 0 {
        return Err(anyhow!("PEBBLE_API_PERF_METRICS_MAX_SERIES must be > 0"));
    }
    let perf_metrics_log_dir = match std::env::var("PEBBLE_API_PERF_METRICS_LOG_DIR") {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(PathBuf::from(trimmed))
            }
        }
        Err(_) => Some(PathBuf::from(API_PERF_METRICS_DEFAULT_LOG_DIR)),
    };
    let perf_metrics_summary_seconds = std::env::var("PEBBLE_API_PERF_METRICS_SUMMARY_SECONDS")
        .unwrap_or_else(|_| API_PERF_METRICS_DEFAULT_SUMMARY_SECONDS.to_string())
        .parse::<u64>()
        .context("parse PEBBLE_API_PERF_METRICS_SUMMARY_SECONDS")?;
    if perf_metrics_summary_seconds == 0 {
        return Err(anyhow!(
            "PEBBLE_API_PERF_METRICS_SUMMARY_SECONDS must be > 0"
        ));
    }
    let perf_metrics = perf_metrics::ApiPerfMetrics::new(
        perf_metrics_enabled,
        perf_metrics_window_size,
        perf_metrics_max_series,
        perf_metrics_log_dir,
        Duration::from_secs(perf_metrics_summary_seconds),
    );

    sqlx::migrate!("../../infra/sql/migrations")
        .run(&db)
        .await
        .context("run migrations")?;

    let account_lock_namespace = parse_account_lock_namespace_env()?;
    let active_withdrawals_disabled = match std::env::var("PEBBLE_ACTIVE_WITHDRAWALS_DISABLED") {
        Ok(raw) => parse_env_bool("PEBBLE_ACTIVE_WITHDRAWALS_DISABLED", &raw)?,
        Err(_) => false,
    };
    let active_withdrawals_enabled = !active_withdrawals_disabled;
    let withdrawal_request_state_disabled =
        match std::env::var("PEBBLE_WITHDRAWAL_REQUEST_STATE_DISABLED") {
            Ok(raw) => parse_env_bool("PEBBLE_WITHDRAWAL_REQUEST_STATE_DISABLED", &raw)?,
            Err(_) => false,
        };
    let withdrawal_request_state_enabled = !withdrawal_request_state_disabled;
    let withdrawal_accept_ineligible_at_create =
        match std::env::var("PEBBLE_WITHDRAWAL_ACCEPT_INELIGIBLE_AT_CREATE") {
            Ok(raw) => parse_env_bool("PEBBLE_WITHDRAWAL_ACCEPT_INELIGIBLE_AT_CREATE", &raw)?,
            Err(_) => false,
        };
    let withdrawal_reject_ineligible_at_create = !withdrawal_accept_ineligible_at_create;
    let withdrawal_submission_saga_disabled =
        match std::env::var("PEBBLE_WITHDRAWAL_SUBMISSION_SAGA_DISABLED") {
            Ok(raw) => parse_env_bool("PEBBLE_WITHDRAWAL_SUBMISSION_SAGA_DISABLED", &raw)?,
            Err(_) => false,
        };
    let withdrawal_submission_saga_enabled = !withdrawal_submission_saga_disabled;
    let withdrawal_one_shot_auto_accept_enabled =
        match std::env::var("PEBBLE_WITHDRAWAL_ONE_SHOT_AUTO_ACCEPT_ENABLED") {
            Ok(raw) => parse_env_bool("PEBBLE_WITHDRAWAL_ONE_SHOT_AUTO_ACCEPT_ENABLED", &raw)?,
            Err(_) => false,
        };
    let withdrawal_one_shot_require_policy =
        match std::env::var("PEBBLE_WITHDRAWAL_ONE_SHOT_REQUIRE_POLICY") {
            Ok(raw) => parse_env_bool("PEBBLE_WITHDRAWAL_ONE_SHOT_REQUIRE_POLICY", &raw)?,
            Err(_) => withdrawal_one_shot_auto_accept_enabled,
        };
    let order_available_excludes_withdrawal_reserves =
        match std::env::var("PEBBLE_ORDER_AVAILABLE_EXCLUDES_WITHDRAWAL_RESERVES") {
            Ok(raw) => parse_env_bool("PEBBLE_ORDER_AVAILABLE_EXCLUDES_WITHDRAWAL_RESERVES", &raw)?,
            Err(_) => false,
        };
    let order_available_includes_withdrawal_reserves =
        !order_available_excludes_withdrawal_reserves;
    let account_writer_enabled = match std::env::var("PEBBLE_ACCOUNT_WRITER_ENABLE") {
        Ok(raw) => parse_env_bool("PEBBLE_ACCOUNT_WRITER_ENABLE", &raw)?,
        Err(_) => false,
    };
    if withdrawal_submission_saga_enabled && !withdrawal_request_state_enabled {
        return Err(anyhow!(
            "PEBBLE_WITHDRAWAL_SUBMISSION_SAGA_DISABLED=false requires PEBBLE_WITHDRAWAL_REQUEST_STATE_DISABLED=false"
        ));
    }
    if withdrawal_submission_saga_enabled && !withdrawal_reject_ineligible_at_create {
        return Err(anyhow!(
            "PEBBLE_WITHDRAWAL_SUBMISSION_SAGA_DISABLED=false requires PEBBLE_WITHDRAWAL_ACCEPT_INELIGIBLE_AT_CREATE=false"
        ));
    }
    if active_withdrawals_enabled && !withdrawal_submission_saga_enabled {
        return Err(anyhow!(
            "PEBBLE_ACTIVE_WITHDRAWALS_DISABLED=false requires PEBBLE_WITHDRAWAL_SUBMISSION_SAGA_DISABLED=false"
        ));
    }
    if active_withdrawals_enabled && !order_available_includes_withdrawal_reserves {
        return Err(anyhow!(
            "PEBBLE_ACTIVE_WITHDRAWALS_DISABLED=false requires PEBBLE_ORDER_AVAILABLE_EXCLUDES_WITHDRAWAL_RESERVES=false"
        ));
    }
    if withdrawal_one_shot_auto_accept_enabled && !withdrawal_one_shot_require_policy {
        return Err(anyhow!(
            "PEBBLE_WITHDRAWAL_ONE_SHOT_AUTO_ACCEPT_ENABLED=true requires PEBBLE_WITHDRAWAL_ONE_SHOT_REQUIRE_POLICY=true"
        ));
    }
    upsert_account_lock_capability(
        &db,
        "api",
        account_lock_namespace,
        active_withdrawals_enabled,
        &serde_json::json!({
            "account_writer_enabled": account_writer_enabled,
            "withdrawal_request_state_enabled": withdrawal_request_state_enabled,
            "withdrawal_accept_ineligible_at_create": withdrawal_accept_ineligible_at_create,
            "withdrawal_reject_ineligible_at_create": withdrawal_reject_ineligible_at_create,
            "withdrawal_submission_saga_enabled": withdrawal_submission_saga_enabled,
            "withdrawal_one_shot_auto_accept_enabled": withdrawal_one_shot_auto_accept_enabled,
            "withdrawal_one_shot_require_policy": withdrawal_one_shot_require_policy,
            "order_available_excludes_withdrawal_reserves": order_available_excludes_withdrawal_reserves,
            "order_available_includes_withdrawal_reserves": order_available_includes_withdrawal_reserves,
        }),
    )
    .await?;
    if active_withdrawals_enabled {
        ensure_account_lock_namespace_parity(
            &db,
            account_lock_namespace,
            active_withdrawals_enabled,
            &["api", "treasury", "clearing"],
        )
        .await?;
    }

    let ledger_cfg = LedgerConfig::from_env().await?;

    let m3_cfg = m3_orders::M3ApiConfig::from_env().context("load m3 api config")?;
    let auth_keys = auth_keys::AuthKeyStore::default();
    auth_keys
        .reload_from_db(&db)
        .await
        .context("load auth keys from database")?;
    for (api_key, account_id) in &m3_cfg.api_keys {
        auth_keys
            .upsert_user_api_key(&db, api_key, account_id)
            .await
            .context("seed user API key from environment")?;
    }
    for admin_key in &m3_cfg.admin_keys {
        auth_keys
            .upsert_admin_api_key(&db, admin_key)
            .await
            .context("seed admin API key from environment")?;
    }
    let committee_submitter =
        CommitteeSubmitter::direct(ledger_cfg.channel.clone(), ledger_cfg.auth_header.clone());
    let google_oauth_cfg = GoogleOAuthConfig::from_env().context("load google oauth config")?;

    // ── Fluid engine initialization ──
    let fluid_enabled = match std::env::var("PEBBLE_FLUID_ENABLED") {
        Ok(raw) => parse_env_bool("PEBBLE_FLUID_ENABLED", &raw)?,
        Err(_) => false,
    };
    let durability_config =
        parse_fluid_durability_config().context("load fluid durability config")?;
    let reconcile_config = parse_reconcile_config().context("load reconcile config")?;
    if fluid_enabled
        && durability_config.replication_mode == FluidReplicationMode::SemiSync
        && reconcile_config.hmac_key.is_none()
    {
        return Err(anyhow!(
            "PEBBLE_FLUID_RECONCILE_HMAC_KEY is required when semi-sync replication is enabled"
        ));
    }
    let advance_committed_with_tip =
        durability_config.replication_mode == FluidReplicationMode::Async;
    let publish_committed_in_watchdog =
        durability_config.replication_mode == FluidReplicationMode::Async;
    let leader_connection_healthy = Arc::new(AtomicBool::new(true));
    let fluid_leader_detected = Arc::new(AtomicBool::new(false));
    let write_mode_state = fluid_bridge::new_write_mode_state(fluid_bridge::WriteMode::Legacy);
    let leader_epoch = Arc::new(AtomicU64::new(0));
    let committed_offset = Arc::new(AtomicU64::new(0));
    let pg_publish_unreachable_since = Arc::new(std::sync::Mutex::new(None));
    let runtime_state = FluidRuntimeState {
        write_mode: write_mode_state.clone(),
        leader_connection_healthy: leader_connection_healthy.clone(),
        leader_epoch: leader_epoch.clone(),
        committed_offset: committed_offset.clone(),
        pg_publish_unreachable_since: pg_publish_unreachable_since.clone(),
    };
    let mut fluid_node_id: Option<String> = None;

    let (
        write_mode,
        fluid,
        wal_handle,
        projection_tx,
        projector_bootstrap,
        replication_broadcaster,
        wal_dir,
    ) = if fluid_enabled {
        let wal_config = parse_wal_config().context("load WAL config")?;
        let projector_config = parse_projector_config().context("load WAL projector config")?;
        let fluid_role = parse_fluid_role().context("load fluid role")?;
        let replication_config =
            parse_fluid_replication_config().context("load fluid replication config")?;
        fluid_node_id = Some(replication_config.node_id.clone());

        let try_leader = matches!(
            fluid_role,
            FluidRuntimeRole::Auto | FluidRuntimeRole::Leader
        );
        if try_leader {
            let initial_tip = pebble_fluid::wal::local_wal_tip(&wal_config.wal_dir)
                .context("fluid: determine local WAL tip for replication")?;
            let broadcaster =
                pebble_fluid::replication::ReplicationBroadcaster::with_default_capacity(
                    initial_tip,
                )
                .context("fluid: initialize replication broadcaster")?;
            let (replication_tx, replication_rx) =
                std::sync::mpsc::channel::<Arc<Vec<pebble_fluid::replication::WalRecordPayload>>>();

            match fluid_bridge::initialize_fluid(
                &db,
                &db_url,
                &m3_cfg.engine_version,
                order_available_includes_withdrawal_reserves,
                wal_config.clone(),
                fluid_bridge::LeaderIdentity {
                    node_id: &replication_config.node_id,
                    repl_addr: &replication_config.advertise_addr,
                },
                Some(replication_tx),
            )
            .await
            {
                Ok(init) => {
                    leader_epoch.store(init.leader_epoch, std::sync::atomic::Ordering::Relaxed);
                    committed_offset.store(
                        init.committed_offset.0,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    let fluid_arc = init.fluid.clone();
                    let wal_handle = init.wal_handle.clone();
                    let sync_watermarks = init.sync_watermarks.clone();
                    let after_offset = pebble_fluid::wal::WalOffset(
                        u64::try_from(sync_watermarks.wal_projection_cursor)
                            .context("fluid: wal_projection_cursor must be non-negative")?,
                    );
                    let (projection_tx, projection_rx) =
                        tokio::sync::mpsc::channel(WAL_PROJECTION_CHANNEL_CAPACITY);

                    fluid_bridge::spawn_state_sync_task(
                        db.clone(),
                        init.fluid.clone(),
                        init.sync_watermarks,
                        wal_handle.clone(),
                        wal_config.wal_dir.clone(),
                    );
                    fluid_bridge::spawn_leader_watchdog(
                        init.leader_conn,
                        fluid_bridge::LeaderWatchdogConfig {
                            leader_connection_healthy: leader_connection_healthy.clone(),
                            leader_epoch: leader_epoch.clone(),
                            committed_offset: committed_offset.clone(),
                            leader_node_id: replication_config.node_id.clone(),
                            db_url: db_url.clone(),
                            fluid: init.fluid.clone(),
                            publish_committed_in_watchdog,
                            failure_policy: durability_config.failure_policy,
                            pg_unreachable_demote_after: durability_config
                                .pg_unreachable_demote_after,
                            pg_publish_unreachable_since: pg_publish_unreachable_since.clone(),
                        },
                    );
                    fluid_bridge::spawn_replication_fanout_task(
                        replication_rx,
                        broadcaster.clone(),
                        committed_offset.clone(),
                        advance_committed_with_tip,
                    );
                    fluid_bridge::spawn_leader_replication_listener(
                        wal_config.wal_dir.clone(),
                        broadcaster.clone(),
                        fluid_bridge::LeaderReplicationConfig {
                            bind_addr: replication_config.bind_addr.clone(),
                            engine_version: m3_cfg.engine_version.clone(),
                            auth_token: replication_config.auth_token.clone(),
                            heartbeat_interval: replication_config.heartbeat_interval,
                            leader_epoch: leader_epoch.clone(),
                            committed_offset: committed_offset.clone(),
                        },
                    );

                    tracing::info!(
                        advertise_addr = %replication_config.advertise_addr,
                        bind_addr = %replication_config.bind_addr,
                        node_id = %replication_config.node_id,
                        "fluid: running as FluidLeader"
                    );
                    (
                        fluid_bridge::WriteMode::FluidLeader,
                        Some(fluid_arc),
                        Some(wal_handle),
                        Some(projection_tx),
                        Some(ProjectorBootstrap {
                            shadow_state: init.shadow_state,
                            wal_dir: wal_config.wal_dir.clone(),
                            after_offset,
                            projector_config,
                            projection_rx,
                        }),
                        Some(broadcaster),
                        Some(wal_config.wal_dir.clone()),
                    )
                }
                Err(fluid_bridge::FluidInitError::IneligibleForLeader {
                    local_wal_tip,
                    committed_offset: required_offset,
                }) => {
                    if matches!(fluid_role, FluidRuntimeRole::Leader) {
                        return Err(anyhow!(
                            "Ineligible: local_wal_tip {} < committed_offset {}. Start as auto or follower to catch up first.",
                            local_wal_tip.0,
                            required_offset.0
                        ));
                    }
                    tracing::warn!(
                        local_wal_tip = local_wal_tip.0,
                        committed_offset = required_offset.0,
                        "fluid: local WAL behind committed offset, falling back to follower mode"
                    );
                    start_fluid_follower_mode(
                        db.clone(),
                        FollowerBootstrapConfig {
                            db_url: db_url.clone(),
                            wal_config: wal_config.clone(),
                            engine_version: m3_cfg.engine_version.clone(),
                            order_available_includes_withdrawal_reserves,
                            replication_config: replication_config.clone(),
                            allow_promotion: true,
                            advance_committed_with_tip,
                            publish_committed_in_watchdog,
                            failure_policy: durability_config.failure_policy,
                            pg_unreachable_demote_after: durability_config
                                .pg_unreachable_demote_after,
                        },
                        runtime_state.clone(),
                    )
                    .await
                    .context("fluid: auto follower fallback (ineligible leader)")?
                }
                Err(fluid_bridge::FluidInitError::LockNotAcquired) => {
                    if matches!(fluid_role, FluidRuntimeRole::Leader) {
                        return Err(anyhow!(
                            "fluid: PEBBLE_FLUID_ROLE=leader but leader lock was not acquired"
                        ));
                    }
                    tracing::warn!("fluid: leader lock held by another instance, falling back to follower mode");
                    start_fluid_follower_mode(
                        db.clone(),
                        FollowerBootstrapConfig {
                            db_url: db_url.clone(),
                            wal_config: wal_config.clone(),
                            engine_version: m3_cfg.engine_version.clone(),
                            order_available_includes_withdrawal_reserves,
                            replication_config: replication_config.clone(),
                            allow_promotion: true,
                            advance_committed_with_tip,
                            publish_committed_in_watchdog,
                            failure_policy: durability_config.failure_policy,
                            pg_unreachable_demote_after: durability_config
                                .pg_unreachable_demote_after,
                        },
                        runtime_state.clone(),
                    )
                    .await
                    .context("fluid: auto follower fallback")?
                }
                Err(fluid_bridge::FluidInitError::Hard(err)) => {
                    return Err(err.context("fluid: initialization failed"));
                }
            }
        } else {
            tracing::warn!("fluid: PEBBLE_FLUID_ROLE=follower, skipping leader election");
            start_fluid_follower_mode(
                db.clone(),
                FollowerBootstrapConfig {
                    db_url: db_url.clone(),
                    wal_config: wal_config.clone(),
                    engine_version: m3_cfg.engine_version.clone(),
                    order_available_includes_withdrawal_reserves,
                    replication_config: replication_config.clone(),
                    allow_promotion: false,
                    advance_committed_with_tip,
                    publish_committed_in_watchdog,
                    failure_policy: durability_config.failure_policy,
                    pg_unreachable_demote_after: durability_config.pg_unreachable_demote_after,
                },
                runtime_state.clone(),
            )
            .await
            .context("fluid: follower startup failed")?
        }
    } else {
        // Fluid disabled — check if a leader is active (startup guard).
        match fluid_bridge::probe_fluid_leader(&db_url).await {
            Ok(true) => {
                return Err(anyhow!(
                    "Fluid leader detected on lock(99,0). Set PEBBLE_FLUID_ENABLED=true or shut down the Fluid leader before running legacy instances."
                ));
            }
            Ok(false) => {
                tracing::info!("fluid: no leader detected, running as Legacy");
            }
            Err(err) => {
                tracing::warn!(error = ?err, "fluid: probe failed, assuming no leader — running as Legacy");
            }
        }
        // Spawn legacy probe task to detect a leader appearing at runtime.
        fluid_bridge::spawn_legacy_probe_task(db_url.clone(), fluid_leader_detected.clone());
        (
            fluid_bridge::WriteMode::Legacy,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    };
    fluid_bridge::store_write_mode(&write_mode_state, write_mode);
    tracing::info!(?write_mode, "fluid: write mode determined");

    let mut state = AppState {
        db,
        ledger_cfg,
        m3_cfg,
        auth_keys,
        account_lock_namespace,
        active_withdrawals_enabled,
        withdrawal_request_state_enabled,
        withdrawal_reject_ineligible_at_create,
        withdrawal_submission_saga_enabled,
        withdrawal_one_shot_auto_accept_enabled,
        withdrawal_one_shot_require_policy,
        order_available_includes_withdrawal_reserves,
        account_writer_enabled,
        rate_limiter: ApiRateLimiter::from_env().context("load api rate limit config")?,
        registration_cfg: RegistrationConfig::from_env().context("load registration config")?,
        faucet_cfg: FaucetConfig::from_env().context("load faucet config")?,
        google_oauth_cfg,
        disclosure_cfg: DisclosureServiceConfig::from_env()
            .context("load disclosure service config")?,
        market_assets_cfg: MarketAssetsConfig::from_env().context("load market assets config")?,
        settlement_cfg: SettlementConfig::from_env().context("load settlement config")?,
        perf_metrics,
        committee_submitter,
        public_markets_cache: public_cache::CoalescingCache::new(public_cache_max_entries),
        public_market_order_book_cache: public_cache::CoalescingCache::new(
            public_cache_max_entries,
        ),
        public_market_fills_cache: public_cache::CoalescingCache::new(public_cache_max_entries),
        public_stats_overview_cache: public_cache::CoalescingCache::new(public_cache_max_entries),
        public_market_stats_cache: public_cache::CoalescingCache::new(public_cache_max_entries),
        public_market_metadata_cache: public_cache::CoalescingCache::new(public_cache_max_entries),
        order_book_stream_hub: m4_views::OrderBookStreamHub::new(
            Duration::from_millis(orderbook_stream_poll_ms),
            Duration::from_millis(orderbook_stream_idle_ttl_ms),
        ),
        write_mode: write_mode_state.clone(),
        wal_handle,
        projection_tx,
        projector_handle: None,
        fluid,
        replication_broadcaster,
        wal_dir,
        leader_connection_healthy,
        fluid_leader_detected,
        leader_epoch,
        committed_offset,
        durability_config,
        reconcile_config,
        consecutive_ack_timeouts: Arc::new(AtomicU64::new(0)),
        pg_publish_unreachable_since,
        fluid_node_id,
    };
    if let Some(bootstrap) = projector_bootstrap {
        let handle = wal_projector::spawn_wal_projector(
            state.clone(),
            bootstrap.shadow_state,
            bootstrap.projector_config,
            bootstrap.wal_dir,
            bootstrap.after_offset,
            bootstrap.projection_rx,
        );
        state.projector_handle = Some(handle);
    }
    if state.perf_metrics.enabled() {
        let log_dir = state
            .perf_metrics
            .log_dir()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "disabled".to_string());
        tracing::info!(
            summary_seconds = state.perf_metrics.summary_period().as_secs(),
            window_size = state.perf_metrics.window_size(),
            max_series = state.perf_metrics.max_series(),
            log_dir = %log_dir,
            "api stage latency instrumentation enabled"
        );
        let perf_metrics_task = state.perf_metrics.clone();
        tokio::spawn(async move {
            perf_metrics_task.run_summary_loop().await;
        });
    } else {
        tracing::info!("api stage latency instrumentation disabled");
    }
    if state.settlement_cfg.enabled {
        let backfill_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = resolved_market_settlement_backfill_loop(backfill_state).await {
                tracing::error!(error = ?err, "resolved market settlement backfill loop crashed");
            }
        });
    }

    let cors_origin_matcher =
        cors_origin_matcher_from_env().context("parse PEBBLE_API_CORS_ALLOWED_ORIGINS")?;
    let cors_layer = CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(move |origin, _| {
            cors_origin_matcher.matches_header(origin) || is_loopback_dev_origin_header(origin)
        }))
        .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])
        .allow_credentials(true)
        .allow_headers([
            header::CONTENT_TYPE,
            header::AUTHORIZATION,
            HeaderName::from_static("x-api-key"),
            HeaderName::from_static("x-admin-key"),
            HeaderName::from_static("x-request-id"),
        ]);

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .route("/role", get(role))
        .route("/auth/session", get(m4_views::get_session))
        .route("/auth/google/start", get(start_google_oauth))
        .route("/auth/google/callback", get(handle_google_oauth_callback))
        .route("/auth/logout", post(logout_user_session))
        .route("/auth/register", post(register_user))
        .route("/accounts", post(create_account))
        .route("/accounts/:account_id", get(get_account))
        .route("/withdrawals", post(create_withdrawal_request))
        .route("/markets", get(list_markets).post(create_market))
        .route("/markets/metadata", get(m4_views::list_market_metadata))
        .route("/markets/:market_id", get(m4_views::get_market))
        .route(
            "/markets/:market_id/book",
            get(m4_views::get_market_order_book),
        )
        .route(
            "/markets/:market_id/book/stream",
            get(m4_views::stream_market_order_book),
        )
        .route(
            "/markets/:market_id/fills",
            get(m4_views::list_market_fills),
        )
        .route(
            "/markets/:market_id/chart/snapshot",
            get(m4_views::get_market_chart_snapshot),
        )
        .route(
            "/markets/:market_id/chart/updates",
            get(m4_views::stream_market_chart_updates),
        )
        .route("/markets/:market_id/close", post(close_market))
        .route("/markets/:market_id/resolve", post(resolve_market))
        .route("/stats/overview", get(m4_views::get_public_stats_overview))
        .route("/stats/markets", get(m4_views::list_public_market_stats))
        .route(
            "/orders",
            get(m3_orders::list_orders).post(m3_orders::place_order),
        )
        .route(
            "/v1/accounts/:account_id/orders/reconcile",
            get(m3_orders::reconcile_order_outcome),
        )
        .route("/orders/:order_id/cancel", post(m3_orders::cancel_order))
        .route("/fills", get(m3_orders::list_fills))
        .route("/me/summary", get(m4_views::get_my_summary))
        .route(
            "/me/funding-capacity",
            get(m4_views::get_my_funding_capacity),
        )
        .route("/me/deposits", post(create_deposit_request))
        .route(
            "/me/withdrawal-claims/pending",
            get(list_my_withdrawal_claims_pending),
        )
        .route(
            "/me/withdrawal-claims/claim",
            post(claim_my_withdrawal_claims),
        )
        .route(
            "/me/withdrawal-eligibility",
            get(get_my_withdrawal_eligibility),
        )
        .route("/me/positions", get(m4_views::list_my_positions))
        .route(
            "/me/portfolio-history",
            get(m4_views::list_my_portfolio_history),
        )
        .route(
            "/me/api-keys",
            get(list_my_api_keys).post(create_my_api_key),
        )
        .route("/me/api-keys/bootstrap", get(consume_my_bootstrap_api_key))
        .route("/me/api-keys/:key_id", delete(revoke_my_api_key))
        .route(
            "/me/deposit-instructions",
            get(m4_views::get_my_deposit_instructions),
        )
        .route(
            "/me/deposits/pending",
            get(m4_views::list_my_deposit_pendings),
        )
        .route(
            "/me/withdrawals/pending",
            get(m4_views::list_my_withdrawal_pendings),
        )
        .route("/me/receipts", get(m4_views::list_my_receipts))
        .route("/me/onboard", post(onboard_me_account))
        .route("/me/faucet", post(me_faucet_credit))
        .route("/admin/quarantine", get(m4_views::admin_list_quarantine))
        .route(
            "/admin/markets/:market_id/metadata",
            post(m4_views::admin_upsert_market_metadata),
        )
        .route(
            "/admin/assets/upload",
            post(m4_views::admin_upload_market_asset)
                .layer(DefaultBodyLimit::max(ADMIN_ASSET_UPLOAD_MAX_BYTES)),
        )
        .route(
            "/admin/markets/:market_id/settlement",
            get(admin_get_market_settlement),
        )
        .route(
            "/admin/markets/:market_id/settlement/retry",
            post(admin_retry_market_settlement),
        )
        .route(
            "/admin/markets/:market_id/settlement/deltas",
            get(admin_list_market_settlement_deltas),
        )
        .route(
            "/admin/quarantine/:contract_id/closeout",
            post(admin_closeout_quarantine_holding),
        )
        .route(
            "/admin/withdrawals/escalated",
            get(m4_views::admin_list_cancel_escalated),
        )
        .route(
            "/admin/withdrawals/escalated/:contract_id/reconcile",
            post(admin_reconcile_escalated_withdrawal),
        )
        .route(
            "/admin/treasury/inventory",
            get(m4_views::admin_treasury_inventory),
        )
        .route(
            "/admin/treasury/ops",
            get(m4_views::admin_list_treasury_ops),
        )
        .route("/admin/drift", get(m4_views::admin_drift_dashboard))
        .route(
            "/admin/disclosures/reference/fetch",
            post(admin_fetch_reference_disclosure),
        )
        .layer(middleware::from_fn_with_state(
            state.clone(),
            api_rate_limit_middleware,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            fatal_mode_guard_middleware,
        ))
        .layer(middleware::from_fn(request_correlation_middleware))
        .with_state(state)
        .layer(TraceLayer::new_for_http())
        .layer(cors_layer);

    tracing::info!(%addr, "pebble-api listening");

    let listener = tokio::net::TcpListener::bind(addr).await.context("bind")?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve")?;

    Ok(())
}

#[derive(Clone)]
struct AppState {
    db: PgPool,
    ledger_cfg: LedgerConfig,
    m3_cfg: m3_orders::M3ApiConfig,
    auth_keys: auth_keys::AuthKeyStore,
    account_lock_namespace: i32,
    active_withdrawals_enabled: bool,
    withdrawal_request_state_enabled: bool,
    withdrawal_reject_ineligible_at_create: bool,
    withdrawal_submission_saga_enabled: bool,
    withdrawal_one_shot_auto_accept_enabled: bool,
    withdrawal_one_shot_require_policy: bool,
    order_available_includes_withdrawal_reserves: bool,
    account_writer_enabled: bool,
    rate_limiter: ApiRateLimiter,
    registration_cfg: RegistrationConfig,
    faucet_cfg: FaucetConfig,
    google_oauth_cfg: Option<GoogleOAuthConfig>,
    disclosure_cfg: DisclosureServiceConfig,
    market_assets_cfg: MarketAssetsConfig,
    settlement_cfg: SettlementConfig,
    perf_metrics: perf_metrics::ApiPerfMetrics,
    committee_submitter: CommitteeSubmitter,
    public_markets_cache: public_cache::CoalescingCache<Vec<MarketRow>>,
    public_market_order_book_cache: public_cache::CoalescingCache<m4_views::MarketOrderBookView>,
    public_market_fills_cache: public_cache::CoalescingCache<Vec<m4_views::MarketFillView>>,
    public_stats_overview_cache: public_cache::CoalescingCache<m4_views::PublicStatsOverviewView>,
    public_market_stats_cache: public_cache::CoalescingCache<Vec<m4_views::PublicMarketStatsView>>,
    public_market_metadata_cache: public_cache::CoalescingCache<Vec<m4_views::MarketMetadataView>>,
    order_book_stream_hub: m4_views::OrderBookStreamHub,
    /// Fluid write mode — may change during follower promotion.
    write_mode: Arc<AtomicU8>,
    /// Fluid engine state — present when Fluid mode is enabled.
    fluid: Option<Arc<pebble_fluid::engine::ExchangeEngine>>,
    /// WAL writer handle — present for Fluid leader/follower modes.
    wal_handle: Option<pebble_fluid::wal::WalWriterHandle>,
    /// Transition cache sender used by hot path to accelerate async WAL projection.
    projection_tx: Option<tokio::sync::mpsc::Sender<wal_projector::ProjectionPayload>>,
    /// Async projector handle, used for persistence fences.
    projector_handle: Option<wal_projector::WalProjectorHandle>,
    /// Leader-side replication fanout state (None when not leader).
    replication_broadcaster: Option<pebble_fluid::replication::ReplicationBroadcaster>,
    /// Local WAL directory for role metrics.
    wal_dir: Option<PathBuf>,
    /// Runtime guard for FluidLeader mode: set to `false` when leader connection is lost.
    leader_connection_healthy: Arc<AtomicBool>,
    /// Runtime guard for Legacy mode: set to `true` when a Fluid leader is detected.
    fluid_leader_detected: Arc<AtomicBool>,
    /// Current leader epoch observed by this node (leader self, or follower remote epoch).
    leader_epoch: Arc<AtomicU64>,
    /// Current committed offset observed by this node.
    committed_offset: Arc<AtomicU64>,
    /// Durability policy for post-fsync response behavior.
    durability_config: FluidDurabilityConfig,
    /// Reconciliation policy and signing keys for unknown-outcome responses.
    reconcile_config: ReconcileConfig,
    /// Consecutive semisync durable-ack timeouts (strict mode demotion guard).
    consecutive_ack_timeouts: Arc<AtomicU64>,
    /// Tracks how long committed-offset publication to PG has been unavailable.
    pg_publish_unreachable_since: Arc<std::sync::Mutex<Option<Instant>>>,
    /// Local fluid node id for fenced committed-offset publication.
    fluid_node_id: Option<String>,
}

impl AppState {
    fn current_write_mode(&self) -> fluid_bridge::WriteMode {
        fluid_bridge::load_write_mode(&self.write_mode)
    }

    fn sign_reconcile_token(&self, payload: &ReconcileTokenPayload) -> Result<String, ApiError> {
        sign_reconcile_token(&self.reconcile_config, payload)
    }

    pub(crate) fn verify_reconcile_token(
        &self,
        account_id: &str,
        nonce: i64,
        wal_offset: u64,
        token: &str,
    ) -> Result<VerifiedReconcileToken, ApiError> {
        verify_reconcile_token(&self.reconcile_config, account_id, nonce, wal_offset, token)
    }

    pub(crate) fn pending_reconciliation_timed_out(&self, unknown_at: DateTime<Utc>) -> bool {
        pending_reconciliation_timed_out(&self.reconcile_config, unknown_at)
    }
}

fn duration_to_millis_i64(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn sign_reconcile_token(
    config: &ReconcileConfig,
    payload: &ReconcileTokenPayload,
) -> Result<String, ApiError> {
    let key = config
        .hmac_key
        .as_ref()
        .ok_or_else(|| ApiError::internal("reconciliation HMAC key is not configured"))?;
    let payload_bytes = rmp_serde::to_vec(payload)
        .context("serialize reconciliation token payload")
        .map_err(ApiError::from)?;
    let mut mac = HmacSha256::new_from_slice(key)
        .context("initialize reconciliation HMAC signer")
        .map_err(ApiError::from)?;
    mac.update(&payload_bytes);
    let signature = mac.finalize().into_bytes();
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload_bytes);
    let signature_b64 = URL_SAFE_NO_PAD.encode(signature);
    Ok(format!("{payload_b64}.{signature_b64}"))
}

fn verify_reconcile_token(
    config: &ReconcileConfig,
    account_id: &str,
    nonce: i64,
    wal_offset: u64,
    token: &str,
) -> Result<VerifiedReconcileToken, ApiError> {
    let (payload_b64, signature_b64) = token
        .split_once('.')
        .ok_or_else(|| ApiError::bad_request("invalid reconciliation token format"))?;
    let payload_bytes = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .context("decode reconciliation payload")
        .map_err(|_| ApiError::bad_request("invalid reconciliation token payload"))?;
    let signature_bytes = URL_SAFE_NO_PAD
        .decode(signature_b64)
        .context("decode reconciliation signature")
        .map_err(|_| ApiError::bad_request("invalid reconciliation token signature"))?;

    let mut keys = Vec::with_capacity(2);
    if let Some(active) = config.hmac_key.as_ref() {
        keys.push(active.as_slice());
    }
    if let Some(previous) = config.hmac_key_previous.as_ref() {
        keys.push(previous.as_slice());
    }
    if keys.is_empty() {
        return Err(ApiError::internal(
            "reconciliation HMAC key is not configured",
        ));
    }

    let mut verified = false;
    for key in keys {
        let mut mac = HmacSha256::new_from_slice(key)
            .context("initialize reconciliation HMAC verifier")
            .map_err(ApiError::from)?;
        mac.update(&payload_bytes);
        if mac.verify_slice(&signature_bytes).is_ok() {
            verified = true;
            break;
        }
    }
    if !verified {
        return Err(ApiError::bad_request(
            "invalid reconciliation token signature",
        ));
    }

    let payload: ReconcileTokenPayload = rmp_serde::from_slice(&payload_bytes)
        .context("decode reconciliation token payload")
        .map_err(|_| ApiError::bad_request("invalid reconciliation token payload"))?;
    if payload.account_id != account_id
        || payload.nonce != nonce
        || payload.wal_offset != wal_offset
    {
        return Err(ApiError::bad_request(
            "reconciliation token does not match request parameters",
        ));
    }
    let unknown_at = DateTime::<Utc>::from_timestamp_millis(payload.unknown_at_unix_ms)
        .ok_or_else(|| ApiError::bad_request("invalid reconciliation token timestamp"))?;
    Ok(VerifiedReconcileToken { unknown_at })
}

fn pending_reconciliation_timed_out(config: &ReconcileConfig, unknown_at: DateTime<Utc>) -> bool {
    let now_ms = Utc::now().timestamp_millis();
    let unknown_ms = unknown_at.timestamp_millis();
    if now_ms <= unknown_ms {
        return false;
    }
    let elapsed_ms = now_ms.saturating_sub(unknown_ms);
    let ttl_ms = duration_to_millis_i64(config.pending_ttl);
    let skew_ms = duration_to_millis_i64(config.ttl_skew_tolerance);
    elapsed_ms > ttl_ms.saturating_add(skew_ms)
}

#[derive(Debug, Clone)]
struct LedgerConfig {
    channel: tonic::transport::Channel,
    auth_header: Option<MetadataValue<tonic::metadata::Ascii>>,
    user_id: String,
    committee_party: String,
}

impl LedgerConfig {
    async fn from_env() -> Result<Self> {
        let ledger_host = std::env::var("PEBBLE_CANTON_LEDGER_API_HOST")
            .unwrap_or_else(|_| "127.0.0.1".to_string());

        let ledger_port = std::env::var("PEBBLE_CANTON_LEDGER_API_PORT")
            .unwrap_or_else(|_| "6865".to_string())
            .parse::<u16>()
            .context("parse PEBBLE_CANTON_LEDGER_API_PORT")?;

        let committee_party = std::env::var("PEBBLE_COMMITTEE_PARTY")
            .context("PEBBLE_COMMITTEE_PARTY is required")?;

        let user_id =
            std::env::var("PEBBLE_LEDGER_USER_ID").unwrap_or_else(|_| "pebble-api".to_string());

        let auth_header = match std::env::var("PEBBLE_CANTON_AUTH_TOKEN") {
            Ok(token) if !token.trim().is_empty() => Some(parse_auth_header(&token)?),
            _ => None,
        };

        let endpoint = Endpoint::from_shared(format!("http://{}:{}", ledger_host, ledger_port))
            .context("build ledger endpoint")?;
        let channel = endpoint.connect().await.context("connect to ledger")?;

        Ok(Self {
            channel,
            auth_header,
            user_id,
            committee_party,
        })
    }
}

#[derive(Clone)]
struct CommitteeSubmitter {
    mode: CommitteeSubmitterMode,
}

#[derive(Clone)]
enum CommitteeSubmitterMode {
    Direct {
        channel: tonic::transport::Channel,
        auth_header: Option<MetadataValue<tonic::metadata::Ascii>>,
    },
}

impl CommitteeSubmitter {
    fn direct(
        channel: tonic::transport::Channel,
        auth_header: Option<MetadataValue<tonic::metadata::Ascii>>,
    ) -> Self {
        Self {
            mode: CommitteeSubmitterMode::Direct {
                channel,
                auth_header,
            },
        }
    }

    async fn submit_and_wait_for_transaction(
        &self,
        commands: lapi::Commands,
    ) -> Result<lapi::Transaction> {
        let command_id = commands.command_id.clone();
        let command_count = commands.commands.len();
        tracing::debug!(command_id = %command_id, command_count, "submitting committee command");

        match &self.mode {
            CommitteeSubmitterMode::Direct {
                channel,
                auth_header,
            } => {
                let mut client =
                    lapi::command_service_client::CommandServiceClient::new(channel.clone());
                let mut req = Request::new(lapi::SubmitAndWaitForTransactionRequest {
                    commands: Some(commands),
                    transaction_format: None,
                });
                if let Some(auth_header) = auth_header.clone() {
                    req.metadata_mut().insert("authorization", auth_header);
                }

                let resp = client
                    .submit_and_wait_for_transaction(req)
                    .await
                    .context("submit_and_wait_for_transaction")?
                    .into_inner();
                let tx = resp
                    .transaction
                    .ok_or_else(|| anyhow!("missing transaction in response"))?;
                tracing::info!(
                    command_id = %command_id,
                    update_id = %tx.update_id,
                    offset = tx.offset,
                    "committee command committed"
                );

                Ok(tx)
            }
        }
    }
}

async fn request_correlation_middleware(request: axum::extract::Request, next: Next) -> Response {
    let request_id = request_id_from_headers(request.headers());

    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let started = Instant::now();
    let span = tracing::info_span!(
        "api_request",
        request_id = %request_id,
        method = %method,
        path = %path
    );
    let mut response = next.run(request).instrument(span).await;
    let elapsed_ms = started.elapsed().as_millis();
    tracing::info!(status = %response.status(), elapsed_ms, "api request completed");

    if let Ok(header_value) = HeaderValue::from_str(&request_id) {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-request-id"), header_value);
    }

    response
}

async fn fatal_mode_guard_middleware(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    if state.current_write_mode() == fluid_bridge::WriteMode::Fatal {
        let path = request.uri().path();
        if path != "/healthz" && path != "/metrics" && path != "/role" {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "error": "node is in fatal mode",
                    "code": "node_fatal"
                })),
            )
                .into_response();
        }
    }
    next.run(request).await
}

fn request_id_from_headers(headers: &HeaderMap) -> String {
    let header_name = HeaderName::from_static("x-request-id");
    if let Some(raw) = headers.get(header_name) {
        if let Ok(value) = raw.to_str() {
            let trimmed = value.trim();
            if !trimmed.is_empty() && trimmed.len() <= 128 {
                return trimmed.to_string();
            }
        }
    }

    Uuid::new_v4().to_string()
}

fn parse_auth_header(token: &str) -> Result<MetadataValue<tonic::metadata::Ascii>> {
    let token = token.trim();
    let header_value = if token.to_ascii_lowercase().starts_with("bearer ") {
        token.to_string()
    } else {
        format!("Bearer {token}")
    };

    header_value
        .parse()
        .context("parse auth token as gRPC metadata value")
}

fn parse_env_bool(key: &str, raw: &str) -> Result<bool> {
    let v = raw.trim().to_ascii_lowercase();
    match v.as_str() {
        "1" | "true" | "yes" | "y" | "on" => Ok(true),
        "0" | "false" | "no" | "n" | "off" => Ok(false),
        "" => Err(anyhow!("{key} must not be empty")),
        _ => Err(anyhow!("{key} must be a boolean (got: {raw:?})")),
    }
}

fn parse_account_lock_namespace_env() -> Result<i32> {
    let raw = std::env::var("PEBBLE_ACCOUNT_LOCK_NAMESPACE").unwrap_or_else(|_| "11".to_string());
    let value = raw
        .parse::<i32>()
        .context("parse PEBBLE_ACCOUNT_LOCK_NAMESPACE")?;
    if value <= 0 {
        return Err(anyhow!("PEBBLE_ACCOUNT_LOCK_NAMESPACE must be > 0"));
    }
    Ok(value)
}

fn parse_fluid_role() -> Result<FluidRuntimeRole> {
    let raw = std::env::var("PEBBLE_FLUID_ROLE").unwrap_or_else(|_| "auto".to_string());
    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "auto" => Ok(FluidRuntimeRole::Auto),
        "leader" => Ok(FluidRuntimeRole::Leader),
        "follower" => Ok(FluidRuntimeRole::Follower),
        _ => Err(anyhow!(
            "PEBBLE_FLUID_ROLE must be one of: auto, leader, follower"
        )),
    }
}

fn default_fluid_node_id() -> String {
    if let Ok(node_id) = std::env::var("HOSTNAME") {
        let trimmed = node_id.trim();
        if !trimmed.is_empty() {
            return trimmed.to_string();
        }
    }
    "pebble-node".to_string()
}

fn parse_fluid_replication_config() -> Result<FluidReplicationConfig> {
    let node_id = std::env::var("PEBBLE_FLUID_NODE_ID")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(default_fluid_node_id);
    if node_id.trim().is_empty() {
        return Err(anyhow!("PEBBLE_FLUID_NODE_ID must not be empty"));
    }

    let bind_addr = std::env::var("PEBBLE_FLUID_REPLICATION_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:3031".to_string());
    let bind_socket: SocketAddr = bind_addr
        .parse()
        .context("parse PEBBLE_FLUID_REPLICATION_ADDR")?;

    let advertise_addr = std::env::var("PEBBLE_FLUID_REPLICATION_ADVERTISE_ADDR")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| format!("{node_id}:{}", bind_socket.port()));

    let auth_token = std::env::var("PEBBLE_FLUID_REPLICATION_AUTH_TOKEN")
        .map(|value| value.trim().to_string())
        .unwrap_or_default();
    if auth_token.is_empty() {
        return Err(anyhow!(
            "PEBBLE_FLUID_REPLICATION_AUTH_TOKEN is required when PEBBLE_FLUID_ENABLED=true"
        ));
    }

    let heartbeat_ms = std::env::var("PEBBLE_FLUID_HEARTBEAT_INTERVAL_MS")
        .unwrap_or_else(|_| "250".to_string())
        .trim()
        .parse::<u64>()
        .context("parse PEBBLE_FLUID_HEARTBEAT_INTERVAL_MS")?;
    if heartbeat_ms == 0 {
        return Err(anyhow!(
            "PEBBLE_FLUID_HEARTBEAT_INTERVAL_MS must be greater than zero"
        ));
    }
    let default_timeout_ms = heartbeat_ms.saturating_mul(4).max(500);
    let follower_timeout_ms = std::env::var("PEBBLE_FLUID_FOLLOWER_HEARTBEAT_TIMEOUT_MS")
        .unwrap_or_else(|_| default_timeout_ms.to_string())
        .trim()
        .parse::<u64>()
        .context("parse PEBBLE_FLUID_FOLLOWER_HEARTBEAT_TIMEOUT_MS")?;
    if follower_timeout_ms == 0 {
        return Err(anyhow!(
            "PEBBLE_FLUID_FOLLOWER_HEARTBEAT_TIMEOUT_MS must be greater than zero"
        ));
    }
    if follower_timeout_ms < heartbeat_ms {
        return Err(anyhow!(
            "PEBBLE_FLUID_FOLLOWER_HEARTBEAT_TIMEOUT_MS must be >= PEBBLE_FLUID_HEARTBEAT_INTERVAL_MS"
        ));
    }

    Ok(FluidReplicationConfig {
        node_id,
        bind_addr,
        advertise_addr,
        auth_token,
        heartbeat_interval: Duration::from_millis(heartbeat_ms),
        follower_heartbeat_timeout: Duration::from_millis(follower_timeout_ms),
    })
}

fn parse_fluid_durability_config() -> Result<FluidDurabilityConfig> {
    let mode_raw = std::env::var("PEBBLE_FLUID_REPLICATION_MODE")
        .unwrap_or_else(|_| "async".to_string())
        .trim()
        .to_ascii_lowercase();
    let replication_mode = match mode_raw.as_str() {
        "async" => FluidReplicationMode::Async,
        "semi-sync" | "semisync" => FluidReplicationMode::SemiSync,
        _ => {
            return Err(anyhow!(
                "PEBBLE_FLUID_REPLICATION_MODE must be one of: async, semi-sync"
            ));
        }
    };

    let min_ack_count = std::env::var("PEBBLE_FLUID_MIN_ACK_COUNT")
        .unwrap_or_else(|_| "1".to_string())
        .trim()
        .parse::<usize>()
        .context("parse PEBBLE_FLUID_MIN_ACK_COUNT")?;
    if min_ack_count == 0 {
        return Err(anyhow!("PEBBLE_FLUID_MIN_ACK_COUNT must be > 0"));
    }

    let ack_timeout_ms = std::env::var("PEBBLE_FLUID_ACK_TIMEOUT_MS")
        .unwrap_or_else(|_| "250".to_string())
        .trim()
        .parse::<u64>()
        .context("parse PEBBLE_FLUID_ACK_TIMEOUT_MS")?;
    if ack_timeout_ms == 0 {
        return Err(anyhow!("PEBBLE_FLUID_ACK_TIMEOUT_MS must be > 0"));
    }

    let policy_raw = std::env::var("PEBBLE_FLUID_POST_FSYNC_FAILURE_POLICY")
        .unwrap_or_else(|_| "strict".to_string())
        .trim()
        .to_ascii_lowercase();
    let failure_policy = match policy_raw.as_str() {
        "strict" => PostFsyncFailurePolicy::Strict,
        "degrade-to-async" | "degrade_to_async" | "degrade" => {
            PostFsyncFailurePolicy::DegradeToAsync
        }
        _ => {
            return Err(anyhow!(
                "PEBBLE_FLUID_POST_FSYNC_FAILURE_POLICY must be one of: strict, degrade-to-async"
            ));
        }
    };

    let ack_timeout_demote_count = std::env::var("PEBBLE_FLUID_ACK_TIMEOUT_DEMOTE_COUNT")
        .unwrap_or_else(|_| "3".to_string())
        .trim()
        .parse::<u64>()
        .context("parse PEBBLE_FLUID_ACK_TIMEOUT_DEMOTE_COUNT")?;
    if ack_timeout_demote_count == 0 {
        return Err(anyhow!("PEBBLE_FLUID_ACK_TIMEOUT_DEMOTE_COUNT must be > 0"));
    }

    let pg_unreachable_demote_secs = std::env::var("PEBBLE_FLUID_PG_UNREACHABLE_DEMOTE_SECS")
        .unwrap_or_else(|_| "30".to_string())
        .trim()
        .parse::<u64>()
        .context("parse PEBBLE_FLUID_PG_UNREACHABLE_DEMOTE_SECS")?;
    if pg_unreachable_demote_secs == 0 {
        return Err(anyhow!(
            "PEBBLE_FLUID_PG_UNREACHABLE_DEMOTE_SECS must be > 0"
        ));
    }

    Ok(FluidDurabilityConfig {
        replication_mode,
        min_ack_count,
        ack_timeout: Duration::from_millis(ack_timeout_ms),
        failure_policy,
        ack_timeout_demote_count,
        pg_unreachable_demote_after: Duration::from_secs(pg_unreachable_demote_secs),
    })
}

fn parse_reconcile_config() -> Result<ReconcileConfig> {
    let pending_ttl_secs = std::env::var("PEBBLE_FLUID_RECONCILE_PENDING_TTL_SECS")
        .unwrap_or_else(|_| "60".to_string())
        .trim()
        .parse::<u64>()
        .context("parse PEBBLE_FLUID_RECONCILE_PENDING_TTL_SECS")?;
    if pending_ttl_secs == 0 {
        return Err(anyhow!(
            "PEBBLE_FLUID_RECONCILE_PENDING_TTL_SECS must be > 0"
        ));
    }

    let hmac_key = match std::env::var("PEBBLE_FLUID_RECONCILE_HMAC_KEY") {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.as_bytes().to_vec())
            }
        }
        Err(_) => None,
    };
    let hmac_key_previous = match std::env::var("PEBBLE_FLUID_RECONCILE_HMAC_KEY_PREVIOUS") {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.as_bytes().to_vec())
            }
        }
        Err(_) => None,
    };

    Ok(ReconcileConfig {
        pending_ttl: Duration::from_secs(pending_ttl_secs),
        hmac_key,
        hmac_key_previous,
        ttl_skew_tolerance: Duration::from_secs(RECONCILE_TTL_SKEW_TOLERANCE_SECS),
    })
}

fn parse_wal_config() -> Result<pebble_fluid::wal::WalConfig> {
    let mut config = pebble_fluid::wal::WalConfig::default();

    if let Ok(raw) = std::env::var("PEBBLE_WAL_DIR") {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("PEBBLE_WAL_DIR must not be empty"));
        }
        config.wal_dir = PathBuf::from(trimmed);
    }

    if let Ok(raw) = std::env::var("PEBBLE_WAL_MAX_BATCH_SIZE") {
        let value = raw
            .trim()
            .parse::<usize>()
            .context("parse PEBBLE_WAL_MAX_BATCH_SIZE")?;
        if value == 0 {
            return Err(anyhow!("PEBBLE_WAL_MAX_BATCH_SIZE must be > 0"));
        }
        config.max_batch_size = value;
    }

    if let Ok(raw) = std::env::var("PEBBLE_WAL_MAX_BATCH_WAIT_MS") {
        let value = raw
            .trim()
            .parse::<u64>()
            .context("parse PEBBLE_WAL_MAX_BATCH_WAIT_MS")?;
        if value == 0 {
            return Err(anyhow!("PEBBLE_WAL_MAX_BATCH_WAIT_MS must be > 0"));
        }
        config.max_batch_wait = Duration::from_millis(value);
    }

    if let Ok(raw) = std::env::var("PEBBLE_WAL_SEGMENT_SIZE_BYTES") {
        let value = raw
            .trim()
            .parse::<u64>()
            .context("parse PEBBLE_WAL_SEGMENT_SIZE_BYTES")?;
        if value == 0 {
            return Err(anyhow!("PEBBLE_WAL_SEGMENT_SIZE_BYTES must be > 0"));
        }
        config.segment_size_bytes = value;
    }

    Ok(config)
}

fn parse_projector_config() -> Result<wal_projector::WalProjectorConfig> {
    let mut config = wal_projector::WalProjectorConfig::default();
    if let Ok(raw) = std::env::var("PEBBLE_WAL_PROJECTION_LAG_THRESHOLD_SECS") {
        let value = raw
            .trim()
            .parse::<u64>()
            .context("parse PEBBLE_WAL_PROJECTION_LAG_THRESHOLD_SECS")?;
        if value == 0 {
            return Err(anyhow!(
                "PEBBLE_WAL_PROJECTION_LAG_THRESHOLD_SECS must be > 0"
            ));
        }
        config.projection_lag_unhealthy_threshold = Duration::from_secs(value);
    }
    Ok(config)
}

async fn upsert_account_lock_capability(
    db: &PgPool,
    service_name: &str,
    lock_namespace: i32,
    active_withdrawals_enabled: bool,
    metadata: &serde_json::Value,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO account_lock_capabilities (
          service_name,
          lock_namespace,
          active_withdrawals_enabled,
          metadata,
          updated_at
        )
        VALUES ($1, $2, $3, $4, now())
        ON CONFLICT (service_name) DO UPDATE SET
          lock_namespace = EXCLUDED.lock_namespace,
          active_withdrawals_enabled = EXCLUDED.active_withdrawals_enabled,
          metadata = EXCLUDED.metadata,
          updated_at = now()
        "#,
    )
    .bind(service_name)
    .bind(lock_namespace)
    .bind(active_withdrawals_enabled)
    .bind(metadata)
    .execute(db)
    .await
    .with_context(|| format!("upsert account lock capability for service {service_name}"))?;

    Ok(())
}

async fn ensure_account_lock_namespace_parity(
    db: &PgPool,
    expected_namespace: i32,
    expected_active_withdrawals_enabled: bool,
    required_services: &[&str],
) -> Result<()> {
    #[derive(sqlx::FromRow)]
    struct CapabilityRow {
        service_name: String,
        lock_namespace: i32,
        active_withdrawals_enabled: bool,
    }

    let required = required_services
        .iter()
        .map(|service| (*service).to_string())
        .collect::<Vec<_>>();

    let rows: Vec<CapabilityRow> = sqlx::query_as(
        r#"
        SELECT service_name, lock_namespace, active_withdrawals_enabled
        FROM account_lock_capabilities
        WHERE service_name = ANY($1::TEXT[])
        "#,
    )
    .bind(&required)
    .fetch_all(db)
    .await
    .context("query account lock capabilities for parity check")?;

    if rows.len() != required.len() {
        let missing = required
            .into_iter()
            .filter(|service| !rows.iter().any(|row| row.service_name == *service))
            .collect::<Vec<_>>();
        return Err(anyhow!(
            "account lock namespace parity check missing services: {}",
            missing.join(",")
        ));
    }

    let mismatches = rows
        .iter()
        .filter(|row| row.lock_namespace != expected_namespace)
        .map(|row| format!("{}={}", row.service_name, row.lock_namespace))
        .collect::<Vec<_>>();
    if !mismatches.is_empty() {
        return Err(anyhow!(
            "account lock namespace mismatch (expected {}): {}",
            expected_namespace,
            mismatches.join(",")
        ));
    }
    if expected_active_withdrawals_enabled {
        let disabled = rows
            .iter()
            .filter(|row| !row.active_withdrawals_enabled)
            .map(|row| row.service_name.clone())
            .collect::<Vec<_>>();
        if !disabled.is_empty() {
            return Err(anyhow!(
                "active withdrawals enabled but required services are not in active mode: {}",
                disabled.join(",")
            ));
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct SettlementConfig {
    enabled: bool,
    backfill_poll_ms: u64,
}

impl SettlementConfig {
    fn from_env() -> Result<Self> {
        let enabled = match std::env::var("PEBBLE_MARKET_SETTLEMENT_ENABLE") {
            Ok(raw) => parse_env_bool("PEBBLE_MARKET_SETTLEMENT_ENABLE", &raw)?,
            Err(_) => true,
        };
        let backfill_poll_ms = std::env::var("PEBBLE_MARKET_SETTLEMENT_BACKFILL_POLL_MS")
            .unwrap_or_else(|_| "30000".to_string())
            .parse::<u64>()
            .context("parse PEBBLE_MARKET_SETTLEMENT_BACKFILL_POLL_MS")?;
        if backfill_poll_ms == 0 {
            return Err(anyhow!(
                "PEBBLE_MARKET_SETTLEMENT_BACKFILL_POLL_MS must be > 0"
            ));
        }

        Ok(Self {
            enabled,
            backfill_poll_ms,
        })
    }
}

#[derive(Debug, Clone)]
struct MarketAssetsConfig {
    data_dir: PathBuf,
    public_base_url: String,
    max_upload_bytes: usize,
}

impl MarketAssetsConfig {
    fn from_env() -> Result<Self> {
        let data_dir = std::env::var("PEBBLE_ASSETS_DATA_DIR")
            .unwrap_or_else(|_| "./data/assets".to_string())
            .trim()
            .to_string();
        if data_dir.is_empty() {
            return Err(anyhow!("PEBBLE_ASSETS_DATA_DIR must be non-empty"));
        }

        let public_base_url = std::env::var("PEBBLE_ASSETS_PUBLIC_BASE_URL")
            .ok()
            .map(|raw| raw.trim().trim_end_matches('/').to_string())
            .filter(|value| !value.is_empty())
            .or_else(|| {
                std::env::var("PEBBLE_PUBLIC_ASSETS_HOST")
                    .ok()
                    .map(|raw| raw.trim().to_string())
                    .filter(|value| !value.is_empty())
                    .map(|host| format!("https://{host}/assets"))
            })
            .or_else(|| {
                std::env::var("PEBBLE_PUBLIC_API_HOST")
                    .ok()
                    .map(|raw| raw.trim().to_string())
                    .filter(|value| !value.is_empty())
                    .map(|host| format!("https://{host}/assets"))
            })
            .unwrap_or_else(|| "http://127.0.0.1:3060/assets".to_string());
        if public_base_url.is_empty() {
            return Err(anyhow!("PEBBLE_ASSETS_PUBLIC_BASE_URL must be non-empty"));
        }

        let max_upload_bytes = std::env::var("PEBBLE_ASSETS_MAX_UPLOAD_BYTES")
            .unwrap_or_else(|_| ADMIN_ASSET_UPLOAD_MAX_BYTES.to_string())
            .trim()
            .parse::<usize>()
            .context("parse PEBBLE_ASSETS_MAX_UPLOAD_BYTES")?;
        if max_upload_bytes == 0 {
            return Err(anyhow!("PEBBLE_ASSETS_MAX_UPLOAD_BYTES must be > 0"));
        }

        Ok(Self {
            data_dir: PathBuf::from(data_dir),
            public_base_url,
            max_upload_bytes,
        })
    }
}

#[derive(Debug, Clone)]
struct RegistrationConfig {
    json_api_base: String,
    json_api_auth_header: Option<String>,
    party_hint_prefix: String,
    auto_grant_user_rights: bool,
    http_client: reqwest::Client,
}

impl RegistrationConfig {
    fn from_env() -> Result<Self> {
        let json_api_base = std::env::var("PEBBLE_CANTON_JSON_API_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:7575".to_string())
            .trim()
            .trim_end_matches('/')
            .to_string();
        if json_api_base.is_empty() {
            return Err(anyhow!("PEBBLE_CANTON_JSON_API_URL must be non-empty"));
        }

        let json_api_auth_header = std::env::var("PEBBLE_CANTON_JWT")
            .ok()
            .map(|raw| raw.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(|token| {
                if token.to_ascii_lowercase().starts_with("bearer ") {
                    token
                } else {
                    format!("Bearer {token}")
                }
            });

        let party_hint_prefix = std::env::var("PEBBLE_REGISTRATION_PARTY_HINT_PREFIX")
            .unwrap_or_else(|_| "PebbleWebUser".to_string())
            .trim()
            .to_string();
        if party_hint_prefix.is_empty() {
            return Err(anyhow!(
                "PEBBLE_REGISTRATION_PARTY_HINT_PREFIX must be non-empty"
            ));
        }

        let auto_grant_user_rights =
            match std::env::var("PEBBLE_REGISTRATION_AUTO_GRANT_USER_RIGHTS") {
                Ok(raw) => parse_env_bool("PEBBLE_REGISTRATION_AUTO_GRANT_USER_RIGHTS", &raw)?,
                Err(_) => false,
            };

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .context("build registration HTTP client")?;

        Ok(Self {
            json_api_base,
            json_api_auth_header,
            party_hint_prefix,
            auto_grant_user_rights,
            http_client,
        })
    }
}

#[derive(Debug, Clone)]
struct FaucetConfig {
    enabled: bool,
    default_amount_minor: i64,
    max_amount_minor: i64,
}

impl FaucetConfig {
    fn from_env() -> Result<Self> {
        let enabled = match std::env::var("PEBBLE_API_FAUCET_ENABLED") {
            Ok(raw) => parse_env_bool("PEBBLE_API_FAUCET_ENABLED", &raw)?,
            Err(_) => true,
        };

        let default_amount_minor = match std::env::var("PEBBLE_API_FAUCET_DEFAULT_AMOUNT_MINOR") {
            Ok(raw) => raw
                .trim()
                .parse::<i64>()
                .context("parse PEBBLE_API_FAUCET_DEFAULT_AMOUNT_MINOR")?,
            Err(_) => 1_000_000,
        };
        if default_amount_minor <= 0 {
            return Err(anyhow!(
                "PEBBLE_API_FAUCET_DEFAULT_AMOUNT_MINOR must be > 0"
            ));
        }

        let max_amount_minor = match std::env::var("PEBBLE_API_FAUCET_MAX_AMOUNT_MINOR") {
            Ok(raw) => raw
                .trim()
                .parse::<i64>()
                .context("parse PEBBLE_API_FAUCET_MAX_AMOUNT_MINOR")?,
            Err(_) => 1_000_000,
        };
        if max_amount_minor <= 0 {
            return Err(anyhow!("PEBBLE_API_FAUCET_MAX_AMOUNT_MINOR must be > 0"));
        }

        if default_amount_minor > max_amount_minor {
            return Err(anyhow!(
                "PEBBLE_API_FAUCET_DEFAULT_AMOUNT_MINOR must be <= PEBBLE_API_FAUCET_MAX_AMOUNT_MINOR"
            ));
        }

        Ok(Self {
            enabled,
            default_amount_minor,
            max_amount_minor,
        })
    }
}

#[derive(Debug, Clone)]
struct GoogleOAuthConfig {
    client_id: String,
    client_secret: String,
    redirect_uri: String,
    auth_url: String,
    token_url: String,
    userinfo_url: String,
    success_redirect_url: String,
    failure_redirect_url: String,
    session_cookie_secure: bool,
    session_cookie_domain: Option<String>,
    session_ttl: Duration,
    oauth_state_ttl: Duration,
    http_client: reqwest::Client,
}

impl GoogleOAuthConfig {
    fn from_env() -> Result<Option<Self>> {
        let enabled = match std::env::var("PEBBLE_API_GOOGLE_OAUTH_ENABLED") {
            Ok(raw) => parse_env_bool("PEBBLE_API_GOOGLE_OAUTH_ENABLED", &raw)?,
            Err(_) => false,
        };
        if !enabled {
            return Ok(None);
        }

        let client_id = std::env::var("PEBBLE_API_GOOGLE_OAUTH_CLIENT_ID")
            .context("PEBBLE_API_GOOGLE_OAUTH_CLIENT_ID is required when Google OAuth is enabled")?
            .trim()
            .to_string();
        if client_id.is_empty() {
            return Err(anyhow!(
                "PEBBLE_API_GOOGLE_OAUTH_CLIENT_ID must be non-empty when Google OAuth is enabled"
            ));
        }

        let client_secret = std::env::var("PEBBLE_API_GOOGLE_OAUTH_CLIENT_SECRET")
            .context(
                "PEBBLE_API_GOOGLE_OAUTH_CLIENT_SECRET is required when Google OAuth is enabled",
            )?
            .trim()
            .to_string();
        if client_secret.is_empty() {
            return Err(anyhow!(
                "PEBBLE_API_GOOGLE_OAUTH_CLIENT_SECRET must be non-empty when Google OAuth is enabled"
            ));
        }

        let redirect_uri = std::env::var("PEBBLE_API_GOOGLE_OAUTH_REDIRECT_URI")
            .context(
                "PEBBLE_API_GOOGLE_OAUTH_REDIRECT_URI is required when Google OAuth is enabled",
            )?
            .trim()
            .to_string();
        if redirect_uri.is_empty() {
            return Err(anyhow!(
                "PEBBLE_API_GOOGLE_OAUTH_REDIRECT_URI must be non-empty when Google OAuth is enabled"
            ));
        }

        let auth_url = std::env::var("PEBBLE_API_GOOGLE_OAUTH_AUTH_URL")
            .unwrap_or_else(|_| "https://accounts.google.com/o/oauth2/v2/auth".to_string())
            .trim()
            .to_string();
        if auth_url.is_empty() {
            return Err(anyhow!(
                "PEBBLE_API_GOOGLE_OAUTH_AUTH_URL must be non-empty"
            ));
        }

        let token_url = std::env::var("PEBBLE_API_GOOGLE_OAUTH_TOKEN_URL")
            .unwrap_or_else(|_| "https://oauth2.googleapis.com/token".to_string())
            .trim()
            .to_string();
        if token_url.is_empty() {
            return Err(anyhow!(
                "PEBBLE_API_GOOGLE_OAUTH_TOKEN_URL must be non-empty"
            ));
        }

        let userinfo_url = std::env::var("PEBBLE_API_GOOGLE_OAUTH_USERINFO_URL")
            .unwrap_or_else(|_| "https://openidconnect.googleapis.com/v1/userinfo".to_string())
            .trim()
            .to_string();
        if userinfo_url.is_empty() {
            return Err(anyhow!(
                "PEBBLE_API_GOOGLE_OAUTH_USERINFO_URL must be non-empty"
            ));
        }

        let success_redirect_url = std::env::var("PEBBLE_API_GOOGLE_OAUTH_SUCCESS_REDIRECT_URL")
            .unwrap_or_else(|_| "http://localhost:5173/".to_string())
            .trim()
            .to_string();
        if success_redirect_url.is_empty() {
            return Err(anyhow!(
                "PEBBLE_API_GOOGLE_OAUTH_SUCCESS_REDIRECT_URL must be non-empty"
            ));
        }

        let failure_redirect_url = std::env::var("PEBBLE_API_GOOGLE_OAUTH_FAILURE_REDIRECT_URL")
            .unwrap_or_else(|_| success_redirect_url.clone())
            .trim()
            .to_string();
        if failure_redirect_url.is_empty() {
            return Err(anyhow!(
                "PEBBLE_API_GOOGLE_OAUTH_FAILURE_REDIRECT_URL must be non-empty"
            ));
        }

        let session_cookie_secure = match std::env::var("PEBBLE_API_SESSION_COOKIE_SECURE") {
            Ok(raw) => parse_env_bool("PEBBLE_API_SESSION_COOKIE_SECURE", &raw)?,
            Err(_) => false,
        };
        let session_cookie_domain = std::env::var("PEBBLE_API_SESSION_COOKIE_DOMAIN")
            .ok()
            .map(|raw| raw.trim().to_string())
            .filter(|raw| !raw.is_empty());

        let session_ttl_seconds = std::env::var("PEBBLE_API_SESSION_TTL_SECONDS")
            .unwrap_or_else(|_| "2592000".to_string())
            .parse::<u64>()
            .context("parse PEBBLE_API_SESSION_TTL_SECONDS")?;
        if session_ttl_seconds == 0 {
            return Err(anyhow!("PEBBLE_API_SESSION_TTL_SECONDS must be > 0"));
        }

        let oauth_state_ttl_seconds = std::env::var("PEBBLE_API_OAUTH_STATE_TTL_SECONDS")
            .unwrap_or_else(|_| "900".to_string())
            .parse::<u64>()
            .context("parse PEBBLE_API_OAUTH_STATE_TTL_SECONDS")?;
        if oauth_state_ttl_seconds == 0 {
            return Err(anyhow!("PEBBLE_API_OAUTH_STATE_TTL_SECONDS must be > 0"));
        }

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .context("build Google OAuth HTTP client")?;

        Ok(Some(Self {
            client_id,
            client_secret,
            redirect_uri,
            auth_url,
            token_url,
            userinfo_url,
            success_redirect_url,
            failure_redirect_url,
            session_cookie_secure,
            session_cookie_domain,
            session_ttl: Duration::from_secs(session_ttl_seconds),
            oauth_state_ttl: Duration::from_secs(oauth_state_ttl_seconds),
            http_client,
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DisclosureTemplateId {
    module_name: String,
    entity_name: String,
}

impl DisclosureTemplateId {
    fn parse(raw: &str) -> Result<Self> {
        let trimmed = raw.trim();
        let (module_name, entity_name) = trimmed.split_once(':').ok_or_else(|| {
            anyhow!("disclosure template must be module:entity (got {trimmed:?})")
        })?;
        let module_name = module_name.trim();
        let entity_name = entity_name.trim();
        if module_name.is_empty() || entity_name.is_empty() {
            return Err(anyhow!(
                "disclosure template must have non-empty module/entity (got {trimmed:?})"
            ));
        }

        Ok(Self {
            module_name: module_name.to_string(),
            entity_name: entity_name.to_string(),
        })
    }
}

#[derive(Clone)]
struct DisclosureServiceConfig {
    allowed_templates: HashSet<DisclosureTemplateId>,
}

impl DisclosureServiceConfig {
    fn from_env() -> Result<Self> {
        let raw = std::env::var("PEBBLE_API_DISCLOSURE_TEMPLATE_ALLOWLIST").unwrap_or_else(|_| {
            "Pebble.Reference:TokenConfig,Pebble.Reference:FeeSchedule,Pebble.Reference:OracleConfig"
                .to_string()
        });

        let mut allowed_templates = HashSet::new();
        for item in raw.split(',') {
            let trimmed = item.trim();
            if trimmed.is_empty() {
                continue;
            }

            let template = DisclosureTemplateId::parse(trimmed).with_context(|| {
                format!("parse PEBBLE_API_DISCLOSURE_TEMPLATE_ALLOWLIST entry {trimmed:?}")
            })?;
            allowed_templates.insert(template);
        }

        if allowed_templates.is_empty() {
            return Err(anyhow!(
                "PEBBLE_API_DISCLOSURE_TEMPLATE_ALLOWLIST must contain at least one module:entity entry"
            ));
        }

        Ok(Self { allowed_templates })
    }

    fn allows(&self, module_name: &str, entity_name: &str) -> bool {
        self.allowed_templates.contains(&DisclosureTemplateId {
            module_name: module_name.to_string(),
            entity_name: entity_name.to_string(),
        })
    }
}

#[derive(Clone)]
struct ApiRateLimiter {
    enabled: bool,
    max_requests: u64,
    window: std::time::Duration,
    buckets: Arc<Vec<tokio::sync::Mutex<HashMap<String, RateBucket>>>>,
}

#[derive(Debug, Clone)]
struct RateBucket {
    window_start: std::time::Instant,
    count: u64,
}

impl ApiRateLimiter {
    fn from_env() -> Result<Self> {
        let enabled = match std::env::var("PEBBLE_API_RATE_LIMIT_ENABLED") {
            Ok(v) => parse_env_bool("PEBBLE_API_RATE_LIMIT_ENABLED", &v)?,
            Err(_) => true,
        };
        let window_seconds = std::env::var("PEBBLE_API_RATE_LIMIT_WINDOW_SECONDS")
            .unwrap_or_else(|_| "60".to_string())
            .parse::<u64>()
            .context("parse PEBBLE_API_RATE_LIMIT_WINDOW_SECONDS")?;
        if window_seconds == 0 {
            return Err(anyhow!("PEBBLE_API_RATE_LIMIT_WINDOW_SECONDS must be > 0"));
        }
        let max_requests = std::env::var("PEBBLE_API_RATE_LIMIT_MAX_REQUESTS")
            .unwrap_or_else(|_| "120".to_string())
            .parse::<u64>()
            .context("parse PEBBLE_API_RATE_LIMIT_MAX_REQUESTS")?;
        if max_requests == 0 {
            return Err(anyhow!("PEBBLE_API_RATE_LIMIT_MAX_REQUESTS must be > 0"));
        }

        Ok(Self {
            enabled,
            max_requests,
            window: std::time::Duration::from_secs(window_seconds),
            buckets: Arc::new(
                (0..RATE_LIMIT_SHARD_COUNT)
                    .map(|_| tokio::sync::Mutex::new(HashMap::new()))
                    .collect(),
            ),
        })
    }

    async fn check_and_note(&self, key: &str) -> Result<(), u64> {
        if !self.enabled {
            return Ok(());
        }

        let now = std::time::Instant::now();
        let bucket_shard_index = rate_limiter_bucket_shard_index(key);
        let mut buckets = self.buckets[bucket_shard_index].lock().await;

        if buckets.len() > RATE_LIMIT_PRUNE_THRESHOLD_PER_SHARD {
            let window = self.window;
            buckets.retain(|_, bucket| now.duration_since(bucket.window_start) < window);
        }

        let bucket = buckets.entry(key.to_string()).or_insert(RateBucket {
            window_start: now,
            count: 0,
        });

        let elapsed = now.duration_since(bucket.window_start);
        if elapsed >= self.window {
            bucket.window_start = now;
            bucket.count = 0;
        }

        if bucket.count >= self.max_requests {
            let elapsed_after_reset = now.duration_since(bucket.window_start);
            let retry_after = self
                .window
                .saturating_sub(elapsed_after_reset)
                .as_secs()
                .max(1);
            return Err(retry_after);
        }

        bucket.count = bucket.count.saturating_add(1);
        Ok(())
    }
}

fn rate_limiter_bucket_shard_index(key: &str) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    let shard_u64 = hasher.finish() % RATE_LIMIT_SHARD_COUNT_U64;
    // Safe because shard_u64 is in [0, RATE_LIMIT_SHARD_COUNT).
    shard_u64 as usize
}

fn rate_limit_subject(headers: &HeaderMap) -> String {
    if let Some(value) = headers.get("x-admin-key") {
        if let Ok(v) = value.to_str() {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                return format!("admin:{trimmed}");
            }
        }
    }
    if let Some(value) = headers.get("x-api-key") {
        if let Ok(v) = value.to_str() {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                return format!("api:{trimmed}");
            }
        }
    }
    if let Ok(Some(session_token)) = optional_cookie(headers, USER_SESSION_COOKIE_NAME) {
        let hashed = sha256_hex(session_token.as_bytes());
        return format!("session:{hashed}");
    }
    if let Some(value) = headers.get("x-forwarded-for") {
        if let Ok(v) = value.to_str() {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                return format!("ip:{trimmed}");
            }
        }
    }

    "anon".to_string()
}

async fn api_rate_limit_middleware(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let key = rate_limit_subject(request.headers());
    match state.rate_limiter.check_and_note(&key).await {
        Ok(()) => next.run(request).await,
        Err(retry_after) => {
            let body = Json(serde_json::json!({
                "error": "rate limit exceeded",
                "retry_after_seconds": retry_after,
            }));
            let mut response = (StatusCode::TOO_MANY_REQUESTS, body).into_response();
            if let Ok(retry_after_header) = HeaderValue::from_str(&retry_after.to_string()) {
                response
                    .headers_mut()
                    .insert(header::RETRY_AFTER, retry_after_header);
            }
            response
        }
    }
}

fn parse_cors_origin_header(origin: &str) -> Result<HeaderValue> {
    if !(origin.starts_with("http://") || origin.starts_with("https://")) {
        return Err(anyhow!(
            "CORS origin must start with http:// or https:// (got {origin:?})"
        ));
    }

    origin
        .parse::<HeaderValue>()
        .context("parse CORS origin as header value")
}

#[derive(Debug, Clone)]
struct CorsOriginMatcher {
    exact: HashSet<HeaderValue>,
    wildcard: Vec<CorsWildcardOrigin>,
}

impl CorsOriginMatcher {
    fn matches_header(&self, origin: &HeaderValue) -> bool {
        if self.exact.contains(origin) {
            return true;
        }
        let origin_str = match origin.to_str() {
            Ok(value) => value,
            Err(_) => return false,
        };
        self.matches_str(origin_str)
    }

    fn matches_str(&self, origin: &str) -> bool {
        self.wildcard.iter().any(|rule| rule.matches(origin))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CorsWildcardOrigin {
    scheme: String,
    host_suffix: String,
    port: Option<u16>,
}

impl CorsWildcardOrigin {
    fn matches(&self, origin: &str) -> bool {
        let (scheme, authority) = match parse_origin_scheme_authority(origin) {
            Some(parts) => parts,
            None => return false,
        };
        if !scheme.eq_ignore_ascii_case(&self.scheme) {
            return false;
        }

        let (host, port) = match split_authority_host_port(authority) {
            Some(parts) => parts,
            None => return false,
        };
        if self.port.is_some() && self.port != port {
            return false;
        }

        let host_lower = host.to_ascii_lowercase();
        if host_lower == self.host_suffix {
            return false;
        }
        let suffix = format!(".{}", self.host_suffix);
        host_lower.ends_with(&suffix)
    }
}

fn is_loopback_dev_origin_header(origin: &HeaderValue) -> bool {
    let origin_str = match origin.to_str() {
        Ok(value) => value,
        Err(_) => return false,
    };

    is_loopback_dev_origin(origin_str)
}

fn matches_host_with_optional_port(authority: &str, host: &str) -> bool {
    if authority.eq_ignore_ascii_case(host) {
        return true;
    }

    let (candidate_host, candidate_port) = match authority.rsplit_once(':') {
        Some(parts) => parts,
        None => return false,
    };

    candidate_host.eq_ignore_ascii_case(host)
        && !candidate_port.is_empty()
        && candidate_port.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_loopback_dev_origin(origin: &str) -> bool {
    let (_scheme, authority) = match parse_origin_scheme_authority(origin) {
        Some(parts) => parts,
        None => return false,
    };

    matches_host_with_optional_port(authority, "localhost")
        || matches_host_with_optional_port(authority, "127.0.0.1")
}

fn parse_origin_scheme_authority(origin: &str) -> Option<(&str, &str)> {
    let (scheme, authority) = origin.split_once("://")?;
    if !(scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")) {
        return None;
    }
    if authority.is_empty()
        || authority.contains('/')
        || authority.contains('@')
        || authority.contains('?')
        || authority.contains('#')
    {
        return None;
    }
    Some((scheme, authority))
}

fn split_authority_host_port(authority: &str) -> Option<(&str, Option<u16>)> {
    if authority.is_empty() || authority.starts_with('[') || authority.ends_with(']') {
        return None;
    }
    if let Some((host, port_raw)) = authority.rsplit_once(':') {
        if host.contains(':') {
            return None;
        }
        if !port_raw.is_empty() && port_raw.bytes().all(|byte| byte.is_ascii_digit()) {
            let port = port_raw.parse::<u16>().ok()?;
            return Some((host, Some(port)));
        }
    }
    Some((authority, None))
}

fn parse_cors_wildcard_origin(origin: &str) -> Result<CorsWildcardOrigin> {
    let (scheme, authority) = parse_origin_scheme_authority(origin).ok_or_else(|| {
        anyhow!("wildcard CORS origin must be a valid http(s) origin (got {origin:?})")
    })?;
    let (host, port) = split_authority_host_port(authority)
        .ok_or_else(|| anyhow!("wildcard CORS origin has invalid authority: {origin:?}"))?;
    if !host.starts_with("*.") || host.len() <= 2 {
        return Err(anyhow!(
            "wildcard CORS origin host must start with '*.' (got {origin:?})"
        ));
    }

    let host_suffix = host[2..].to_ascii_lowercase();
    if host_suffix.contains('*') || !host_suffix.contains('.') {
        return Err(anyhow!(
            "wildcard CORS origin host suffix is invalid: {origin:?}"
        ));
    }

    Ok(CorsWildcardOrigin {
        scheme: scheme.to_ascii_lowercase(),
        host_suffix,
        port,
    })
}

fn cors_origin_matcher_from_env() -> Result<CorsOriginMatcher> {
    let raw = std::env::var("PEBBLE_API_CORS_ALLOWED_ORIGINS")
        .unwrap_or_else(|_| "http://127.0.0.1:5173,http://localhost:5173".to_string());

    let mut exact = HashSet::new();
    let mut wildcard = Vec::new();
    for origin in raw.split(',') {
        let trimmed = origin.trim();
        if trimmed.is_empty() {
            continue;
        }

        if trimmed.contains("://*.") {
            let parsed = parse_cors_wildcard_origin(trimmed).with_context(|| {
                format!("parse PEBBLE_API_CORS_ALLOWED_ORIGINS wildcard entry {trimmed:?}")
            })?;
            wildcard.push(parsed);
            continue;
        }

        let parsed = parse_cors_origin_header(trimmed).with_context(|| {
            format!("parse PEBBLE_API_CORS_ALLOWED_ORIGINS exact entry {trimmed:?}")
        })?;
        exact.insert(parsed);
    }

    if exact.is_empty() && wildcard.is_empty() {
        return Err(anyhow!(
            "PEBBLE_API_CORS_ALLOWED_ORIGINS must contain at least one origin"
        ));
    }

    Ok(CorsOriginMatcher { exact, wildcard })
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
    code: Option<String>,
    details: Option<serde_json::Value>,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
            code: None,
            details: None,
        }
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
            code: None,
            details: None,
        }
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
            code: None,
            details: None,
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
            code: None,
            details: None,
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
            code: None,
            details: None,
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
            code: None,
            details: None,
        }
    }

    fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    fn bad_request_code(message: impl Into<String>, code: impl Into<String>) -> Self {
        Self::bad_request(message).with_code(code)
    }

    fn conflict_code(message: impl Into<String>, code: impl Into<String>) -> Self {
        Self::conflict(message).with_code(code)
    }

    fn service_unavailable_code(message: impl Into<String>, code: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message: message.into(),
            code: Some(code.into()),
            details: None,
        }
    }

    fn status_code(&self) -> StatusCode {
        self.status
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        // Use Debug formatting to preserve anyhow's error chain (tonic status, sources, etc.).
        tracing::error!(error = ?err, "api error");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "internal server error".to_string(),
            code: None,
            details: None,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut body_map = serde_json::Map::new();
        body_map.insert("error".to_string(), serde_json::Value::String(self.message));
        body_map.insert(
            "code".to_string(),
            self.code
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null),
        );
        if let Some(details) = self.details {
            match details {
                serde_json::Value::Object(map) => {
                    body_map.extend(map);
                }
                other => {
                    body_map.insert("details".to_string(), other);
                }
            }
        }
        let body = Json(serde_json::Value::Object(body_map));
        (self.status, body).into_response()
    }
}

fn role_label(mode: fluid_bridge::WriteMode) -> &'static str {
    match mode {
        fluid_bridge::WriteMode::FluidLeader => "leader",
        fluid_bridge::WriteMode::FluidFollower => "follower",
        fluid_bridge::WriteMode::Legacy => "readonly",
        fluid_bridge::WriteMode::Fatal => "fatal",
    }
}

async fn role(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        role_label(state.current_write_mode()),
    )
}

async fn healthz(State(state): State<AppState>) -> Json<serde_json::Value> {
    let wal_writer_metrics = state.wal_handle.as_ref().map(|handle| handle.stats());
    let projector_metrics = state
        .projector_handle
        .as_ref()
        .map(|handle| handle.metrics());
    let role = role_label(state.current_write_mode());
    let connected_followers = state
        .replication_broadcaster
        .as_ref()
        .and_then(|broadcaster| broadcaster.connected_followers().ok());
    let replication_tip_offset = state
        .replication_broadcaster
        .as_ref()
        .and_then(|broadcaster| broadcaster.tip_offset().ok())
        .map(|offset| offset.0);
    let durable_applied = state
        .wal_dir
        .as_ref()
        .and_then(|dir| pebble_fluid::wal::local_wal_tip(dir).ok())
        .map(|offset| offset.0);
    let leader_epoch = state
        .leader_epoch
        .load(std::sync::atomic::Ordering::Relaxed);
    let committed_offset = state
        .committed_offset
        .load(std::sync::atomic::Ordering::Relaxed);
    Json(serde_json::json!({
      "status": "ok",
      "role": role,
      "leader_epoch": leader_epoch,
      "committed_offset": committed_offset,
      "m3_api_keys": state.auth_keys.api_key_count(),
      "m4_admin_keys": state.auth_keys.admin_key_count(),
      "m3_engine_version": state.m3_cfg.engine_version,
      "account_lock_namespace": state.account_lock_namespace,
      "active_withdrawals_enabled": state.active_withdrawals_enabled,
      "withdrawal_request_state_enabled": state.withdrawal_request_state_enabled,
      "withdrawal_reject_ineligible_at_create": state.withdrawal_reject_ineligible_at_create,
      "withdrawal_submission_saga_enabled": state.withdrawal_submission_saga_enabled,
      "withdrawal_one_shot_auto_accept_enabled": state.withdrawal_one_shot_auto_accept_enabled,
      "withdrawal_one_shot_require_policy": state.withdrawal_one_shot_require_policy,
      "order_available_includes_withdrawal_reserves": state.order_available_includes_withdrawal_reserves,
      "account_writer_enabled": state.account_writer_enabled,
      "faucet_enabled": state.faucet_cfg.enabled,
      "faucet_default_amount_minor": state.faucet_cfg.default_amount_minor,
      "faucet_max_amount_minor": state.faucet_cfg.max_amount_minor,
      "google_oauth_enabled": state.google_oauth_cfg.is_some(),
      "assets_public_base_url": state.market_assets_cfg.public_base_url,
      "perf_metrics_enabled": state.perf_metrics.enabled(),
      "perf_metrics_summary_seconds": state.perf_metrics.summary_period().as_secs(),
      "perf_metrics_window_size": state.perf_metrics.window_size(),
      "perf_metrics_max_series": state.perf_metrics.max_series(),
      "perf_metrics_log_dir": state.perf_metrics.log_dir().map(|path| path.display().to_string()),
      "connected_followers": connected_followers,
      "replication_tip_offset": replication_tip_offset,
      "durable_applied": durable_applied,
      "wal_writer_metrics": wal_writer_metrics,
      "wal_projector_metrics": projector_metrics
    }))
}

#[derive(Clone, Debug, Default)]
struct FollowerMetricsSnapshot {
    node_id: String,
    durable_applied: u64,
    lag_entries: u64,
}

#[derive(Clone, Debug, Default)]
struct MetricsSnapshot {
    role: &'static str,
    leader_epoch: u64,
    committed_offset: u64,
    min_ack_count: usize,
    durable_ack_count_at_committed: Option<usize>,
    consecutive_ack_timeouts: u64,
    connected_followers: Option<usize>,
    follower_metrics: Vec<FollowerMetricsSnapshot>,
    replication_tip_offset: Option<u64>,
    durable_applied: Option<u64>,
    wal_writer_metrics: Option<pebble_fluid::wal::WalWriterStats>,
    wal_projector_metrics: Option<wal_projector::WalProjectorMetrics>,
}

fn escape_prometheus_label_value(raw: &str) -> String {
    let mut escaped = String::with_capacity(raw.len());
    for c in raw.chars() {
        match c {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            _ => escaped.push(c),
        }
    }
    escaped
}

fn render_metrics(snapshot: &MetricsSnapshot) -> String {
    fn bool_value(value: bool) -> u64 {
        if value {
            1
        } else {
            0
        }
    }

    let role_leader = bool_value(snapshot.role == "leader");
    let role_follower = bool_value(snapshot.role == "follower");
    let role_readonly = bool_value(snapshot.role == "readonly");
    let role_fatal = bool_value(snapshot.role == "fatal");
    let min_ack_count = snapshot.min_ack_count;
    let durable_ack_count_at_committed = snapshot.durable_ack_count_at_committed.unwrap_or(0);
    let consecutive_ack_timeouts = snapshot.consecutive_ack_timeouts;
    let connected_followers = snapshot.connected_followers.unwrap_or(0);
    let replication_tip_offset = snapshot.replication_tip_offset.unwrap_or(0);
    let durable_applied = snapshot.durable_applied.unwrap_or(0);
    let wal_writer = snapshot.wal_writer_metrics.unwrap_or_default();
    let projector = snapshot.wal_projector_metrics.unwrap_or_default();

    let mut text = String::new();
    text.push_str("# TYPE pebble_fluid_role_leader gauge\n");
    text.push_str(&format!("pebble_fluid_role_leader {role_leader}\n"));
    text.push_str("# TYPE pebble_fluid_role_follower gauge\n");
    text.push_str(&format!("pebble_fluid_role_follower {role_follower}\n"));
    text.push_str("# TYPE pebble_fluid_role_readonly gauge\n");
    text.push_str(&format!("pebble_fluid_role_readonly {role_readonly}\n"));
    text.push_str("# TYPE pebble_fluid_role_fatal gauge\n");
    text.push_str(&format!("pebble_fluid_role_fatal {role_fatal}\n"));
    text.push_str("# TYPE pebble_fluid_leader_epoch gauge\n");
    text.push_str(&format!(
        "pebble_fluid_leader_epoch {}\n",
        snapshot.leader_epoch
    ));
    text.push_str("# TYPE pebble_fluid_committed_offset gauge\n");
    text.push_str(&format!(
        "pebble_fluid_committed_offset {}\n",
        snapshot.committed_offset
    ));
    text.push_str("# TYPE pebble_fluid_min_ack_count gauge\n");
    text.push_str(&format!("pebble_fluid_min_ack_count {min_ack_count}\n"));
    text.push_str("# TYPE pebble_fluid_durable_ack_count_at_committed gauge\n");
    text.push_str(&format!(
        "pebble_fluid_durable_ack_count_at_committed {durable_ack_count_at_committed}\n"
    ));
    text.push_str("# TYPE pebble_fluid_consecutive_ack_timeouts gauge\n");
    text.push_str(&format!(
        "pebble_fluid_consecutive_ack_timeouts {consecutive_ack_timeouts}\n"
    ));
    text.push_str("# TYPE pebble_fluid_connected_followers gauge\n");
    text.push_str(&format!(
        "pebble_fluid_connected_followers {connected_followers}\n"
    ));
    text.push_str("# TYPE pebble_fluid_replication_tip_offset gauge\n");
    text.push_str(&format!(
        "pebble_fluid_replication_tip_offset {replication_tip_offset}\n"
    ));
    text.push_str("# TYPE pebble_fluid_durable_applied gauge\n");
    text.push_str(&format!("pebble_fluid_durable_applied {durable_applied}\n"));
    text.push_str("# TYPE pebble_fluid_follower_durable_applied gauge\n");
    text.push_str("# TYPE pebble_fluid_follower_lag_entries gauge\n");
    for follower in &snapshot.follower_metrics {
        let escaped_node_id = escape_prometheus_label_value(&follower.node_id);
        text.push_str(&format!(
            "pebble_fluid_follower_durable_applied{{node_id=\"{escaped_node_id}\"}} {}\n",
            follower.durable_applied
        ));
        text.push_str(&format!(
            "pebble_fluid_follower_lag_entries{{node_id=\"{escaped_node_id}\"}} {}\n",
            follower.lag_entries
        ));
    }
    text.push_str("# TYPE pebble_fluid_wal_batches_total counter\n");
    text.push_str(&format!(
        "pebble_fluid_wal_batches_total {}\n",
        wal_writer.batches
    ));
    text.push_str("# TYPE pebble_fluid_wal_entries_total counter\n");
    text.push_str(&format!(
        "pebble_fluid_wal_entries_total {}\n",
        wal_writer.entries
    ));
    text.push_str("# TYPE pebble_fluid_wal_fsyncs_total counter\n");
    text.push_str(&format!(
        "pebble_fluid_wal_fsyncs_total {}\n",
        wal_writer.fsyncs
    ));
    text.push_str("# TYPE pebble_fluid_wal_last_fsync_latency_micros gauge\n");
    text.push_str(&format!(
        "pebble_fluid_wal_last_fsync_latency_micros {}\n",
        wal_writer.last_fsync_latency_micros
    ));
    text.push_str("# TYPE pebble_fluid_wal_max_fsync_latency_micros gauge\n");
    text.push_str(&format!(
        "pebble_fluid_wal_max_fsync_latency_micros {}\n",
        wal_writer.max_fsync_latency_micros
    ));
    text.push_str("# TYPE pebble_fluid_projected_offset gauge\n");
    text.push_str(&format!(
        "pebble_fluid_projected_offset {}\n",
        projector.projected_offset
    ));
    text.push_str("# TYPE pebble_fluid_projection_lag_entries gauge\n");
    text.push_str(&format!(
        "pebble_fluid_projection_lag_entries {}\n",
        projector.lag_entries
    ));
    text.push_str("# TYPE pebble_fluid_wal_disk_usage_bytes gauge\n");
    text.push_str(&format!(
        "pebble_fluid_wal_disk_usage_bytes {}\n",
        projector.wal_disk_usage_bytes
    ));
    text
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let committed_offset = state
        .committed_offset
        .load(std::sync::atomic::Ordering::Relaxed);
    let connected_followers = state
        .replication_broadcaster
        .as_ref()
        .and_then(|broadcaster| broadcaster.connected_followers().ok());
    let durable_ack_count_at_committed =
        state
            .replication_broadcaster
            .as_ref()
            .and_then(|broadcaster| {
                broadcaster
                    .durable_ack_count_at_or_above(pebble_fluid::wal::WalOffset(committed_offset))
                    .ok()
            });
    let replication_tip_offset = state
        .replication_broadcaster
        .as_ref()
        .and_then(|broadcaster| broadcaster.tip_offset().ok())
        .map(|offset| offset.0);
    let follower_metrics = state
        .replication_broadcaster
        .as_ref()
        .and_then(|broadcaster| broadcaster.follower_durability_snapshot().ok())
        .map(|followers| {
            let tip_offset = replication_tip_offset.unwrap_or_default();
            followers
                .into_iter()
                .map(|follower| FollowerMetricsSnapshot {
                    node_id: follower.node_id,
                    durable_applied: follower.durable_applied.0,
                    lag_entries: tip_offset.saturating_sub(follower.durable_applied.0),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let durable_applied = state
        .wal_dir
        .as_ref()
        .and_then(|dir| pebble_fluid::wal::local_wal_tip(dir).ok())
        .map(|offset| offset.0);
    let snapshot = MetricsSnapshot {
        role: role_label(state.current_write_mode()),
        leader_epoch: state
            .leader_epoch
            .load(std::sync::atomic::Ordering::Relaxed),
        committed_offset,
        min_ack_count: state.durability_config.min_ack_count,
        durable_ack_count_at_committed,
        consecutive_ack_timeouts: state
            .consecutive_ack_timeouts
            .load(std::sync::atomic::Ordering::Relaxed),
        connected_followers,
        follower_metrics,
        replication_tip_offset,
        durable_applied,
        wal_writer_metrics: state.wal_handle.as_ref().map(|handle| handle.stats()),
        wal_projector_metrics: state
            .projector_handle
            .as_ref()
            .map(|handle| handle.metrics()),
    };
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        render_metrics(&snapshot),
    )
}

#[derive(Debug, Clone)]
pub(crate) struct UserAuthContext {
    pub(crate) account_id: String,
    pub(crate) session_id: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct UserSessionPrincipal {
    pub(crate) session_id: String,
    pub(crate) account_id: String,
}

#[derive(Debug, sqlx::FromRow)]
struct ActiveUserSessionRow {
    session_id: String,
    account_id: String,
}

#[derive(Debug, Deserialize)]
struct GoogleOAuthCallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GoogleOAuthTokenResponse {
    access_token: String,
}

#[derive(Debug, Deserialize)]
struct GoogleOAuthUserInfoResponse {
    sub: String,
    email: Option<String>,
    email_verified: Option<bool>,
    name: Option<String>,
    picture: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct GoogleIdentityLinkRow {
    username: String,
    account_id: String,
}

#[derive(Debug, Serialize)]
struct LogoutUserSessionResponse {
    logged_out: bool,
}

fn optional_cookie(headers: &HeaderMap, name: &str) -> Result<Option<String>, ApiError> {
    let Some(raw) = headers.get(header::COOKIE) else {
        return Ok(None);
    };

    let cookie_header = raw
        .to_str()
        .map_err(|_| ApiError::unauthorized("cookie header must be valid ASCII"))?;
    for part in cookie_header.split(';') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some((key, value)) = trimmed.split_once('=') else {
            continue;
        };
        if key.trim() != name {
            continue;
        }
        let normalized = value.trim().to_string();
        if normalized.is_empty() {
            return Err(ApiError::unauthorized(format!(
                "cookie {name} must be non-empty"
            )));
        }
        return Ok(Some(normalized));
    }

    Ok(None)
}

fn make_cookie_header(
    name: &str,
    value: &str,
    max_age_seconds: i64,
    secure: bool,
    domain: Option<&str>,
) -> Result<HeaderValue, ApiError> {
    let mut cookie =
        format!("{name}={value}; Max-Age={max_age_seconds}; Path=/; HttpOnly; SameSite=Lax");
    if secure {
        cookie.push_str("; Secure");
    }
    if let Some(domain) = domain {
        if !domain.trim().is_empty() {
            cookie.push_str("; Domain=");
            cookie.push_str(domain.trim());
        }
    }

    HeaderValue::from_str(&cookie).map_err(|_| ApiError::internal("failed to build cookie header"))
}

fn make_session_cookie_header(
    session_token: &str,
    cfg: &GoogleOAuthConfig,
) -> Result<HeaderValue, ApiError> {
    let max_age_seconds = i64::try_from(cfg.session_ttl.as_secs())
        .map_err(|_| ApiError::internal("session ttl exceeds supported range"))?;
    make_cookie_header(
        USER_SESSION_COOKIE_NAME,
        session_token,
        max_age_seconds,
        cfg.session_cookie_secure,
        cfg.session_cookie_domain.as_deref(),
    )
}

fn clear_session_cookie_header(cfg: Option<&GoogleOAuthConfig>) -> Result<HeaderValue, ApiError> {
    let secure = cfg
        .map(|value| value.session_cookie_secure)
        .unwrap_or(false);
    let domain = cfg.and_then(|value| value.session_cookie_domain.as_deref());
    make_cookie_header(USER_SESSION_COOKIE_NAME, "", 0, secure, domain)
}

fn make_oauth_state_cookie_header(
    state_token: &str,
    cfg: &GoogleOAuthConfig,
) -> Result<HeaderValue, ApiError> {
    let max_age_seconds = i64::try_from(cfg.oauth_state_ttl.as_secs())
        .map_err(|_| ApiError::internal("oauth state ttl exceeds supported range"))?;
    make_cookie_header(
        GOOGLE_OAUTH_STATE_COOKIE_NAME,
        state_token,
        max_age_seconds,
        cfg.session_cookie_secure,
        cfg.session_cookie_domain.as_deref(),
    )
}

fn clear_oauth_state_cookie_header(cfg: &GoogleOAuthConfig) -> Result<HeaderValue, ApiError> {
    make_cookie_header(
        GOOGLE_OAUTH_STATE_COOKIE_NAME,
        "",
        0,
        cfg.session_cookie_secure,
        cfg.session_cookie_domain.as_deref(),
    )
}

async fn lookup_active_user_session(
    db: &PgPool,
    session_token: &str,
) -> Result<Option<ActiveUserSessionRow>, ApiError> {
    let token_hash = sha256_hex(session_token.as_bytes());
    sqlx::query_as(
        r#"
        SELECT session_id, account_id
        FROM user_sessions
        WHERE session_token_hash = $1
          AND revoked_at IS NULL
          AND expires_at > now()
        LIMIT 1
        "#,
    )
    .bind(token_hash)
    .fetch_optional(db)
    .await
    .context("query active user session")
    .map_err(ApiError::from)
}

pub(crate) async fn resolve_user_session_account(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Option<UserSessionPrincipal>, ApiError> {
    let Some(session_token) = optional_cookie(headers, USER_SESSION_COOKIE_NAME)? else {
        return Ok(None);
    };

    let Some(row) = lookup_active_user_session(&state.db, &session_token).await? else {
        return Err(ApiError::unauthorized("invalid or expired session"));
    };

    Ok(Some(UserSessionPrincipal {
        session_id: row.session_id,
        account_id: row.account_id,
    }))
}

pub(crate) async fn authenticate_user_request(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<UserAuthContext, ApiError> {
    let admin_key = optional_ascii_header(headers, "x-admin-key")?;
    if admin_key.is_some() {
        return Err(ApiError::forbidden(
            "x-admin-key is not allowed on user endpoints",
        ));
    }

    if let Some(api_key) = optional_ascii_header(headers, "x-api-key")? {
        let Some(account_id) = state.auth_keys.account_id_for_api_key(&api_key) else {
            return Err(ApiError::unauthorized("invalid API key"));
        };
        return Ok(UserAuthContext {
            account_id,
            session_id: None,
        });
    }

    if let Some(session) = resolve_user_session_account(state, headers).await? {
        return Ok(UserAuthContext {
            account_id: session.account_id,
            session_id: Some(session.session_id),
        });
    }

    Err(ApiError::unauthorized(
        "missing x-api-key header or active session",
    ))
}

async fn start_google_oauth(State(state): State<AppState>) -> Result<Response, ApiError> {
    let Some(cfg) = state.google_oauth_cfg.as_ref() else {
        return Err(ApiError::not_found("google oauth is disabled"));
    };

    let oauth_state = format!("gos_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let mut auth_url = reqwest::Url::parse(&cfg.auth_url).context("parse Google OAuth auth URL")?;
    {
        let mut query = auth_url.query_pairs_mut();
        query.append_pair("client_id", &cfg.client_id);
        query.append_pair("redirect_uri", &cfg.redirect_uri);
        query.append_pair("response_type", "code");
        query.append_pair("scope", GOOGLE_OAUTH_SCOPE);
        query.append_pair("state", &oauth_state);
    }

    let mut response = Redirect::temporary(auth_url.as_str()).into_response();
    response.headers_mut().append(
        header::SET_COOKIE,
        make_oauth_state_cookie_header(&oauth_state, cfg)?,
    );
    Ok(response)
}

async fn exchange_google_oauth_code(
    cfg: &GoogleOAuthConfig,
    code: &str,
) -> Result<GoogleOAuthTokenResponse, ApiError> {
    let response = cfg
        .http_client
        .post(cfg.token_url.clone())
        .form(&[
            ("code", code),
            ("client_id", cfg.client_id.as_str()),
            ("client_secret", cfg.client_secret.as_str()),
            ("redirect_uri", cfg.redirect_uri.as_str()),
            ("grant_type", "authorization_code"),
        ])
        .send()
        .await
        .context("request Google OAuth token")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(ApiError::unauthorized(format!(
            "google token exchange failed ({status}): {body}"
        )));
    }

    response
        .json::<GoogleOAuthTokenResponse>()
        .await
        .context("decode Google OAuth token response")
        .map_err(ApiError::from)
}

async fn fetch_google_user_info(
    cfg: &GoogleOAuthConfig,
    access_token: &str,
) -> Result<GoogleOAuthUserInfoResponse, ApiError> {
    let response = cfg
        .http_client
        .get(cfg.userinfo_url.clone())
        .bearer_auth(access_token)
        .send()
        .await
        .context("request Google user info")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(ApiError::unauthorized(format!(
            "google user info fetch failed ({status}): {body}"
        )));
    }

    response
        .json::<GoogleOAuthUserInfoResponse>()
        .await
        .context("decode Google user info response")
        .map_err(ApiError::from)
}

fn google_registration_username_base(
    email: &str,
    google_subject: &str,
) -> Result<String, ApiError> {
    let email_local = email.split('@').next().unwrap_or_default();
    let mut fragment = email_local
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|ch| ch.to_ascii_lowercase())
        .take(32)
        .collect::<String>();
    if fragment.is_empty() {
        fragment = "user".to_string();
    }
    let hash = short_registration_hash(&format!("google:{google_subject}"));
    normalize_registration_username(&format!("{fragment}-{hash}"))
}

async fn ensure_google_identity_registration(
    state: &AppState,
    google_subject: &str,
    email: &str,
    full_name: Option<&str>,
    avatar_url: Option<&str>,
) -> Result<UserRegistrationRow, ApiError> {
    let existing_link: Option<GoogleIdentityLinkRow> = sqlx::query_as(
        r#"
        SELECT username, account_id
        FROM user_google_identities
        WHERE google_subject = $1
        LIMIT 1
        "#,
    )
    .bind(google_subject)
    .fetch_optional(&state.db)
    .await
    .context("query google identity link")?;

    let registration = if let Some(existing_link) = existing_link {
        let row = load_registration_by_username(&state.db, &existing_link.username).await?;
        let Some(row) = row else {
            return Err(ApiError::internal(
                "google identity link points to missing registration row",
            ));
        };
        if row.account_id != existing_link.account_id {
            return Err(ApiError::internal(
                "google identity link account does not match registration account",
            ));
        }
        row
    } else {
        let base_username = google_registration_username_base(email, google_subject)?;
        let mut created = None;
        for suffix in 0..32_u32 {
            let candidate = if suffix == 0 {
                base_username.clone()
            } else {
                normalize_registration_username(&format!("{base_username}-{suffix}"))?
            };
            match ensure_user_registration(state, &candidate).await {
                Ok(row) => {
                    created = Some(row);
                    break;
                }
                Err(err) if err.status_code() == StatusCode::CONFLICT => {
                    continue;
                }
                Err(err) => return Err(err),
            }
        }

        created.ok_or_else(|| {
            ApiError::service_unavailable_code(
                "failed to allocate a unique Google registration username; retry shortly",
                "google_registration_username_conflict",
            )
        })?
    };

    sqlx::query(
        r#"
        INSERT INTO user_google_identities (
          google_subject,
          username,
          account_id,
          email,
          email_verified,
          full_name,
          avatar_url,
          created_at,
          updated_at,
          last_login_at
        )
        VALUES ($1, $2, $3, $4, TRUE, $5, $6, now(), now(), now())
        ON CONFLICT (google_subject) DO UPDATE
        SET
          username = EXCLUDED.username,
          account_id = EXCLUDED.account_id,
          email = EXCLUDED.email,
          email_verified = EXCLUDED.email_verified,
          full_name = EXCLUDED.full_name,
          avatar_url = EXCLUDED.avatar_url,
          updated_at = now(),
          last_login_at = now()
        "#,
    )
    .bind(google_subject)
    .bind(&registration.username)
    .bind(&registration.account_id)
    .bind(email)
    .bind(full_name)
    .bind(avatar_url)
    .execute(&state.db)
    .await
    .context("upsert google identity link")?;

    Ok(registration)
}

fn request_user_agent(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn request_remote_addr(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

async fn create_user_session(
    db: &PgPool,
    account_id: &str,
    bootstrap_key_id: Option<&str>,
    user_agent: Option<String>,
    remote_addr: Option<String>,
    cfg: &GoogleOAuthConfig,
) -> Result<String, ApiError> {
    let session_ttl = chrono::Duration::from_std(cfg.session_ttl)
        .map_err(|_| ApiError::internal("session ttl exceeds supported range"))?;
    let expires_at: DateTime<Utc> = Utc::now() + session_ttl;

    for _attempt in 0..USER_SESSION_CREATE_MAX_RETRIES {
        let session_id = format!("uss_{}", Uuid::new_v4().simple());
        let session_token = format!(
            "pbl_ses_{}{}",
            Uuid::new_v4().simple(),
            Uuid::new_v4().simple()
        );
        let token_hash = sha256_hex(session_token.as_bytes());

        let insert_result = sqlx::query(
            r#"
            INSERT INTO user_sessions (
              session_id,
              session_token_hash,
              account_id,
              auth_source,
              bootstrap_key_id,
              created_at,
              expires_at,
              last_seen_at,
              user_agent,
              remote_addr
            )
            VALUES ($1, $2, $3, $4, $5, now(), $6, now(), $7, $8)
            "#,
        )
        .bind(&session_id)
        .bind(&token_hash)
        .bind(account_id)
        .bind(GOOGLE_OAUTH_AUTH_SOURCE)
        .bind(bootstrap_key_id)
        .bind(expires_at)
        .bind(user_agent.as_deref())
        .bind(remote_addr.as_deref())
        .execute(db)
        .await;

        match insert_result {
            Ok(_) => return Ok(session_token),
            Err(err) if is_unique_violation(&err) => continue,
            Err(err) => return Err(ApiError::from(anyhow::Error::from(err))),
        }
    }

    Err(ApiError::service_unavailable_code(
        "failed to allocate a unique session token; retry shortly",
        "session_generation_conflict",
    ))
}

async fn process_google_oauth_callback(
    state: &AppState,
    headers: &HeaderMap,
    query: &GoogleOAuthCallbackQuery,
    cfg: &GoogleOAuthConfig,
) -> Result<HeaderValue, ApiError> {
    if let Some(error) = query.error.as_deref() {
        let description = query
            .error_description
            .as_deref()
            .unwrap_or("google oauth denied");
        return Err(ApiError::unauthorized(format!(
            "google oauth denied ({error}): {description}"
        )));
    }

    let code = query
        .code
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_request("missing google oauth code"))?;
    let state_param = query
        .state
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_request("missing google oauth state"))?;

    let cookie_state = optional_cookie(headers, GOOGLE_OAUTH_STATE_COOKIE_NAME)?
        .ok_or_else(|| ApiError::unauthorized("missing google oauth state cookie"))?;
    if cookie_state != state_param {
        return Err(ApiError::unauthorized("google oauth state mismatch"));
    }

    let token = exchange_google_oauth_code(cfg, code).await?;
    let user_info = fetch_google_user_info(cfg, &token.access_token).await?;
    let google_subject = user_info.sub.trim();
    if google_subject.is_empty() {
        return Err(ApiError::unauthorized(
            "google oauth user info is missing subject",
        ));
    }

    let email = user_info
        .email
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::unauthorized("google oauth user info is missing email"))?;
    if !user_info.email_verified.unwrap_or(false) {
        return Err(ApiError::unauthorized(
            "google oauth account email is not verified",
        ));
    }

    let registration = ensure_google_identity_registration(
        state,
        google_subject,
        email,
        user_info.name.as_deref().map(str::trim),
        user_info.picture.as_deref().map(str::trim),
    )
    .await?;

    let instrument = load_default_registration_instrument(&state.db).await?;
    onboard_account_for_user(
        state,
        &registration.account_id,
        Some(registration.owner_party.as_str()),
        Some(instrument.instrument_admin.as_str()),
        Some(instrument.instrument_id.as_str()),
    )
    .await?;

    let bootstrap_key = maybe_insert_first_user_api_key(
        &state.db,
        &state.auth_keys,
        &registration.account_id,
        Some("google-initial"),
    )
    .await?;

    let session_token = create_user_session(
        &state.db,
        &registration.account_id,
        bootstrap_key.as_ref().map(|row| row.key_id.as_str()),
        request_user_agent(headers),
        request_remote_addr(headers),
        cfg,
    )
    .await?;

    make_session_cookie_header(&session_token, cfg)
}

fn redirect_with_query(base: &str, key: &str, value: &str) -> String {
    match reqwest::Url::parse(base) {
        Ok(mut parsed) => {
            parsed.query_pairs_mut().append_pair(key, value);
            parsed.to_string()
        }
        Err(_) => base.to_string(),
    }
}

async fn handle_google_oauth_callback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<GoogleOAuthCallbackQuery>,
) -> Result<Response, ApiError> {
    let Some(cfg) = state.google_oauth_cfg.as_ref() else {
        return Err(ApiError::not_found("google oauth is disabled"));
    };

    let mut response = match process_google_oauth_callback(&state, &headers, &query, cfg).await {
        Ok(session_cookie) => {
            let mut success = Redirect::temporary(&cfg.success_redirect_url).into_response();
            success
                .headers_mut()
                .append(header::SET_COOKIE, session_cookie);
            success
        }
        Err(err) => {
            tracing::warn!(error = ?err, "google oauth callback failed");
            let failure_url = redirect_with_query(&cfg.failure_redirect_url, "oauth_error", "1");
            let mut failure = Redirect::temporary(&failure_url).into_response();
            failure
                .headers_mut()
                .append(header::SET_COOKIE, clear_session_cookie_header(Some(cfg))?);
            failure
        }
    };
    response
        .headers_mut()
        .append(header::SET_COOKIE, clear_oauth_state_cookie_header(cfg)?);

    Ok(response)
}

async fn logout_user_session(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if let Some(session_token) = optional_cookie(&headers, USER_SESSION_COOKIE_NAME)? {
        let token_hash = sha256_hex(session_token.as_bytes());
        sqlx::query(
            r#"
            UPDATE user_sessions
            SET revoked_at = now()
            WHERE session_token_hash = $1
              AND revoked_at IS NULL
            "#,
        )
        .bind(token_hash)
        .execute(&state.db)
        .await
        .context("revoke user session")?;
    }

    let mut response = Json(LogoutUserSessionResponse { logged_out: true }).into_response();
    response.headers_mut().append(
        header::SET_COOKIE,
        clear_session_cookie_header(state.google_oauth_cfg.as_ref())?,
    );
    Ok(response)
}

async fn authorize_account_read_access(
    state: &AppState,
    headers: &HeaderMap,
    account_id: &str,
) -> Result<(), ApiError> {
    let user_key = optional_ascii_header(headers, "x-api-key")?;
    let admin_key = optional_ascii_header(headers, "x-admin-key")?;

    match (user_key, admin_key) {
        (Some(_), Some(_)) => Err(ApiError::forbidden(
            "x-api-key and x-admin-key cannot be used together",
        )),
        (Some(_), None) => {
            let authenticated_account_id = m4_views::require_user_account(state, headers).await?;
            if authenticated_account_id != account_id {
                return Err(ApiError::forbidden("API key cannot read another account"));
            }
            Ok(())
        }
        (None, Some(_)) => m4_views::require_admin(state, headers),
        (None, None) => {
            let authenticated_account_id = m4_views::require_user_account(state, headers).await?;
            if authenticated_account_id != account_id {
                return Err(ApiError::forbidden("session cannot read another account"));
            }
            Ok(())
        }
    }
}

#[derive(sqlx::FromRow)]
struct AccountRefRowDb {
    contract_id: String,
    account_id: String,
    owner_party: String,
    committee_party: String,
    instrument_admin: String,
    instrument_id: String,
    status: String,
    finalized_epoch: Option<i64>,
    created_at: chrono::DateTime<chrono::Utc>,
    active: bool,
    last_offset: i64,
}

#[derive(Serialize)]
struct AccountRefRow {
    contract_id: String,
    account_id: String,
    owner_party: String,
    committee_party: String,
    instrument_admin: String,
    instrument_id: String,
    status: String,
    finalized_epoch: Option<i64>,
    created_at: chrono::DateTime<chrono::Utc>,
    active: bool,
    last_offset: i64,
}

#[derive(sqlx::FromRow)]
struct AccountStateRowDb {
    contract_id: String,
    account_id: String,
    owner_party: String,
    committee_party: String,
    instrument_admin: String,
    instrument_id: String,
    cleared_cash_minor: i64,
    last_applied_epoch: i64,
    last_applied_batch_anchor: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    active: bool,
    last_offset: i64,
}

#[derive(Serialize)]
struct AccountStateRow {
    contract_id: String,
    account_id: String,
    owner_party: String,
    committee_party: String,
    instrument_admin: String,
    instrument_id: String,
    cleared_cash_minor: i64,
    last_applied_epoch: i64,
    last_applied_batch_anchor: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    active: bool,
    last_offset: i64,
}

#[derive(Serialize)]
struct AccountView {
    account_ref: AccountRefRow,
    account_state: AccountStateRow,
}

async fn get_account(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
) -> Result<Json<AccountView>, ApiError> {
    let account_id =
        parse_account_id(&account_id).map_err(|err| ApiError::bad_request(err.to_string()))?;
    authorize_account_read_access(&state, &headers, &account_id).await?;

    let account_ref: Option<AccountRefRowDb> = sqlx::query_as(
        r#"
        SELECT
          ar.contract_id,
          ar.account_id,
          ar.owner_party,
          ar.committee_party,
          ar.instrument_admin,
          ar.instrument_id,
          ar.status,
          ar.finalized_epoch,
          ar.created_at,
          ar.active,
          ar.last_offset
        FROM account_refs ar
        JOIN account_ref_latest l
          ON l.account_id = ar.account_id
         AND l.contract_id = ar.contract_id
        WHERE l.account_id = $1
        "#,
    )
    .bind(&account_id)
    .fetch_optional(&state.db)
    .await
    .context("query account_ref_latest")?;

    let account_state: Option<AccountStateRowDb> = sqlx::query_as(
        r#"
        SELECT
          ast.contract_id,
          ast.account_id,
          ast.owner_party,
          ast.committee_party,
          ast.instrument_admin,
          ast.instrument_id,
          ast.cleared_cash_minor,
          ast.last_applied_epoch,
          ast.last_applied_batch_anchor,
          ast.created_at,
          ast.active,
          ast.last_offset
        FROM account_states ast
        JOIN account_state_latest l
          ON l.account_id = ast.account_id
         AND l.contract_id = ast.contract_id
        WHERE l.account_id = $1
        "#,
    )
    .bind(&account_id)
    .fetch_optional(&state.db)
    .await
    .context("query account_state_latest")?;

    let Some(account_ref) = account_ref else {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            message: "account not found".to_string(),
            code: None,
            details: None,
        });
    };
    let Some(account_state) = account_state else {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            message: "account state not found".to_string(),
            code: None,
            details: None,
        });
    };

    Ok(Json(AccountView {
        account_ref: AccountRefRow {
            contract_id: account_ref.contract_id,
            account_id: account_ref.account_id,
            owner_party: account_ref.owner_party,
            committee_party: account_ref.committee_party,
            instrument_admin: account_ref.instrument_admin,
            instrument_id: account_ref.instrument_id,
            status: account_ref.status,
            finalized_epoch: account_ref.finalized_epoch,
            created_at: account_ref.created_at,
            active: account_ref.active,
            last_offset: account_ref.last_offset,
        },
        account_state: AccountStateRow {
            contract_id: account_state.contract_id,
            account_id: account_state.account_id,
            owner_party: account_state.owner_party,
            committee_party: account_state.committee_party,
            instrument_admin: account_state.instrument_admin,
            instrument_id: account_state.instrument_id,
            cleared_cash_minor: account_state.cleared_cash_minor,
            last_applied_epoch: account_state.last_applied_epoch,
            last_applied_batch_anchor: account_state.last_applied_batch_anchor,
            created_at: account_state.created_at,
            active: account_state.active,
            last_offset: account_state.last_offset,
        },
    }))
}

#[derive(Deserialize)]
struct CreateAccountRequest {
    account_id: Option<String>,
    owner_party: String,
    instrument_admin: String,
    instrument_id: String,
}

#[derive(Serialize)]
struct CreateAccountResponse {
    account_id: String,
    account_ref_contract_id: String,
    account_state_contract_id: String,
    update_id: String,
    offset: i64,
}

async fn create_account(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateAccountRequest>,
) -> Result<Json<CreateAccountResponse>, ApiError> {
    m4_views::require_admin(&state, &headers)?;

    let owner_party = req.owner_party.trim().to_string();
    if owner_party.is_empty() {
        return Err(ApiError::bad_request("owner_party must be non-empty"));
    }

    let instrument_admin = req.instrument_admin.trim().to_string();
    if instrument_admin.is_empty() {
        return Err(ApiError::bad_request("instrument_admin must be non-empty"));
    }

    let instrument_id = req.instrument_id.trim().to_string();
    if instrument_id.is_empty() {
        return Err(ApiError::bad_request("instrument_id must be non-empty"));
    }

    let account_id = match req.account_id {
        Some(id) if !id.trim().is_empty() => {
            parse_account_id(&id).map_err(|err| ApiError::bad_request(err.to_string()))?
        }
        _ => parse_account_id(&Uuid::new_v4().to_string())
            .map_err(|err| ApiError::bad_request(err.to_string()))?,
    };

    let commands = build_create_account_commands(
        &state.ledger_cfg.user_id,
        &state.ledger_cfg.committee_party,
        &account_id,
        &owner_party,
        &instrument_admin,
        &instrument_id,
    )?;
    let tx = state
        .committee_submitter
        .submit_and_wait_for_transaction(commands)
        .await
        .context("submit_and_wait_for_transaction")?;

    let account_ref_contract_id = extract_created_contract_id(&tx, "Pebble.Account", "AccountRef")?;
    let account_state_contract_id =
        extract_created_contract_id(&tx, "Pebble.Account", "AccountState")?;

    Ok(Json(CreateAccountResponse {
        account_id,
        account_ref_contract_id,
        account_state_contract_id,
        update_id: tx.update_id,
        offset: tx.offset,
    }))
}

#[derive(sqlx::FromRow)]
struct AccountForWithdrawalRow {
    account_id: String,
    owner_party: String,
    committee_party: String,
    instrument_admin: String,
    instrument_id: String,
    status: String,
    finalized_epoch: Option<i64>,
    cleared_cash_minor: i64,
    last_applied_epoch: i64,
    delta_pending_trades_minor: i64,
    locked_open_orders_minor: i64,
    pending_withdrawals_reserved_minor: i64,
}

#[derive(sqlx::FromRow)]
struct AccountForDepositRow {
    account_id: String,
    owner_party: String,
    committee_party: String,
    instrument_admin: String,
    instrument_id: String,
    status: String,
}

#[derive(sqlx::FromRow)]
struct UnlockedHoldingRow {
    contract_id: String,
    amount_minor: i64,
}

#[derive(sqlx::FromRow)]
struct PendingWithdrawalClaimRow {
    claim_contract_id: String,
    withdrawal_id: String,
    amount_minor: i64,
    origin_instruction_cid: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(sqlx::FromRow)]
struct PendingWithdrawalClaimSummaryRow {
    pending_count: i64,
    pending_sum_minor: i64,
}

#[derive(Deserialize)]
struct CreateDepositRequest {
    amount_minor: i64,
    deposit_id: Option<String>,
}

#[derive(Serialize)]
struct CreateDepositResponse {
    account_id: String,
    deposit_id: String,
    wallet_contract_id: String,
    transfer_instruction_contract_id: String,
    input_holding_cids: Vec<String>,
    update_id: String,
    offset: i64,
}

#[derive(Debug, Deserialize)]
struct ListMyWithdrawalClaimsPendingQuery {
    limit: Option<i64>,
}

#[derive(Debug, Serialize)]
struct WithdrawalClaimPendingView {
    claim_contract_id: String,
    account_id: String,
    withdrawal_id: String,
    amount_minor: i64,
    origin_instruction_cid: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Deserialize)]
struct ClaimWithdrawalClaimsRequest {
    claim_contract_ids: Option<Vec<String>>,
    withdrawal_id: Option<String>,
    limit: Option<i64>,
}

#[derive(Debug, Serialize)]
struct ClaimWithdrawalClaimFailure {
    claim_contract_id: String,
    code: String,
    message: String,
}

#[derive(Debug, Serialize)]
struct ClaimWithdrawalClaimsResponse {
    account_id: String,
    attempted: i64,
    claimed: i64,
    already_claimed: i64,
    failed: i64,
    failures: Vec<ClaimWithdrawalClaimFailure>,
}

#[derive(Deserialize)]
struct CreateWithdrawalRequest {
    withdrawal_id: Option<String>,
    idempotency_key: Option<String>,
    amount_minor: i64,
}

#[derive(Serialize)]
struct CreateWithdrawalResponse {
    account_id: String,
    withdrawal_id: String,
    contract_id: Option<String>,
    update_id: Option<String>,
    offset: Option<i64>,
    request_state: String,
    create_intent_state: String,
    withdrawable_minor_before: i64,
    pending_withdrawals_reserved_minor_after: i64,
}

#[derive(sqlx::FromRow)]
struct ExistingCreateIntentRow {
    account_id: String,
    withdrawal_id: String,
    state: String,
    request_contract_id: Option<String>,
}

struct WithdrawalEligibilityResult {
    eligible: bool,
    withdrawable_minor: i64,
    blocking_reasons: Vec<&'static str>,
}

#[derive(Serialize)]
struct WithdrawalEligibilityResponse {
    eligible: bool,
    account_status: String,
    withdrawable_minor: i64,
    pending_withdrawals_reserved_minor: i64,
    blocking_reasons: Vec<String>,
}

async fn create_deposit_request(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateDepositRequest>,
) -> Result<Json<CreateDepositResponse>, ApiError> {
    let account_id = m4_views::require_user_account(&state, &headers).await?;
    let account_id =
        parse_account_id(&account_id).map_err(|err| ApiError::bad_request(err.to_string()))?;

    if req.amount_minor <= 0 {
        return Err(ApiError::bad_request("amount_minor must be > 0"));
    }

    let deposit_id = match req.deposit_id {
        Some(id) if !id.trim().is_empty() => {
            parse_deposit_id(id.trim()).map_err(|err| ApiError::bad_request(err.to_string()))?
        }
        _ => parse_deposit_id(&Uuid::new_v4().to_string())
            .map_err(|err| ApiError::bad_request(err.to_string()))?,
    };

    let mut tx = state
        .db
        .begin()
        .await
        .context("begin create_deposit_request tx")?;
    advisory_lock_account(&mut tx, state.account_lock_namespace, &account_id).await?;

    let account = load_account_for_deposit(&mut tx, &account_id).await?;
    if account.status == "Closed" {
        return Err(ApiError::bad_request("account is closed"));
    }

    let holdings = load_unlocked_user_holdings_for_deposit(&mut tx, &account).await?;
    let (input_holding_cids, selected_sum_minor) =
        select_input_holdings_for_deposit(&holdings, req.amount_minor);
    if selected_sum_minor < req.amount_minor {
        let pending_claim_summary =
            load_pending_withdrawal_claim_summary_for_account(&mut tx, &account).await?;
        tx.commit()
            .await
            .context("commit create_deposit_request tx (insufficient holdings)")?;
        let mut message = format!(
            "insufficient unlocked token holdings: available={selected_sum_minor}, requested={}",
            req.amount_minor
        );
        if pending_claim_summary.pending_count > 0 {
            message.push_str(&format!(
                "; pending withdrawal claims detected: count={}, amount_minor={}; claim withdrawn funds then retry deposit",
                pending_claim_summary.pending_count, pending_claim_summary.pending_sum_minor
            ));
        }
        return Err(ApiError::bad_request(message));
    }

    tx.commit()
        .await
        .context("commit create_deposit_request tx")?;

    let wallet_contract_id = ensure_user_wallet_contract_id(&state, &account).await?;

    let command_id = format!("deposit:offer:{}:{}", account_id, deposit_id);
    let lock_expires_at_micros =
        (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp_micros();
    let command_input = CreateDepositOfferCommandInput {
        user_id: &state.ledger_cfg.user_id,
        owner_party: &account.owner_party,
        committee_party: &account.committee_party,
        wallet_contract_id: &wallet_contract_id,
        account_id: &account.account_id,
        deposit_id: &deposit_id,
        amount_minor: req.amount_minor,
        input_holding_cids: &input_holding_cids,
        lock_expires_at_micros,
        command_id: &command_id,
    };
    let commands = build_create_deposit_offer_commands(&command_input)?;
    let submitted_tx = state
        .committee_submitter
        .submit_and_wait_for_transaction(commands)
        .await
        .context("submit deposit offer command")?;
    let transfer_instruction_contract_id = extract_created_contract_id(
        &submitted_tx,
        "Wizardcat.Token.Standard",
        "TransferInstruction",
    )?;

    Ok(Json(CreateDepositResponse {
        account_id,
        deposit_id,
        wallet_contract_id,
        transfer_instruction_contract_id,
        input_holding_cids,
        update_id: submitted_tx.update_id,
        offset: submitted_tx.offset,
    }))
}

async fn list_my_withdrawal_claims_pending(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListMyWithdrawalClaimsPendingQuery>,
) -> Result<Json<Vec<WithdrawalClaimPendingView>>, ApiError> {
    let account_id = m4_views::require_user_account(&state, &headers).await?;
    let account_id =
        parse_account_id(&account_id).map_err(|err| ApiError::bad_request(err.to_string()))?;
    let limit = parse_withdrawal_claim_query_limit(query.limit)?;
    let account = load_active_account_for_user(&state.db, &account_id).await?;
    let pending_claims =
        load_pending_withdrawal_claims_for_account(&state.db, &account, None, None, limit).await?;

    let rows = pending_claims
        .into_iter()
        .map(|row| WithdrawalClaimPendingView {
            claim_contract_id: row.claim_contract_id,
            account_id: account_id.clone(),
            withdrawal_id: row.withdrawal_id,
            amount_minor: row.amount_minor,
            origin_instruction_cid: row.origin_instruction_cid,
            created_at: row.created_at,
        })
        .collect::<Vec<_>>();

    Ok(Json(rows))
}

async fn claim_my_withdrawal_claims(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ClaimWithdrawalClaimsRequest>,
) -> Result<Json<ClaimWithdrawalClaimsResponse>, ApiError> {
    let account_id = m4_views::require_user_account(&state, &headers).await?;
    let account_id =
        parse_account_id(&account_id).map_err(|err| ApiError::bad_request(err.to_string()))?;
    let account = load_active_account_for_user(&state.db, &account_id).await?;
    let withdrawal_id = normalize_optional_withdrawal_id(req.withdrawal_id)?;
    let claim_contract_ids = normalize_requested_claim_contract_ids(req.claim_contract_ids)?;

    let query_limit = if let Some(claim_contract_ids) = claim_contract_ids.as_ref() {
        i64::try_from(claim_contract_ids.len()).map_err(|_| {
            ApiError::bad_request_code(
                "too many claim_contract_ids",
                "WITHDRAWAL_CLAIM_INVALID_INPUT",
            )
        })?
    } else {
        parse_withdrawal_claim_query_limit(req.limit)?
    };

    let pending_claims = load_pending_withdrawal_claims_for_account(
        &state.db,
        &account,
        withdrawal_id.as_deref(),
        claim_contract_ids.as_deref(),
        query_limit,
    )
    .await?;

    let mut claimed = 0_i64;
    let mut already_claimed = 0_i64;
    let mut failed = 0_i64;
    let mut failures = Vec::new();
    let mut attempted = i64::try_from(pending_claims.len())
        .map_err(|_| ApiError::internal("attempted count overflow"))?;

    if let Some(requested_ids) = claim_contract_ids.as_ref() {
        attempted = i64::try_from(requested_ids.len())
            .map_err(|_| ApiError::internal("attempted count overflow"))?;
        let found = pending_claims
            .iter()
            .map(|row| row.claim_contract_id.as_str())
            .collect::<HashSet<_>>();
        for claim_contract_id in requested_ids {
            if !found.contains(claim_contract_id.as_str()) {
                failed = failed.saturating_add(1);
                failures.push(ClaimWithdrawalClaimFailure {
                    claim_contract_id: claim_contract_id.clone(),
                    code: "WITHDRAWAL_CLAIM_NOT_FOUND".to_string(),
                    message: "claim not found for authenticated account or already consumed"
                        .to_string(),
                });
            }
        }
    }

    for claim in pending_claims {
        let command_id = withdrawal_claim_command_id(&account_id, &claim.claim_contract_id);
        let commands = build_claim_holding_claim_commands(
            &state.ledger_cfg.user_id,
            &account.owner_party,
            &claim.claim_contract_id,
            &command_id,
        );

        match state
            .committee_submitter
            .submit_and_wait_for_transaction(commands)
            .await
        {
            Ok(_tx) => {
                claimed = claimed.saturating_add(1);
            }
            Err(err) => {
                if is_idempotent_contract_advance_error(&err) {
                    already_claimed = already_claimed.saturating_add(1);
                    continue;
                }

                failed = failed.saturating_add(1);
                if is_permission_denied_submission_error(&err) {
                    failures.push(ClaimWithdrawalClaimFailure {
                        claim_contract_id: claim.claim_contract_id,
                        code: "WITHDRAWAL_CLAIM_PERMISSION_DENIED".to_string(),
                        message: "claim submission denied by ledger authorization".to_string(),
                    });
                    continue;
                }

                let (code, message) = if is_unknown_submission_error(&err) {
                    (
                        "WITHDRAWAL_CLAIM_SUBMISSION_UNKNOWN",
                        "claim submission outcome unknown; retry later",
                    )
                } else {
                    (
                        "WITHDRAWAL_CLAIM_SUBMISSION_UNKNOWN",
                        "claim submission failed; retry later",
                    )
                };
                failures.push(ClaimWithdrawalClaimFailure {
                    claim_contract_id: claim.claim_contract_id,
                    code: code.to_string(),
                    message: format!("{message}: {}", error_string(&err)),
                });
            }
        }
    }

    Ok(Json(ClaimWithdrawalClaimsResponse {
        account_id,
        attempted,
        claimed,
        already_claimed,
        failed,
        failures,
    }))
}

async fn create_withdrawal_request(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateWithdrawalRequest>,
) -> Result<Json<CreateWithdrawalResponse>, ApiError> {
    let account_id = m4_views::require_user_account(&state, &headers).await?;
    let account_id =
        parse_account_id(&account_id).map_err(|err| ApiError::bad_request(err.to_string()))?;

    if req.amount_minor <= 0 {
        return Err(ApiError::bad_request("amount_minor must be > 0"));
    }

    let idempotency_key = match req.idempotency_key {
        Some(raw) if !raw.trim().is_empty() => raw.trim().to_string(),
        _ => request_id_from_headers(&headers),
    };
    if idempotency_key.len() > 128 {
        return Err(ApiError::bad_request(
            "idempotency_key length must be <= 128",
        ));
    }

    let withdrawal_id = match req.withdrawal_id {
        Some(id) if !id.trim().is_empty() => {
            parse_withdrawal_id(id.trim()).map_err(|err| ApiError::bad_request(err.to_string()))?
        }
        _ => parse_withdrawal_id(&Uuid::new_v4().to_string())
            .map_err(|err| ApiError::bad_request(err.to_string()))?,
    };

    if state.withdrawal_submission_saga_enabled {
        return create_withdrawal_request_saga(
            &state,
            &account_id,
            req.amount_minor,
            &idempotency_key,
            &withdrawal_id,
        )
        .await;
    }

    create_withdrawal_request_legacy(&state, &account_id, req.amount_minor, &withdrawal_id).await
}

fn initial_withdrawal_request_state_label(state: &AppState) -> String {
    if state.withdrawal_request_state_enabled {
        "Queued".to_string()
    } else {
        "Legacy".to_string()
    }
}

async fn create_withdrawal_request_legacy(
    state: &AppState,
    account_id: &str,
    amount_minor: i64,
    withdrawal_id: &str,
) -> Result<Json<CreateWithdrawalResponse>, ApiError> {
    let mut tx = state
        .db
        .begin()
        .await
        .context("begin create_withdrawal_request legacy tx")?;
    advisory_lock_account(&mut tx, state.account_lock_namespace, account_id).await?;

    let account = load_account_for_withdrawal(&mut tx, account_id).await?;
    let eligibility = evaluate_withdrawal_eligibility(
        &account,
        Some(amount_minor),
        state.active_withdrawals_enabled,
    )?;

    if state.withdrawal_reject_ineligible_at_create && !eligibility.eligible {
        let reason_code = eligibility
            .blocking_reasons
            .first()
            .copied()
            .unwrap_or("WITHDRAWAL_INELIGIBLE_STATUS");
        let reason_message = withdrawal_error_message(
            reason_code,
            &account.status,
            eligibility.withdrawable_minor,
            amount_minor,
        );
        tx.commit()
            .await
            .context("commit create_withdrawal_request legacy tx (ineligible)")?;
        return Err(ApiError::bad_request_code(reason_message, reason_code));
    }
    ensure_withdrawal_one_shot_policy_for_account(&mut tx, state, &account).await?;

    tx.commit()
        .await
        .context("commit create_withdrawal_request legacy tx")?;

    let command_id = withdrawal_create_command_id(account_id, withdrawal_id);
    let command_input = CreateWithdrawalRequestCommandInput {
        user_id: &state.ledger_cfg.user_id,
        owner_party: &account.owner_party,
        committee_party: &account.committee_party,
        account_id: &account.account_id,
        instrument_admin: &account.instrument_admin,
        instrument_id: &account.instrument_id,
        withdrawal_id,
        amount_minor,
        command_id: &command_id,
    };
    let commands = build_create_withdrawal_request_commands(&command_input)?;
    let submit_result = state
        .committee_submitter
        .submit_and_wait_for_transaction(commands)
        .await;

    match submit_result {
        Ok(submitted_tx) => {
            let contract_id =
                extract_created_contract_id(&submitted_tx, "Pebble.Treasury", "WithdrawalRequest")?;
            Ok(Json(CreateWithdrawalResponse {
                account_id: account_id.to_string(),
                withdrawal_id: withdrawal_id.to_string(),
                contract_id: Some(contract_id),
                update_id: Some(submitted_tx.update_id),
                offset: Some(submitted_tx.offset),
                request_state: initial_withdrawal_request_state_label(state),
                create_intent_state: "Bypassed".to_string(),
                withdrawable_minor_before: eligibility.withdrawable_minor,
                pending_withdrawals_reserved_minor_after: account
                    .pending_withdrawals_reserved_minor,
            }))
        }
        Err(err) => {
            if is_unknown_submission_error(&err) {
                return Err(ApiError::service_unavailable_code(
                    "withdrawal submission outcome unknown; reconciliation pending",
                    "WITHDRAWAL_SUBMISSION_UNKNOWN",
                ));
            }

            Err(ApiError::internal("withdrawal submission failed"))
        }
    }
}

async fn create_withdrawal_request_saga(
    state: &AppState,
    account_id: &str,
    amount_minor: i64,
    idempotency_key: &str,
    withdrawal_id: &str,
) -> Result<Json<CreateWithdrawalResponse>, ApiError> {
    let mut tx = state
        .db
        .begin()
        .await
        .context("begin create_withdrawal_request tx")?;
    advisory_lock_account(&mut tx, state.account_lock_namespace, account_id).await?;

    let account = load_account_for_withdrawal(&mut tx, account_id).await?;
    let eligibility = evaluate_withdrawal_eligibility(
        &account,
        Some(amount_minor),
        state.active_withdrawals_enabled,
    )?;

    let existing_intent: Option<ExistingCreateIntentRow> = sqlx::query_as(
        r#"
        SELECT
          account_id,
          withdrawal_id,
          state,
          request_contract_id
        FROM withdrawal_create_intents
        WHERE idempotency_key = $1
        LIMIT 1
        FOR UPDATE
        "#,
    )
    .bind(idempotency_key)
    .fetch_optional(&mut *tx)
    .await
    .context("query existing withdrawal create intent by idempotency key")?;
    if let Some(existing) = existing_intent {
        match existing.state.as_str() {
            "Committed" => {
                tx.commit()
                    .await
                    .context("commit create_withdrawal_request tx (idempotent committed)")?;
                return Ok(Json(CreateWithdrawalResponse {
                    account_id: existing.account_id,
                    withdrawal_id: existing.withdrawal_id,
                    contract_id: existing.request_contract_id,
                    update_id: None,
                    offset: None,
                    request_state: initial_withdrawal_request_state_label(state),
                    create_intent_state: existing.state,
                    withdrawable_minor_before: eligibility.withdrawable_minor,
                    pending_withdrawals_reserved_minor_after: account
                        .pending_withdrawals_reserved_minor,
                }));
            }
            "Prepared" | "Submitted" | "FailedSubmitUnknown" => {
                return Err(ApiError::conflict_code(
                    "duplicate in-flight withdrawal submission",
                    "WITHDRAWAL_DUPLICATE_IN_FLIGHT",
                ));
            }
            "RejectedIneligible" | "Failed" => {
                return Err(ApiError::conflict_code(
                    "duplicate withdrawal idempotency_key has terminal failed intent",
                    "WITHDRAWAL_DUPLICATE_IN_FLIGHT",
                ));
            }
            _ => {
                return Err(ApiError::internal(
                    "unknown withdrawal create intent state in database",
                ));
            }
        }
    }
    ensure_withdrawal_one_shot_policy_for_account(&mut tx, state, &account).await?;

    let intent_id = Uuid::new_v4().to_string();
    let insert_intent_res = sqlx::query(
        r#"
        INSERT INTO withdrawal_create_intents (
          intent_id,
          idempotency_key,
          account_id,
          instrument_admin,
          instrument_id,
          withdrawal_id,
          amount_minor,
          command_id,
          request_contract_id,
          state,
          last_error,
          created_at,
          updated_at
        )
        VALUES ($1::uuid,$2,$3,$4,$5,$6,$7,NULL,NULL,'Prepared',NULL,now(),now())
        "#,
    )
    .bind(&intent_id)
    .bind(idempotency_key)
    .bind(&account.account_id)
    .bind(&account.instrument_admin)
    .bind(&account.instrument_id)
    .bind(withdrawal_id)
    .bind(amount_minor)
    .execute(&mut *tx)
    .await;
    if let Err(err) = insert_intent_res {
        if is_unique_violation(&err) {
            return Err(ApiError::conflict_code(
                "duplicate in-flight withdrawal submission",
                "WITHDRAWAL_DUPLICATE_IN_FLIGHT",
            ));
        }
        return Err(anyhow::Error::new(err)
            .context("insert withdrawal create intent")
            .into());
    }

    if !eligibility.eligible {
        let reason_code = eligibility
            .blocking_reasons
            .first()
            .copied()
            .unwrap_or("WITHDRAWAL_INELIGIBLE_STATUS");
        let reason_message = withdrawal_error_message(
            reason_code,
            &account.status,
            eligibility.withdrawable_minor,
            amount_minor,
        );

        sqlx::query(
            r#"
            UPDATE withdrawal_create_intents
            SET
              state = 'RejectedIneligible',
              last_error = $2,
              updated_at = now()
            WHERE intent_id = $1::uuid
            "#,
        )
        .bind(&intent_id)
        .bind(format!("{reason_code}: {reason_message}"))
        .execute(&mut *tx)
        .await
        .context("mark withdrawal create intent rejected-ineligible")?;
        tx.commit()
            .await
            .context("commit create_withdrawal_request tx (ineligible)")?;

        return Err(ApiError::bad_request_code(reason_message, reason_code));
    }

    let pending_withdrawals_reserved_minor_after =
        reserve_withdrawal_liquidity(&mut tx, &account, amount_minor).await?;

    tx.commit()
        .await
        .context("commit create_withdrawal_request tx (prepared+reserved)")?;

    let command_id = withdrawal_create_command_id(account_id, withdrawal_id);
    let mark_submitted_result = sqlx::query(
        r#"
        UPDATE withdrawal_create_intents
        SET
          state = 'Submitted',
          command_id = $2,
          updated_at = now()
        WHERE intent_id = $1::uuid
          AND state = 'Prepared'
        "#,
    )
    .bind(&intent_id)
    .bind(&command_id)
    .execute(&state.db)
    .await;
    match mark_submitted_result {
        Ok(result) if result.rows_affected() == 1 => {}
        Ok(_) => {
            let mut fail_tx = state
                .db
                .begin()
                .await
                .context("begin withdrawal create intent failed-submitted tx")?;
            let mark_error = "intent no longer Prepared before submit";
            sqlx::query(
                r#"
                UPDATE withdrawal_create_intents
                SET
                  state = 'Failed',
                  command_id = $2,
                  last_error = $3,
                  updated_at = now()
                WHERE intent_id = $1::uuid
                "#,
            )
            .bind(&intent_id)
            .bind(&command_id)
            .bind(mark_error)
            .execute(&mut *fail_tx)
            .await
            .context("mark withdrawal create intent failed after submitted-state mismatch")?;
            release_withdrawal_liquidity(
                &mut fail_tx,
                state.account_lock_namespace,
                &account.account_id,
                amount_minor,
            )
            .await?;
            fail_tx
                .commit()
                .await
                .context("commit withdrawal create intent failed-submitted tx")?;
            return Err(ApiError::internal(mark_error));
        }
        Err(err) => {
            let mut fail_tx = state
                .db
                .begin()
                .await
                .context("begin withdrawal create intent submitted-error tx")?;
            let mark_error = anyhow::Error::new(err)
                .context("mark withdrawal create intent submitted")
                .to_string();
            sqlx::query(
                r#"
                UPDATE withdrawal_create_intents
                SET
                  state = 'Failed',
                  command_id = $2,
                  last_error = $3,
                  updated_at = now()
                WHERE intent_id = $1::uuid
                "#,
            )
            .bind(&intent_id)
            .bind(&command_id)
            .bind(&mark_error)
            .execute(&mut *fail_tx)
            .await
            .context("mark withdrawal create intent failed after submitted update error")?;
            release_withdrawal_liquidity(
                &mut fail_tx,
                state.account_lock_namespace,
                &account.account_id,
                amount_minor,
            )
            .await?;
            fail_tx
                .commit()
                .await
                .context("commit withdrawal create intent submitted-error tx")?;
            return Err(ApiError::internal(
                "failed to persist withdrawal intent submitted state",
            ));
        }
    }

    let command_input = CreateWithdrawalRequestCommandInput {
        user_id: &state.ledger_cfg.user_id,
        owner_party: &account.owner_party,
        committee_party: &account.committee_party,
        account_id: &account.account_id,
        instrument_admin: &account.instrument_admin,
        instrument_id: &account.instrument_id,
        withdrawal_id,
        amount_minor,
        command_id: &command_id,
    };
    let commands = build_create_withdrawal_request_commands(&command_input)?;
    let submit_result = state
        .committee_submitter
        .submit_and_wait_for_transaction(commands)
        .await;

    match submit_result {
        Ok(submitted_tx) => {
            let contract_id =
                extract_created_contract_id(&submitted_tx, "Pebble.Treasury", "WithdrawalRequest")?;
            sqlx::query(
                r#"
                UPDATE withdrawal_create_intents
                SET
                  state = 'Committed',
                  command_id = $2,
                  request_contract_id = $3,
                  last_error = NULL,
                  updated_at = now()
                WHERE intent_id = $1::uuid
                "#,
            )
            .bind(&intent_id)
            .bind(&command_id)
            .bind(&contract_id)
            .execute(&state.db)
            .await
            .context("mark withdrawal create intent committed")?;

            Ok(Json(CreateWithdrawalResponse {
                account_id: account_id.to_string(),
                withdrawal_id: withdrawal_id.to_string(),
                contract_id: Some(contract_id),
                update_id: Some(submitted_tx.update_id),
                offset: Some(submitted_tx.offset),
                request_state: initial_withdrawal_request_state_label(state),
                create_intent_state: "Committed".to_string(),
                withdrawable_minor_before: eligibility.withdrawable_minor,
                pending_withdrawals_reserved_minor_after,
            }))
        }
        Err(err) => {
            let unknown_submit = is_unknown_submission_error(&err);
            let mut fail_tx = state
                .db
                .begin()
                .await
                .context("begin withdrawal create intent failure tx")?;

            let failure_state = if unknown_submit {
                "FailedSubmitUnknown"
            } else {
                "Failed"
            };
            sqlx::query(
                r#"
                UPDATE withdrawal_create_intents
                SET
                  state = $2,
                  command_id = $3,
                  last_error = $4,
                  updated_at = now()
                WHERE intent_id = $1::uuid
                "#,
            )
            .bind(&intent_id)
            .bind(failure_state)
            .bind(&command_id)
            .bind(error_string(&err))
            .execute(&mut *fail_tx)
            .await
            .context("mark withdrawal create intent failed")?;

            if !unknown_submit {
                release_withdrawal_liquidity(
                    &mut fail_tx,
                    state.account_lock_namespace,
                    &account.account_id,
                    amount_minor,
                )
                .await?;
            }
            fail_tx
                .commit()
                .await
                .context("commit withdrawal create intent failure tx")?;

            if unknown_submit {
                Err(ApiError::service_unavailable_code(
                    "withdrawal submission outcome unknown; reconciliation pending",
                    "WITHDRAWAL_SUBMISSION_UNKNOWN",
                ))
            } else {
                Err(ApiError::internal("withdrawal submission failed"))
            }
        }
    }
}

async fn get_my_withdrawal_eligibility(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<WithdrawalEligibilityResponse>, ApiError> {
    let account_id = m4_views::require_user_account(&state, &headers).await?;
    let account_id =
        parse_account_id(&account_id).map_err(|err| ApiError::bad_request(err.to_string()))?;

    let mut tx = state
        .db
        .begin()
        .await
        .context("begin get_my_withdrawal_eligibility tx")?;
    advisory_lock_account(&mut tx, state.account_lock_namespace, &account_id).await?;

    let account = load_account_for_withdrawal(&mut tx, &account_id).await?;
    let eligibility =
        evaluate_withdrawal_eligibility(&account, None, state.active_withdrawals_enabled)?;

    tx.commit()
        .await
        .context("commit get_my_withdrawal_eligibility tx")?;

    Ok(Json(WithdrawalEligibilityResponse {
        eligible: eligibility.eligible,
        account_status: account.status,
        withdrawable_minor: eligibility.withdrawable_minor,
        pending_withdrawals_reserved_minor: account.pending_withdrawals_reserved_minor,
        blocking_reasons: eligibility
            .blocking_reasons
            .into_iter()
            .map(std::string::ToString::to_string)
            .collect(),
    }))
}

fn parse_withdrawal_claim_query_limit(limit: Option<i64>) -> Result<i64, ApiError> {
    let limit = limit.unwrap_or(WITHDRAWAL_CLAIM_QUERY_DEFAULT_LIMIT);
    if limit <= 0 {
        return Err(ApiError::bad_request_code(
            "limit must be > 0",
            "WITHDRAWAL_CLAIM_INVALID_INPUT",
        ));
    }
    Ok(limit.min(WITHDRAWAL_CLAIM_QUERY_MAX_LIMIT))
}

fn normalize_optional_withdrawal_id(raw: Option<String>) -> Result<Option<String>, ApiError> {
    let Some(raw) = raw else {
        return Ok(None);
    };

    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    parse_withdrawal_id(trimmed).map(Some).map_err(|err| {
        ApiError::bad_request_code(err.to_string(), "WITHDRAWAL_CLAIM_INVALID_INPUT")
    })
}

fn normalize_requested_claim_contract_ids(
    raw: Option<Vec<String>>,
) -> Result<Option<Vec<String>>, ApiError> {
    let Some(raw_ids) = raw else {
        return Ok(None);
    };

    if raw_ids.is_empty() {
        return Err(ApiError::bad_request_code(
            "claim_contract_ids must not be empty",
            "WITHDRAWAL_CLAIM_INVALID_INPUT",
        ));
    }

    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for claim_contract_id in raw_ids {
        let trimmed = claim_contract_id.trim();
        if trimmed.is_empty() {
            return Err(ApiError::bad_request_code(
                "claim_contract_ids entries must be non-empty",
                "WITHDRAWAL_CLAIM_INVALID_INPUT",
            ));
        }

        let id = trimmed.to_string();
        if seen.insert(id.clone()) {
            out.push(id);
        }
    }

    if out.len() > WITHDRAWAL_CLAIM_QUERY_MAX_LIMIT_USIZE {
        return Err(ApiError::bad_request_code(
            format!(
                "claim_contract_ids length must be <= {}",
                WITHDRAWAL_CLAIM_QUERY_MAX_LIMIT
            ),
            "WITHDRAWAL_CLAIM_INVALID_INPUT",
        ));
    }

    Ok(Some(out))
}

async fn load_active_account_for_user(
    db: &PgPool,
    account_id: &str,
) -> Result<AccountForDepositRow, ApiError> {
    let row: Option<AccountForDepositRow> = sqlx::query_as(
        r#"
        SELECT
          ar.account_id,
          ar.owner_party,
          ar.committee_party,
          ar.instrument_admin,
          ar.instrument_id,
          ar.status
        FROM account_refs ar
        JOIN account_ref_latest arl
          ON arl.account_id = ar.account_id
         AND arl.contract_id = ar.contract_id
        WHERE ar.account_id = $1
          AND ar.active = TRUE
        LIMIT 1
        "#,
    )
    .bind(account_id)
    .fetch_optional(db)
    .await
    .context("query active account for user")?;

    row.ok_or_else(|| ApiError::not_found("account not found"))
}

async fn load_pending_withdrawal_claims_for_account(
    db: &PgPool,
    account: &AccountForDepositRow,
    withdrawal_id: Option<&str>,
    claim_contract_ids: Option<&[String]>,
    limit: i64,
) -> Result<Vec<PendingWithdrawalClaimRow>, ApiError> {
    if let Some(claim_contract_ids) = claim_contract_ids {
        return sqlx::query_as(
            r#"
            SELECT
              h.contract_id AS claim_contract_id,
              (
                SELECT wr.withdrawal_id
                FROM withdrawal_receipts wr
                WHERE wr.active = TRUE
                  AND wr.status = $4
                  AND wr.lineage_root_instruction_cid = h.origin_instruction_cid
                  AND wr.owner_party = h.owner_party
                  AND wr.instrument_admin = h.instrument_admin
                  AND wr.instrument_id = h.instrument_id
                  AND ($5::TEXT IS NULL OR wr.withdrawal_id = $5)
                ORDER BY wr.created_at DESC, wr.contract_id DESC
                LIMIT 1
              ) AS withdrawal_id,
              h.amount_minor,
              h.origin_instruction_cid AS origin_instruction_cid,
              h.created_at
            FROM token_holding_claims h
            WHERE h.active = TRUE
              AND h.owner_party = $1
              AND h.instrument_admin = $2
              AND h.instrument_id = $3
              AND h.origin_instruction_cid IS NOT NULL
              AND h.contract_id = ANY($6::TEXT[])
              AND EXISTS (
                SELECT 1
                FROM withdrawal_receipts wr
                WHERE wr.active = TRUE
                  AND wr.status = $4
                  AND wr.lineage_root_instruction_cid = h.origin_instruction_cid
                  AND wr.owner_party = h.owner_party
                  AND wr.instrument_admin = h.instrument_admin
                  AND wr.instrument_id = h.instrument_id
                  AND ($5::TEXT IS NULL OR wr.withdrawal_id = $5)
              )
            ORDER BY h.created_at ASC, h.contract_id ASC
            LIMIT $7
            "#,
        )
        .bind(&account.owner_party)
        .bind(&account.instrument_admin)
        .bind(&account.instrument_id)
        .bind(WITHDRAWAL_RECEIPT_STATUS_COMPLETED)
        .bind(withdrawal_id)
        .bind(claim_contract_ids.to_vec())
        .bind(limit)
        .fetch_all(db)
        .await
        .context("query pending withdrawal claims by contract ids")
        .map_err(ApiError::from);
    }

    sqlx::query_as(
        r#"
        SELECT
          h.contract_id AS claim_contract_id,
          (
            SELECT wr.withdrawal_id
            FROM withdrawal_receipts wr
            WHERE wr.active = TRUE
              AND wr.status = $4
              AND wr.lineage_root_instruction_cid = h.origin_instruction_cid
              AND wr.owner_party = h.owner_party
              AND wr.instrument_admin = h.instrument_admin
              AND wr.instrument_id = h.instrument_id
              AND ($5::TEXT IS NULL OR wr.withdrawal_id = $5)
            ORDER BY wr.created_at DESC, wr.contract_id DESC
            LIMIT 1
          ) AS withdrawal_id,
          h.amount_minor,
          h.origin_instruction_cid AS origin_instruction_cid,
          h.created_at
        FROM token_holding_claims h
        WHERE h.active = TRUE
          AND h.owner_party = $1
          AND h.instrument_admin = $2
          AND h.instrument_id = $3
          AND h.origin_instruction_cid IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM withdrawal_receipts wr
            WHERE wr.active = TRUE
              AND wr.status = $4
              AND wr.lineage_root_instruction_cid = h.origin_instruction_cid
              AND wr.owner_party = h.owner_party
              AND wr.instrument_admin = h.instrument_admin
              AND wr.instrument_id = h.instrument_id
              AND ($5::TEXT IS NULL OR wr.withdrawal_id = $5)
          )
        ORDER BY h.created_at ASC, h.contract_id ASC
        LIMIT $6
        "#,
    )
    .bind(&account.owner_party)
    .bind(&account.instrument_admin)
    .bind(&account.instrument_id)
    .bind(WITHDRAWAL_RECEIPT_STATUS_COMPLETED)
    .bind(withdrawal_id)
    .bind(limit)
    .fetch_all(db)
    .await
    .context("query pending withdrawal claims")
    .map_err(ApiError::from)
}

async fn load_pending_withdrawal_claim_summary_for_account(
    tx: &mut Transaction<'_, Postgres>,
    account: &AccountForDepositRow,
) -> Result<PendingWithdrawalClaimSummaryRow, ApiError> {
    sqlx::query_as(
        r#"
        SELECT
          count(*)::BIGINT AS pending_count,
          COALESCE(sum(h.amount_minor), 0)::BIGINT AS pending_sum_minor
        FROM token_holding_claims h
        WHERE h.active = TRUE
          AND h.owner_party = $1
          AND h.instrument_admin = $2
          AND h.instrument_id = $3
          AND h.origin_instruction_cid IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM withdrawal_receipts wr
            WHERE wr.active = TRUE
              AND wr.status = $4
              AND wr.lineage_root_instruction_cid = h.origin_instruction_cid
              AND wr.owner_party = h.owner_party
              AND wr.instrument_admin = h.instrument_admin
              AND wr.instrument_id = h.instrument_id
          )
        "#,
    )
    .bind(&account.owner_party)
    .bind(&account.instrument_admin)
    .bind(&account.instrument_id)
    .bind(WITHDRAWAL_RECEIPT_STATUS_COMPLETED)
    .fetch_one(&mut **tx)
    .await
    .context("query pending withdrawal claim summary")
    .map_err(ApiError::from)
}

async fn load_account_for_deposit(
    tx: &mut Transaction<'_, Postgres>,
    account_id: &str,
) -> Result<AccountForDepositRow, ApiError> {
    let row: Option<AccountForDepositRow> = sqlx::query_as(
        r#"
        SELECT
          ar.account_id,
          ar.owner_party,
          ar.committee_party,
          ar.instrument_admin,
          ar.instrument_id,
          ar.status
        FROM account_refs ar
        JOIN account_ref_latest arl
          ON arl.account_id = ar.account_id
         AND arl.contract_id = ar.contract_id
        WHERE ar.account_id = $1
          AND ar.active = TRUE
        LIMIT 1
        FOR UPDATE OF ar
        "#,
    )
    .bind(account_id)
    .fetch_optional(&mut **tx)
    .await
    .context("query account for deposit")?;

    row.ok_or_else(|| ApiError::not_found("account not found"))
}

async fn load_unlocked_user_holdings_for_deposit(
    tx: &mut Transaction<'_, Postgres>,
    account: &AccountForDepositRow,
) -> Result<Vec<UnlockedHoldingRow>, ApiError> {
    sqlx::query_as(
        r#"
        SELECT contract_id, amount_minor
        FROM token_holdings
        WHERE active = TRUE
          AND owner_party = $1
          AND instrument_admin = $2
          AND instrument_id = $3
          AND lock_status_class IS NULL
        ORDER BY amount_minor DESC, contract_id ASC
        LIMIT 200
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(&account.owner_party)
    .bind(&account.instrument_admin)
    .bind(&account.instrument_id)
    .fetch_all(&mut **tx)
    .await
    .context("query unlocked user holdings for deposit")
    .map_err(ApiError::from)
}

fn select_input_holdings_for_deposit(
    holdings: &[UnlockedHoldingRow],
    amount_minor: i64,
) -> (Vec<String>, i64) {
    let mut selected = Vec::new();
    let mut sum_minor = 0_i64;
    for holding in holdings {
        selected.push(holding.contract_id.clone());
        sum_minor = sum_minor.saturating_add(holding.amount_minor);
        if sum_minor >= amount_minor {
            break;
        }
    }
    (selected, sum_minor)
}

async fn ensure_user_wallet_contract_id(
    state: &AppState,
    account: &AccountForDepositRow,
) -> Result<String, ApiError> {
    let load_existing_wallet = || async {
        let existing_wallet: Option<(String,)> = sqlx::query_as(
            r#"
            SELECT contract_id
            FROM token_wallets
            WHERE active = TRUE
              AND owner_party = $1
              AND instrument_admin = $2
              AND instrument_id = $3
            ORDER BY created_at DESC, contract_id DESC
            LIMIT 1
            "#,
        )
        .bind(&account.owner_party)
        .bind(&account.instrument_admin)
        .bind(&account.instrument_id)
        .fetch_optional(&state.db)
        .await
        .context("query existing user wallet")?;
        Ok::<Option<String>, ApiError>(existing_wallet.map(|(contract_id,)| contract_id))
    };

    if let Some(wallet_contract_id) = load_existing_wallet().await? {
        return Ok(wallet_contract_id);
    }

    let command_id = format!(
        "wallet:create:{}:{}:{}",
        account.owner_party, account.instrument_admin, account.instrument_id
    );
    let commands = build_create_wallet_commands(
        &state.ledger_cfg.user_id,
        &command_id,
        &account.owner_party,
        &account.instrument_admin,
        &account.instrument_id,
    );
    let tx = match state
        .committee_submitter
        .submit_and_wait_for_transaction(commands)
        .await
    {
        Ok(tx) => tx,
        Err(err) => {
            if !is_duplicate_command_submission_error(&err) {
                return Err(ApiError::from(err.context("submit wallet create command")));
            }

            // Duplicate command means the create was already submitted. Wait for projection.
            let started = Instant::now();
            let max_wait = Duration::from_secs(8);
            loop {
                if let Some(wallet_contract_id) = load_existing_wallet().await? {
                    tracing::warn!(
                        owner_party = %account.owner_party,
                        instrument_admin = %account.instrument_admin,
                        instrument_id = %account.instrument_id,
                        elapsed_ms = started.elapsed().as_millis(),
                        "recovered wallet id after duplicate wallet-create command"
                    );
                    return Ok(wallet_contract_id);
                }

                if started.elapsed() >= max_wait {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }

            return Err(ApiError::service_unavailable_code(
                "wallet is being initialized; retry shortly",
                "wallet_projection_lag",
            ));
        }
    };
    extract_created_contract_id(&tx, "Wizardcat.Token.Standard", "Wallet").map_err(ApiError::from)
}

fn is_duplicate_command_submission_error(err: &anyhow::Error) -> bool {
    if let Some(status) = err.downcast_ref::<tonic::Status>() {
        if status.code() == tonic::Code::AlreadyExists {
            return true;
        }
        let message = status.message().to_ascii_lowercase();
        if message.contains("duplicate_command")
            || message.contains("command submission already exists")
        {
            return true;
        }
    }

    let message = error_string(err).to_ascii_lowercase();
    message.contains("duplicate_command") || message.contains("command submission already exists")
}

async fn load_account_for_withdrawal(
    tx: &mut Transaction<'_, Postgres>,
    account_id: &str,
) -> Result<AccountForWithdrawalRow, ApiError> {
    let row: Option<AccountForWithdrawalRow> = sqlx::query_as(
        r#"
        SELECT
          ar.account_id,
          ar.owner_party,
          ar.committee_party,
          ar.instrument_admin,
          ar.instrument_id,
          ar.status,
          ar.finalized_epoch,
          st.cleared_cash_minor,
          st.last_applied_epoch,
          COALESCE(rs.delta_pending_trades_minor, 0) AS delta_pending_trades_minor,
          COALESCE(rs.locked_open_orders_minor, 0) AS locked_open_orders_minor,
          COALESCE(ls.pending_withdrawals_reserved_minor, 0) AS pending_withdrawals_reserved_minor
        FROM account_refs ar
        JOIN account_ref_latest arl
          ON arl.account_id = ar.account_id
         AND arl.contract_id = ar.contract_id
        JOIN account_states st
          ON st.account_id = ar.account_id
        JOIN account_state_latest asl
          ON asl.account_id = st.account_id
         AND asl.contract_id = st.contract_id
        LEFT JOIN account_risk_state rs
          ON rs.account_id = ar.account_id
        LEFT JOIN account_liquidity_state ls
          ON ls.account_id = ar.account_id
        WHERE ar.account_id = $1
          AND ar.active = TRUE
          AND st.active = TRUE
        LIMIT 1
        FOR UPDATE OF ar, st
        "#,
    )
    .bind(account_id)
    .fetch_optional(&mut **tx)
    .await
    .context("query account for withdrawal")?;

    row.ok_or_else(|| ApiError::not_found("account not found"))
}

async fn ensure_withdrawal_one_shot_policy_for_account(
    tx: &mut Transaction<'_, Postgres>,
    state: &AppState,
    account: &AccountForWithdrawalRow,
) -> Result<(), ApiError> {
    if !state.withdrawal_one_shot_auto_accept_enabled || !state.withdrawal_one_shot_require_policy {
        return Ok(());
    }

    let policy_exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
          SELECT 1
          FROM operator_accept_policies
          WHERE active = TRUE
            AND instrument_admin = $1
            AND instrument_id = $2
            AND authorizer_party = $3
            AND operator_party = $3
            AND mode = $4
        )
        "#,
    )
    .bind(&account.instrument_admin)
    .bind(&account.instrument_id)
    .bind(&account.committee_party)
    .bind(OPERATOR_ACCEPT_MODE_WITHDRAWAL_AUTO_ACCEPT)
    .fetch_one(&mut **tx)
    .await
    .context("query withdrawal one-shot operator policy coverage")?;
    if policy_exists {
        return Ok(());
    }

    Err(ApiError::service_unavailable_code(
        "withdrawal automation policy missing for instrument; contact operator",
        "WITHDRAWAL_AUTOMATION_POLICY_MISSING",
    ))
}

fn evaluate_withdrawal_eligibility(
    account: &AccountForWithdrawalRow,
    requested_amount_minor: Option<i64>,
    active_withdrawals_enabled: bool,
) -> Result<WithdrawalEligibilityResult, ApiError> {
    let withdrawable_minor = {
        let value = i128::from(account.cleared_cash_minor)
            + i128::from(account.delta_pending_trades_minor)
            - i128::from(account.locked_open_orders_minor)
            - i128::from(account.pending_withdrawals_reserved_minor);
        i64::try_from(value).map_err(|_| ApiError::internal("withdrawable overflow"))?
    };

    let mut blocking_reasons = Vec::new();
    let status_allowed = match account.status.as_str() {
        "Active" => active_withdrawals_enabled,
        "Suspended" => true,
        "Closed" | "Suspending" => false,
        _ => false,
    };
    if !status_allowed {
        blocking_reasons.push("WITHDRAWAL_INELIGIBLE_STATUS");
    }

    if account.status == "Suspended" {
        match account.finalized_epoch {
            Some(finalized_epoch) => {
                if account.last_applied_epoch < finalized_epoch {
                    blocking_reasons.push("WITHDRAWAL_ACCOUNT_STATE_STALE");
                }
            }
            None => blocking_reasons.push("WITHDRAWAL_ACCOUNT_STATE_STALE"),
        }
    }

    if let Some(requested) = requested_amount_minor {
        if withdrawable_minor < requested {
            blocking_reasons.push("WITHDRAWAL_INSUFFICIENT_WITHDRAWABLE");
        }
    } else if withdrawable_minor <= 0 {
        blocking_reasons.push("WITHDRAWAL_INSUFFICIENT_WITHDRAWABLE");
    }

    Ok(WithdrawalEligibilityResult {
        eligible: blocking_reasons.is_empty(),
        withdrawable_minor,
        blocking_reasons,
    })
}

fn withdrawal_error_message(
    code: &str,
    account_status: &str,
    withdrawable_minor: i64,
    requested_minor: i64,
) -> String {
    match code {
        "WITHDRAWAL_INELIGIBLE_STATUS" => format!(
            "account status {account_status} is not eligible for withdrawal under current policy"
        ),
        "WITHDRAWAL_INSUFFICIENT_WITHDRAWABLE" => format!(
            "insufficient withdrawable balance: withdrawable={withdrawable_minor}, requested={requested_minor}"
        ),
        "WITHDRAWAL_ACCOUNT_STATE_STALE" => {
            "account state is stale relative to required finalized epoch".to_string()
        }
        _ => "withdrawal request is ineligible".to_string(),
    }
}

async fn reserve_withdrawal_liquidity(
    tx: &mut Transaction<'_, Postgres>,
    account: &AccountForWithdrawalRow,
    amount_minor: i64,
) -> Result<i64, ApiError> {
    let pending_reserved_after: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO account_liquidity_state (
          account_id,
          instrument_admin,
          instrument_id,
          pending_withdrawals_reserved_minor,
          version,
          updated_at
        )
        VALUES ($1, $2, $3, $4, 1, now())
        ON CONFLICT (account_id) DO UPDATE SET
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          pending_withdrawals_reserved_minor =
            account_liquidity_state.pending_withdrawals_reserved_minor
            + EXCLUDED.pending_withdrawals_reserved_minor,
          version = account_liquidity_state.version + 1,
          updated_at = now()
        RETURNING pending_withdrawals_reserved_minor
        "#,
    )
    .bind(&account.account_id)
    .bind(&account.instrument_admin)
    .bind(&account.instrument_id)
    .bind(amount_minor)
    .fetch_one(&mut **tx)
    .await
    .context("reserve withdrawal liquidity")?;

    Ok(pending_reserved_after)
}

async fn release_withdrawal_liquidity(
    tx: &mut Transaction<'_, Postgres>,
    account_lock_namespace: i32,
    account_id: &str,
    amount_minor: i64,
) -> Result<(), ApiError> {
    advisory_lock_account(tx, account_lock_namespace, account_id).await?;

    let updated_reserved: Option<i64> = sqlx::query_scalar(
        r#"
        UPDATE account_liquidity_state
        SET
          pending_withdrawals_reserved_minor = pending_withdrawals_reserved_minor - $2,
          version = version + 1,
          updated_at = now()
        WHERE account_id = $1
          AND pending_withdrawals_reserved_minor >= $2
        RETURNING pending_withdrawals_reserved_minor
        "#,
    )
    .bind(account_id)
    .bind(amount_minor)
    .fetch_optional(&mut **tx)
    .await
    .context("release withdrawal liquidity")?;

    if updated_reserved.is_none() {
        return Err(ApiError::internal(
            "cannot release withdrawal reservation: insufficient reserved amount",
        ));
    }
    Ok(())
}

fn is_unknown_submission_error(err: &anyhow::Error) -> bool {
    let Some(status) = err.downcast_ref::<tonic::Status>() else {
        return false;
    };

    matches!(
        status.code(),
        tonic::Code::Unknown
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Unavailable
            | tonic::Code::Cancelled
            | tonic::Code::ResourceExhausted
    )
}

fn is_idempotent_contract_advance_error(err: &anyhow::Error) -> bool {
    let Some(status) = err.downcast_ref::<tonic::Status>() else {
        return false;
    };

    if matches!(
        status.code(),
        tonic::Code::NotFound | tonic::Code::AlreadyExists
    ) {
        return true;
    }

    let msg = status.message().to_ascii_lowercase();
    msg.contains("not active")
        || msg.contains("already archived")
        || msg.contains("contract not found")
}

fn is_permission_denied_submission_error(err: &anyhow::Error) -> bool {
    let Some(status) = err.downcast_ref::<tonic::Status>() else {
        return false;
    };

    matches!(
        status.code(),
        tonic::Code::PermissionDenied | tonic::Code::Unauthenticated
    )
}

fn is_unique_violation(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => db_err.code().as_deref() == Some("23505"),
        _ => false,
    }
}

fn error_string(err: &anyhow::Error) -> String {
    let mut out = String::new();
    for (idx, part) in err.chain().map(ToString::to_string).enumerate() {
        if idx > 0 {
            out.push_str(": ");
        }
        out.push_str(&part);
    }
    out
}

#[derive(Debug, Deserialize)]
struct MeOnboardRequest {
    owner_party: Option<String>,
    instrument_admin: Option<String>,
    instrument_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct MeOnboardResponse {
    account_id: String,
    owner_party: String,
    instrument_admin: String,
    instrument_id: String,
    account_status: String,
    wallet_contract_id: String,
    account_created: bool,
    account_ref_contract_id: String,
    account_state_contract_id: String,
    update_id: Option<String>,
    offset: Option<i64>,
}

#[derive(Debug, sqlx::FromRow)]
struct AccountOnboardRow {
    account_id: String,
    owner_party: String,
    committee_party: String,
    instrument_admin: String,
    instrument_id: String,
    account_status: String,
    account_ref_contract_id: String,
    account_state_contract_id: String,
}

#[derive(Debug, Deserialize)]
struct RegisterUserRequest {
    username: String,
    api_key: String,
}

#[derive(Debug, Serialize)]
struct RegisterUserResponse {
    username: String,
    account_id: String,
    owner_party: String,
    instrument_admin: String,
    instrument_id: String,
    account_status: String,
    wallet_contract_id: String,
    account_created: bool,
    key_created: bool,
    account_ref_contract_id: String,
    account_state_contract_id: String,
    update_id: Option<String>,
    offset: Option<i64>,
}

#[derive(Debug, sqlx::FromRow)]
struct UserRegistrationRow {
    username: String,
    account_id: String,
    owner_party: String,
}

#[derive(Debug, sqlx::FromRow)]
struct InstrumentIdentityRow {
    instrument_admin: String,
    instrument_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonApiPartiesResponse {
    #[serde(default)]
    party_details: Vec<JsonApiPartyDetails>,
    #[serde(default)]
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JsonApiPartyDetails {
    party: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct JsonApiAllocatePartyRequest {
    party_id_hint: String,
    identity_provider_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonApiAllocatePartyResponse {
    party_details: JsonApiPartyDetails,
}

async fn register_user(
    State(state): State<AppState>,
    Json(req): Json<RegisterUserRequest>,
) -> Result<Json<RegisterUserResponse>, ApiError> {
    let username = normalize_registration_username(&req.username)?;
    let api_key = normalize_registration_api_key(&req.api_key)?;

    if state.auth_keys.is_admin_key(&api_key) {
        return Err(ApiError::conflict(
            "api_key is already configured as an admin key",
        ));
    }

    let registration = ensure_user_registration(&state, &username).await?;

    let existing_account_for_key = state.auth_keys.account_id_for_api_key(&api_key);
    if let Some(existing_account_id) = &existing_account_for_key {
        if existing_account_id != &registration.account_id {
            return Err(ApiError::conflict(
                "api_key is already configured for another account",
            ));
        }
    }

    state
        .auth_keys
        .upsert_user_api_key(&state.db, &api_key, &registration.account_id)
        .await
        .context("upsert registered user API key")?;

    let instrument = load_default_registration_instrument(&state.db).await?;
    let onboard = onboard_account_for_user(
        &state,
        &registration.account_id,
        Some(registration.owner_party.as_str()),
        Some(instrument.instrument_admin.as_str()),
        Some(instrument.instrument_id.as_str()),
    )
    .await?;

    Ok(Json(RegisterUserResponse {
        username: registration.username,
        account_id: onboard.account_id,
        owner_party: onboard.owner_party,
        instrument_admin: onboard.instrument_admin,
        instrument_id: onboard.instrument_id,
        account_status: onboard.account_status,
        wallet_contract_id: onboard.wallet_contract_id,
        account_created: onboard.account_created,
        key_created: existing_account_for_key.is_none(),
        account_ref_contract_id: onboard.account_ref_contract_id,
        account_state_contract_id: onboard.account_state_contract_id,
        update_id: onboard.update_id,
        offset: onboard.offset,
    }))
}

async fn onboard_me_account(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<MeOnboardRequest>,
) -> Result<Json<MeOnboardResponse>, ApiError> {
    let account_id = m4_views::require_user_account(&state, &headers).await?;
    let account_id =
        parse_account_id(&account_id).map_err(|err| ApiError::bad_request(err.to_string()))?;

    let response = onboard_account_for_user(
        &state,
        &account_id,
        req.owner_party.as_deref(),
        req.instrument_admin.as_deref(),
        req.instrument_id.as_deref(),
    )
    .await?;

    Ok(Json(response))
}

async fn onboard_account_for_user(
    state: &AppState,
    account_id: &str,
    owner_party: Option<&str>,
    instrument_admin: Option<&str>,
    instrument_id: Option<&str>,
) -> Result<MeOnboardResponse, ApiError> {
    let existing: Option<AccountOnboardRow> = sqlx::query_as(
        r#"
        SELECT
          ar.account_id,
          ar.owner_party,
          ar.committee_party,
          ar.instrument_admin,
          ar.instrument_id,
          ar.status AS account_status,
          ar.contract_id AS account_ref_contract_id,
          ast.contract_id AS account_state_contract_id
        FROM account_refs ar
        JOIN account_ref_latest arl
          ON arl.account_id = ar.account_id
         AND arl.contract_id = ar.contract_id
        JOIN account_states ast
          ON ast.account_id = ar.account_id
        JOIN account_state_latest asl
          ON asl.account_id = ast.account_id
         AND asl.contract_id = ast.contract_id
        WHERE ar.account_id = $1
          AND ar.active = TRUE
          AND ast.active = TRUE
        LIMIT 1
        "#,
    )
    .bind(account_id)
    .fetch_optional(&state.db)
    .await
    .context("query existing account for me onboarding")?;

    if let Some(existing) = existing {
        let wallet_contract_id = ensure_user_wallet_contract_id(
            state,
            &AccountForDepositRow {
                account_id: existing.account_id.clone(),
                owner_party: existing.owner_party.clone(),
                committee_party: existing.committee_party.clone(),
                instrument_admin: existing.instrument_admin.clone(),
                instrument_id: existing.instrument_id.clone(),
                status: existing.account_status.clone(),
            },
        )
        .await?;

        return Ok(MeOnboardResponse {
            account_id: existing.account_id,
            owner_party: existing.owner_party,
            instrument_admin: existing.instrument_admin,
            instrument_id: existing.instrument_id,
            account_status: existing.account_status,
            wallet_contract_id,
            account_created: false,
            account_ref_contract_id: existing.account_ref_contract_id,
            account_state_contract_id: existing.account_state_contract_id,
            update_id: None,
            offset: None,
        });
    }

    let owner_party = owner_party.unwrap_or_default().trim().to_string();
    if owner_party.is_empty() {
        return Err(ApiError::bad_request(
            "owner_party is required when onboarding a new account",
        ));
    }

    let instrument_admin = instrument_admin.unwrap_or_default().trim().to_string();
    if instrument_admin.is_empty() {
        return Err(ApiError::bad_request(
            "instrument_admin is required when onboarding a new account",
        ));
    }

    let instrument_id = instrument_id.unwrap_or_default().trim().to_string();
    if instrument_id.is_empty() {
        return Err(ApiError::bad_request(
            "instrument_id is required when onboarding a new account",
        ));
    }

    let commands = build_create_account_commands(
        &state.ledger_cfg.user_id,
        &state.ledger_cfg.committee_party,
        account_id,
        &owner_party,
        &instrument_admin,
        &instrument_id,
    )?;
    let tx = state
        .committee_submitter
        .submit_and_wait_for_transaction(commands)
        .await
        .context("submit_and_wait_for_transaction")?;

    let account_ref_contract_id = extract_created_contract_id(&tx, "Pebble.Account", "AccountRef")?;
    let account_state_contract_id =
        extract_created_contract_id(&tx, "Pebble.Account", "AccountState")?;
    let wallet_contract_id = ensure_user_wallet_contract_id(
        state,
        &AccountForDepositRow {
            account_id: account_id.to_string(),
            owner_party: owner_party.clone(),
            committee_party: state.ledger_cfg.committee_party.clone(),
            instrument_admin: instrument_admin.clone(),
            instrument_id: instrument_id.clone(),
            status: "Active".to_string(),
        },
    )
    .await?;

    Ok(MeOnboardResponse {
        account_id: account_id.to_string(),
        owner_party,
        instrument_admin,
        instrument_id,
        account_status: "Active".to_string(),
        wallet_contract_id,
        account_created: true,
        account_ref_contract_id,
        account_state_contract_id,
        update_id: Some(tx.update_id),
        offset: Some(tx.offset),
    })
}

async fn ensure_user_registration(
    state: &AppState,
    username: &str,
) -> Result<UserRegistrationRow, ApiError> {
    if let Some(existing) = load_registration_by_username(&state.db, username).await? {
        ensure_registration_party_rights(state, &existing.owner_party).await?;
        return Ok(existing);
    }

    let account_id = registration_account_id_for_username(username)?;
    let owner_party = allocate_or_reuse_registration_party(state, username).await?;
    ensure_registration_party_rights(state, &owner_party).await?;
    insert_registration(&state.db, username, &account_id, &owner_party).await
}

async fn load_registration_by_username(
    db: &PgPool,
    username: &str,
) -> Result<Option<UserRegistrationRow>, ApiError> {
    sqlx::query_as(
        r#"
        SELECT username, account_id, owner_party
        FROM user_registrations
        WHERE username = $1
        LIMIT 1
        "#,
    )
    .bind(username)
    .fetch_optional(db)
    .await
    .context("query user registration by username")
    .map_err(ApiError::from)
}

async fn insert_registration(
    db: &PgPool,
    username: &str,
    account_id: &str,
    owner_party: &str,
) -> Result<UserRegistrationRow, ApiError> {
    let insert_result: Result<UserRegistrationRow, sqlx::Error> = sqlx::query_as(
        r#"
        INSERT INTO user_registrations (
          username,
          account_id,
          owner_party,
          created_at,
          updated_at
        )
        VALUES ($1, $2, $3, now(), now())
        ON CONFLICT (username) DO UPDATE
        SET updated_at = now()
        RETURNING username, account_id, owner_party
        "#,
    )
    .bind(username)
    .bind(account_id)
    .bind(owner_party)
    .fetch_one(db)
    .await;

    match insert_result {
        Ok(row) => Ok(row),
        Err(err) if is_unique_violation(&err) => Err(ApiError::conflict(
            "username, account_id, or owner_party is already registered",
        )),
        Err(err) => Err(ApiError::from(anyhow::Error::from(err))),
    }
}

async fn load_default_registration_instrument(
    db: &PgPool,
) -> Result<InstrumentIdentityRow, ApiError> {
    let row: Option<InstrumentIdentityRow> = sqlx::query_as(
        r#"
        SELECT instrument_admin, instrument_id
        FROM token_configs
        WHERE active = TRUE
        ORDER BY created_at DESC, contract_id DESC
        LIMIT 1
        "#,
    )
    .fetch_optional(db)
    .await
    .context("query default instrument for registration")?;

    row.ok_or_else(|| {
        ApiError::bad_request(
            "no active instrument configuration found; bootstrap an instrument first",
        )
    })
}

fn normalize_registration_username(raw: &str) -> Result<String, ApiError> {
    let username = raw.trim().to_ascii_lowercase();
    if username.is_empty() {
        return Err(ApiError::bad_request("username must be non-empty"));
    }
    if username.len() > 64 {
        return Err(ApiError::bad_request("username length must be <= 64"));
    }

    for ch in username.chars() {
        if !ch.is_ascii_alphanumeric() && !matches!(ch, '-' | '_' | '.') {
            return Err(ApiError::bad_request(
                "username may only contain ASCII letters, digits, '-', '_', '.'",
            ));
        }
    }

    if !username.chars().any(|ch| ch.is_ascii_alphanumeric()) {
        return Err(ApiError::bad_request(
            "username must include at least one letter or digit",
        ));
    }

    Ok(username)
}

fn normalize_registration_api_key(raw: &str) -> Result<String, ApiError> {
    let api_key = raw.trim().to_string();
    if api_key.is_empty() {
        return Err(ApiError::bad_request("api_key must be non-empty"));
    }
    if api_key.len() > 256 {
        return Err(ApiError::bad_request("api_key length must be <= 256"));
    }
    if !api_key.is_ascii() {
        return Err(ApiError::bad_request("api_key must be valid ASCII"));
    }

    Ok(api_key)
}

fn registration_account_id_for_username(username: &str) -> Result<String, ApiError> {
    let hash = short_registration_hash(username);
    let raw = format!("acc-web-{username}-{hash}");
    parse_account_id(&raw).map_err(|err| ApiError::bad_request(err.to_string()))
}

fn short_registration_hash(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let digest = hasher.finalize();
    let mut out = String::with_capacity(12);
    for byte in digest.iter().take(6) {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn registration_party_hint(prefix: &str, username: &str) -> String {
    let mut fragment = username
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .take(24)
        .map(|ch| ch.to_ascii_uppercase())
        .collect::<String>();
    if fragment.is_empty() {
        fragment = "USER".to_string();
    }
    let hash = short_registration_hash(username);
    format!("{prefix}{fragment}{hash}")
}

fn apply_registration_auth_header(
    request: reqwest::RequestBuilder,
    registration_cfg: &RegistrationConfig,
) -> reqwest::RequestBuilder {
    if let Some(value) = registration_cfg.json_api_auth_header.as_deref() {
        return request.header(reqwest::header::AUTHORIZATION, value);
    }
    request
}

async fn allocate_or_reuse_registration_party(
    state: &AppState,
    username: &str,
) -> Result<String, ApiError> {
    let party_hint = registration_party_hint(&state.registration_cfg.party_hint_prefix, username);
    let known_prefix = format!("{party_hint}::");

    if let Some(existing) = find_registration_party_by_hint_prefix(state, &known_prefix).await? {
        return Ok(existing);
    }

    let allocate_url = format!("{}/v2/parties", state.registration_cfg.json_api_base);
    let allocate_request_payload = JsonApiAllocatePartyRequest {
        party_id_hint: party_hint,
        identity_provider_id: String::new(),
    };
    let allocate_request = apply_registration_auth_header(
        state
            .registration_cfg
            .http_client
            .post(allocate_url)
            .json(&allocate_request_payload),
        &state.registration_cfg,
    );
    let allocate_response = allocate_request
        .send()
        .await
        .context("request allocate party")?;
    if !allocate_response.status().is_success() {
        let status = allocate_response.status();
        let body = allocate_response.text().await.unwrap_or_default();
        if is_party_allocation_already_exists(status, &body) {
            tracing::warn!(
                party_hint = %allocate_request_payload.party_id_hint,
                status = %status,
                "registration party allocation reported already existing party; resolving from party list"
            );
            for attempt in 0..REGISTRATION_PARTY_RESOLVE_RETRY_COUNT {
                if let Some(existing) =
                    find_registration_party_by_hint_prefix(state, &known_prefix).await?
                {
                    return Ok(existing);
                }
                if attempt + 1 < REGISTRATION_PARTY_RESOLVE_RETRY_COUNT {
                    tokio::time::sleep(Duration::from_millis(
                        REGISTRATION_PARTY_RESOLVE_RETRY_DELAY_MS,
                    ))
                    .await;
                }
            }
            return Err(ApiError::internal(format!(
                "party allocation reported existing party for hint \"{}\" but resolution by hint failed",
                allocate_request_payload.party_id_hint
            )));
        }
        return Err(ApiError::internal(format!(
            "party allocation failed with status {status}: {body}"
        )));
    }
    let allocate_payload: JsonApiAllocatePartyResponse = allocate_response
        .json()
        .await
        .context("decode allocate party response")?;
    let party = allocate_payload.party_details.party.unwrap_or_default();
    if party.is_empty() {
        return Err(ApiError::internal(
            "party allocation response missing party identifier",
        ));
    }

    Ok(party)
}

fn is_party_allocation_already_exists(status: reqwest::StatusCode, body: &str) -> bool {
    if status != reqwest::StatusCode::BAD_REQUEST && status != reqwest::StatusCode::CONFLICT {
        return false;
    }
    let body_lower = body.to_ascii_lowercase();
    body_lower.contains("party already exists")
        || body_lower.contains("already allocated on this node")
}

async fn find_registration_party_by_hint_prefix(
    state: &AppState,
    known_prefix: &str,
) -> Result<Option<String>, ApiError> {
    let base = format!("{}/v2/parties", state.registration_cfg.json_api_base);
    let mut next_page_token: Option<String> = None;

    for _page in 0..REGISTRATION_PARTY_LIST_MAX_PAGES {
        let mut list_url =
            reqwest::Url::parse(&base).context("build registration party listing url")?;
        {
            let mut query = list_url.query_pairs_mut();
            query.append_pair("pageSize", &REGISTRATION_PARTY_LIST_PAGE_SIZE.to_string());
            if let Some(token) = next_page_token.as_deref() {
                query.append_pair("pageToken", token);
            }
        }

        let request = apply_registration_auth_header(
            state.registration_cfg.http_client.get(list_url),
            &state.registration_cfg,
        );
        let response = request.send().await.context("request list parties")?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(ApiError::internal(format!(
                "party listing failed with status {status}: {body}"
            )));
        }

        let payload: JsonApiPartiesResponse = response
            .json()
            .await
            .context("decode list parties response")?;
        if let Some(existing) = payload
            .party_details
            .iter()
            .filter_map(|row| row.party.as_ref())
            .find(|party| party.starts_with(known_prefix))
        {
            return Ok(Some(existing.to_string()));
        }

        next_page_token = payload
            .next_page_token
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if next_page_token.is_none() {
            return Ok(None);
        }
    }

    Err(ApiError::internal(
        "party listing exceeded pagination safety limit while resolving registration party",
    ))
}

fn registration_user_rights_url(
    registration_cfg: &RegistrationConfig,
    user_id: &str,
) -> Result<reqwest::Url, ApiError> {
    let mut url = reqwest::Url::parse(&registration_cfg.json_api_base)
        .context("parse registration json-api base url")?;
    {
        let mut segments = url.path_segments_mut().map_err(|_| {
            ApiError::internal("registration json-api base url cannot be used for path segments")
        })?;
        segments.push("v2");
        segments.push("users");
        segments.push(user_id);
        segments.push("rights");
    }
    Ok(url)
}

fn right_key_from_json(right: &serde_json::Value) -> Option<String> {
    let kind = right.get("kind")?;
    if let Some(party) = kind
        .get("CanActAs")
        .and_then(|value| value.get("value"))
        .and_then(|value| value.get("party"))
        .and_then(serde_json::Value::as_str)
    {
        return Some(format!("act:{party}"));
    }
    if let Some(party) = kind
        .get("CanReadAs")
        .and_then(|value| value.get("value"))
        .and_then(|value| value.get("party"))
        .and_then(serde_json::Value::as_str)
    {
        return Some(format!("read:{party}"));
    }
    None
}

fn right_keys_from_payload(payload: &serde_json::Value) -> HashSet<String> {
    let rights = payload
        .get("rights")
        .or_else(|| {
            payload
                .get("result")
                .and_then(|result| result.get("rights"))
        })
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();

    rights
        .into_iter()
        .filter_map(|right| right_key_from_json(&right))
        .collect()
}

fn desired_party_rights(owner_party: &str) -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "kind": {
                "CanActAs": {
                    "value": { "party": owner_party }
                }
            }
        }),
        serde_json::json!({
            "kind": {
                "CanReadAs": {
                    "value": { "party": owner_party }
                }
            }
        }),
    ]
}

async fn ensure_registration_party_rights(
    state: &AppState,
    owner_party: &str,
) -> Result<(), ApiError> {
    if !state.registration_cfg.auto_grant_user_rights {
        return Ok(());
    }
    if owner_party.trim().is_empty() {
        return Err(ApiError::internal(
            "registration owner party must be non-empty for rights grant",
        ));
    }

    let user_id = state.ledger_cfg.user_id.trim();
    if user_id.is_empty() {
        return Err(ApiError::internal(
            "PEBBLE_LEDGER_USER_ID must be non-empty for rights grant",
        ));
    }

    let rights_url = registration_user_rights_url(&state.registration_cfg, user_id)?;
    let get_request = apply_registration_auth_header(
        state.registration_cfg.http_client.get(rights_url.clone()),
        &state.registration_cfg,
    );
    let get_response = get_request.send().await.context("request user rights")?;
    if !get_response.status().is_success() {
        let status = get_response.status();
        let body = get_response.text().await.unwrap_or_default();
        return Err(ApiError::internal(format!(
            "fetch user rights failed with status {status}: {body}"
        )));
    }

    let rights_payload: serde_json::Value = get_response
        .json()
        .await
        .context("decode user rights response")?;
    let existing_keys = right_keys_from_payload(&rights_payload);

    let missing_rights: Vec<serde_json::Value> = desired_party_rights(owner_party)
        .into_iter()
        .filter(|right| match right_key_from_json(right) {
            Some(key) => !existing_keys.contains(&key),
            None => false,
        })
        .collect();

    if missing_rights.is_empty() {
        return Ok(());
    }

    let grant_body = serde_json::json!({
        "userId": user_id,
        "rights": missing_rights,
    });
    let post_request = apply_registration_auth_header(
        state
            .registration_cfg
            .http_client
            .post(rights_url)
            .json(&grant_body),
        &state.registration_cfg,
    );
    let post_response = post_request.send().await.context("request rights grant")?;
    if !post_response.status().is_success() {
        let status = post_response.status();
        let body = post_response.text().await.unwrap_or_default();
        return Err(ApiError::internal(format!(
            "grant user rights failed with status {status}: {body}"
        )));
    }

    Ok(())
}

#[derive(Debug, sqlx::FromRow)]
struct UserApiKeyViewRow {
    key_id: String,
    api_key: String,
    label: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    last_used_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Serialize)]
struct MyApiKeyView {
    key_id: String,
    label: Option<String>,
    masked_key: String,
    created_at: chrono::DateTime<chrono::Utc>,
    last_used_at: Option<chrono::DateTime<chrono::Utc>>,
    is_current: bool,
}

#[derive(Debug, Deserialize)]
struct CreateMyApiKeyRequest {
    label: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct CreatedUserApiKeyRow {
    key_id: String,
    api_key: String,
    label: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    last_used_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Serialize)]
struct CreateMyApiKeyResponse {
    key_id: String,
    api_key: String,
    label: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    last_used_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, sqlx::FromRow)]
struct BootstrapMyApiKeyRevealRow {
    key_id: String,
    api_key: String,
    label: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    last_used_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, sqlx::FromRow)]
struct RevocableUserApiKeyRow {
    api_key: String,
}

#[derive(Debug, Serialize)]
struct RevokeMyApiKeyResponse {
    key_id: String,
    revoked: bool,
}

fn created_user_api_key_response(row: CreatedUserApiKeyRow) -> CreateMyApiKeyResponse {
    CreateMyApiKeyResponse {
        key_id: row.key_id,
        api_key: row.api_key,
        label: row.label,
        created_at: row.created_at,
        last_used_at: row.last_used_at,
    }
}

async fn insert_user_api_key(
    db: &PgPool,
    auth_keys: &auth_keys::AuthKeyStore,
    account_id: &str,
    label: Option<&str>,
) -> Result<CreatedUserApiKeyRow, ApiError> {
    for _attempt in 0..USER_API_KEY_CREATE_MAX_RETRIES {
        let (key_id, api_key) = generate_user_api_key_material();
        let insert_result: Result<CreatedUserApiKeyRow, sqlx::Error> = sqlx::query_as(
            r#"
            INSERT INTO user_api_keys (
              api_key,
              account_id,
              key_id,
              label,
              created_at,
              updated_at
            )
            VALUES ($1, $2, $3, $4, now(), now())
            RETURNING key_id, api_key, label, created_at, last_used_at
            "#,
        )
        .bind(&api_key)
        .bind(account_id)
        .bind(&key_id)
        .bind(label)
        .fetch_one(db)
        .await;

        match insert_result {
            Ok(created) => {
                auth_keys
                    .reload_from_db(db)
                    .await
                    .context("reload auth keys after user key creation")?;
                return Ok(created);
            }
            Err(err) if is_unique_violation(&err) => {
                continue;
            }
            Err(err) => {
                return Err(ApiError::from(anyhow::Error::from(err)));
            }
        }
    }

    Err(ApiError::service_unavailable_code(
        "failed to allocate a unique API key; retry shortly",
        "api_key_generation_conflict",
    ))
}

async fn maybe_insert_first_user_api_key(
    db: &PgPool,
    auth_keys: &auth_keys::AuthKeyStore,
    account_id: &str,
    label: Option<&str>,
) -> Result<Option<CreatedUserApiKeyRow>, ApiError> {
    for _attempt in 0..USER_API_KEY_CREATE_MAX_RETRIES {
        let (key_id, api_key) = generate_user_api_key_material();
        let insert_result: Result<Option<CreatedUserApiKeyRow>, sqlx::Error> = sqlx::query_as(
            r#"
            INSERT INTO user_api_keys (
              api_key,
              account_id,
              key_id,
              label,
              created_at,
              updated_at
            )
            SELECT $1, $2, $3, $4, now(), now()
            WHERE NOT EXISTS (
              SELECT 1
              FROM user_api_keys
              WHERE account_id = $2
                AND revoked_at IS NULL
            )
            RETURNING key_id, api_key, label, created_at, last_used_at
            "#,
        )
        .bind(&api_key)
        .bind(account_id)
        .bind(&key_id)
        .bind(label)
        .fetch_optional(db)
        .await;

        match insert_result {
            Ok(Some(created)) => {
                auth_keys
                    .reload_from_db(db)
                    .await
                    .context("reload auth keys after first key creation")?;
                return Ok(Some(created));
            }
            Ok(None) => return Ok(None),
            Err(err) if is_unique_violation(&err) => {
                continue;
            }
            Err(err) => return Err(ApiError::from(anyhow::Error::from(err))),
        }
    }

    Err(ApiError::service_unavailable_code(
        "failed to allocate a unique API key; retry shortly",
        "api_key_generation_conflict",
    ))
}

async fn list_my_api_keys(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<MyApiKeyView>>, ApiError> {
    let account_id = m4_views::require_user_account(&state, &headers).await?;
    let current_api_key = optional_ascii_header(&headers, "x-api-key")?;
    if let Some(key) = current_api_key.as_deref() {
        touch_user_api_key_last_used(&state.db, key).await?;
    }

    let rows: Vec<UserApiKeyViewRow> = sqlx::query_as(
        r#"
        SELECT
          key_id,
          api_key,
          label,
          created_at,
          last_used_at
        FROM user_api_keys
        WHERE account_id = $1
          AND revoked_at IS NULL
        ORDER BY created_at DESC, key_id DESC
        "#,
    )
    .bind(&account_id)
    .fetch_all(&state.db)
    .await
    .context("query active API keys for account")?;

    let views = rows
        .into_iter()
        .map(|row| MyApiKeyView {
            key_id: row.key_id,
            label: row.label,
            masked_key: mask_user_api_key(&row.api_key),
            created_at: row.created_at,
            last_used_at: row.last_used_at,
            is_current: current_api_key.as_deref() == Some(row.api_key.as_str()),
        })
        .collect();

    Ok(Json(views))
}

async fn create_my_api_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateMyApiKeyRequest>,
) -> Result<Json<CreateMyApiKeyResponse>, ApiError> {
    let account_id = m4_views::require_user_account(&state, &headers).await?;
    let current_api_key = optional_ascii_header(&headers, "x-api-key")?;
    if let Some(key) = current_api_key.as_deref() {
        touch_user_api_key_last_used(&state.db, key).await?;
    }
    let label = normalize_user_api_key_label(req.label)?;
    let created =
        insert_user_api_key(&state.db, &state.auth_keys, &account_id, label.as_deref()).await?;
    Ok(Json(created_user_api_key_response(created)))
}

async fn consume_my_bootstrap_api_key(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Option<CreateMyApiKeyResponse>>, ApiError> {
    let auth = authenticate_user_request(&state, &headers).await?;
    let Some(session_id) = auth.session_id else {
        return Ok(Json(None));
    };

    let mut tx = state
        .db
        .begin()
        .await
        .context("begin bootstrap API key reveal tx")?;

    let row: Option<BootstrapMyApiKeyRevealRow> = sqlx::query_as(
        r#"
        SELECT
          u.key_id,
          u.api_key,
          u.label,
          u.created_at,
          u.last_used_at
        FROM user_sessions s
        JOIN user_api_keys u
          ON u.key_id = s.bootstrap_key_id
        WHERE s.session_id = $1
          AND s.account_id = $2
          AND s.revoked_at IS NULL
          AND s.expires_at > now()
          AND s.bootstrap_key_id IS NOT NULL
          AND s.bootstrap_key_revealed_at IS NULL
          AND u.account_id = s.account_id
          AND u.revoked_at IS NULL
        FOR UPDATE OF s
        LIMIT 1
        "#,
    )
    .bind(&session_id)
    .bind(&auth.account_id)
    .fetch_optional(&mut *tx)
    .await
    .context("query bootstrap API key reveal row")?;

    let Some(row) = row else {
        tx.commit()
            .await
            .context("commit bootstrap API key reveal tx (empty)")?;
        return Ok(Json(None));
    };

    let update_result = sqlx::query(
        r#"
        UPDATE user_sessions
        SET bootstrap_key_revealed_at = now()
        WHERE session_id = $1
          AND account_id = $2
          AND bootstrap_key_revealed_at IS NULL
        "#,
    )
    .bind(&session_id)
    .bind(&auth.account_id)
    .execute(&mut *tx)
    .await
    .context("mark bootstrap API key revealed")?;

    tx.commit()
        .await
        .context("commit bootstrap API key reveal tx")?;

    if update_result.rows_affected() == 0 {
        return Ok(Json(None));
    }

    Ok(Json(Some(CreateMyApiKeyResponse {
        key_id: row.key_id,
        api_key: row.api_key,
        label: row.label,
        created_at: row.created_at,
        last_used_at: row.last_used_at,
    })))
}

async fn revoke_my_api_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(key_id): Path<String>,
) -> Result<Json<RevokeMyApiKeyResponse>, ApiError> {
    let account_id = m4_views::require_user_account(&state, &headers).await?;
    let current_api_key = optional_ascii_header(&headers, "x-api-key")?;
    if let Some(key) = current_api_key.as_deref() {
        touch_user_api_key_last_used(&state.db, key).await?;
    }
    let key_id = normalize_user_api_key_id(&key_id)?;

    let key_row: Option<RevocableUserApiKeyRow> = sqlx::query_as(
        r#"
        SELECT api_key
        FROM user_api_keys
        WHERE account_id = $1
          AND key_id = $2
          AND revoked_at IS NULL
        LIMIT 1
        "#,
    )
    .bind(&account_id)
    .bind(&key_id)
    .fetch_optional(&state.db)
    .await
    .context("query user API key for revoke")?;

    let Some(key_row) = key_row else {
        return Err(ApiError::not_found("API key not found"));
    };

    if current_api_key.as_deref() == Some(key_row.api_key.as_str()) {
        return Err(ApiError::bad_request(
            "cannot revoke the API key used for this request",
        ));
    }

    let update_result = sqlx::query(
        r#"
        UPDATE user_api_keys
        SET revoked_at = now(), updated_at = now()
        WHERE account_id = $1
          AND key_id = $2
          AND revoked_at IS NULL
        "#,
    )
    .bind(&account_id)
    .bind(&key_id)
    .execute(&state.db)
    .await
    .context("revoke user API key")?;

    if update_result.rows_affected() == 0 {
        return Err(ApiError::not_found("API key not found"));
    }

    state
        .auth_keys
        .reload_from_db(&state.db)
        .await
        .context("reload auth keys after user key revoke")?;

    Ok(Json(RevokeMyApiKeyResponse {
        key_id,
        revoked: true,
    }))
}

async fn touch_user_api_key_last_used(db: &PgPool, api_key: &str) -> Result<(), ApiError> {
    sqlx::query(
        r#"
        UPDATE user_api_keys
        SET last_used_at = now(), updated_at = now()
        WHERE api_key = $1
          AND revoked_at IS NULL
        "#,
    )
    .bind(api_key)
    .execute(db)
    .await
    .context("update user API key last_used_at")?;
    Ok(())
}

fn generate_user_api_key_material() -> (String, String) {
    let key_id = format!("uak_{}", Uuid::new_v4().simple());
    let api_key = format!(
        "pbl_uak_{}{}",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple()
    );
    (key_id, api_key)
}

fn normalize_user_api_key_label(raw: Option<String>) -> Result<Option<String>, ApiError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let label = raw.trim().to_string();
    if label.is_empty() {
        return Ok(None);
    }
    if !label.is_ascii() {
        return Err(ApiError::bad_request("label must be valid ASCII"));
    }
    if label.len() > USER_API_KEY_LABEL_MAX_LEN {
        return Err(ApiError::bad_request(format!(
            "label length must be <= {USER_API_KEY_LABEL_MAX_LEN}"
        )));
    }
    if label.as_bytes().iter().any(|byte| byte.is_ascii_control()) {
        return Err(ApiError::bad_request(
            "label must not contain control characters",
        ));
    }
    Ok(Some(label))
}

fn normalize_user_api_key_id(raw: &str) -> Result<String, ApiError> {
    let key_id = raw.trim().to_string();
    if key_id.is_empty() {
        return Err(ApiError::bad_request("key_id must be non-empty"));
    }
    if !key_id.is_ascii() {
        return Err(ApiError::bad_request("key_id must be valid ASCII"));
    }
    if key_id.len() > USER_API_KEY_ID_MAX_LEN {
        return Err(ApiError::bad_request(format!(
            "key_id length must be <= {USER_API_KEY_ID_MAX_LEN}"
        )));
    }
    Ok(key_id)
}

fn mask_user_api_key(raw: &str) -> String {
    let bytes = raw.as_bytes();
    if bytes.is_empty() {
        return "****".to_string();
    }
    if bytes.len() <= 4 {
        return "*".repeat(bytes.len());
    }

    let (prefix_len, suffix_len) = if bytes.len() <= 8 {
        (2_usize, 2_usize)
    } else {
        (4_usize, 4_usize)
    };
    if prefix_len + suffix_len >= bytes.len() {
        return "*".repeat(bytes.len());
    }

    let prefix = String::from_utf8_lossy(&bytes[..prefix_len]);
    let suffix_start = bytes.len() - suffix_len;
    let suffix = String::from_utf8_lossy(&bytes[suffix_start..]);
    format!("{prefix}...{suffix}")
}

#[derive(Debug, Deserialize)]
struct MeFaucetRequest {
    amount_minor: Option<i64>,
}

#[derive(Debug, Serialize)]
struct MeFaucetResponse {
    account_id: String,
    amount_minor: i64,
    wallet_contract_id: String,
    holding_claim_contract_id: String,
    holding_contract_id: String,
    update_id: String,
    offset: i64,
}

async fn me_faucet_credit(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<MeFaucetRequest>,
) -> Result<Json<MeFaucetResponse>, ApiError> {
    if !state.faucet_cfg.enabled {
        return Err(ApiError::forbidden("faucet is disabled"));
    }

    let account_id = m4_views::require_user_account(&state, &headers).await?;
    let account_id =
        parse_account_id(&account_id).map_err(|err| ApiError::bad_request(err.to_string()))?;

    let amount_minor = req
        .amount_minor
        .unwrap_or(state.faucet_cfg.default_amount_minor);
    if amount_minor <= 0 {
        return Err(ApiError::bad_request("amount_minor must be > 0"));
    }
    if amount_minor > state.faucet_cfg.max_amount_minor {
        return Err(ApiError::bad_request(format!(
            "amount_minor exceeds faucet max ({})",
            state.faucet_cfg.max_amount_minor
        )));
    }

    let mut tx = state
        .db
        .begin()
        .await
        .context("begin me_faucet_credit tx")?;
    advisory_lock_account(&mut tx, state.account_lock_namespace, &account_id).await?;
    let account = load_account_for_deposit(&mut tx, &account_id).await?;
    if account.status == "Closed" {
        tx.commit()
            .await
            .context("commit me_faucet_credit tx (closed account)")?;
        return Err(ApiError::bad_request("account is closed"));
    }
    tx.commit().await.context("commit me_faucet_credit tx")?;

    let wallet_contract_id = ensure_user_wallet_contract_id(&state, &account).await?;

    let mint_command_id = format!("faucet:mint:{}:{}", account_id, Uuid::new_v4());
    let mint_input = FaucetMintCommandInput {
        user_id: &state.ledger_cfg.user_id,
        instrument_admin: &account.instrument_admin,
        instrument_id: &account.instrument_id,
        owner_party: &account.owner_party,
        amount_minor,
        command_id: &mint_command_id,
    };
    let mint_commands = build_faucet_mint_commands(&mint_input);
    let mint_tx = state
        .committee_submitter
        .submit_and_wait_for_transaction(mint_commands)
        .await
        .context("submit faucet mint command")?;
    let holding_claim_contract_id =
        extract_created_contract_id(&mint_tx, "Wizardcat.Token.Standard", "HoldingClaim")?;

    let claim_command_id = format!("faucet:claim:{}:{}", account_id, Uuid::new_v4());
    let claim_commands = build_claim_holding_claim_commands(
        &state.ledger_cfg.user_id,
        &account.owner_party,
        &holding_claim_contract_id,
        &claim_command_id,
    );
    let claim_tx = state
        .committee_submitter
        .submit_and_wait_for_transaction(claim_commands)
        .await
        .context("submit faucet claim command")?;
    let holding_contract_id =
        extract_created_contract_id(&claim_tx, "Wizardcat.Token.Standard", "Holding")?;

    Ok(Json(MeFaucetResponse {
        account_id,
        amount_minor,
        wallet_contract_id,
        holding_claim_contract_id,
        holding_contract_id,
        update_id: claim_tx.update_id,
        offset: claim_tx.offset,
    }))
}

#[derive(sqlx::FromRow)]
struct MarketRowDb {
    contract_id: String,
    market_id: String,
    question: String,
    outcomes: sqlx::types::Json<Vec<String>>,
    status: String,
    resolved_outcome: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    active: bool,
    last_offset: i64,
}

#[derive(Clone, Serialize)]
struct MarketRow {
    contract_id: String,
    market_id: String,
    question: String,
    outcomes: Vec<String>,
    status: String,
    resolved_outcome: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    active: bool,
    last_offset: i64,
}

#[derive(Debug, Deserialize)]
struct ListMarketsQuery {
    limit: Option<i64>,
    offset: Option<i64>,
}

fn parse_markets_limit(limit: Option<i64>) -> Result<i64, ApiError> {
    let limit = limit.unwrap_or(MARKETS_QUERY_DEFAULT_LIMIT);
    if limit <= 0 {
        return Err(ApiError::bad_request("limit must be > 0"));
    }
    Ok(limit.min(MARKETS_QUERY_MAX_LIMIT))
}

fn parse_markets_offset(offset: Option<i64>) -> Result<i64, ApiError> {
    let offset = offset.unwrap_or(0);
    if offset < 0 {
        return Err(ApiError::bad_request("offset must be >= 0"));
    }
    Ok(offset)
}

async fn list_markets(
    State(state): State<AppState>,
    Query(query): Query<ListMarketsQuery>,
) -> Result<Json<Vec<MarketRow>>, ApiError> {
    let limit = parse_markets_limit(query.limit)?;
    let offset = parse_markets_offset(query.offset)?;
    let cache_key = format!("markets:{limit}:{offset}");
    let ttl = Duration::from_secs(10);

    let markets = state
        .public_markets_cache
        .get_or_load(cache_key, ttl, || async {
            let rows: Vec<MarketRowDb> = sqlx::query_as(
                r#"
                SELECT
                  contract_id,
                  market_id,
                  question,
                  outcomes,
                  status,
                  resolved_outcome,
                  created_at,
                  active,
                  last_offset
                FROM markets
                WHERE active = TRUE
                ORDER BY created_at DESC
                LIMIT $1
                OFFSET $2
                "#,
            )
            .bind(limit)
            .bind(offset)
            .fetch_all(&state.db)
            .await
            .context("query markets")?;

            let markets = rows
                .into_iter()
                .map(|r| MarketRow {
                    contract_id: r.contract_id,
                    market_id: r.market_id,
                    question: r.question,
                    outcomes: r.outcomes.0,
                    status: r.status,
                    resolved_outcome: r.resolved_outcome,
                    created_at: r.created_at,
                    active: r.active,
                    last_offset: r.last_offset,
                })
                .collect();
            Ok(markets)
        })
        .await?;

    Ok(Json(markets))
}

#[derive(Deserialize)]
struct CreateMarketRequest {
    market_id: Option<String>,
    question: String,
    outcomes: Vec<String>,
}

#[derive(Serialize)]
struct CreateMarketResponse {
    market_id: String,
    contract_id: String,
    update_id: String,
    offset: i64,
}

async fn create_market(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateMarketRequest>,
) -> Result<Json<CreateMarketResponse>, ApiError> {
    m4_views::require_admin(&state, &headers)?;

    let outcomes = normalize_outcomes(req.outcomes)?;
    if outcomes.len() < 2 {
        return Err(ApiError::bad_request(
            "outcomes must have at least 2 unique entries",
        ));
    }

    let market_id = match req.market_id {
        Some(id) if !id.trim().is_empty() => {
            parse_market_id(&id).map_err(|err| ApiError::bad_request(err.to_string()))?
        }
        _ => parse_market_id(&Uuid::new_v4().to_string())
            .map_err(|err| ApiError::bad_request(err.to_string()))?,
    };

    let question = req.question.trim().to_string();
    if question.is_empty() {
        return Err(ApiError::bad_request("question must be non-empty"));
    }

    let commands = build_create_market_commands(
        &state.ledger_cfg.user_id,
        &state.ledger_cfg.committee_party,
        &market_id,
        &question,
        &outcomes,
    )?;
    let tx = state
        .committee_submitter
        .submit_and_wait_for_transaction(commands)
        .await
        .context("submit_and_wait_for_transaction")?;

    let contract_id =
        extract_created_contract_id(&tx, "Pebble.MarketLifecycle", "MarketLifecycle")?;

    Ok(Json(CreateMarketResponse {
        market_id,
        contract_id,
        update_id: tx.update_id,
        offset: tx.offset,
    }))
}

#[derive(Serialize)]
struct ExerciseMarketResponse {
    contract_id: String,
    update_id: String,
    offset: i64,
}

async fn close_market(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(market_id): Path<String>,
) -> Result<Json<ExerciseMarketResponse>, ApiError> {
    m4_views::require_admin(&state, &headers)?;
    let market_id =
        parse_market_id(&market_id).map_err(|err| ApiError::bad_request(err.to_string()))?;

    let contract_id = lookup_active_market_contract_id(&state.db, &market_id).await?;

    let commands = build_exercise_market_commands(
        &state.ledger_cfg.user_id,
        &state.ledger_cfg.committee_party,
        &contract_id,
        "Close",
        value_record_empty(),
    )?;
    let tx = state
        .committee_submitter
        .submit_and_wait_for_transaction(commands)
        .await
        .context("submit_and_wait_for_transaction")?;

    let new_contract_id =
        extract_created_contract_id(&tx, "Pebble.MarketLifecycle", "MarketLifecycle")?;

    Ok(Json(ExerciseMarketResponse {
        contract_id: new_contract_id,
        update_id: tx.update_id,
        offset: tx.offset,
    }))
}

#[derive(Deserialize)]
struct ResolveMarketRequest {
    outcome: String,
}

#[derive(Serialize)]
struct ResolveMarketResponse {
    contract_id: String,
    update_id: Option<String>,
    offset: Option<i64>,
    resolve_status: String,
    settlement_state: String,
    settlement_job_market_id: String,
}

#[derive(sqlx::FromRow)]
struct ResolveMarketContextRow {
    contract_id: String,
    status: String,
    outcomes: sqlx::types::Json<Vec<String>>,
    resolved_outcome: Option<String>,
    instrument_admin: String,
    instrument_id: String,
    payout_per_share_minor: i64,
}

#[derive(sqlx::FromRow, Clone)]
struct MarketSettlementJobRow {
    market_id: String,
    market_contract_id: String,
    instrument_admin: String,
    instrument_id: String,
    resolved_outcome: String,
    payout_per_share_minor: i64,
    state: String,
    target_epoch: Option<i64>,
    error: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(sqlx::FromRow)]
struct MarketSettlementAggRow {
    account_count: i64,
    total_delta_minor: i64,
    total_abs_delta_minor: i64,
    total_source_position_count: i64,
    assigned_account_count: i64,
    unassigned_account_count: i64,
}

#[derive(Serialize)]
struct AdminMarketSettlementResponse {
    market_id: String,
    market_contract_id: String,
    instrument_admin: String,
    instrument_id: String,
    resolved_outcome: String,
    payout_per_share_minor: i64,
    state: String,
    target_epoch: Option<i64>,
    error: Option<String>,
    account_count: i64,
    total_delta_minor: i64,
    total_abs_delta_minor: i64,
    total_source_position_count: i64,
    assigned_account_count: i64,
    unassigned_account_count: i64,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Serialize)]
struct AdminMarketSettlementRetryResponse {
    market_id: String,
    state: String,
}

#[derive(sqlx::FromRow, Serialize)]
struct AdminMarketSettlementDeltaRow {
    account_id: String,
    delta_minor: i64,
    source_position_count: i32,
    assigned_epoch: Option<i64>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

async fn resolve_market(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(market_id): Path<String>,
    Json(req): Json<ResolveMarketRequest>,
) -> Result<Json<ResolveMarketResponse>, ApiError> {
    m4_views::require_admin(&state, &headers)?;
    let market_id =
        parse_market_id(&market_id).map_err(|err| ApiError::bad_request(err.to_string()))?;

    let outcome = req.outcome.trim().to_string();
    if outcome.is_empty() {
        return Err(ApiError::bad_request("outcome must be non-empty"));
    }

    let mut dbtx = state.db.begin().await.context("begin resolve_market tx")?;
    advisory_lock_market(&mut dbtx, &market_id).await?;

    let market = load_active_market_for_resolve(&mut dbtx, &market_id).await?;
    if !market.outcomes.0.iter().any(|value| value == &outcome) {
        return Err(ApiError::bad_request("outcome not in market"));
    }

    let existing_job = load_market_settlement_job_for_update(&mut dbtx, &market_id).await?;
    if let Some(existing_job) = existing_job {
        if existing_job.resolved_outcome != outcome {
            return Err(ApiError::conflict(format!(
                "market already resolved to {}, cannot resolve to {}",
                existing_job.resolved_outcome, outcome
            )));
        }
        if market.status == "Open" {
            return Err(ApiError::conflict(
                "market has settlement job but latest status is Open",
            ));
        }

        let settlement_state = ensure_market_settlement_job(
            &mut dbtx,
            &SettlementJobSeed {
                market_id: &market_id,
                market_contract_id: &existing_job.market_contract_id,
                instrument_admin: &market.instrument_admin,
                instrument_id: &market.instrument_id,
                resolved_outcome: &outcome,
                payout_per_share_minor: market.payout_per_share_minor,
            },
            "resolve-idempotent",
        )
        .await?;
        dbtx.commit().await.context("commit resolve_market tx")?;

        return Ok(Json(ResolveMarketResponse {
            contract_id: existing_job.market_contract_id,
            update_id: None,
            offset: None,
            resolve_status: "AlreadyResolved".to_string(),
            settlement_state,
            settlement_job_market_id: market_id,
        }));
    }

    if market.status == "Resolved" {
        let Some(existing_outcome) = market.resolved_outcome.as_deref() else {
            return Err(ApiError::internal(
                "resolved market missing resolved_outcome",
            ));
        };
        if existing_outcome != outcome {
            return Err(ApiError::conflict(format!(
                "market already resolved to {}, cannot resolve to {}",
                existing_outcome, outcome
            )));
        }

        let settlement_state = ensure_market_settlement_job(
            &mut dbtx,
            &SettlementJobSeed {
                market_id: &market_id,
                market_contract_id: &market.contract_id,
                instrument_admin: &market.instrument_admin,
                instrument_id: &market.instrument_id,
                resolved_outcome: &outcome,
                payout_per_share_minor: market.payout_per_share_minor,
            },
            "resolve-existing",
        )
        .await?;
        dbtx.commit().await.context("commit resolve_market tx")?;

        return Ok(Json(ResolveMarketResponse {
            contract_id: market.contract_id,
            update_id: None,
            offset: None,
            resolve_status: "AlreadyResolved".to_string(),
            settlement_state,
            settlement_job_market_id: market_id,
        }));
    }

    if market.status != "Closed" {
        return Err(ApiError::bad_request(
            "market must be Closed before resolve",
        ));
    }

    let choice_argument = value_record(lapi::Record {
        record_id: None,
        fields: vec![record_field("outcome", value_text(&outcome))],
    });
    let commands = build_exercise_market_commands(
        &state.ledger_cfg.user_id,
        &state.ledger_cfg.committee_party,
        &market.contract_id,
        "Resolve",
        choice_argument,
    )?;
    let tx = state
        .committee_submitter
        .submit_and_wait_for_transaction(commands)
        .await
        .context("submit_and_wait_for_transaction")?;
    let new_contract_id =
        extract_created_contract_id(&tx, "Pebble.MarketLifecycle", "MarketLifecycle")?;

    let settlement_state = ensure_market_settlement_job(
        &mut dbtx,
        &SettlementJobSeed {
            market_id: &market_id,
            market_contract_id: &new_contract_id,
            instrument_admin: &market.instrument_admin,
            instrument_id: &market.instrument_id,
            resolved_outcome: &outcome,
            payout_per_share_minor: market.payout_per_share_minor,
        },
        "resolve-now",
    )
    .await?;
    dbtx.commit().await.context("commit resolve_market tx")?;

    Ok(Json(ResolveMarketResponse {
        contract_id: new_contract_id,
        update_id: Some(tx.update_id),
        offset: Some(tx.offset),
        resolve_status: "ResolvedNow".to_string(),
        settlement_state,
        settlement_job_market_id: market_id,
    }))
}

async fn admin_get_market_settlement(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(market_id): Path<String>,
) -> Result<Json<AdminMarketSettlementResponse>, ApiError> {
    m4_views::require_admin(&state, &headers)?;
    let market_id =
        parse_market_id(&market_id).map_err(|err| ApiError::bad_request(err.to_string()))?;

    let job: Option<MarketSettlementJobRow> = sqlx::query_as(
        r#"
        SELECT
          market_id,
          market_contract_id,
          instrument_admin,
          instrument_id,
          resolved_outcome,
          payout_per_share_minor,
          state,
          target_epoch,
          error,
          created_at,
          updated_at
        FROM market_settlement_jobs
        WHERE market_id = $1
        LIMIT 1
        "#,
    )
    .bind(&market_id)
    .fetch_optional(&state.db)
    .await
    .context("query market settlement job")?;
    let Some(job) = job else {
        return Err(ApiError::not_found("market settlement job not found"));
    };

    let agg: MarketSettlementAggRow = sqlx::query_as(
        r#"
        SELECT
          COUNT(*)::BIGINT AS account_count,
          COALESCE(SUM(delta_minor), 0)::BIGINT AS total_delta_minor,
          COALESCE(SUM(ABS(delta_minor)), 0)::BIGINT AS total_abs_delta_minor,
          COALESCE(SUM(source_position_count), 0)::BIGINT AS total_source_position_count,
          COUNT(*) FILTER (WHERE assigned_epoch IS NOT NULL)::BIGINT AS assigned_account_count,
          COUNT(*) FILTER (WHERE assigned_epoch IS NULL)::BIGINT AS unassigned_account_count
        FROM market_settlement_deltas
        WHERE market_id = $1
        "#,
    )
    .bind(&market_id)
    .fetch_one(&state.db)
    .await
    .context("query market settlement aggregates")?;

    Ok(Json(AdminMarketSettlementResponse {
        market_id: job.market_id,
        market_contract_id: job.market_contract_id,
        instrument_admin: job.instrument_admin,
        instrument_id: job.instrument_id,
        resolved_outcome: job.resolved_outcome,
        payout_per_share_minor: job.payout_per_share_minor,
        state: job.state,
        target_epoch: job.target_epoch,
        error: job.error,
        account_count: agg.account_count,
        total_delta_minor: agg.total_delta_minor,
        total_abs_delta_minor: agg.total_abs_delta_minor,
        total_source_position_count: agg.total_source_position_count,
        assigned_account_count: agg.assigned_account_count,
        unassigned_account_count: agg.unassigned_account_count,
        created_at: job.created_at,
        updated_at: job.updated_at,
    }))
}

async fn admin_retry_market_settlement(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(market_id): Path<String>,
) -> Result<Json<AdminMarketSettlementRetryResponse>, ApiError> {
    m4_views::require_admin(&state, &headers)?;
    let market_id =
        parse_market_id(&market_id).map_err(|err| ApiError::bad_request(err.to_string()))?;

    let mut tx = state
        .db
        .begin()
        .await
        .context("begin settlement retry tx")?;
    advisory_lock_market(&mut tx, &market_id).await?;

    let job = load_market_settlement_job_for_update(&mut tx, &market_id).await?;
    let Some(job) = job else {
        return Err(ApiError::not_found("market settlement job not found"));
    };
    if job.state != "Failed" {
        return Err(ApiError::bad_request(
            "retry is only allowed when settlement job state is Failed",
        ));
    }

    sqlx::query(
        r#"
        UPDATE market_settlement_jobs
        SET
          state = 'Pending',
          target_epoch = NULL,
          error = NULL,
          updated_at = now()
        WHERE market_id = $1
        "#,
    )
    .bind(&market_id)
    .execute(&mut *tx)
    .await
    .context("mark market settlement job pending for retry")?;

    insert_market_settlement_event(
        &mut tx,
        &market_id,
        "RetryRequested",
        serde_json::json!({ "source": "admin" }),
    )
    .await?;

    tx.commit().await.context("commit settlement retry tx")?;

    Ok(Json(AdminMarketSettlementRetryResponse {
        market_id,
        state: "Pending".to_string(),
    }))
}

async fn admin_list_market_settlement_deltas(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(market_id): Path<String>,
) -> Result<Json<Vec<AdminMarketSettlementDeltaRow>>, ApiError> {
    m4_views::require_admin(&state, &headers)?;
    let market_id =
        parse_market_id(&market_id).map_err(|err| ApiError::bad_request(err.to_string()))?;

    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM market_settlement_jobs WHERE market_id = $1)",
    )
    .bind(&market_id)
    .fetch_one(&state.db)
    .await
    .context("check market settlement job exists")?;
    if !exists {
        return Err(ApiError::not_found("market settlement job not found"));
    }

    let rows: Vec<AdminMarketSettlementDeltaRow> = sqlx::query_as(
        r#"
        SELECT
          account_id,
          delta_minor,
          source_position_count,
          assigned_epoch,
          created_at,
          updated_at
        FROM market_settlement_deltas
        WHERE market_id = $1
        ORDER BY account_id ASC
        "#,
    )
    .bind(&market_id)
    .fetch_all(&state.db)
    .await
    .context("query market settlement deltas")?;

    Ok(Json(rows))
}

#[derive(sqlx::FromRow)]
struct EscalatedWithdrawalForReconcileRow {
    contract_id: String,
    account_id: String,
    committee_party: String,
    current_instruction_cid: String,
    lineage_root_instruction_cid: String,
}

#[derive(sqlx::FromRow)]
struct LatestInstructionForReconcileRow {
    contract_id: String,
    output: String,
}

#[derive(Serialize)]
struct ReconcileEscalatedWithdrawalResponse {
    contract_id: String,
    action: String,
    latest_instruction_cid: String,
    latest_output: String,
    update_id: Option<String>,
    offset: Option<i64>,
}

async fn admin_reconcile_escalated_withdrawal(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(contract_id): Path<String>,
) -> Result<Json<ReconcileEscalatedWithdrawalResponse>, ApiError> {
    m4_views::require_admin(&state, &headers)?;

    let pending: Option<EscalatedWithdrawalForReconcileRow> = sqlx::query_as(
        r#"
        SELECT
          contract_id,
          account_id,
          committee_party,
          current_instruction_cid,
          lineage_root_instruction_cid
        FROM withdrawal_pendings
        WHERE contract_id = $1
          AND active = TRUE
          AND pending_state = 'CancelEscalated'
        LIMIT 1
        "#,
    )
    .bind(&contract_id)
    .fetch_optional(&state.db)
    .await
    .context("query escalated withdrawal pending")?;

    let Some(pending) = pending else {
        return Err(ApiError::not_found(
            "escalated withdrawal pending not found",
        ));
    };

    let latest_instruction: Option<LatestInstructionForReconcileRow> = sqlx::query_as(
        r#"
        SELECT
          ti.contract_id,
          ti.output
        FROM token_transfer_instruction_latest latest
        JOIN token_transfer_instructions ti
          ON ti.contract_id = latest.contract_id
        WHERE latest.lineage_root_instruction_cid = $1
          AND ti.active = TRUE
        LIMIT 1
        "#,
    )
    .bind(&pending.lineage_root_instruction_cid)
    .fetch_optional(&state.db)
    .await
    .context("query latest transfer instruction for escalation")?;

    let Some(latest_instruction) = latest_instruction else {
        return Err(ApiError::not_found(
            "latest transfer instruction for escalation not found",
        ));
    };

    let (choice, choice_argument) = match latest_instruction.output.as_str() {
        "Pending" => {
            if latest_instruction.contract_id == pending.current_instruction_cid {
                return Ok(Json(ReconcileEscalatedWithdrawalResponse {
                    contract_id: pending.contract_id,
                    action: "Noop".to_string(),
                    latest_instruction_cid: latest_instruction.contract_id,
                    latest_output: latest_instruction.output,
                    update_id: None,
                    offset: None,
                }));
            }

            (
                "AdvanceWithdrawalPending",
                value_record(lapi::Record {
                    record_id: None,
                    fields: vec![record_field(
                        "nextInstructionCid",
                        value_contract_id(&latest_instruction.contract_id),
                    )],
                }),
            )
        }
        "Completed" => (
            "FinalizeAccepted",
            value_record(lapi::Record {
                record_id: None,
                fields: vec![record_field(
                    "terminalInstructionCid",
                    value_contract_id(&latest_instruction.contract_id),
                )],
            }),
        ),
        "Failed" => {
            let account_state_cid: Option<(String,)> = sqlx::query_as(
                r#"
                SELECT contract_id
                FROM account_state_latest
                WHERE account_id = $1
                LIMIT 1
                "#,
            )
            .bind(&pending.account_id)
            .fetch_optional(&state.db)
            .await
            .context("query account state for escalated withdrawal reconcile")?;

            let Some((account_state_cid,)) = account_state_cid else {
                return Err(ApiError::not_found(
                    "account state not found for escalated withdrawal",
                ));
            };

            (
                "FinalizeRejected",
                value_record(lapi::Record {
                    record_id: None,
                    fields: vec![
                        record_field("accountStateCid", value_contract_id(&account_state_cid)),
                        record_field(
                            "terminalInstructionCid",
                            value_contract_id(&latest_instruction.contract_id),
                        ),
                    ],
                }),
            )
        }
        other => {
            return Err(ApiError::bad_request(format!(
                "unsupported transfer instruction output for escalation reconcile: {other}"
            )))
        }
    };

    let commands = build_exercise_withdrawal_pending_commands(
        &state.ledger_cfg.user_id,
        &pending.committee_party,
        &pending.contract_id,
        choice,
        choice_argument,
    )?;
    let tx = state
        .committee_submitter
        .submit_and_wait_for_transaction(commands)
        .await
        .context("submit_and_wait_for_transaction")?;

    Ok(Json(ReconcileEscalatedWithdrawalResponse {
        contract_id: pending.contract_id,
        action: choice.to_string(),
        latest_instruction_cid: latest_instruction.contract_id,
        latest_output: latest_instruction.output,
        update_id: Some(tx.update_id),
        offset: Some(tx.offset),
    }))
}

#[derive(sqlx::FromRow)]
struct QuarantinedHoldingForCloseoutRow {
    contract_id: String,
    committee_party: String,
}

#[derive(Serialize)]
struct QuarantineCloseoutResponse {
    contract_id: String,
    action: String,
    update_id: String,
    offset: i64,
}

async fn admin_closeout_quarantine_holding(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(contract_id): Path<String>,
) -> Result<Json<QuarantineCloseoutResponse>, ApiError> {
    m4_views::require_admin(&state, &headers)?;

    let row: Option<QuarantinedHoldingForCloseoutRow> = sqlx::query_as(
        r#"
        SELECT contract_id, committee_party
        FROM quarantined_holdings
        WHERE contract_id = $1
          AND active = TRUE
        LIMIT 1
        "#,
    )
    .bind(&contract_id)
    .fetch_optional(&state.db)
    .await
    .context("query quarantined holding for closeout")?;

    let Some(row) = row else {
        return Err(ApiError::not_found("quarantined holding not found"));
    };

    let choice = "Archive";
    let commands = build_exercise_quarantined_holding_commands(
        &state.ledger_cfg.user_id,
        &row.committee_party,
        &row.contract_id,
        choice,
        value_record_empty(),
    )?;
    let tx = state
        .committee_submitter
        .submit_and_wait_for_transaction(commands)
        .await
        .context("submit_and_wait_for_transaction")?;

    Ok(Json(QuarantineCloseoutResponse {
        contract_id: row.contract_id,
        action: "AdministrativeCloseout".to_string(),
        update_id: tx.update_id,
        offset: tx.offset,
    }))
}

#[derive(Debug, Deserialize, Serialize)]
struct FetchReferenceDisclosureRequest {
    module_name: String,
    entity_name: String,
    contract_id: String,
    package_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct FetchReferenceDisclosureResponse {
    contract_id: String,
    module_name: String,
    entity_name: String,
    package_id: String,
    created_offset: i64,
    archived_offset: Option<i64>,
    created_event_blob_hex: String,
    created_event_blob_sha256: String,
    created_event_blob_bytes: usize,
}

#[derive(Debug)]
struct DisclosureAuditLogInsert {
    requester_type: String,
    requester_subject_hash: String,
    requester_ip: Option<String>,
    module_name: String,
    entity_name: String,
    package_id: Option<String>,
    contract_id: String,
    allowed: bool,
    outcome: String,
    http_status: i32,
    request_json: serde_json::Value,
    response_json: Option<serde_json::Value>,
    error_text: Option<String>,
}

async fn admin_fetch_reference_disclosure(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<FetchReferenceDisclosureRequest>,
) -> Result<Json<FetchReferenceDisclosureResponse>, ApiError> {
    m4_views::require_admin(&state, &headers)?;

    let module_name = request.module_name.trim().to_string();
    if module_name.is_empty() {
        return Err(ApiError::bad_request("module_name must be non-empty"));
    }
    let entity_name = request.entity_name.trim().to_string();
    if entity_name.is_empty() {
        return Err(ApiError::bad_request("entity_name must be non-empty"));
    }
    let contract_id = request.contract_id.trim().to_string();
    if contract_id.is_empty() {
        return Err(ApiError::bad_request("contract_id must be non-empty"));
    }
    let package_id = request
        .package_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);

    let requester_subject_hash =
        sha256_hex(required_ascii_header(&headers, "x-admin-key")?.as_bytes());
    let requester_ip = optional_ascii_header(&headers, "x-forwarded-for")?;

    let normalized_request = FetchReferenceDisclosureRequest {
        module_name: module_name.clone(),
        entity_name: entity_name.clone(),
        contract_id: contract_id.clone(),
        package_id: package_id.clone(),
    };
    let request_json = serde_json::to_value(&normalized_request)
        .context("serialize disclosure request for audit")?;

    if !state.disclosure_cfg.allows(&module_name, &entity_name) {
        let message = format!("template {module_name}:{entity_name} is not allowlisted");
        let audit_row = DisclosureAuditLogInsert {
            requester_type: "admin_key".to_string(),
            requester_subject_hash,
            requester_ip,
            module_name,
            entity_name,
            package_id,
            contract_id,
            allowed: false,
            outcome: "TemplateNotAllowlisted".to_string(),
            http_status: i32::from(StatusCode::FORBIDDEN.as_u16()),
            request_json,
            response_json: Some(serde_json::json!({ "error": message })),
            error_text: None,
        };
        insert_disclosure_audit_log(&state.db, &audit_row)
            .await
            .context("insert disclosure audit log for allowlist rejection")?;
        return Err(ApiError::forbidden(message));
    }

    let template_id = lapi::Identifier {
        package_id: package_id.clone().unwrap_or_else(|| "#pebble".to_string()),
        module_name: module_name.clone(),
        entity_name: entity_name.clone(),
    };
    let event_format = disclosure_event_format(&state.ledger_cfg.committee_party, template_id);

    let mut client = lapi::event_query_service_client::EventQueryServiceClient::new(
        state.ledger_cfg.channel.clone(),
    );
    let mut grpc_req = Request::new(lapi::GetEventsByContractIdRequest {
        contract_id: contract_id.clone(),
        event_format: Some(event_format),
    });
    if let Some(auth_header) = state.ledger_cfg.auth_header.clone() {
        grpc_req.metadata_mut().insert("authorization", auth_header);
    }

    let resp = match client.get_events_by_contract_id(grpc_req).await {
        Ok(resp) => resp.into_inner(),
        Err(status) => {
            let (api_error, outcome) = match status.code() {
                tonic::Code::NotFound => (
                    ApiError::not_found("contract events not found or not visible to committee"),
                    "ContractEventsNotFound",
                ),
                tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => (
                    ApiError::forbidden("ledger denied disclosure fetch"),
                    "LedgerAccessDenied",
                ),
                _ => (
                    ApiError::internal("ledger disclosure fetch failed"),
                    "LedgerFetchError",
                ),
            };
            let audit_row = DisclosureAuditLogInsert {
                requester_type: "admin_key".to_string(),
                requester_subject_hash,
                requester_ip,
                module_name,
                entity_name,
                package_id,
                contract_id,
                allowed: true,
                outcome: outcome.to_string(),
                http_status: i32::from(api_error.status.as_u16()),
                request_json,
                response_json: Some(serde_json::json!({ "error": api_error.message })),
                error_text: Some(status.to_string()),
            };
            insert_disclosure_audit_log(&state.db, &audit_row)
                .await
                .context("insert disclosure audit log for ledger error")?;
            return Err(api_error);
        }
    };

    let archived_offset = resp
        .archived
        .as_ref()
        .and_then(|archived| archived.archived_event.as_ref())
        .map(|event| event.offset);

    let created = match resp.created.and_then(|created| created.created_event) {
        Some(created) => created,
        None => {
            let error = ApiError::not_found("contract create event not found");
            let audit_row = DisclosureAuditLogInsert {
                requester_type: "admin_key".to_string(),
                requester_subject_hash,
                requester_ip,
                module_name,
                entity_name,
                package_id,
                contract_id,
                allowed: true,
                outcome: "CreatedEventMissing".to_string(),
                http_status: i32::from(error.status.as_u16()),
                request_json,
                response_json: Some(serde_json::json!({ "error": error.message })),
                error_text: None,
            };
            insert_disclosure_audit_log(&state.db, &audit_row)
                .await
                .context("insert disclosure audit log for missing create event")?;
            return Err(error);
        }
    };

    let Some(template_id) = created.template_id.as_ref() else {
        let error = ApiError::internal("created event missing template_id");
        let audit_row = DisclosureAuditLogInsert {
            requester_type: "admin_key".to_string(),
            requester_subject_hash,
            requester_ip,
            module_name,
            entity_name,
            package_id,
            contract_id,
            allowed: true,
            outcome: "TemplateMissing".to_string(),
            http_status: i32::from(error.status.as_u16()),
            request_json,
            response_json: Some(serde_json::json!({ "error": error.message })),
            error_text: None,
        };
        insert_disclosure_audit_log(&state.db, &audit_row)
            .await
            .context("insert disclosure audit log for missing template id")?;
        return Err(error);
    };

    if template_id.module_name != module_name || template_id.entity_name != entity_name {
        let message = format!(
            "contract template mismatch: expected {module_name}:{entity_name}, got {}:{}",
            template_id.module_name, template_id.entity_name
        );
        let error = ApiError::conflict(message.clone());
        let audit_row = DisclosureAuditLogInsert {
            requester_type: "admin_key".to_string(),
            requester_subject_hash,
            requester_ip,
            module_name,
            entity_name,
            package_id,
            contract_id,
            allowed: true,
            outcome: "TemplateMismatch".to_string(),
            http_status: i32::from(error.status.as_u16()),
            request_json,
            response_json: Some(serde_json::json!({ "error": message })),
            error_text: None,
        };
        insert_disclosure_audit_log(&state.db, &audit_row)
            .await
            .context("insert disclosure audit log for template mismatch")?;
        return Err(error);
    }

    if let Some(expected_package_id) = &package_id {
        if template_id.package_id != *expected_package_id {
            let message = format!(
                "contract package mismatch: expected {expected_package_id}, got {}",
                template_id.package_id
            );
            let error = ApiError::conflict(message.clone());
            let audit_row = DisclosureAuditLogInsert {
                requester_type: "admin_key".to_string(),
                requester_subject_hash,
                requester_ip,
                module_name,
                entity_name,
                package_id,
                contract_id,
                allowed: true,
                outcome: "PackageMismatch".to_string(),
                http_status: i32::from(error.status.as_u16()),
                request_json,
                response_json: Some(serde_json::json!({ "error": message })),
                error_text: None,
            };
            insert_disclosure_audit_log(&state.db, &audit_row)
                .await
                .context("insert disclosure audit log for package mismatch")?;
            return Err(error);
        }
    }

    if created.created_event_blob.is_empty() {
        let error = ApiError::internal("created event blob missing from ledger response");
        let audit_row = DisclosureAuditLogInsert {
            requester_type: "admin_key".to_string(),
            requester_subject_hash,
            requester_ip,
            module_name,
            entity_name,
            package_id,
            contract_id,
            allowed: true,
            outcome: "CreatedEventBlobMissing".to_string(),
            http_status: i32::from(error.status.as_u16()),
            request_json,
            response_json: Some(serde_json::json!({ "error": error.message })),
            error_text: None,
        };
        insert_disclosure_audit_log(&state.db, &audit_row)
            .await
            .context("insert disclosure audit log for missing event blob")?;
        return Err(error);
    }

    let blob_hex = bytes_to_hex(&created.created_event_blob);
    let blob_sha256 = sha256_hex(&created.created_event_blob);

    let response = FetchReferenceDisclosureResponse {
        contract_id,
        module_name,
        entity_name,
        package_id: template_id.package_id.clone(),
        created_offset: created.offset,
        archived_offset,
        created_event_blob_hex: blob_hex,
        created_event_blob_sha256: blob_sha256.clone(),
        created_event_blob_bytes: created.created_event_blob.len(),
    };
    let response_json = serde_json::json!({
        "contract_id": response.contract_id,
        "module_name": response.module_name,
        "entity_name": response.entity_name,
        "package_id": response.package_id,
        "created_offset": response.created_offset,
        "archived_offset": response.archived_offset,
        "created_event_blob_sha256": blob_sha256,
        "created_event_blob_bytes": response.created_event_blob_bytes
    });
    let audit_row = DisclosureAuditLogInsert {
        requester_type: "admin_key".to_string(),
        requester_subject_hash,
        requester_ip,
        module_name: response.module_name.clone(),
        entity_name: response.entity_name.clone(),
        package_id: Some(response.package_id.clone()),
        contract_id: response.contract_id.clone(),
        allowed: true,
        outcome: "Success".to_string(),
        http_status: i32::from(StatusCode::OK.as_u16()),
        request_json,
        response_json: Some(response_json),
        error_text: None,
    };
    insert_disclosure_audit_log(&state.db, &audit_row)
        .await
        .context("insert disclosure audit log for success")?;

    Ok(Json(response))
}

fn disclosure_event_format(party: &str, template_id: lapi::Identifier) -> lapi::EventFormat {
    let filters = lapi::Filters {
        cumulative: vec![lapi::CumulativeFilter {
            identifier_filter: Some(lapi::cumulative_filter::IdentifierFilter::TemplateFilter(
                lapi::TemplateFilter {
                    template_id: Some(template_id),
                    include_created_event_blob: true,
                },
            )),
        }],
    };

    let mut filters_by_party = HashMap::new();
    filters_by_party.insert(party.to_string(), filters);

    lapi::EventFormat {
        filters_by_party,
        filters_for_any_party: None,
        verbose: true,
    }
}

async fn insert_disclosure_audit_log(db: &PgPool, row: &DisclosureAuditLogInsert) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO disclosure_audit_logs (
          requester_type,
          requester_subject_hash,
          requester_ip,
          module_name,
          entity_name,
          package_id,
          contract_id,
          allowed,
          outcome,
          http_status,
          request_json,
          response_json,
          error_text
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        "#,
    )
    .bind(&row.requester_type)
    .bind(&row.requester_subject_hash)
    .bind(&row.requester_ip)
    .bind(&row.module_name)
    .bind(&row.entity_name)
    .bind(&row.package_id)
    .bind(&row.contract_id)
    .bind(row.allowed)
    .bind(&row.outcome)
    .bind(row.http_status)
    .bind(&row.request_json)
    .bind(&row.response_json)
    .bind(&row.error_text)
    .execute(db)
    .await
    .context("insert disclosure audit log")?;
    Ok(())
}

fn normalize_outcomes(outcomes: Vec<String>) -> Result<Vec<String>, ApiError> {
    let mut out = Vec::new();
    let mut seen = HashSet::<String>::new();

    for raw in outcomes {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let key = trimmed.to_ascii_lowercase();
        if seen.insert(key) {
            out.push(trimmed.to_string());
        }
    }

    Ok(out)
}

fn required_ascii_header(headers: &HeaderMap, name: &str) -> Result<String, ApiError> {
    let value = headers
        .get(name)
        .ok_or_else(|| ApiError::unauthorized(format!("missing {name} header")))?
        .to_str()
        .map_err(|_| ApiError::unauthorized(format!("{name} must be valid ASCII")))?
        .trim()
        .to_string();
    if value.is_empty() {
        return Err(ApiError::unauthorized(format!("{name} must be non-empty")));
    }

    Ok(value)
}

fn optional_ascii_header(headers: &HeaderMap, name: &str) -> Result<Option<String>, ApiError> {
    let Some(raw) = headers.get(name) else {
        return Ok(None);
    };

    let value = raw
        .to_str()
        .map_err(|_| ApiError::unauthorized(format!("{name} must be valid ASCII")))?
        .trim()
        .to_string();
    if value.is_empty() {
        return Err(ApiError::unauthorized(format!("{name} must be non-empty")));
    }

    Ok(Some(value))
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    bytes_to_hex(&digest)
}

fn bounded_command_id(prefix: &str, action: &str, step_seq: i64, digest_input: &str) -> String {
    let digest = sha256_hex(digest_input.as_bytes());
    let digest_short = digest[..32].to_string();
    let primary = format!("{prefix}:{action}:{step_seq}:{digest_short}");
    if primary.len() <= MAX_BOUNDED_COMMAND_ID_LEN {
        return primary;
    }

    let compact = format!("{prefix}:{step_seq}:{digest_short}");
    if compact.len() <= MAX_BOUNDED_COMMAND_ID_LEN {
        return compact;
    }

    digest_short
}

fn withdrawal_create_command_id(account_id: &str, withdrawal_id: &str) -> String {
    let digest_input = format!("withdrawal:create:1:{account_id}:{withdrawal_id}");
    bounded_command_id("withdrawal", "create", 1, &digest_input)
}

fn withdrawal_claim_command_id(account_id: &str, claim_contract_id: &str) -> String {
    let digest_input = format!("withdrawal-claim:user:1:{account_id}:{claim_contract_id}");
    bounded_command_id("withdrawal-claim", "user", 1, &digest_input)
}

fn extract_created_contract_id(
    tx: &lapi::Transaction,
    module: &str,
    entity: &str,
) -> Result<String> {
    for evt in &tx.events {
        let Some(body) = evt.event.as_ref() else {
            continue;
        };
        let lapi::event::Event::Created(created) = body else {
            continue;
        };
        let Some(template_id) = created.template_id.as_ref() else {
            continue;
        };
        if template_id.module_name == module && template_id.entity_name == entity {
            return Ok(created.contract_id.clone());
        }
    }

    Err(anyhow!(
        "transaction missing expected create: {module}:{entity}"
    ))
}

fn build_create_market_commands(
    user_id: &str,
    committee_party: &str,
    market_id: &str,
    question: &str,
    outcomes: &[String],
) -> Result<lapi::Commands> {
    let command_id = format!("market:create:{}", Uuid::new_v4());

    let template_id = lapi::Identifier {
        package_id: "#pebble".to_string(),
        module_name: "Pebble.MarketLifecycle".to_string(),
        entity_name: "MarketLifecycle".to_string(),
    };

    let create_arguments = lapi::Record {
        record_id: None,
        fields: vec![
            record_field("committee", value_party(committee_party)),
            record_field("creator", value_party(committee_party)),
            record_field("marketId", value_text(market_id)),
            record_field("question", value_text(question)),
            record_field("outcomes", value_list_text(outcomes)),
            record_field("status", value_enum("Open")),
            record_field("resolvedOutcome", value_optional_none()),
        ],
    };

    let create = lapi::CreateCommand {
        template_id: Some(template_id),
        create_arguments: Some(create_arguments),
    };

    let cmd = lapi::Command {
        command: Some(lapi::command::Command::Create(create)),
    };

    Ok(lapi::Commands {
        workflow_id: String::new(),
        user_id: user_id.to_string(),
        command_id,
        commands: vec![cmd],
        deduplication_period: None,
        min_ledger_time_abs: None,
        min_ledger_time_rel: None,
        act_as: vec![committee_party.to_string()],
        read_as: vec![],
        submission_id: Uuid::new_v4().to_string(),
        disclosed_contracts: vec![],
        synchronizer_id: String::new(),
        package_id_selection_preference: vec![],
        prefetch_contract_keys: vec![],
    })
}

fn build_create_account_commands(
    user_id: &str,
    committee_party: &str,
    account_id: &str,
    owner_party: &str,
    instrument_admin: &str,
    instrument_id: &str,
) -> Result<lapi::Commands> {
    let command_id = format!("account:create:{}", Uuid::new_v4());

    let account_ref_template_id = lapi::Identifier {
        package_id: "#pebble".to_string(),
        module_name: "Pebble.Account".to_string(),
        entity_name: "AccountRef".to_string(),
    };
    let account_state_template_id = lapi::Identifier {
        package_id: "#pebble".to_string(),
        module_name: "Pebble.Account".to_string(),
        entity_name: "AccountState".to_string(),
    };

    let account_ref_args = lapi::Record {
        record_id: None,
        fields: vec![
            record_field("committee", value_party(committee_party)),
            record_field("owner", value_party(owner_party)),
            record_field("accountId", value_text(account_id)),
            record_field("instrumentAdmin", value_party(instrument_admin)),
            record_field("instrumentId", value_text(instrument_id)),
            record_field("status", value_enum("Active")),
            record_field("finalizedEpoch", value_optional_none()),
        ],
    };

    let account_state_args = lapi::Record {
        record_id: None,
        fields: vec![
            record_field("committee", value_party(committee_party)),
            record_field("owner", value_party(owner_party)),
            record_field("accountId", value_text(account_id)),
            record_field("instrumentAdmin", value_party(instrument_admin)),
            record_field("instrumentId", value_text(instrument_id)),
            record_field("clearedCashMinor", value_int64(0)),
            record_field("lastAppliedEpoch", value_int64(0)),
            record_field("lastAppliedBatchAnchor", value_optional_none()),
        ],
    };

    let create_ref = lapi::CreateCommand {
        template_id: Some(account_ref_template_id),
        create_arguments: Some(account_ref_args),
    };
    let create_state = lapi::CreateCommand {
        template_id: Some(account_state_template_id),
        create_arguments: Some(account_state_args),
    };

    let cmd_ref = lapi::Command {
        command: Some(lapi::command::Command::Create(create_ref)),
    };
    let cmd_state = lapi::Command {
        command: Some(lapi::command::Command::Create(create_state)),
    };

    Ok(lapi::Commands {
        workflow_id: String::new(),
        user_id: user_id.to_string(),
        command_id,
        commands: vec![cmd_ref, cmd_state],
        deduplication_period: None,
        min_ledger_time_abs: None,
        min_ledger_time_rel: None,
        act_as: vec![committee_party.to_string()],
        read_as: vec![],
        submission_id: Uuid::new_v4().to_string(),
        disclosed_contracts: vec![],
        synchronizer_id: String::new(),
        package_id_selection_preference: vec![],
        prefetch_contract_keys: vec![],
    })
}

struct FaucetMintCommandInput<'a> {
    user_id: &'a str,
    instrument_admin: &'a str,
    instrument_id: &'a str,
    owner_party: &'a str,
    amount_minor: i64,
    command_id: &'a str,
}

fn build_faucet_mint_commands(input: &FaucetMintCommandInput<'_>) -> lapi::Commands {
    let template_id = lapi::Identifier {
        package_id: "#wizardcat-token-standard".to_string(),
        module_name: "Wizardcat.Token.Standard".to_string(),
        entity_name: "Faucet".to_string(),
    };

    let create_arguments = lapi::Record {
        record_id: None,
        fields: vec![
            record_field("instrumentAdmin", value_party(input.instrument_admin)),
            record_field("instrumentId", value_text(input.instrument_id)),
        ],
    };
    let choice_argument = value_record(lapi::Record {
        record_id: None,
        fields: vec![
            record_field("owner", value_party(input.owner_party)),
            record_field("amountMinor", value_int64(input.amount_minor)),
        ],
    });

    let cmd = lapi::Command {
        command: Some(lapi::command::Command::CreateAndExercise(
            lapi::CreateAndExerciseCommand {
                template_id: Some(template_id),
                create_arguments: Some(create_arguments),
                choice: "Mint".to_string(),
                choice_argument: Some(choice_argument),
            },
        )),
    };

    lapi::Commands {
        workflow_id: String::new(),
        user_id: input.user_id.to_string(),
        command_id: input.command_id.to_string(),
        commands: vec![cmd],
        deduplication_period: None,
        min_ledger_time_abs: None,
        min_ledger_time_rel: None,
        act_as: vec![input.instrument_admin.to_string()],
        read_as: vec![],
        submission_id: Uuid::new_v4().to_string(),
        disclosed_contracts: vec![],
        synchronizer_id: String::new(),
        package_id_selection_preference: vec![],
        prefetch_contract_keys: vec![],
    }
}

fn build_claim_holding_claim_commands(
    user_id: &str,
    owner_party: &str,
    holding_claim_contract_id: &str,
    command_id: &str,
) -> lapi::Commands {
    let template_id = lapi::Identifier {
        package_id: "#wizardcat-token-standard".to_string(),
        module_name: "Wizardcat.Token.Standard".to_string(),
        entity_name: "HoldingClaim".to_string(),
    };

    let exercise = lapi::ExerciseCommand {
        template_id: Some(template_id),
        contract_id: holding_claim_contract_id.to_string(),
        choice: "Claim".to_string(),
        choice_argument: Some(value_record_empty()),
    };
    let cmd = lapi::Command {
        command: Some(lapi::command::Command::Exercise(exercise)),
    };

    lapi::Commands {
        workflow_id: String::new(),
        user_id: user_id.to_string(),
        command_id: command_id.to_string(),
        commands: vec![cmd],
        deduplication_period: None,
        min_ledger_time_abs: None,
        min_ledger_time_rel: None,
        act_as: vec![owner_party.to_string()],
        read_as: vec![],
        submission_id: Uuid::new_v4().to_string(),
        disclosed_contracts: vec![],
        synchronizer_id: String::new(),
        package_id_selection_preference: vec![],
        prefetch_contract_keys: vec![],
    }
}

fn build_create_wallet_commands(
    user_id: &str,
    command_id: &str,
    owner_party: &str,
    instrument_admin: &str,
    instrument_id: &str,
) -> lapi::Commands {
    let template_id = lapi::Identifier {
        package_id: "#wizardcat-token-standard".to_string(),
        module_name: "Wizardcat.Token.Standard".to_string(),
        entity_name: "Wallet".to_string(),
    };
    let create_arguments = lapi::Record {
        record_id: None,
        fields: vec![
            record_field("owner", value_party(owner_party)),
            record_field("instrumentAdmin", value_party(instrument_admin)),
            record_field("instrumentId", value_text(instrument_id)),
        ],
    };
    let create = lapi::CreateCommand {
        template_id: Some(template_id),
        create_arguments: Some(create_arguments),
    };
    let cmd = lapi::Command {
        command: Some(lapi::command::Command::Create(create)),
    };

    lapi::Commands {
        workflow_id: String::new(),
        user_id: user_id.to_string(),
        command_id: command_id.to_string(),
        commands: vec![cmd],
        deduplication_period: None,
        min_ledger_time_abs: None,
        min_ledger_time_rel: None,
        act_as: vec![owner_party.to_string()],
        read_as: vec![],
        submission_id: Uuid::new_v4().to_string(),
        disclosed_contracts: vec![],
        synchronizer_id: String::new(),
        package_id_selection_preference: vec![],
        prefetch_contract_keys: vec![],
    }
}

struct CreateDepositOfferCommandInput<'a> {
    user_id: &'a str,
    owner_party: &'a str,
    committee_party: &'a str,
    wallet_contract_id: &'a str,
    account_id: &'a str,
    deposit_id: &'a str,
    amount_minor: i64,
    input_holding_cids: &'a [String],
    lock_expires_at_micros: i64,
    command_id: &'a str,
}

fn build_create_deposit_offer_commands(
    input: &CreateDepositOfferCommandInput<'_>,
) -> Result<lapi::Commands> {
    if input.input_holding_cids.is_empty() {
        return Err(anyhow!("input_holding_cids must be non-empty"));
    }

    let template_id = lapi::Identifier {
        package_id: "#wizardcat-token-standard".to_string(),
        module_name: "Wizardcat.Token.Standard".to_string(),
        entity_name: "Wallet".to_string(),
    };

    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "wizardcat.xyz/pebble.accountId".to_string(),
        serde_json::Value::String(input.account_id.to_string()),
    );
    metadata.insert(
        "wizardcat.xyz/pebble.depositId".to_string(),
        serde_json::Value::String(input.deposit_id.to_string()),
    );

    let input_holding_values = input
        .input_holding_cids
        .iter()
        .map(|holding_cid| value_contract_id(holding_cid))
        .collect::<Vec<_>>();

    let choice_argument = value_record(lapi::Record {
        record_id: None,
        fields: vec![
            record_field("receiver", value_party(input.committee_party)),
            record_field("amountMinor", value_int64(input.amount_minor)),
            record_field("inputHoldingCids", value_list(input_holding_values)),
            record_field("metadata", value_map_text(&metadata)?),
            record_field("reason", value_text(input.account_id)),
            record_field(
                "lockExpiresAt",
                value_time_micros(input.lock_expires_at_micros),
            ),
        ],
    });
    let exercise = lapi::ExerciseCommand {
        template_id: Some(template_id),
        contract_id: input.wallet_contract_id.to_string(),
        choice: "Offer".to_string(),
        choice_argument: Some(choice_argument),
    };
    let cmd = lapi::Command {
        command: Some(lapi::command::Command::Exercise(exercise)),
    };

    Ok(lapi::Commands {
        workflow_id: String::new(),
        user_id: input.user_id.to_string(),
        command_id: input.command_id.to_string(),
        commands: vec![cmd],
        deduplication_period: None,
        min_ledger_time_abs: None,
        min_ledger_time_rel: None,
        act_as: vec![input.owner_party.to_string()],
        read_as: vec![],
        submission_id: Uuid::new_v4().to_string(),
        disclosed_contracts: vec![],
        synchronizer_id: String::new(),
        package_id_selection_preference: vec![],
        prefetch_contract_keys: vec![],
    })
}

struct CreateWithdrawalRequestCommandInput<'a> {
    user_id: &'a str,
    owner_party: &'a str,
    committee_party: &'a str,
    account_id: &'a str,
    instrument_admin: &'a str,
    instrument_id: &'a str,
    withdrawal_id: &'a str,
    amount_minor: i64,
    command_id: &'a str,
}

fn build_create_withdrawal_request_commands(
    input: &CreateWithdrawalRequestCommandInput<'_>,
) -> Result<lapi::Commands> {
    let template_id = lapi::Identifier {
        package_id: "#pebble".to_string(),
        module_name: "Pebble.Treasury".to_string(),
        entity_name: "WithdrawalRequest".to_string(),
    };

    let create_arguments = lapi::Record {
        record_id: None,
        fields: vec![
            record_field("committee", value_party(input.committee_party)),
            record_field("owner", value_party(input.owner_party)),
            record_field("accountId", value_text(input.account_id)),
            record_field("instrumentAdmin", value_party(input.instrument_admin)),
            record_field("instrumentId", value_text(input.instrument_id)),
            record_field("withdrawalId", value_text(input.withdrawal_id)),
            record_field("amountMinor", value_int64(input.amount_minor)),
        ],
    };

    let create = lapi::CreateCommand {
        template_id: Some(template_id),
        create_arguments: Some(create_arguments),
    };

    let cmd = lapi::Command {
        command: Some(lapi::command::Command::Create(create)),
    };

    Ok(lapi::Commands {
        workflow_id: String::new(),
        user_id: input.user_id.to_string(),
        command_id: input.command_id.to_string(),
        commands: vec![cmd],
        deduplication_period: None,
        min_ledger_time_abs: None,
        min_ledger_time_rel: None,
        act_as: vec![input.owner_party.to_string()],
        read_as: vec![],
        submission_id: Uuid::new_v4().to_string(),
        disclosed_contracts: vec![],
        synchronizer_id: String::new(),
        package_id_selection_preference: vec![],
        prefetch_contract_keys: vec![],
    })
}

fn build_exercise_market_commands(
    user_id: &str,
    committee_party: &str,
    contract_id: &str,
    choice: &str,
    choice_argument: lapi::Value,
) -> Result<lapi::Commands> {
    let command_id = format!("market:exercise:{}:{}", choice, Uuid::new_v4());

    let template_id = lapi::Identifier {
        package_id: "#pebble".to_string(),
        module_name: "Pebble.MarketLifecycle".to_string(),
        entity_name: "MarketLifecycle".to_string(),
    };

    let exercise = lapi::ExerciseCommand {
        template_id: Some(template_id),
        contract_id: contract_id.to_string(),
        choice: choice.to_string(),
        choice_argument: Some(choice_argument),
    };

    let cmd = lapi::Command {
        command: Some(lapi::command::Command::Exercise(exercise)),
    };

    Ok(lapi::Commands {
        workflow_id: String::new(),
        user_id: user_id.to_string(),
        command_id,
        commands: vec![cmd],
        deduplication_period: None,
        min_ledger_time_abs: None,
        min_ledger_time_rel: None,
        act_as: vec![committee_party.to_string()],
        read_as: vec![],
        submission_id: Uuid::new_v4().to_string(),
        disclosed_contracts: vec![],
        synchronizer_id: String::new(),
        package_id_selection_preference: vec![],
        prefetch_contract_keys: vec![],
    })
}

fn build_exercise_withdrawal_pending_commands(
    user_id: &str,
    committee_party: &str,
    contract_id: &str,
    choice: &str,
    choice_argument: lapi::Value,
) -> Result<lapi::Commands> {
    let command_id = format!("withdrawal:exercise:{}:{}", choice, Uuid::new_v4());

    let template_id = lapi::Identifier {
        package_id: "#pebble".to_string(),
        module_name: "Pebble.Treasury".to_string(),
        entity_name: "WithdrawalPending".to_string(),
    };

    let exercise = lapi::ExerciseCommand {
        template_id: Some(template_id),
        contract_id: contract_id.to_string(),
        choice: choice.to_string(),
        choice_argument: Some(choice_argument),
    };

    let cmd = lapi::Command {
        command: Some(lapi::command::Command::Exercise(exercise)),
    };

    Ok(lapi::Commands {
        workflow_id: String::new(),
        user_id: user_id.to_string(),
        command_id,
        commands: vec![cmd],
        deduplication_period: None,
        min_ledger_time_abs: None,
        min_ledger_time_rel: None,
        act_as: vec![committee_party.to_string()],
        read_as: vec![],
        submission_id: Uuid::new_v4().to_string(),
        disclosed_contracts: vec![],
        synchronizer_id: String::new(),
        package_id_selection_preference: vec![],
        prefetch_contract_keys: vec![],
    })
}

fn build_exercise_quarantined_holding_commands(
    user_id: &str,
    committee_party: &str,
    contract_id: &str,
    choice: &str,
    choice_argument: lapi::Value,
) -> Result<lapi::Commands> {
    let command_id = format!("quarantine:exercise:{}:{}", choice, Uuid::new_v4());

    let template_id = lapi::Identifier {
        package_id: "#pebble".to_string(),
        module_name: "Pebble.Treasury".to_string(),
        entity_name: "QuarantinedHolding".to_string(),
    };

    let exercise = lapi::ExerciseCommand {
        template_id: Some(template_id),
        contract_id: contract_id.to_string(),
        choice: choice.to_string(),
        choice_argument: Some(choice_argument),
    };

    let cmd = lapi::Command {
        command: Some(lapi::command::Command::Exercise(exercise)),
    };

    Ok(lapi::Commands {
        workflow_id: String::new(),
        user_id: user_id.to_string(),
        command_id,
        commands: vec![cmd],
        deduplication_period: None,
        min_ledger_time_abs: None,
        min_ledger_time_rel: None,
        act_as: vec![committee_party.to_string()],
        read_as: vec![],
        submission_id: Uuid::new_v4().to_string(),
        disclosed_contracts: vec![],
        synchronizer_id: String::new(),
        package_id_selection_preference: vec![],
        prefetch_contract_keys: vec![],
    })
}

fn record_field(label: &str, value: lapi::Value) -> lapi::RecordField {
    lapi::RecordField {
        label: label.to_string(),
        value: Some(value),
    }
}

fn value_text(value: &str) -> lapi::Value {
    lapi::Value {
        sum: Some(lapi::value::Sum::Text(value.to_string())),
    }
}

fn value_party(value: &str) -> lapi::Value {
    lapi::Value {
        sum: Some(lapi::value::Sum::Party(value.to_string())),
    }
}

fn value_enum(constructor: &str) -> lapi::Value {
    lapi::Value {
        sum: Some(lapi::value::Sum::Enum(lapi::Enum {
            enum_id: None,
            constructor: constructor.to_string(),
        })),
    }
}

fn value_int64(value: i64) -> lapi::Value {
    lapi::Value {
        sum: Some(lapi::value::Sum::Int64(value)),
    }
}

fn value_contract_id(value: &str) -> lapi::Value {
    lapi::Value {
        sum: Some(lapi::value::Sum::ContractId(value.to_string())),
    }
}

fn value_time_micros(micros: i64) -> lapi::Value {
    lapi::Value {
        sum: Some(lapi::value::Sum::Timestamp(micros)),
    }
}

fn value_list(values: Vec<lapi::Value>) -> lapi::Value {
    lapi::Value {
        sum: Some(lapi::value::Sum::List(lapi::List { elements: values })),
    }
}

fn value_list_text(values: &[String]) -> lapi::Value {
    lapi::Value {
        sum: Some(lapi::value::Sum::List(lapi::List {
            elements: values.iter().map(|t| value_text(t)).collect(),
        })),
    }
}

fn value_record(record: lapi::Record) -> lapi::Value {
    lapi::Value {
        sum: Some(lapi::value::Sum::Record(record)),
    }
}

fn value_record_empty() -> lapi::Value {
    value_record(lapi::Record {
        record_id: None,
        fields: vec![],
    })
}

fn value_optional_none() -> lapi::Value {
    lapi::Value {
        sum: Some(lapi::value::Sum::Optional(Box::new(lapi::Optional {
            value: None,
        }))),
    }
}

fn value_map_text(values: &serde_json::Map<String, serde_json::Value>) -> Result<lapi::Value> {
    let mut entries = Vec::with_capacity(values.len());
    for (key, value) in values {
        let value_text = value
            .as_str()
            .ok_or_else(|| anyhow!("expected string value for metadata key {key:?}"))?;
        entries.push(lapi::text_map::Entry {
            key: key.clone(),
            value: Some(lapi::Value {
                sum: Some(lapi::value::Sum::Text(value_text.to_string())),
            }),
        });
    }

    Ok(lapi::Value {
        sum: Some(lapi::value::Sum::TextMap(lapi::TextMap { entries })),
    })
}

struct SettlementJobSeed<'a> {
    market_id: &'a str,
    market_contract_id: &'a str,
    instrument_admin: &'a str,
    instrument_id: &'a str,
    resolved_outcome: &'a str,
    payout_per_share_minor: i64,
}

#[derive(sqlx::FromRow)]
struct ResolvedMarketMissingJobRow {
    market_id: String,
    contract_id: String,
    instrument_admin: String,
    instrument_id: String,
    resolved_outcome: String,
    payout_per_share_minor: i64,
}

async fn resolved_market_settlement_backfill_loop(state: AppState) -> Result<()> {
    let period = Duration::from_millis(state.settlement_cfg.backfill_poll_ms);
    loop {
        if let Err(err) = resolved_market_settlement_backfill_tick(&state).await {
            tracing::warn!(error = ?err, "resolved market settlement backfill tick failed");
        }
        tokio::time::sleep(period).await;
    }
}

async fn resolved_market_settlement_backfill_tick(state: &AppState) -> Result<()> {
    let rows: Vec<ResolvedMarketMissingJobRow> = sqlx::query_as(
        r#"
        SELECT
          m.market_id,
          m.contract_id,
          m.instrument_admin,
          m.instrument_id,
          m.resolved_outcome,
          m.payout_per_share_minor
        FROM markets m
        LEFT JOIN market_settlement_jobs j
          ON j.market_id = m.market_id
        WHERE m.active = TRUE
          AND m.status = 'Resolved'
          AND m.resolved_outcome IS NOT NULL
          AND j.market_id IS NULL
        ORDER BY m.created_at ASC, m.market_id ASC
        LIMIT 100
        "#,
    )
    .fetch_all(&state.db)
    .await
    .context("query resolved markets missing settlement jobs")?;

    for row in rows {
        if let Err(err) = backfill_single_resolved_market(state, row).await {
            tracing::warn!(error = ?err, "failed to backfill settlement job for resolved market");
        }
    }

    Ok(())
}

async fn backfill_single_resolved_market(
    state: &AppState,
    row: ResolvedMarketMissingJobRow,
) -> Result<()> {
    parse_market_id(&row.market_id)
        .with_context(|| format!("validate canonical market id {}", row.market_id))?;

    let mut tx = state
        .db
        .begin()
        .await
        .context("begin settlement backfill tx")?;
    advisory_lock_market(&mut tx, &row.market_id)
        .await
        .map_err(|err| {
            anyhow!(
                "acquire market lock for settlement backfill failed: {}",
                err.message
            )
        })?;

    let _ = ensure_market_settlement_job(
        &mut tx,
        &SettlementJobSeed {
            market_id: &row.market_id,
            market_contract_id: &row.contract_id,
            instrument_admin: &row.instrument_admin,
            instrument_id: &row.instrument_id,
            resolved_outcome: &row.resolved_outcome,
            payout_per_share_minor: row.payout_per_share_minor,
        },
        "backfill",
    )
    .await
    .map_err(|err| {
        anyhow!(
            "ensure market settlement job from backfill failed: {}",
            err.message
        )
    })?;

    tx.commit().await.context("commit settlement backfill tx")?;

    Ok(())
}

async fn advisory_lock_market(
    tx: &mut Transaction<'_, Postgres>,
    market_id: &str,
) -> Result<(), ApiError> {
    sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
        .bind(MARKET_LOCK_NAMESPACE)
        .bind(market_id)
        .execute(&mut **tx)
        .await
        .context("market advisory lock")?;
    Ok(())
}

async fn advisory_lock_account(
    tx: &mut Transaction<'_, Postgres>,
    namespace: i32,
    account_id: &str,
) -> Result<(), ApiError> {
    sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
        .bind(namespace)
        .bind(account_id)
        .execute(&mut **tx)
        .await
        .context("account advisory lock")?;
    Ok(())
}

async fn load_active_market_for_resolve(
    tx: &mut Transaction<'_, Postgres>,
    market_id: &str,
) -> Result<ResolveMarketContextRow, ApiError> {
    let row: Option<ResolveMarketContextRow> = sqlx::query_as(
        r#"
        SELECT
          contract_id,
          status,
          outcomes,
          resolved_outcome,
          instrument_admin,
          instrument_id,
          payout_per_share_minor
        FROM markets
        WHERE market_id = $1
          AND active = TRUE
        ORDER BY created_at DESC, contract_id DESC
        LIMIT 1
        "#,
    )
    .bind(market_id)
    .fetch_optional(&mut **tx)
    .await
    .context("query active market for resolve")?;
    let Some(row) = row else {
        return Err(ApiError::not_found("market not found"));
    };

    if row.payout_per_share_minor <= 0 {
        return Err(ApiError::internal(
            "market payout_per_share_minor must be positive",
        ));
    }

    Ok(row)
}

async fn load_market_settlement_job_for_update(
    tx: &mut Transaction<'_, Postgres>,
    market_id: &str,
) -> Result<Option<MarketSettlementJobRow>, ApiError> {
    let row: Option<MarketSettlementJobRow> = sqlx::query_as(
        r#"
        SELECT
          market_id,
          market_contract_id,
          instrument_admin,
          instrument_id,
          resolved_outcome,
          payout_per_share_minor,
          state,
          target_epoch,
          error,
          created_at,
          updated_at
        FROM market_settlement_jobs
        WHERE market_id = $1
        FOR UPDATE
        "#,
    )
    .bind(market_id)
    .fetch_optional(&mut **tx)
    .await
    .context("query market settlement job for update")?;

    Ok(row)
}

async fn ensure_market_settlement_job(
    tx: &mut Transaction<'_, Postgres>,
    seed: &SettlementJobSeed<'_>,
    source: &str,
) -> Result<String, ApiError> {
    let existing = load_market_settlement_job_for_update(tx, seed.market_id).await?;
    if let Some(existing) = existing {
        if existing.resolved_outcome != seed.resolved_outcome {
            return Err(ApiError::conflict(format!(
                "market already resolved to {}, cannot resolve to {}",
                existing.resolved_outcome, seed.resolved_outcome
            )));
        }

        sqlx::query(
            r#"
            UPDATE market_settlement_jobs
            SET
              market_contract_id = $2,
              instrument_admin = $3,
              instrument_id = $4,
              payout_per_share_minor = $5,
              updated_at = now()
            WHERE market_id = $1
            "#,
        )
        .bind(seed.market_id)
        .bind(seed.market_contract_id)
        .bind(seed.instrument_admin)
        .bind(seed.instrument_id)
        .bind(seed.payout_per_share_minor)
        .execute(&mut **tx)
        .await
        .context("refresh existing market settlement job")?;

        insert_market_settlement_event(
            tx,
            seed.market_id,
            "JobObserved",
            serde_json::json!({
              "source": source,
              "state": existing.state,
              "market_contract_id": seed.market_contract_id,
            }),
        )
        .await?;

        return Ok(existing.state);
    }

    sqlx::query(
        r#"
        INSERT INTO market_settlement_jobs (
          market_id,
          market_contract_id,
          instrument_admin,
          instrument_id,
          resolved_outcome,
          payout_per_share_minor,
          state,
          target_epoch,
          error,
          created_at,
          updated_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,'Pending',NULL,NULL,now(),now())
        "#,
    )
    .bind(seed.market_id)
    .bind(seed.market_contract_id)
    .bind(seed.instrument_admin)
    .bind(seed.instrument_id)
    .bind(seed.resolved_outcome)
    .bind(seed.payout_per_share_minor)
    .execute(&mut **tx)
    .await
    .context("insert market settlement job")?;

    insert_market_settlement_event(
        tx,
        seed.market_id,
        "JobEnqueued",
        serde_json::json!({
          "source": source,
          "market_contract_id": seed.market_contract_id,
          "resolved_outcome": seed.resolved_outcome,
        }),
    )
    .await?;

    Ok("Pending".to_string())
}

async fn insert_market_settlement_event(
    tx: &mut Transaction<'_, Postgres>,
    market_id: &str,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<(), ApiError> {
    sqlx::query(
        r#"
        INSERT INTO market_settlement_events (
          market_id,
          event_type,
          payload,
          created_at
        )
        VALUES ($1,$2,$3,now())
        "#,
    )
    .bind(market_id)
    .bind(event_type)
    .bind(payload)
    .execute(&mut **tx)
    .await
    .context("insert market settlement event")?;
    Ok(())
}

async fn lookup_active_market_contract_id(
    db: &PgPool,
    market_id: &str,
) -> Result<String, ApiError> {
    let contract_id: Option<String> = sqlx::query_scalar(
        r#"
        SELECT contract_id
        FROM markets
        WHERE market_id = $1 AND active = TRUE
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(market_id)
    .fetch_optional(db)
    .await
    .context("query active market contract_id")?;

    match contract_id {
        Some(cid) => Ok(cid),
        None => Err(ApiError {
            status: StatusCode::NOT_FOUND,
            message: "market not found".to_string(),
            code: None,
            details: None,
        }),
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
            }
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    "failed to install SIGTERM handler; Ctrl-C will still work"
                );
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("shutdown signal received");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{self, AssertUnwindSafe};
    use std::sync::{Mutex, OnceLock};
    use std::time::Duration;

    static WAL_TEST_ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn with_wal_env_overrides(overrides: &[(&str, Option<&str>)], test: impl FnOnce()) {
        let lock = WAL_TEST_ENV_LOCK.get_or_init(|| Mutex::new(()));
        let _guard = lock.lock().expect("acquire wal env test lock");

        let mut previous_values = Vec::with_capacity(overrides.len());
        for (key, value) in overrides {
            let previous = std::env::var(key).ok();
            previous_values.push((key, previous));

            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            };
        }

        let result = panic::catch_unwind(AssertUnwindSafe(test));

        for (key, previous) in previous_values {
            match previous {
                Some(previous) => std::env::set_var(key, previous),
                None => std::env::remove_var(key),
            };
        }

        if let Err(panic) = result {
            panic::resume_unwind(panic);
        }
    }

    #[test]
    fn parse_cors_wildcard_origin_accepts_valid_entry() {
        let parsed = parse_cors_wildcard_origin("https://*.pebble.markets").unwrap();
        assert_eq!(parsed.scheme, "https");
        assert_eq!(parsed.host_suffix, "pebble.markets");
        assert_eq!(parsed.port, None);
    }

    #[test]
    fn cors_wildcard_requires_subdomain() {
        let parsed = parse_cors_wildcard_origin("https://*.pebble.markets").unwrap();
        assert!(parsed.matches("https://api.pebble.markets"));
        assert!(!parsed.matches("https://pebble.markets"));
    }

    #[test]
    fn cors_wildcard_honors_scheme_and_port() {
        let parsed = parse_cors_wildcard_origin("https://*.pebble.markets:8443").unwrap();
        assert!(parsed.matches("https://api.pebble.markets:8443"));
        assert!(!parsed.matches("http://api.pebble.markets:8443"));
        assert!(!parsed.matches("https://api.pebble.markets:443"));
    }

    #[test]
    fn right_key_from_json_extracts_known_kinds() {
        let act = serde_json::json!({
            "kind": {
                "CanActAs": {
                    "value": { "party": "Alice::1220" }
                }
            }
        });
        let read = serde_json::json!({
            "kind": {
                "CanReadAs": {
                    "value": { "party": "Alice::1220" }
                }
            }
        });
        assert_eq!(
            right_key_from_json(&act),
            Some("act:Alice::1220".to_string())
        );
        assert_eq!(
            right_key_from_json(&read),
            Some("read:Alice::1220".to_string())
        );
    }

    #[test]
    fn party_allocation_already_exists_detection_accepts_known_bad_request() {
        let body = "{\"cause\":\"Party already exists: party Foo::1220 is already allocated on this node\"}";
        assert!(is_party_allocation_already_exists(
            reqwest::StatusCode::BAD_REQUEST,
            body
        ));
    }

    #[test]
    fn party_allocation_already_exists_detection_rejects_other_errors() {
        assert!(!is_party_allocation_already_exists(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            "Party already exists",
        ));
        assert!(!is_party_allocation_already_exists(
            reqwest::StatusCode::BAD_REQUEST,
            "missing required field",
        ));
    }

    #[test]
    fn normalize_user_api_key_label_trims_and_accepts_ascii() {
        let normalized = normalize_user_api_key_label(Some("  bot-a  ".to_string())).unwrap();
        assert_eq!(normalized, Some("bot-a".to_string()));
    }

    #[test]
    fn normalize_user_api_key_label_rejects_control_characters() {
        let err = normalize_user_api_key_label(Some("bad\nlabel".to_string())).unwrap_err();
        assert_eq!(err.status_code(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn mask_user_api_key_redacts_middle_segment() {
        let masked = mask_user_api_key("pbl_uak_1234567890abcdef");
        assert!(masked.starts_with("pbl_"));
        assert!(masked.contains("..."));
        assert!(masked.ends_with("cdef"));
        assert!(!masked.contains("1234567890ab"));
    }

    #[test]
    fn parse_wal_config_defaults() {
        with_wal_env_overrides(
            &[
                ("PEBBLE_WAL_DIR", None),
                ("PEBBLE_WAL_MAX_BATCH_SIZE", None),
                ("PEBBLE_WAL_MAX_BATCH_WAIT_MS", None),
                ("PEBBLE_WAL_SEGMENT_SIZE_BYTES", None),
            ],
            || {
                let config = parse_wal_config().expect("parse default WAL config");
                assert_eq!(config.wal_dir, std::path::PathBuf::from("./wal"));
                assert_eq!(config.max_batch_size, 1000);
                assert_eq!(config.max_batch_wait, Duration::from_millis(1));
                assert_eq!(config.segment_size_bytes, 64 * 1024 * 1024);
            },
        );
    }

    #[test]
    fn parse_wal_config_accepts_custom_values() {
        with_wal_env_overrides(
            &[
                ("PEBBLE_WAL_DIR", Some("/tmp/pebble-fluids")),
                ("PEBBLE_WAL_MAX_BATCH_SIZE", Some("7")),
                ("PEBBLE_WAL_MAX_BATCH_WAIT_MS", Some("42")),
                ("PEBBLE_WAL_SEGMENT_SIZE_BYTES", Some("8192")),
            ],
            || {
                let config = parse_wal_config().expect("parse custom WAL config");
                assert_eq!(
                    config.wal_dir,
                    std::path::PathBuf::from("/tmp/pebble-fluids")
                );
                assert_eq!(config.max_batch_size, 7);
                assert_eq!(config.max_batch_wait, Duration::from_millis(42));
                assert_eq!(config.segment_size_bytes, 8192);
            },
        );
    }

    #[test]
    fn parse_wal_config_rejects_zero_values() {
        with_wal_env_overrides(&[("PEBBLE_WAL_MAX_BATCH_SIZE", Some("0"))], || {
            assert!(parse_wal_config().is_err());
        });
        with_wal_env_overrides(&[("PEBBLE_WAL_MAX_BATCH_WAIT_MS", Some("0"))], || {
            assert!(parse_wal_config().is_err());
        });
        with_wal_env_overrides(&[("PEBBLE_WAL_SEGMENT_SIZE_BYTES", Some("0"))], || {
            assert!(parse_wal_config().is_err());
        });
    }

    #[test]
    fn parse_fluid_role_defaults_to_auto() {
        with_wal_env_overrides(&[("PEBBLE_FLUID_ROLE", None)], || {
            assert_eq!(
                parse_fluid_role().expect("parse role"),
                FluidRuntimeRole::Auto
            );
        });
    }

    #[test]
    fn parse_fluid_role_accepts_follower() {
        with_wal_env_overrides(&[("PEBBLE_FLUID_ROLE", Some("follower"))], || {
            assert_eq!(
                parse_fluid_role().expect("parse follower role"),
                FluidRuntimeRole::Follower
            );
        });
    }

    #[test]
    fn parse_fluid_replication_config_uses_bind_port_for_default_advertise() {
        with_wal_env_overrides(
            &[
                ("PEBBLE_FLUID_NODE_ID", Some("node-a")),
                ("PEBBLE_FLUID_REPLICATION_ADDR", Some("0.0.0.0:4001")),
                ("PEBBLE_FLUID_REPLICATION_ADVERTISE_ADDR", None),
                ("PEBBLE_FLUID_REPLICATION_AUTH_TOKEN", Some("shared-token")),
                ("PEBBLE_FLUID_HEARTBEAT_INTERVAL_MS", Some("250")),
            ],
            || {
                let cfg = parse_fluid_replication_config().expect("parse replication config");
                assert_eq!(cfg.node_id, "node-a");
                assert_eq!(cfg.bind_addr, "0.0.0.0:4001");
                assert_eq!(cfg.advertise_addr, "node-a:4001");
                assert_eq!(cfg.auth_token, "shared-token");
                assert_eq!(cfg.heartbeat_interval, Duration::from_millis(250));
                assert_eq!(cfg.follower_heartbeat_timeout, Duration::from_millis(1000));
            },
        );
    }

    #[test]
    fn parse_fluid_replication_config_requires_auth_token() {
        with_wal_env_overrides(
            &[
                ("PEBBLE_FLUID_NODE_ID", Some("node-a")),
                ("PEBBLE_FLUID_REPLICATION_ADDR", Some("0.0.0.0:4001")),
                (
                    "PEBBLE_FLUID_REPLICATION_ADVERTISE_ADDR",
                    Some("node-a:4001"),
                ),
                ("PEBBLE_FLUID_REPLICATION_AUTH_TOKEN", None),
            ],
            || {
                assert!(parse_fluid_replication_config().is_err());
            },
        );
    }

    #[test]
    fn parse_fluid_durability_config_defaults_to_async() {
        with_wal_env_overrides(
            &[
                ("PEBBLE_FLUID_REPLICATION_MODE", None),
                ("PEBBLE_FLUID_MIN_ACK_COUNT", None),
                ("PEBBLE_FLUID_ACK_TIMEOUT_MS", None),
                ("PEBBLE_FLUID_POST_FSYNC_FAILURE_POLICY", None),
                ("PEBBLE_FLUID_ACK_TIMEOUT_DEMOTE_COUNT", None),
                ("PEBBLE_FLUID_PG_UNREACHABLE_DEMOTE_SECS", None),
            ],
            || {
                let cfg = parse_fluid_durability_config().expect("parse default durability config");
                assert_eq!(cfg.replication_mode, FluidReplicationMode::Async);
                assert_eq!(cfg.min_ack_count, 1);
                assert_eq!(cfg.ack_timeout, Duration::from_millis(250));
                assert_eq!(cfg.failure_policy, PostFsyncFailurePolicy::Strict);
                assert_eq!(cfg.ack_timeout_demote_count, 3);
                assert_eq!(cfg.pg_unreachable_demote_after, Duration::from_secs(30));
            },
        );
    }

    #[test]
    fn parse_fluid_durability_config_accepts_semisync_degrade_mode() {
        with_wal_env_overrides(
            &[
                ("PEBBLE_FLUID_REPLICATION_MODE", Some("semi-sync")),
                ("PEBBLE_FLUID_MIN_ACK_COUNT", Some("2")),
                ("PEBBLE_FLUID_ACK_TIMEOUT_MS", Some("500")),
                (
                    "PEBBLE_FLUID_POST_FSYNC_FAILURE_POLICY",
                    Some("degrade-to-async"),
                ),
                ("PEBBLE_FLUID_ACK_TIMEOUT_DEMOTE_COUNT", Some("5")),
                ("PEBBLE_FLUID_PG_UNREACHABLE_DEMOTE_SECS", Some("45")),
            ],
            || {
                let cfg =
                    parse_fluid_durability_config().expect("parse semisync durability config");
                assert_eq!(cfg.replication_mode, FluidReplicationMode::SemiSync);
                assert_eq!(cfg.min_ack_count, 2);
                assert_eq!(cfg.ack_timeout, Duration::from_millis(500));
                assert_eq!(cfg.failure_policy, PostFsyncFailurePolicy::DegradeToAsync);
                assert_eq!(cfg.ack_timeout_demote_count, 5);
                assert_eq!(cfg.pg_unreachable_demote_after, Duration::from_secs(45));
            },
        );
    }

    #[test]
    fn parse_reconcile_config_defaults() {
        with_wal_env_overrides(
            &[
                ("PEBBLE_FLUID_RECONCILE_PENDING_TTL_SECS", None),
                ("PEBBLE_FLUID_RECONCILE_HMAC_KEY", None),
                ("PEBBLE_FLUID_RECONCILE_HMAC_KEY_PREVIOUS", None),
            ],
            || {
                let cfg = parse_reconcile_config().expect("parse default reconcile config");
                assert_eq!(cfg.pending_ttl, Duration::from_secs(60));
                assert_eq!(cfg.hmac_key, None);
                assert_eq!(cfg.hmac_key_previous, None);
                assert_eq!(
                    cfg.ttl_skew_tolerance,
                    Duration::from_secs(RECONCILE_TTL_SKEW_TOLERANCE_SECS)
                );
            },
        );
    }

    #[test]
    fn parse_reconcile_config_accepts_keys() {
        with_wal_env_overrides(
            &[
                ("PEBBLE_FLUID_RECONCILE_PENDING_TTL_SECS", Some("90")),
                ("PEBBLE_FLUID_RECONCILE_HMAC_KEY", Some("active-key")),
                (
                    "PEBBLE_FLUID_RECONCILE_HMAC_KEY_PREVIOUS",
                    Some("previous-key"),
                ),
            ],
            || {
                let cfg = parse_reconcile_config().expect("parse reconcile config");
                assert_eq!(cfg.pending_ttl, Duration::from_secs(90));
                assert_eq!(cfg.hmac_key, Some(b"active-key".to_vec()));
                assert_eq!(cfg.hmac_key_previous, Some(b"previous-key".to_vec()));
            },
        );
    }

    #[test]
    fn reconcile_token_roundtrip_verifies_payload() {
        let config = ReconcileConfig {
            pending_ttl: Duration::from_secs(60),
            hmac_key: Some(b"active-key".to_vec()),
            hmac_key_previous: None,
            ttl_skew_tolerance: Duration::from_secs(5),
        };
        let payload = ReconcileTokenPayload {
            account_id: "acc-1".to_string(),
            nonce: 42,
            wal_offset: 123,
            unknown_at_unix_ms: 1_700_000_000_000,
        };
        let token = sign_reconcile_token(&config, &payload).expect("sign");
        let verified = verify_reconcile_token(&config, "acc-1", 42, 123, &token).expect("verify");
        assert_eq!(
            verified.unknown_at.timestamp_millis(),
            payload.unknown_at_unix_ms
        );
    }

    #[test]
    fn reconcile_token_accepts_previous_key_during_rotation() {
        let previous_key = b"previous-key".to_vec();
        let old_config = ReconcileConfig {
            pending_ttl: Duration::from_secs(60),
            hmac_key: Some(previous_key.clone()),
            hmac_key_previous: None,
            ttl_skew_tolerance: Duration::from_secs(5),
        };
        let payload = ReconcileTokenPayload {
            account_id: "acc-2".to_string(),
            nonce: 7,
            wal_offset: 88,
            unknown_at_unix_ms: 1_700_000_000_500,
        };
        let token = sign_reconcile_token(&old_config, &payload).expect("sign old");

        let rotated = ReconcileConfig {
            pending_ttl: Duration::from_secs(60),
            hmac_key: Some(b"active-key".to_vec()),
            hmac_key_previous: Some(previous_key),
            ttl_skew_tolerance: Duration::from_secs(5),
        };
        verify_reconcile_token(&rotated, "acc-2", 7, 88, &token).expect("verify previous key");
    }

    #[test]
    fn reconcile_token_rejects_tampered_signature() {
        let config = ReconcileConfig {
            pending_ttl: Duration::from_secs(60),
            hmac_key: Some(b"active-key".to_vec()),
            hmac_key_previous: None,
            ttl_skew_tolerance: Duration::from_secs(5),
        };
        let payload = ReconcileTokenPayload {
            account_id: "acc-3".to_string(),
            nonce: 11,
            wal_offset: 900,
            unknown_at_unix_ms: 1_700_000_001_000,
        };
        let token = sign_reconcile_token(&config, &payload).expect("sign");
        let mut parts = token.split('.').collect::<Vec<_>>();
        assert_eq!(parts.len(), 2);
        parts[1] = "invalidsig";
        let tampered = format!("{}.{}", parts[0], parts[1]);
        let err = verify_reconcile_token(&config, "acc-3", 11, 900, &tampered).unwrap_err();
        assert_eq!(err.status_code(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn pending_reconciliation_timeout_honors_skew_tolerance() {
        let config = ReconcileConfig {
            pending_ttl: Duration::from_secs(60),
            hmac_key: Some(b"active-key".to_vec()),
            hmac_key_previous: None,
            ttl_skew_tolerance: Duration::from_secs(5),
        };
        let pending = Utc::now() - chrono::TimeDelta::seconds(64);
        assert!(!pending_reconciliation_timed_out(&config, pending));

        let timed_out = Utc::now() - chrono::TimeDelta::seconds(66);
        assert!(pending_reconciliation_timed_out(&config, timed_out));
    }

    #[test]
    fn parse_fluid_durability_config_rejects_invalid_policy() {
        with_wal_env_overrides(
            &[("PEBBLE_FLUID_POST_FSYNC_FAILURE_POLICY", Some("invalid"))],
            || {
                assert!(parse_fluid_durability_config().is_err());
            },
        );
    }

    #[test]
    fn parse_fluid_durability_config_rejects_zero_min_ack_count() {
        with_wal_env_overrides(&[("PEBBLE_FLUID_MIN_ACK_COUNT", Some("0"))], || {
            assert!(parse_fluid_durability_config().is_err());
        });
    }

    #[test]
    fn parse_fluid_replication_config_accepts_explicit_follower_timeout() {
        with_wal_env_overrides(
            &[
                ("PEBBLE_FLUID_NODE_ID", Some("node-a")),
                ("PEBBLE_FLUID_REPLICATION_ADDR", Some("0.0.0.0:4001")),
                (
                    "PEBBLE_FLUID_REPLICATION_ADVERTISE_ADDR",
                    Some("node-a:4001"),
                ),
                ("PEBBLE_FLUID_REPLICATION_AUTH_TOKEN", Some("shared-token")),
                ("PEBBLE_FLUID_HEARTBEAT_INTERVAL_MS", Some("250")),
                ("PEBBLE_FLUID_FOLLOWER_HEARTBEAT_TIMEOUT_MS", Some("750")),
            ],
            || {
                let cfg = parse_fluid_replication_config().expect("parse replication config");
                assert_eq!(cfg.follower_heartbeat_timeout, Duration::from_millis(750));
            },
        );
    }

    #[test]
    fn parse_fluid_replication_config_rejects_timeout_below_heartbeat() {
        with_wal_env_overrides(
            &[
                ("PEBBLE_FLUID_NODE_ID", Some("node-a")),
                ("PEBBLE_FLUID_REPLICATION_ADDR", Some("0.0.0.0:4001")),
                (
                    "PEBBLE_FLUID_REPLICATION_ADVERTISE_ADDR",
                    Some("node-a:4001"),
                ),
                ("PEBBLE_FLUID_REPLICATION_AUTH_TOKEN", Some("shared-token")),
                ("PEBBLE_FLUID_HEARTBEAT_INTERVAL_MS", Some("250")),
                ("PEBBLE_FLUID_FOLLOWER_HEARTBEAT_TIMEOUT_MS", Some("200")),
            ],
            || {
                assert!(parse_fluid_replication_config().is_err());
            },
        );
    }

    #[test]
    fn parse_projector_config_defaults() {
        with_wal_env_overrides(
            &[("PEBBLE_WAL_PROJECTION_LAG_THRESHOLD_SECS", None)],
            || {
                let config = parse_projector_config().expect("parse default projector config");
                assert_eq!(
                    config.projection_lag_unhealthy_threshold,
                    Duration::from_secs(60)
                );
            },
        );
    }

    #[test]
    fn parse_projector_config_accepts_custom_value() {
        with_wal_env_overrides(
            &[("PEBBLE_WAL_PROJECTION_LAG_THRESHOLD_SECS", Some("17"))],
            || {
                let config = parse_projector_config().expect("parse custom projector config");
                assert_eq!(
                    config.projection_lag_unhealthy_threshold,
                    Duration::from_secs(17)
                );
            },
        );
    }

    #[test]
    fn parse_projector_config_rejects_zero_threshold() {
        with_wal_env_overrides(
            &[("PEBBLE_WAL_PROJECTION_LAG_THRESHOLD_SECS", Some("0"))],
            || {
                assert!(parse_projector_config().is_err());
            },
        );
    }

    #[test]
    fn render_metrics_includes_stage_d_replication_fields() {
        let text = render_metrics(&MetricsSnapshot {
            role: "leader",
            leader_epoch: 9,
            committed_offset: 42,
            min_ack_count: 2,
            durable_ack_count_at_committed: Some(2),
            consecutive_ack_timeouts: 1,
            connected_followers: Some(2),
            follower_metrics: vec![
                FollowerMetricsSnapshot {
                    node_id: "follower-a".to_string(),
                    durable_applied: 44,
                    lag_entries: 0,
                },
                FollowerMetricsSnapshot {
                    node_id: "follower\\\"b".to_string(),
                    durable_applied: 43,
                    lag_entries: 1,
                },
            ],
            replication_tip_offset: Some(44),
            durable_applied: Some(44),
            wal_writer_metrics: Some(pebble_fluid::wal::WalWriterStats {
                batches: 3,
                entries: 7,
                fsyncs: 3,
                last_batch_size: 2,
                last_fsync_latency_micros: 100,
                max_fsync_latency_micros: 200,
                total_fsync_latency_micros: 300,
            }),
            wal_projector_metrics: Some(wal_projector::WalProjectorMetrics {
                projected_offset: 40,
                lag_entries: 4,
                wal_disk_usage_bytes: 4096,
                projected_records: 10,
            }),
        });
        assert!(text.contains("pebble_fluid_leader_epoch 9"));
        assert!(text.contains("pebble_fluid_committed_offset 42"));
        assert!(text.contains("pebble_fluid_min_ack_count 2"));
        assert!(text.contains("pebble_fluid_durable_ack_count_at_committed 2"));
        assert!(text.contains("pebble_fluid_connected_followers 2"));
        assert!(text.contains("pebble_fluid_follower_durable_applied{node_id=\"follower-a\"} 44"));
        assert!(text.contains("pebble_fluid_follower_lag_entries{node_id=\"follower-a\"} 0"));
        assert!(text
            .contains("pebble_fluid_follower_durable_applied{node_id=\"follower\\\\\\\"b\"} 43"));
        assert!(text.contains("pebble_fluid_wal_fsyncs_total 3"));
        assert!(text.contains("pebble_fluid_projection_lag_entries 4"));
    }
}
