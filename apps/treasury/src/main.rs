use std::{collections::HashMap, net::SocketAddr};

use anyhow::{anyhow, Context as _, Result};
use axum::{routing::get, Json, Router};
use chrono::Utc;
use pebble_daml_grpc::com::daml::ledger::api::v2 as lapi;
use pebble_ids::{parse_account_id, parse_deposit_id, parse_withdrawal_id};
use sha2::{Digest as _, Sha256};
use sqlx::{postgres::PgPoolOptions, PgPool, Postgres, Transaction};
use tonic::{metadata::MetadataValue, transport::Endpoint, Request};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const MAX_BOUNDED_COMMAND_ID_LEN: usize = 120;
const METADATA_FLOW_KEY: &str = "wizardcat.xyz/pebble.flow";
const METADATA_FLOW_WITHDRAWAL: &str = "Withdrawal";
const METADATA_ACCOUNT_ID_KEY: &str = "wizardcat.xyz/pebble.accountId";
const METADATA_WITHDRAWAL_ID_KEY: &str = "wizardcat.xyz/pebble.withdrawalId";
const TRANSFER_PENDING_RECEIVER_ACCEPTANCE: &str = "TransferPendingReceiverAcceptance";
const TRANSFER_PENDING_INTERNAL_WORKFLOW: &str = "TransferPendingInternalWorkflow";
const OPERATOR_ACCEPT_MODE_WITHDRAWAL_AUTO_ACCEPT: &str = "WithdrawalAutoAccept";
const WITHDRAWAL_RECEIPT_STATUS_COMPLETED: &str = "Completed";

#[tokio::main]
async fn main() -> Result<()> {
    // Local dev convenience; no-op if .env doesn't exist.
    let _ = dotenvy::dotenv();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg = Config::from_env().await?;

    let db = PgPoolOptions::new()
        .max_connections(10)
        .connect(&cfg.db_url)
        .await
        .context("connect to postgres")?;

    // Keep schema bootstrapping local to the repo without requiring external tooling.
    sqlx::migrate!("../../infra/sql/migrations")
        .run(&db)
        .await
        .context("run migrations")?;

    upsert_account_lock_capability(
        &db,
        "treasury",
        cfg.account_lock_namespace,
        cfg.active_withdrawals_enabled,
        &serde_json::json!({
            "account_writer_enabled": cfg.account_writer_enabled,
            "withdrawal_request_state_enabled": cfg.withdrawal_request_state_enabled,
            "withdrawal_submission_saga_enabled": cfg.withdrawal_submission_saga_enabled,
            "withdrawal_one_shot_auto_accept_enabled": cfg.withdrawal_one_shot_auto_accept_enabled,
            "withdrawal_one_shot_require_policy": cfg.withdrawal_one_shot_require_policy,
            "withdrawal_one_shot_auto_claim_enabled": cfg.withdrawal_one_shot_auto_claim_enabled,
            "withdrawal_one_shot_auto_claim_poll_ms": cfg.withdrawal_one_shot_auto_claim_poll_ms,
            "withdrawal_one_shot_auto_claim_max_per_tick": cfg.withdrawal_one_shot_auto_claim_max_per_tick,
        }),
    )
    .await?;
    if cfg.active_withdrawals_enabled {
        ensure_account_lock_namespace_parity(
            &db,
            cfg.account_lock_namespace,
            cfg.active_withdrawals_enabled,
            &["api", "treasury", "clearing"],
        )
        .await?;
    }

    let addr = cfg.addr;
    let state = AppState { db, cfg };

    if state.cfg.enable_mock_registry {
        let s = state.clone();
        tokio::spawn(async move {
            if let Err(err) = mock_registry_loop(s).await {
                tracing::error!(error = ?err, "mock registry loop crashed");
            }
        });
    }

    {
        let s = state.clone();
        tokio::spawn(async move {
            if let Err(err) = holding_claim_loop(s).await {
                tracing::error!(error = ?err, "holding claim loop crashed");
            }
        });
    }

    if state.cfg.withdrawal_one_shot_auto_claim_enabled {
        let s = state.clone();
        tokio::spawn(async move {
            if let Err(err) = withdrawal_claim_auto_loop(s).await {
                tracing::error!(error = ?err, "withdrawal claim auto loop crashed");
            }
        });
    }

    {
        let s = state.clone();
        tokio::spawn(async move {
            if let Err(err) = deposit_accept_loop(s).await {
                tracing::error!(error = ?err, "deposit accept loop crashed");
            }
        });
    }

    {
        let s = state.clone();
        tokio::spawn(async move {
            if let Err(err) = deposit_reconcile_loop(s).await {
                tracing::error!(error = ?err, "deposit reconcile loop crashed");
            }
        });
    }

    {
        let s = state.clone();
        tokio::spawn(async move {
            if let Err(err) = withdrawal_process_loop(s).await {
                tracing::error!(error = ?err, "withdrawal process loop crashed");
            }
        });
    }

    {
        let s = state.clone();
        tokio::spawn(async move {
            if let Err(err) = withdrawal_reconcile_loop(s).await {
                tracing::error!(error = ?err, "withdrawal reconcile loop crashed");
            }
        });
    }

    {
        let s = state.clone();
        tokio::spawn(async move {
            if let Err(err) = withdrawal_lock_reclaim_loop(s).await {
                tracing::error!(error = ?err, "withdrawal lock reclaim loop crashed");
            }
        });
    }

    {
        let s = state.clone();
        tokio::spawn(async move {
            if let Err(err) = withdrawal_cancel_loop(s).await {
                tracing::error!(error = ?err, "withdrawal cancel loop crashed");
            }
        });
    }

    {
        let s = state.clone();
        tokio::spawn(async move {
            if let Err(err) = consolidation_loop(s).await {
                tracing::error!(error = ?err, "consolidation loop crashed");
            }
        });
    }

    {
        let s = state.clone();
        tokio::spawn(async move {
            if let Err(err) = monitoring_loop(s).await {
                tracing::error!(error = ?err, "monitoring loop crashed");
            }
        });
    }

    let app = Router::new()
        .route("/healthz", get(healthz))
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    tracing::info!(%addr, "pebble-treasury listening");

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
    cfg: Config,
}

#[derive(Debug, Clone)]
struct Config {
    addr: SocketAddr,
    db_url: String,
    auth_header: Option<MetadataValue<tonic::metadata::Ascii>>,
    ledger_user_id: String,
    committee_party: String,
    instrument_admin_party: String,
    submission_path: String,
    ledger_channel: tonic::transport::Channel,
    enable_mock_registry: bool,
    withdrawal_lock_expires_seconds: i64,
    withdrawal_lock_reclaim_poll_ms: u64,
    lock_expiry_skew_seconds: i64,
    monitor_poll_ms: u64,
    alert_trec_tgt_seconds: i64,
    alert_indexer_lag_seconds: i64,
    alert_drift_coverage_abs_minor_default: i64,
    alert_drift_coverage_abs_minor_by_instrument: HashMap<InstrumentKey, i64>,
    alert_drift_min_liquidity_headroom_minor_default: i64,
    alert_drift_min_liquidity_headroom_minor_by_instrument: HashMap<InstrumentKey, i64>,
    account_lock_namespace: i32,
    active_withdrawals_enabled: bool,
    withdrawal_request_state_enabled: bool,
    withdrawal_submission_saga_enabled: bool,
    withdrawal_one_shot_auto_accept_enabled: bool,
    withdrawal_one_shot_require_policy: bool,
    withdrawal_one_shot_auto_claim_enabled: bool,
    withdrawal_one_shot_auto_claim_poll_ms: u64,
    withdrawal_one_shot_auto_claim_max_per_tick: i64,
    account_writer_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct InstrumentKey {
    instrument_admin: String,
    instrument_id: String,
}

impl Config {
    async fn from_env() -> Result<Self> {
        let addr: SocketAddr = std::env::var("PEBBLE_TREASURY_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:3040".to_string())
            .parse()
            .context("parse PEBBLE_TREASURY_ADDR")?;

        let db_url = std::env::var("PEBBLE_DB_URL")
            .unwrap_or_else(|_| "postgres://pebble:pebble@127.0.0.1:5432/pebble".to_string());

        let ledger_host = std::env::var("PEBBLE_CANTON_LEDGER_API_HOST")
            .unwrap_or_else(|_| "127.0.0.1".to_string());

        let ledger_port = std::env::var("PEBBLE_CANTON_LEDGER_API_PORT")
            .unwrap_or_else(|_| "6865".to_string())
            .parse::<u16>()
            .context("parse PEBBLE_CANTON_LEDGER_API_PORT")?;

        let committee_party = std::env::var("PEBBLE_COMMITTEE_PARTY")
            .context("PEBBLE_COMMITTEE_PARTY is required")?;

        let instrument_admin_party = std::env::var("PEBBLE_INSTRUMENT_ADMIN_PARTY")
            .context("PEBBLE_INSTRUMENT_ADMIN_PARTY is required")?;

        let ledger_user_id = std::env::var("PEBBLE_TREASURY_LEDGER_USER_ID")
            .unwrap_or_else(|_| "pebble-treasury".to_string());

        let auth_header = match std::env::var("PEBBLE_CANTON_AUTH_TOKEN") {
            Ok(token) if !token.trim().is_empty() => Some(parse_auth_header(&token)?),
            _ => None,
        };

        let enable_mock_registry = match std::env::var("PEBBLE_TREASURY_ENABLE_MOCK_REGISTRY") {
            Ok(v) => parse_env_bool("PEBBLE_TREASURY_ENABLE_MOCK_REGISTRY", &v)?,
            Err(_) => true,
        };

        let submission_path = match std::env::var("PEBBLE_TREASURY_SUBMISSION_PATH") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => format!("ledger:{}:{}", ledger_host.trim(), ledger_port),
        };

        let withdrawal_lock_expires_seconds =
            std::env::var("PEBBLE_TREASURY_WITHDRAWAL_LOCK_EXPIRES_SECONDS")
                .unwrap_or_else(|_| "3600".to_string())
                .parse::<i64>()
                .context("parse PEBBLE_TREASURY_WITHDRAWAL_LOCK_EXPIRES_SECONDS")?;
        if withdrawal_lock_expires_seconds <= 0 {
            return Err(anyhow!(
                "PEBBLE_TREASURY_WITHDRAWAL_LOCK_EXPIRES_SECONDS must be > 0"
            ));
        }

        let withdrawal_lock_reclaim_poll_ms =
            std::env::var("PEBBLE_TREASURY_WITHDRAWAL_LOCK_RECLAIM_POLL_MS")
                .unwrap_or_else(|_| "1000".to_string())
                .parse::<u64>()
                .context("parse PEBBLE_TREASURY_WITHDRAWAL_LOCK_RECLAIM_POLL_MS")?;
        if withdrawal_lock_reclaim_poll_ms == 0 {
            return Err(anyhow!(
                "PEBBLE_TREASURY_WITHDRAWAL_LOCK_RECLAIM_POLL_MS must be > 0"
            ));
        }

        let lock_expiry_skew_seconds = std::env::var("PEBBLE_TREASURY_LOCK_EXPIRY_SKEW_SECONDS")
            .unwrap_or_else(|_| "5".to_string())
            .parse::<i64>()
            .context("parse PEBBLE_TREASURY_LOCK_EXPIRY_SKEW_SECONDS")?;
        if lock_expiry_skew_seconds < 0 {
            return Err(anyhow!(
                "PEBBLE_TREASURY_LOCK_EXPIRY_SKEW_SECONDS must be >= 0"
            ));
        }

        let monitor_poll_ms = std::env::var("PEBBLE_TREASURY_MONITOR_POLL_MS")
            .unwrap_or_else(|_| "5000".to_string())
            .parse::<u64>()
            .context("parse PEBBLE_TREASURY_MONITOR_POLL_MS")?;
        if monitor_poll_ms == 0 {
            return Err(anyhow!("PEBBLE_TREASURY_MONITOR_POLL_MS must be > 0"));
        }

        let alert_trec_tgt_seconds = std::env::var("PEBBLE_TREASURY_ALERT_TREC_TGT_SECONDS")
            .unwrap_or_else(|_| "120".to_string())
            .parse::<i64>()
            .context("parse PEBBLE_TREASURY_ALERT_TREC_TGT_SECONDS")?;
        if alert_trec_tgt_seconds <= 0 {
            return Err(anyhow!(
                "PEBBLE_TREASURY_ALERT_TREC_TGT_SECONDS must be > 0"
            ));
        }

        let alert_indexer_lag_seconds = std::env::var("PEBBLE_TREASURY_ALERT_INDEXER_LAG_SECONDS")
            .unwrap_or_else(|_| "30".to_string())
            .parse::<i64>()
            .context("parse PEBBLE_TREASURY_ALERT_INDEXER_LAG_SECONDS")?;
        if alert_indexer_lag_seconds <= 0 {
            return Err(anyhow!(
                "PEBBLE_TREASURY_ALERT_INDEXER_LAG_SECONDS must be > 0"
            ));
        }

        let alert_drift_coverage_abs_minor_default =
            std::env::var("PEBBLE_TREASURY_ALERT_DRIFT_COVERAGE_ABS_MINOR_DEFAULT")
                .unwrap_or_else(|_| "0".to_string())
                .parse::<i64>()
                .context("parse PEBBLE_TREASURY_ALERT_DRIFT_COVERAGE_ABS_MINOR_DEFAULT")?;
        if alert_drift_coverage_abs_minor_default < 0 {
            return Err(anyhow!(
                "PEBBLE_TREASURY_ALERT_DRIFT_COVERAGE_ABS_MINOR_DEFAULT must be >= 0"
            ));
        }

        let alert_drift_coverage_abs_minor_by_instrument = parse_instrument_threshold_map(
            "PEBBLE_TREASURY_ALERT_DRIFT_COVERAGE_ABS_MINOR_BY_INSTRUMENT",
            &std::env::var("PEBBLE_TREASURY_ALERT_DRIFT_COVERAGE_ABS_MINOR_BY_INSTRUMENT")
                .unwrap_or_default(),
        )?;
        if alert_drift_coverage_abs_minor_by_instrument
            .values()
            .any(|value| *value < 0)
        {
            return Err(anyhow!(
                "PEBBLE_TREASURY_ALERT_DRIFT_COVERAGE_ABS_MINOR_BY_INSTRUMENT values must be >= 0"
            ));
        }

        let alert_drift_min_liquidity_headroom_minor_default =
            std::env::var("PEBBLE_TREASURY_ALERT_DRIFT_MIN_LIQUIDITY_HEADROOM_MINOR_DEFAULT")
                .unwrap_or_else(|_| "0".to_string())
                .parse::<i64>()
                .context(
                    "parse PEBBLE_TREASURY_ALERT_DRIFT_MIN_LIQUIDITY_HEADROOM_MINOR_DEFAULT",
                )?;
        if alert_drift_min_liquidity_headroom_minor_default < 0 {
            return Err(anyhow!(
                "PEBBLE_TREASURY_ALERT_DRIFT_MIN_LIQUIDITY_HEADROOM_MINOR_DEFAULT must be >= 0"
            ));
        }

        let alert_drift_min_liquidity_headroom_minor_by_instrument =
            parse_instrument_threshold_map(
                "PEBBLE_TREASURY_ALERT_DRIFT_MIN_LIQUIDITY_HEADROOM_MINOR_BY_INSTRUMENT",
                &std::env::var(
                    "PEBBLE_TREASURY_ALERT_DRIFT_MIN_LIQUIDITY_HEADROOM_MINOR_BY_INSTRUMENT",
                )
                .unwrap_or_default(),
            )?;
        if alert_drift_min_liquidity_headroom_minor_by_instrument
            .values()
            .any(|value| *value < 0)
        {
            return Err(anyhow!(
                "PEBBLE_TREASURY_ALERT_DRIFT_MIN_LIQUIDITY_HEADROOM_MINOR_BY_INSTRUMENT values must be >= 0"
            ));
        }
        let account_lock_namespace = parse_account_lock_namespace_env()?;
        let active_withdrawals_disabled = match std::env::var("PEBBLE_ACTIVE_WITHDRAWALS_DISABLED")
        {
            Ok(v) => parse_env_bool("PEBBLE_ACTIVE_WITHDRAWALS_DISABLED", &v)?,
            Err(_) => false,
        };
        let active_withdrawals_enabled = !active_withdrawals_disabled;
        let withdrawal_request_state_disabled =
            match std::env::var("PEBBLE_WITHDRAWAL_REQUEST_STATE_DISABLED") {
                Ok(v) => parse_env_bool("PEBBLE_WITHDRAWAL_REQUEST_STATE_DISABLED", &v)?,
                Err(_) => false,
            };
        let withdrawal_request_state_enabled = !withdrawal_request_state_disabled;
        let withdrawal_submission_saga_disabled =
            match std::env::var("PEBBLE_WITHDRAWAL_SUBMISSION_SAGA_DISABLED") {
                Ok(v) => parse_env_bool("PEBBLE_WITHDRAWAL_SUBMISSION_SAGA_DISABLED", &v)?,
                Err(_) => false,
            };
        let withdrawal_submission_saga_enabled = !withdrawal_submission_saga_disabled;
        let withdrawal_one_shot_auto_accept_enabled =
            match std::env::var("PEBBLE_WITHDRAWAL_ONE_SHOT_AUTO_ACCEPT_ENABLED") {
                Ok(v) => parse_env_bool("PEBBLE_WITHDRAWAL_ONE_SHOT_AUTO_ACCEPT_ENABLED", &v)?,
                Err(_) => false,
            };
        let withdrawal_one_shot_require_policy =
            match std::env::var("PEBBLE_WITHDRAWAL_ONE_SHOT_REQUIRE_POLICY") {
                Ok(v) => parse_env_bool("PEBBLE_WITHDRAWAL_ONE_SHOT_REQUIRE_POLICY", &v)?,
                Err(_) => withdrawal_one_shot_auto_accept_enabled,
            };
        let withdrawal_one_shot_auto_claim_disabled =
            match std::env::var("PEBBLE_WITHDRAWAL_ONE_SHOT_AUTO_CLAIM_DISABLED") {
                Ok(v) => parse_env_bool("PEBBLE_WITHDRAWAL_ONE_SHOT_AUTO_CLAIM_DISABLED", &v)?,
                Err(_) => false,
            };
        let withdrawal_one_shot_auto_claim_enabled = !withdrawal_one_shot_auto_claim_disabled;
        let withdrawal_one_shot_auto_claim_poll_ms =
            std::env::var("PEBBLE_WITHDRAWAL_ONE_SHOT_AUTO_CLAIM_POLL_MS")
                .unwrap_or_else(|_| "1000".to_string())
                .parse::<u64>()
                .context("parse PEBBLE_WITHDRAWAL_ONE_SHOT_AUTO_CLAIM_POLL_MS")?;
        if withdrawal_one_shot_auto_claim_poll_ms == 0 {
            return Err(anyhow!(
                "PEBBLE_WITHDRAWAL_ONE_SHOT_AUTO_CLAIM_POLL_MS must be > 0"
            ));
        }
        let withdrawal_one_shot_auto_claim_max_per_tick =
            std::env::var("PEBBLE_WITHDRAWAL_ONE_SHOT_AUTO_CLAIM_MAX_PER_TICK")
                .unwrap_or_else(|_| "50".to_string())
                .parse::<i64>()
                .context("parse PEBBLE_WITHDRAWAL_ONE_SHOT_AUTO_CLAIM_MAX_PER_TICK")?;
        if withdrawal_one_shot_auto_claim_max_per_tick <= 0 {
            return Err(anyhow!(
                "PEBBLE_WITHDRAWAL_ONE_SHOT_AUTO_CLAIM_MAX_PER_TICK must be > 0"
            ));
        }
        let account_writer_enabled = match std::env::var("PEBBLE_ACCOUNT_WRITER_ENABLE") {
            Ok(v) => parse_env_bool("PEBBLE_ACCOUNT_WRITER_ENABLE", &v)?,
            Err(_) => false,
        };
        if withdrawal_submission_saga_enabled && !withdrawal_request_state_enabled {
            return Err(anyhow!(
                "PEBBLE_WITHDRAWAL_SUBMISSION_SAGA_DISABLED=false requires PEBBLE_WITHDRAWAL_REQUEST_STATE_DISABLED=false"
            ));
        }
        if active_withdrawals_enabled && !withdrawal_submission_saga_enabled {
            return Err(anyhow!(
                "PEBBLE_ACTIVE_WITHDRAWALS_DISABLED=false requires PEBBLE_WITHDRAWAL_SUBMISSION_SAGA_DISABLED=false"
            ));
        }
        if active_withdrawals_enabled && !withdrawal_request_state_enabled {
            return Err(anyhow!(
                "PEBBLE_ACTIVE_WITHDRAWALS_DISABLED=false requires PEBBLE_WITHDRAWAL_REQUEST_STATE_DISABLED=false"
            ));
        }
        if withdrawal_one_shot_auto_accept_enabled && !withdrawal_one_shot_require_policy {
            return Err(anyhow!(
                "PEBBLE_WITHDRAWAL_ONE_SHOT_AUTO_ACCEPT_ENABLED=true requires PEBBLE_WITHDRAWAL_ONE_SHOT_REQUIRE_POLICY=true"
            ));
        }

        let endpoint = Endpoint::from_shared(format!("http://{}:{}", ledger_host, ledger_port))
            .context("build ledger endpoint")?;
        let channel = endpoint.connect().await.context("connect to ledger")?;

        Ok(Self {
            addr,
            db_url,
            auth_header,
            ledger_user_id,
            committee_party,
            instrument_admin_party,
            submission_path,
            ledger_channel: channel,
            enable_mock_registry,
            withdrawal_lock_expires_seconds,
            withdrawal_lock_reclaim_poll_ms,
            lock_expiry_skew_seconds,
            monitor_poll_ms,
            alert_trec_tgt_seconds,
            alert_indexer_lag_seconds,
            alert_drift_coverage_abs_minor_default,
            alert_drift_coverage_abs_minor_by_instrument,
            alert_drift_min_liquidity_headroom_minor_default,
            alert_drift_min_liquidity_headroom_minor_by_instrument,
            account_lock_namespace,
            active_withdrawals_enabled,
            withdrawal_request_state_enabled,
            withdrawal_submission_saga_enabled,
            withdrawal_one_shot_auto_accept_enabled,
            withdrawal_one_shot_require_policy,
            withdrawal_one_shot_auto_claim_enabled,
            withdrawal_one_shot_auto_claim_poll_ms,
            withdrawal_one_shot_auto_claim_max_per_tick,
            account_writer_enabled,
        })
    }

    fn drift_coverage_abs_minor_threshold(
        &self,
        instrument_admin: &str,
        instrument_id: &str,
    ) -> i64 {
        self.alert_drift_coverage_abs_minor_by_instrument
            .get(&InstrumentKey {
                instrument_admin: instrument_admin.to_string(),
                instrument_id: instrument_id.to_string(),
            })
            .copied()
            .unwrap_or(self.alert_drift_coverage_abs_minor_default)
    }

    fn drift_min_liquidity_headroom_minor_threshold(
        &self,
        instrument_admin: &str,
        instrument_id: &str,
    ) -> i64 {
        self.alert_drift_min_liquidity_headroom_minor_by_instrument
            .get(&InstrumentKey {
                instrument_admin: instrument_admin.to_string(),
                instrument_id: instrument_id.to_string(),
            })
            .copied()
            .unwrap_or(self.alert_drift_min_liquidity_headroom_minor_default)
    }
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

fn parse_instrument_threshold_map(key: &str, raw: &str) -> Result<HashMap<InstrumentKey, i64>> {
    let mut out = HashMap::new();
    for entry in raw.split(',') {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            continue;
        }

        let (instrument_raw, threshold_raw) = trimmed
            .split_once('=')
            .ok_or_else(|| anyhow!("{key} entry must be instrument_admin|instrument_id=value"))?;
        let (instrument_admin, instrument_id) = instrument_raw
            .trim()
            .split_once('|')
            .ok_or_else(|| anyhow!("{key} entry key must be instrument_admin|instrument_id"))?;

        let instrument_admin = instrument_admin.trim();
        let instrument_id = instrument_id.trim();
        if instrument_admin.is_empty() || instrument_id.is_empty() {
            return Err(anyhow!(
                "{key} entry key must contain non-empty instrument_admin and instrument_id"
            ));
        }

        let threshold = threshold_raw.trim().parse::<i64>().with_context(|| {
            format!(
                "parse {key} threshold for {instrument_admin}|{instrument_id} from {threshold_raw:?}"
            )
        })?;

        let map_key = InstrumentKey {
            instrument_admin: instrument_admin.to_string(),
            instrument_id: instrument_id.to_string(),
        };
        if out.insert(map_key, threshold).is_some() {
            return Err(anyhow!(
                "{key} contains duplicate instrument entry {instrument_admin}|{instrument_id}"
            ));
        }
    }

    Ok(out)
}

async fn healthz(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({
      "status": "ok",
      "now": Utc::now(),
      "account_lock_namespace": state.cfg.account_lock_namespace,
      "active_withdrawals_enabled": state.cfg.active_withdrawals_enabled,
      "withdrawal_request_state_enabled": state.cfg.withdrawal_request_state_enabled,
      "withdrawal_submission_saga_enabled": state.cfg.withdrawal_submission_saga_enabled,
      "withdrawal_one_shot_auto_accept_enabled": state.cfg.withdrawal_one_shot_auto_accept_enabled,
      "withdrawal_one_shot_require_policy": state.cfg.withdrawal_one_shot_require_policy,
      "withdrawal_one_shot_auto_claim_enabled": state.cfg.withdrawal_one_shot_auto_claim_enabled,
      "withdrawal_one_shot_auto_claim_poll_ms": state.cfg.withdrawal_one_shot_auto_claim_poll_ms,
      "withdrawal_one_shot_auto_claim_max_per_tick": state.cfg.withdrawal_one_shot_auto_claim_max_per_tick,
      "account_writer_enabled": state.cfg.account_writer_enabled,
    }))
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

async fn monitoring_loop(state: AppState) -> Result<()> {
    loop {
        if let Err(err) = monitoring_tick(&state).await {
            tracing::warn!(error = ?err, "monitoring tick failed");
        }
        tokio::time::sleep(std::time::Duration::from_millis(state.cfg.monitor_poll_ms)).await;
    }
}

async fn monitoring_tick(state: &AppState) -> Result<()> {
    #[derive(sqlx::FromRow)]
    struct StuckSummaryRow {
        stuck_count: i64,
        oldest_updated_at: Option<chrono::DateTime<Utc>>,
    }

    #[derive(sqlx::FromRow)]
    struct StuckDetailRow {
        op_id: String,
        state: String,
        command_id: String,
    }

    let stuck_summary: StuckSummaryRow = sqlx::query_as(
        r#"
        SELECT
          count(*)::bigint AS stuck_count,
          min(updated_at) AS oldest_updated_at
        FROM treasury_ops
        WHERE state <> $1
          AND updated_at <= now() - make_interval(secs => $2)
        "#,
    )
    .bind(TREASURY_OP_STATE_DONE)
    .bind(state.cfg.alert_trec_tgt_seconds)
    .fetch_one(&state.db)
    .await
    .context("query stuck treasury op summary")?;

    if stuck_summary.stuck_count > 0 {
        let stuck_ops: Vec<StuckDetailRow> = sqlx::query_as(
            r#"
            SELECT op_id, state, command_id
            FROM treasury_ops
            WHERE state <> $1
              AND updated_at <= now() - make_interval(secs => $2)
            ORDER BY updated_at ASC
            LIMIT 5
            "#,
        )
        .bind(TREASURY_OP_STATE_DONE)
        .bind(state.cfg.alert_trec_tgt_seconds)
        .fetch_all(&state.db)
        .await
        .context("query stuck treasury op details")?;

        let sample_states = stuck_ops
            .into_iter()
            .map(|row| format!("{}:{}:{}", row.op_id, row.state, row.command_id))
            .collect::<Vec<_>>();

        let oldest_age_seconds = stuck_summary
            .oldest_updated_at
            .map(|ts| (Utc::now() - ts).num_seconds())
            .unwrap_or_default();
        tracing::error!(
            stuck_count = stuck_summary.stuck_count,
            trec_tgt_seconds = state.cfg.alert_trec_tgt_seconds,
            oldest_age_seconds,
            sample_states = ?sample_states,
            "treasury ops exceeded reconciliation target"
        );
    }

    let cancel_escalated_count: (i64,) = sqlx::query_as(
        r#"
        SELECT count(*)::bigint
        FROM withdrawal_pendings
        WHERE active = TRUE
          AND committee_party = $1
          AND pending_state = 'CancelEscalated'
        "#,
    )
    .bind(&state.cfg.committee_party)
    .fetch_one(&state.db)
    .await
    .context("query cancel-escalated pending count")?;
    if cancel_escalated_count.0 > 0 {
        tracing::error!(
            cancel_escalated_count = cancel_escalated_count.0,
            "withdrawal pendings require cancel-escalated manual intervention"
        );
    }

    #[derive(sqlx::FromRow)]
    struct OffsetRow {
        ledger_offset: i64,
        updated_at: chrono::DateTime<Utc>,
    }

    #[derive(sqlx::FromRow)]
    struct WatermarkRow {
        watermark_offset: i64,
        updated_at: chrono::DateTime<Utc>,
    }

    let indexer_offset: Option<OffsetRow> = sqlx::query_as(
        r#"
        SELECT ledger_offset, updated_at
        FROM ledger_offsets
        WHERE consumer = $1
        LIMIT 1
        "#,
    )
    .bind("pebble-indexer")
    .fetch_optional(&state.db)
    .await
    .context("query indexer ledger offset")?;

    if let Some(indexer_offset) = indexer_offset.as_ref() {
        let indexer_age_seconds = (Utc::now() - indexer_offset.updated_at).num_seconds();
        if indexer_age_seconds > state.cfg.alert_indexer_lag_seconds {
            tracing::error!(
                indexer_age_seconds,
                indexer_offset = indexer_offset.ledger_offset,
                lag_threshold_seconds = state.cfg.alert_indexer_lag_seconds,
                "indexer ledger offset appears stale"
            );
        }
    } else {
        tracing::error!("indexer ledger offset row missing");
    }

    let watermark_row: Option<WatermarkRow> = sqlx::query_as(
        r#"
        SELECT watermark_offset, updated_at
        FROM submission_path_watermarks
        WHERE submission_path = $1
        LIMIT 1
        "#,
    )
    .bind(&state.cfg.submission_path)
    .fetch_optional(&state.db)
    .await
    .context("query submission-path watermark")?;

    if let Some(watermark_row) = watermark_row.as_ref() {
        let watermark_age_seconds = (Utc::now() - watermark_row.updated_at).num_seconds();
        if watermark_age_seconds > state.cfg.alert_indexer_lag_seconds {
            tracing::error!(
                submission_path = %state.cfg.submission_path,
                watermark_age_seconds,
                watermark_offset = watermark_row.watermark_offset,
                lag_threshold_seconds = state.cfg.alert_indexer_lag_seconds,
                "submission-path watermark appears stale"
            );
        }

        if let Some(indexer_offset) = indexer_offset.as_ref() {
            let gap = (indexer_offset.ledger_offset - watermark_row.watermark_offset).abs();
            if gap > 5 {
                tracing::warn!(
                    submission_path = %state.cfg.submission_path,
                    indexer_offset = indexer_offset.ledger_offset,
                    watermark_offset = watermark_row.watermark_offset,
                    offset_gap = gap,
                    "indexer offset and submission-path watermark diverged"
                );
            }
        }

        monitor_drift_thresholds(state, watermark_row.watermark_offset).await?;
    } else {
        tracing::error!(
            submission_path = %state.cfg.submission_path,
            "submission-path watermark row missing"
        );
    }

    Ok(())
}

async fn monitor_drift_thresholds(state: &AppState, watermark_offset: i64) -> Result<()> {
    #[derive(sqlx::FromRow)]
    struct DriftRow {
        instrument_admin: String,
        instrument_id: String,
        total_holdings_minor: i64,
        cleared_liabilities_minor: i64,
        pending_withdrawals_minor: i64,
        pending_deposits_minor: i64,
        quarantined_minor: i64,
        available_liquidity_minor: i64,
    }

    let rows: Vec<DriftRow> = sqlx::query_as(
        r#"
        WITH instruments AS (
          SELECT instrument_admin, instrument_id
          FROM token_holdings
          WHERE active = TRUE
            AND owner_party = $1
          UNION
          SELECT st.instrument_admin, st.instrument_id
          FROM account_states st
          JOIN account_state_latest latest
            ON latest.contract_id = st.contract_id
          WHERE st.active = TRUE
          UNION
          SELECT instrument_admin, instrument_id
          FROM withdrawal_pendings
          WHERE active = TRUE
          UNION
          SELECT instrument_admin, instrument_id
          FROM deposit_pendings
          WHERE active = TRUE
        ),
        holdings AS (
          SELECT instrument_admin, instrument_id, COALESCE(SUM(amount_minor), 0)::BIGINT AS total_holdings_minor
          FROM token_holdings
          WHERE active = TRUE
            AND owner_party = $1
          GROUP BY instrument_admin, instrument_id
        ),
        liabilities AS (
          SELECT st.instrument_admin, st.instrument_id, COALESCE(SUM(st.cleared_cash_minor), 0)::BIGINT AS cleared_liabilities_minor
          FROM account_states st
          JOIN account_state_latest latest
            ON latest.contract_id = st.contract_id
          WHERE st.active = TRUE
          GROUP BY st.instrument_admin, st.instrument_id
        ),
        pending_withdrawals AS (
          SELECT instrument_admin, instrument_id, COALESCE(SUM(amount_minor), 0)::BIGINT AS pending_withdrawals_minor
          FROM withdrawal_pendings
          WHERE active = TRUE
          GROUP BY instrument_admin, instrument_id
        ),
        pending_deposits AS (
          SELECT instrument_admin, instrument_id, COALESCE(SUM(expected_amount_minor), 0)::BIGINT AS pending_deposits_minor
          FROM deposit_pendings
          WHERE active = TRUE
          GROUP BY instrument_admin, instrument_id
        ),
        quarantined AS (
          SELECT q.instrument_admin, q.instrument_id, COALESCE(SUM(h.amount_minor), 0)::BIGINT AS quarantined_minor
          FROM quarantined_holdings q
          LEFT JOIN token_holdings h
            ON h.contract_id = q.holding_cid
           AND h.active = TRUE
          WHERE q.active = TRUE
          GROUP BY q.instrument_admin, q.instrument_id
        ),
        reserved AS (
          SELECT DISTINCT holding_cid
          FROM treasury_holding_reservations
          WHERE state IN ('Reserved', 'Submitting')
        ),
        available_liquidity AS (
          SELECT h.instrument_admin, h.instrument_id, COALESCE(SUM(h.amount_minor), 0)::BIGINT AS available_liquidity_minor
          FROM token_holdings h
          LEFT JOIN reserved r
            ON r.holding_cid = h.contract_id
          LEFT JOIN quarantined_holdings q
            ON q.holding_cid = h.contract_id
           AND q.active = TRUE
          WHERE h.active = TRUE
            AND h.owner_party = $1
            AND h.lock_status_class IS NULL
            AND h.last_offset <= $2
            AND r.holding_cid IS NULL
            AND q.holding_cid IS NULL
          GROUP BY h.instrument_admin, h.instrument_id
        )
        SELECT
          i.instrument_admin,
          i.instrument_id,
          COALESCE(h.total_holdings_minor, 0) AS total_holdings_minor,
          COALESCE(l.cleared_liabilities_minor, 0) AS cleared_liabilities_minor,
          COALESCE(w.pending_withdrawals_minor, 0) AS pending_withdrawals_minor,
          COALESCE(d.pending_deposits_minor, 0) AS pending_deposits_minor,
          COALESCE(q.quarantined_minor, 0) AS quarantined_minor,
          COALESCE(a.available_liquidity_minor, 0) AS available_liquidity_minor
        FROM instruments i
        LEFT JOIN holdings h
          ON h.instrument_admin = i.instrument_admin
         AND h.instrument_id = i.instrument_id
        LEFT JOIN liabilities l
          ON l.instrument_admin = i.instrument_admin
         AND l.instrument_id = i.instrument_id
        LEFT JOIN pending_withdrawals w
          ON w.instrument_admin = i.instrument_admin
         AND w.instrument_id = i.instrument_id
        LEFT JOIN pending_deposits d
          ON d.instrument_admin = i.instrument_admin
         AND d.instrument_id = i.instrument_id
        LEFT JOIN quarantined q
          ON q.instrument_admin = i.instrument_admin
         AND q.instrument_id = i.instrument_id
        LEFT JOIN available_liquidity a
          ON a.instrument_admin = i.instrument_admin
         AND a.instrument_id = i.instrument_id
        ORDER BY i.instrument_admin, i.instrument_id
        "#,
    )
    .bind(&state.cfg.committee_party)
    .bind(watermark_offset)
    .fetch_all(&state.db)
    .await
    .context("query drift threshold rows")?;

    for row in rows {
        let implied_obligations_minor = i128::from(row.cleared_liabilities_minor)
            + i128::from(row.pending_withdrawals_minor)
            + i128::from(row.pending_deposits_minor)
            + i128::from(row.quarantined_minor);
        let coverage_minor = i128::from(row.total_holdings_minor) - implied_obligations_minor;
        let coverage_abs_minor = coverage_minor.abs();
        let coverage_threshold_minor = state
            .cfg
            .drift_coverage_abs_minor_threshold(&row.instrument_admin, &row.instrument_id);

        if coverage_abs_minor > i128::from(coverage_threshold_minor) {
            tracing::error!(
                instrument_admin = %row.instrument_admin,
                instrument_id = %row.instrument_id,
                coverage_minor,
                coverage_abs_minor,
                coverage_threshold_minor,
                total_holdings_minor = row.total_holdings_minor,
                implied_obligations_minor,
                cleared_liabilities_minor = row.cleared_liabilities_minor,
                pending_withdrawals_minor = row.pending_withdrawals_minor,
                pending_deposits_minor = row.pending_deposits_minor,
                quarantined_minor = row.quarantined_minor,
                "drift coverage threshold exceeded"
            );
        }

        let liquidity_headroom_minor =
            i128::from(row.available_liquidity_minor) - i128::from(row.pending_withdrawals_minor);
        let min_liquidity_headroom_minor = state.cfg.drift_min_liquidity_headroom_minor_threshold(
            &row.instrument_admin,
            &row.instrument_id,
        );
        if liquidity_headroom_minor < i128::from(min_liquidity_headroom_minor) {
            tracing::error!(
                instrument_admin = %row.instrument_admin,
                instrument_id = %row.instrument_id,
                liquidity_headroom_minor,
                min_liquidity_headroom_minor,
                available_liquidity_minor = row.available_liquidity_minor,
                pending_withdrawals_minor = row.pending_withdrawals_minor,
                watermark_offset,
                "drift liquidity threshold breached"
            );
        }
    }

    Ok(())
}

fn maybe_apply_auth<T>(
    auth_header: &Option<MetadataValue<tonic::metadata::Ascii>>,
    req: &mut Request<T>,
) {
    if let Some(auth_header) = auth_header.clone() {
        req.metadata_mut().insert("authorization", auth_header);
    }
}

fn record_field(label: &str, value: lapi::Value) -> lapi::RecordField {
    lapi::RecordField {
        label: label.to_string(),
        value: Some(value),
    }
}

fn value_record(fields: Vec<lapi::RecordField>) -> lapi::Value {
    lapi::Value {
        sum: Some(lapi::value::Sum::Record(lapi::Record {
            record_id: None,
            fields,
        })),
    }
}

fn value_record_empty() -> lapi::Value {
    value_record(vec![])
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

fn value_map_text(values: &serde_json::Map<String, serde_json::Value>) -> Result<lapi::Value> {
    let mut entries = Vec::with_capacity(values.len());
    for (k, v) in values {
        let s = v
            .as_str()
            .ok_or_else(|| anyhow!("expected string JSON value for metadata key {k:?}"))?;
        entries.push(lapi::text_map::Entry {
            key: k.clone(),
            value: Some(lapi::Value {
                sum: Some(lapi::value::Sum::Text(s.to_string())),
            }),
        });
    }

    Ok(lapi::Value {
        sum: Some(lapi::value::Sum::TextMap(lapi::TextMap { entries })),
    })
}

fn extract_created_contract_ids(
    tx: &lapi::Transaction,
    module_name: &str,
    entity_name: &str,
) -> Vec<String> {
    let mut out = Vec::new();
    for evt in &tx.events {
        let Some(lapi::event::Event::Created(created)) = evt.event.as_ref() else {
            continue;
        };
        let Some(template_id) = created.template_id.as_ref() else {
            continue;
        };
        if template_id.module_name == module_name && template_id.entity_name == entity_name {
            out.push(created.contract_id.clone());
        }
    }
    out
}

const TREASURY_OP_TYPE_WITHDRAWAL: &str = "Withdrawal";
const TREASURY_OP_TYPE_DEPOSIT: &str = "Deposit";
const TREASURY_OP_TYPE_CONSOLIDATION: &str = "Consolidation";
const TREASURY_OP_STATE_PREPARED: &str = "Prepared";
const TREASURY_OP_STATE_SUBMITTED: &str = "Submitted";
const TREASURY_OP_STATE_COMMITTED: &str = "Committed";
const TREASURY_OP_STATE_RECONCILED: &str = "Reconciled";
const TREASURY_OP_STATE_DONE: &str = "Done";

#[derive(Debug, Clone, sqlx::FromRow)]
struct TreasuryOpHead {
    state: String,
    step_seq: i64,
    command_id: String,
    current_instruction_cid: Option<String>,
    observed_offset: Option<i64>,
    terminal_observed_offset: Option<i64>,
}

#[derive(Debug, Clone)]
struct TreasuryOpUpdate {
    op_id: String,
    op_type: String,
    owner_party: Option<String>,
    account_id: Option<String>,
    instrument_admin: String,
    instrument_id: String,
    amount_minor: Option<i64>,
    state: String,
    step_seq: i64,
    command_id: String,
    observed_offset: Option<i64>,
    observed_update_id: Option<String>,
    terminal_observed_offset: Option<i64>,
    terminal_update_id: Option<String>,
    lineage_root_instruction_cid: Option<String>,
    current_instruction_cid: Option<String>,
    input_holding_cids: Vec<String>,
    receiver_holding_cids: Vec<String>,
    extra: serde_json::Value,
    last_error: Option<String>,
}

#[derive(Debug, Clone)]
struct WithdrawalOpUpdate {
    op_id: String,
    owner_party: String,
    account_id: String,
    instrument_admin: String,
    instrument_id: String,
    amount_minor: i64,
    state: String,
    step_seq: i64,
    command_id: String,
    observed_offset: Option<i64>,
    observed_update_id: Option<String>,
    terminal_observed_offset: Option<i64>,
    terminal_update_id: Option<String>,
    lineage_root_instruction_cid: Option<String>,
    current_instruction_cid: Option<String>,
    input_holding_cids: Vec<String>,
    receiver_holding_cids: Vec<String>,
    extra: serde_json::Value,
    last_error: Option<String>,
}

impl From<&WithdrawalOpUpdate> for TreasuryOpUpdate {
    fn from(value: &WithdrawalOpUpdate) -> Self {
        Self {
            op_id: value.op_id.clone(),
            op_type: TREASURY_OP_TYPE_WITHDRAWAL.to_string(),
            owner_party: Some(value.owner_party.clone()),
            account_id: Some(value.account_id.clone()),
            instrument_admin: value.instrument_admin.clone(),
            instrument_id: value.instrument_id.clone(),
            amount_minor: Some(value.amount_minor),
            state: value.state.clone(),
            step_seq: value.step_seq,
            command_id: value.command_id.clone(),
            observed_offset: value.observed_offset,
            observed_update_id: value.observed_update_id.clone(),
            terminal_observed_offset: value.terminal_observed_offset,
            terminal_update_id: value.terminal_update_id.clone(),
            lineage_root_instruction_cid: value.lineage_root_instruction_cid.clone(),
            current_instruction_cid: value.current_instruction_cid.clone(),
            input_holding_cids: value.input_holding_cids.clone(),
            receiver_holding_cids: value.receiver_holding_cids.clone(),
            extra: value.extra.clone(),
            last_error: value.last_error.clone(),
        }
    }
}

fn canonical_withdrawal_op_id(
    owner_party: &str,
    instrument_admin: &str,
    instrument_id: &str,
    withdrawal_id: &str,
) -> String {
    format!(
        "withdrawal:{}:{}:{}:{}",
        owner_party, instrument_admin, instrument_id, withdrawal_id
    )
}

fn canonical_deposit_op_id(
    owner_party: &str,
    instrument_admin: &str,
    instrument_id: &str,
    deposit_id: &str,
) -> String {
    format!(
        "deposit:{}:{}:{}:{}",
        owner_party, instrument_admin, instrument_id, deposit_id
    )
}

fn canonical_op_command_id(op_id: &str, step_seq: i64) -> String {
    format!("{op_id}:{step_seq}")
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

fn withdrawal_command_id(op_id: &str, action: &str, step_seq: i64) -> String {
    let digest_input = format!("{op_id}:{action}:{step_seq}");
    bounded_command_id("withdrawal", action, step_seq, &digest_input)
}

fn deposit_command_id(op_id: &str, action: &str, step_seq: i64) -> String {
    let digest_input = format!("{op_id}:{action}:{step_seq}");
    bounded_command_id("deposit", action, step_seq, &digest_input)
}

fn withdrawal_claim_auto_command_id(claim_contract_id: &str) -> String {
    let digest = sha256_hex(claim_contract_id.as_bytes());
    let digest_short = digest[..32].to_string();
    let command_id = format!("wd-claim:auto:{digest_short}");
    if command_id.len() <= MAX_BOUNDED_COMMAND_ID_LEN {
        return command_id;
    }

    digest_short
}

fn next_command_step(existing: Option<&TreasuryOpHead>, fallback_step_seq: i64) -> i64 {
    let base_step = existing
        .map(|op| op.step_seq.max(fallback_step_seq))
        .unwrap_or(fallback_step_seq);
    let step = base_step.saturating_add(1);
    step.max(1)
}

fn error_string(err: &anyhow::Error) -> String {
    let mut msg = err.to_string();
    if msg.len() > 1024 {
        msg.truncate(1024);
    }
    msg
}

fn metadata_text<'a>(metadata: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    metadata
        .as_object()
        .and_then(|obj| obj.get(key))
        .and_then(serde_json::Value::as_str)
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

async fn get_treasury_op_head(state: &AppState, op_id: &str) -> Result<Option<TreasuryOpHead>> {
    let row: Option<TreasuryOpHead> = sqlx::query_as(
        r#"
        SELECT
          state,
          step_seq,
          command_id,
          current_instruction_cid,
          observed_offset,
          terminal_observed_offset
        FROM treasury_ops
        WHERE op_id = $1
        LIMIT 1
        "#,
    )
    .bind(op_id)
    .fetch_optional(&state.db)
    .await
    .context("query treasury op head")?;

    Ok(row)
}

async fn list_op_input_holding_cids(state: &AppState, op_id: &str) -> Result<Vec<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(
        r#"
        SELECT holding_cid
        FROM treasury_holding_reservations
        WHERE op_id = $1
        ORDER BY holding_cid ASC
        "#,
    )
    .bind(op_id)
    .fetch_all(&state.db)
    .await
    .context("query op holding reservations")?;

    Ok(rows.into_iter().map(|(cid,)| cid).collect())
}

async fn find_operator_accept_policy_contract_id(
    state: &AppState,
    instrument_admin: &str,
    instrument_id: &str,
    authorizer_party: &str,
    operator_party: &str,
) -> Result<Option<String>> {
    sqlx::query_scalar(
        r#"
        SELECT contract_id
        FROM operator_accept_policies
        WHERE active = TRUE
          AND instrument_admin = $1
          AND instrument_id = $2
          AND authorizer_party = $3
          AND operator_party = $4
          AND mode = $5
        ORDER BY created_at DESC, contract_id DESC
        LIMIT 1
        "#,
    )
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(authorizer_party)
    .bind(operator_party)
    .bind(OPERATOR_ACCEPT_MODE_WITHDRAWAL_AUTO_ACCEPT)
    .fetch_optional(&state.db)
    .await
    .context("query operator accept policy")
}

async fn upsert_treasury_op(state: &AppState, update: &TreasuryOpUpdate) -> Result<()> {
    let input_holding_cids =
        serde_json::to_value(&update.input_holding_cids).context("encode input holding cids")?;
    let receiver_holding_cids = serde_json::to_value(&update.receiver_holding_cids)
        .context("encode receiver holding cids")?;

    sqlx::query(
        r#"
        INSERT INTO treasury_ops (
          op_id,
          op_type,
          instrument_admin,
          instrument_id,
          owner_party,
          account_id,
          amount_minor,
          state,
          step_seq,
          command_id,
          observed_offset,
          observed_update_id,
          terminal_observed_offset,
          terminal_update_id,
          lineage_root_instruction_cid,
          current_instruction_cid,
          input_holding_cids,
          receiver_holding_cids,
          extra,
          last_error
        )
        VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
          $11,$12,$13,$14,$15,$16,$17,$18,$19,$20
        )
        ON CONFLICT (op_id) DO UPDATE
        SET
          op_type = EXCLUDED.op_type,
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          owner_party = COALESCE(EXCLUDED.owner_party, treasury_ops.owner_party),
          account_id = COALESCE(EXCLUDED.account_id, treasury_ops.account_id),
          amount_minor = COALESCE(EXCLUDED.amount_minor, treasury_ops.amount_minor),
          state = EXCLUDED.state,
          step_seq = GREATEST(treasury_ops.step_seq, EXCLUDED.step_seq),
          command_id = EXCLUDED.command_id,
          observed_offset = COALESCE(EXCLUDED.observed_offset, treasury_ops.observed_offset),
          observed_update_id = COALESCE(EXCLUDED.observed_update_id, treasury_ops.observed_update_id),
          terminal_observed_offset = COALESCE(
            EXCLUDED.terminal_observed_offset,
            treasury_ops.terminal_observed_offset
          ),
          terminal_update_id = COALESCE(EXCLUDED.terminal_update_id, treasury_ops.terminal_update_id),
          lineage_root_instruction_cid = COALESCE(
            EXCLUDED.lineage_root_instruction_cid,
            treasury_ops.lineage_root_instruction_cid
          ),
          current_instruction_cid = COALESCE(
            EXCLUDED.current_instruction_cid,
            treasury_ops.current_instruction_cid
          ),
          input_holding_cids = CASE
            WHEN EXCLUDED.input_holding_cids = '[]'::jsonb THEN treasury_ops.input_holding_cids
            ELSE EXCLUDED.input_holding_cids
          END,
          receiver_holding_cids = CASE
            WHEN EXCLUDED.receiver_holding_cids = '[]'::jsonb THEN treasury_ops.receiver_holding_cids
            ELSE EXCLUDED.receiver_holding_cids
          END,
          extra = treasury_ops.extra || EXCLUDED.extra,
          last_error = EXCLUDED.last_error,
          updated_at = now()
        "#,
    )
    .bind(&update.op_id)
    .bind(&update.op_type)
    .bind(&update.instrument_admin)
    .bind(&update.instrument_id)
    .bind(update.owner_party.as_deref())
    .bind(update.account_id.as_deref())
    .bind(update.amount_minor)
    .bind(&update.state)
    .bind(update.step_seq)
    .bind(&update.command_id)
    .bind(update.observed_offset)
    .bind(update.observed_update_id.as_deref())
    .bind(update.terminal_observed_offset)
    .bind(update.terminal_update_id.as_deref())
    .bind(update.lineage_root_instruction_cid.as_deref())
    .bind(update.current_instruction_cid.as_deref())
    .bind(input_holding_cids)
    .bind(receiver_holding_cids)
    .bind(&update.extra)
    .bind(update.last_error.as_deref())
    .execute(&state.db)
    .await
    .context("upsert treasury op")?;

    Ok(())
}

async fn upsert_withdrawal_treasury_op(
    state: &AppState,
    update: &WithdrawalOpUpdate,
) -> Result<()> {
    let generic = TreasuryOpUpdate::from(update);
    upsert_treasury_op(state, &generic)
        .await
        .context("upsert withdrawal treasury op")?;

    Ok(())
}

async fn insert_treasury_op_event(
    state: &AppState,
    op_id: &str,
    op_state: &str,
    detail: serde_json::Value,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO treasury_op_events (op_id, state, detail)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(op_id)
    .bind(op_state)
    .bind(detail)
    .execute(&state.db)
    .await
    .context("insert treasury op event")?;

    Ok(())
}

async fn submit_and_wait_for_transaction(
    cfg: &Config,
    command_id: String,
    act_as: Vec<String>,
    commands: Vec<lapi::Command>,
) -> Result<lapi::Transaction> {
    let commands = lapi::Commands {
        workflow_id: String::new(),
        user_id: cfg.ledger_user_id.clone(),
        command_id,
        commands,
        deduplication_period: None,
        min_ledger_time_abs: None,
        min_ledger_time_rel: None,
        act_as,
        read_as: vec![],
        submission_id: uuid::Uuid::new_v4().to_string(),
        disclosed_contracts: vec![],
        synchronizer_id: String::new(),
        package_id_selection_preference: vec![],
        prefetch_contract_keys: vec![],
    };

    let mut client =
        lapi::command_service_client::CommandServiceClient::new(cfg.ledger_channel.clone());

    let mut req = Request::new(lapi::SubmitAndWaitForTransactionRequest {
        commands: Some(commands),
        transaction_format: None,
    });
    maybe_apply_auth(&cfg.auth_header, &mut req);

    let resp = client
        .submit_and_wait_for_transaction(req)
        .await
        .context("submit_and_wait_for_transaction")?
        .into_inner();

    let tx = resp
        .transaction
        .ok_or_else(|| anyhow!("missing transaction in response"))?;

    Ok(tx)
}

async fn mock_registry_loop(state: AppState) -> Result<()> {
    // Dev-only worker that advances any internal-workflow pending instructions.
    // In a real CIP-56 integration, this is replaced by a registry/validator component.
    let poll_ms = std::env::var("PEBBLE_TREASURY_MOCK_REGISTRY_POLL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1000);

    loop {
        if let Err(err) = mock_registry_tick(&state).await {
            tracing::warn!(error = ?err, "mock registry tick failed");
        }
        tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
    }
}

async fn mock_registry_tick(state: &AppState) -> Result<()> {
    #[derive(sqlx::FromRow)]
    struct PendingInstructionRow {
        contract_id: String,
    }

    // Only advance the internal-workflow status class.
    let pending: Vec<PendingInstructionRow> = sqlx::query_as(
        r#"
        SELECT contract_id
        FROM token_transfer_instructions
        WHERE active = TRUE
          AND output = 'Pending'
          AND status_class = 'TransferPendingInternalWorkflow'
        ORDER BY created_at ASC
        LIMIT 25
        "#,
    )
    .fetch_all(&state.db)
    .await
    .context("query pending internal-workflow instructions")?;

    if pending.is_empty() {
        return Ok(());
    }

    let template_id = lapi::Identifier {
        package_id: "#wizardcat-token-standard".to_string(),
        module_name: "Wizardcat.Token.Standard".to_string(),
        entity_name: "TransferInstruction".to_string(),
    };

    for row in pending {
        let cmd = lapi::Command {
            command: Some(lapi::command::Command::Exercise(lapi::ExerciseCommand {
                template_id: Some(template_id.clone()),
                contract_id: row.contract_id.clone(),
                choice: "Advance".to_string(),
                choice_argument: Some(value_record_empty()),
            })),
        };

        let command_id = format!("mock-registry:advance:{}", row.contract_id);
        let act_as = vec![state.cfg.instrument_admin_party.clone()];

        // Best-effort: if another worker already advanced it, submission will fail due to contract not active.
        // Treat that as non-fatal.
        match submit_and_wait_for_transaction(&state.cfg, command_id, act_as, vec![cmd]).await {
            Ok(_tx) => {}
            Err(err) => {
                tracing::debug!(error = ?err, contract_id = %row.contract_id, "mock registry advance failed");
            }
        }
    }

    Ok(())
}

async fn holding_claim_loop(state: AppState) -> Result<()> {
    let poll_ms = std::env::var("PEBBLE_TREASURY_CLAIM_POLL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1000);

    loop {
        if let Err(err) = holding_claim_tick(&state).await {
            tracing::warn!(error = ?err, "holding claim tick failed");
        }
        tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
    }
}

async fn holding_claim_tick(state: &AppState) -> Result<()> {
    #[derive(sqlx::FromRow)]
    struct ClaimRow {
        contract_id: String,
    }

    let claims: Vec<ClaimRow> = sqlx::query_as(
        r#"
        SELECT contract_id
        FROM token_holding_claims
        WHERE active = TRUE
          AND owner_party = $1
        ORDER BY created_at ASC
        LIMIT 50
        "#,
    )
    .bind(&state.cfg.committee_party)
    .fetch_all(&state.db)
    .await
    .context("query holding claims")?;

    if claims.is_empty() {
        return Ok(());
    }

    let template_id = lapi::Identifier {
        package_id: "#wizardcat-token-standard".to_string(),
        module_name: "Wizardcat.Token.Standard".to_string(),
        entity_name: "HoldingClaim".to_string(),
    };

    for row in claims {
        let cmd = lapi::Command {
            command: Some(lapi::command::Command::Exercise(lapi::ExerciseCommand {
                template_id: Some(template_id.clone()),
                contract_id: row.contract_id.clone(),
                choice: "Claim".to_string(),
                choice_argument: Some(value_record_empty()),
            })),
        };

        let command_id = format!("holding-claim:claim:{}", row.contract_id);
        let act_as = vec![state.cfg.committee_party.clone()];

        match submit_and_wait_for_transaction(&state.cfg, command_id, act_as, vec![cmd]).await {
            Ok(_tx) => {}
            Err(err) => {
                tracing::debug!(error = ?err, contract_id = %row.contract_id, "holding claim failed");
            }
        }
    }

    Ok(())
}

async fn withdrawal_claim_auto_loop(state: AppState) -> Result<()> {
    loop {
        if let Err(err) = withdrawal_claim_auto_tick(&state).await {
            tracing::warn!(error = ?err, "withdrawal claim auto tick failed");
        }
        tokio::time::sleep(std::time::Duration::from_millis(
            state.cfg.withdrawal_one_shot_auto_claim_poll_ms,
        ))
        .await;
    }
}

async fn withdrawal_claim_auto_tick(state: &AppState) -> Result<()> {
    #[derive(sqlx::FromRow)]
    struct ClaimRow {
        claim_contract_id: String,
        owner_party: String,
        account_id: String,
        withdrawal_id: String,
        origin_instruction_cid: String,
    }

    let mut dbtx = state
        .db
        .begin()
        .await
        .context("begin withdrawal claim auto tick tx")?;

    let claims: Vec<ClaimRow> = sqlx::query_as(
        r#"
        SELECT
          h.contract_id AS claim_contract_id,
          h.owner_party AS owner_party,
          (
            SELECT wr.account_id
            FROM withdrawal_receipts wr
            WHERE wr.active = TRUE
              AND wr.status = $2
              AND wr.lineage_root_instruction_cid = h.origin_instruction_cid
              AND wr.owner_party = h.owner_party
              AND wr.instrument_admin = h.instrument_admin
              AND wr.instrument_id = h.instrument_id
            ORDER BY wr.created_at DESC, wr.contract_id DESC
            LIMIT 1
          ) AS account_id,
          (
            SELECT wr.withdrawal_id
            FROM withdrawal_receipts wr
            WHERE wr.active = TRUE
              AND wr.status = $2
              AND wr.lineage_root_instruction_cid = h.origin_instruction_cid
              AND wr.owner_party = h.owner_party
              AND wr.instrument_admin = h.instrument_admin
              AND wr.instrument_id = h.instrument_id
            ORDER BY wr.created_at DESC, wr.contract_id DESC
            LIMIT 1
          ) AS withdrawal_id,
          h.origin_instruction_cid AS origin_instruction_cid
        FROM token_holding_claims h
        WHERE h.active = TRUE
          AND h.origin_instruction_cid IS NOT NULL
          AND EXISTS (
            SELECT 1
            FROM withdrawal_receipts wr
            WHERE wr.active = TRUE
              AND wr.status = $2
              AND wr.lineage_root_instruction_cid = h.origin_instruction_cid
              AND wr.owner_party = h.owner_party
              AND wr.instrument_admin = h.instrument_admin
              AND wr.instrument_id = h.instrument_id
          )
        ORDER BY h.created_at ASC, h.contract_id ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(state.cfg.withdrawal_one_shot_auto_claim_max_per_tick)
    .bind(WITHDRAWAL_RECEIPT_STATUS_COMPLETED)
    .fetch_all(&mut *dbtx)
    .await
    .context("query withdrawal-origin holding claims")?;

    if claims.is_empty() {
        dbtx.commit()
            .await
            .context("commit empty auto-claim tick tx")?;
        return Ok(());
    }

    let template_id = lapi::Identifier {
        package_id: "#wizardcat-token-standard".to_string(),
        module_name: "Wizardcat.Token.Standard".to_string(),
        entity_name: "HoldingClaim".to_string(),
    };

    for row in claims {
        let command_id = withdrawal_claim_auto_command_id(&row.claim_contract_id);
        let cmd = lapi::Command {
            command: Some(lapi::command::Command::Exercise(lapi::ExerciseCommand {
                template_id: Some(template_id.clone()),
                contract_id: row.claim_contract_id.clone(),
                choice: "Claim".to_string(),
                choice_argument: Some(value_record_empty()),
            })),
        };

        let act_as = vec![row.owner_party.clone()];
        match submit_and_wait_for_transaction(&state.cfg, command_id.clone(), act_as, vec![cmd])
            .await
        {
            Ok(_tx) => {
                tracing::info!(
                    claim_contract_id = %row.claim_contract_id,
                    owner_party = %row.owner_party,
                    account_id = %row.account_id,
                    withdrawal_id = %row.withdrawal_id,
                    origin_instruction_cid = %row.origin_instruction_cid,
                    command_id = %command_id,
                    result_class = "claimed",
                    "withdrawal claim auto-claimed"
                );
            }
            Err(err) => {
                if is_idempotent_contract_advance_error(&err) {
                    tracing::info!(
                        claim_contract_id = %row.claim_contract_id,
                        owner_party = %row.owner_party,
                        account_id = %row.account_id,
                        withdrawal_id = %row.withdrawal_id,
                        origin_instruction_cid = %row.origin_instruction_cid,
                        command_id = %command_id,
                        result_class = "already_claimed",
                        error = ?err,
                        "withdrawal claim auto-claim treated as already advanced"
                    );
                    continue;
                }

                if is_permission_denied_submission_error(&err) {
                    tracing::warn!(
                        claim_contract_id = %row.claim_contract_id,
                        owner_party = %row.owner_party,
                        account_id = %row.account_id,
                        withdrawal_id = %row.withdrawal_id,
                        origin_instruction_cid = %row.origin_instruction_cid,
                        command_id = %command_id,
                        result_class = "permission_denied",
                        retryable = false,
                        error = ?err,
                        "withdrawal claim auto-claim permission denied"
                    );
                    continue;
                }

                if is_unknown_submission_error(&err) {
                    tracing::warn!(
                        claim_contract_id = %row.claim_contract_id,
                        owner_party = %row.owner_party,
                        account_id = %row.account_id,
                        withdrawal_id = %row.withdrawal_id,
                        origin_instruction_cid = %row.origin_instruction_cid,
                        command_id = %command_id,
                        result_class = "submission_unknown",
                        retryable = true,
                        error = ?err,
                        "withdrawal claim auto-claim submission outcome unknown"
                    );
                    continue;
                }

                tracing::warn!(
                    claim_contract_id = %row.claim_contract_id,
                    owner_party = %row.owner_party,
                    account_id = %row.account_id,
                    withdrawal_id = %row.withdrawal_id,
                    origin_instruction_cid = %row.origin_instruction_cid,
                    command_id = %command_id,
                    result_class = "failed",
                    retryable = true,
                    error = ?err,
                    "withdrawal claim auto-claim failed"
                );
            }
        }
    }

    dbtx.commit()
        .await
        .context("commit withdrawal claim auto tick tx")?;
    Ok(())
}

async fn deposit_accept_loop(state: AppState) -> Result<()> {
    let poll_ms = std::env::var("PEBBLE_TREASURY_DEPOSIT_ACCEPT_POLL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1000);

    loop {
        if let Err(err) = deposit_accept_tick(&state).await {
            tracing::warn!(error = ?err, "deposit accept tick failed");
        }
        tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
    }
}

async fn deposit_accept_tick(state: &AppState) -> Result<()> {
    #[derive(sqlx::FromRow)]
    struct OfferRow {
        contract_id: String,
        instrument_admin: String,
        instrument_id: String,
        sender_party: String,
        amount_minor: i64,
        metadata: serde_json::Value,
    }

    let offers: Vec<OfferRow> = sqlx::query_as(
        r#"
        SELECT
          contract_id,
          instrument_admin,
          instrument_id,
          sender_party,
          amount_minor,
          metadata
        FROM token_transfer_instructions
        WHERE active = TRUE
          AND output = 'Pending'
          AND status_class = 'TransferPendingReceiverAcceptance'
          AND receiver_party = $1
        ORDER BY created_at ASC
        LIMIT 25
        "#,
    )
    .bind(&state.cfg.committee_party)
    .fetch_all(&state.db)
    .await
    .context("query deposit offers")?;

    if offers.is_empty() {
        return Ok(());
    }

    for offer in offers {
        let meta = match offer.metadata.as_object() {
            Some(m) => m,
            None => {
                tracing::warn!(
                    contract_id = %offer.contract_id,
                    "skipping offer with non-object metadata"
                );
                continue;
            }
        };

        let account_id = meta
            .get("wizardcat.xyz/pebble.accountId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let deposit_id = meta
            .get("wizardcat.xyz/pebble.depositId")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let account_id = match parse_account_id(account_id) {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(
                    contract_id = %offer.contract_id,
                    error = %err,
                    "skipping offer with invalid accountId metadata"
                );
                continue;
            }
        };
        let deposit_id = match parse_deposit_id(deposit_id) {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(
                    contract_id = %offer.contract_id,
                    error = %err,
                    "skipping offer with invalid depositId metadata"
                );
                continue;
            }
        };

        let op_id = canonical_deposit_op_id(
            &offer.sender_party,
            &offer.instrument_admin,
            &offer.instrument_id,
            &deposit_id,
        );
        let op_head = get_treasury_op_head(state, &op_id).await?;
        if let Some(existing_op) = op_head.as_ref() {
            if matches!(
                existing_op.state.as_str(),
                TREASURY_OP_STATE_SUBMITTED
                    | TREASURY_OP_STATE_COMMITTED
                    | TREASURY_OP_STATE_RECONCILED
                    | TREASURY_OP_STATE_DONE
            ) {
                continue;
            }
        }

        let already: Option<(String,)> = sqlx::query_as(
            r#"
            SELECT contract_id
            FROM deposit_receipts
            WHERE owner_party = $1
              AND instrument_admin = $2
              AND instrument_id = $3
              AND deposit_id = $4
              AND active = TRUE
            LIMIT 1
            "#,
        )
        .bind(&offer.sender_party)
        .bind(&offer.instrument_admin)
        .bind(&offer.instrument_id)
        .bind(&deposit_id)
        .fetch_optional(&state.db)
        .await
        .context("query existing deposit receipt")?;
        if already.is_some() {
            continue;
        }

        let pending: Option<(String,)> = sqlx::query_as(
            r#"
            SELECT contract_id
            FROM deposit_pendings
            WHERE owner_party = $1
              AND instrument_admin = $2
              AND instrument_id = $3
              AND deposit_id = $4
              AND active = TRUE
            LIMIT 1
            "#,
        )
        .bind(&offer.sender_party)
        .bind(&offer.instrument_admin)
        .bind(&offer.instrument_id)
        .bind(&deposit_id)
        .fetch_optional(&state.db)
        .await
        .context("query existing deposit pending")?;
        if pending.is_some() {
            continue;
        }

        #[derive(sqlx::FromRow)]
        struct AccountRow {
            owner_party: String,
            instrument_admin: String,
            instrument_id: String,
        }

        let acct: Option<AccountRow> = sqlx::query_as(
            r#"
            SELECT ar.owner_party, ar.instrument_admin, ar.instrument_id
            FROM account_ref_latest l
            JOIN account_refs ar ON ar.contract_id = l.contract_id
            WHERE l.account_id = $1
              AND ar.active = TRUE
            LIMIT 1
            "#,
        )
        .bind(&account_id)
        .fetch_optional(&state.db)
        .await
        .context("query account ref")?;

        let Some(acct) = acct else {
            tracing::warn!(account_id = %account_id, "skipping offer for unknown account");
            continue;
        };

        if acct.owner_party != offer.sender_party {
            tracing::warn!(
                account_id = %account_id,
                sender = %offer.sender_party,
                account_owner = %acct.owner_party,
                "skipping offer from non-owner"
            );
            continue;
        }
        if acct.instrument_admin != offer.instrument_admin
            || acct.instrument_id != offer.instrument_id
        {
            tracing::warn!(
                account_id = %account_id,
                "skipping offer with instrument mismatch vs account"
            );
            continue;
        }

        let token_config_cid: Option<(String,)> = sqlx::query_as(
            r#"
            SELECT contract_id
            FROM token_configs
            WHERE instrument_admin = $1
              AND instrument_id = $2
              AND active = TRUE
            ORDER BY last_offset DESC
            LIMIT 1
            "#,
        )
        .bind(&offer.instrument_admin)
        .bind(&offer.instrument_id)
        .fetch_optional(&state.db)
        .await
        .context("query token config")?;
        let Some((token_config_cid,)) = token_config_cid else {
            tracing::warn!(
                instrument_admin = %offer.instrument_admin,
                instrument_id = %offer.instrument_id,
                "skipping offer (no TokenConfig)"
            );
            continue;
        };

        let account_state_cid: Option<(String,)> = sqlx::query_as(
            r#"
            SELECT contract_id
            FROM account_state_latest
            WHERE account_id = $1
            LIMIT 1
            "#,
        )
        .bind(&account_id)
        .fetch_optional(&state.db)
        .await
        .context("query latest account state")?;
        let Some((account_state_cid,)) = account_state_cid else {
            tracing::warn!(account_id = %account_id, "skipping offer (no AccountState)");
            continue;
        };

        let deposit_request_template_id = lapi::Identifier {
            package_id: "#pebble".to_string(),
            module_name: "Pebble.Treasury".to_string(),
            entity_name: "DepositRequest".to_string(),
        };

        let create_args = lapi::Record {
            record_id: None,
            fields: vec![
                record_field("committee", value_party(&state.cfg.committee_party)),
                record_field("owner", value_party(&offer.sender_party)),
                record_field("accountId", value_text(&account_id)),
                record_field("instrumentAdmin", value_party(&offer.instrument_admin)),
                record_field("instrumentId", value_text(&offer.instrument_id)),
                record_field("depositId", value_text(&deposit_id)),
                record_field("tokenConfigCid", value_contract_id(&token_config_cid)),
                record_field("accountStateCid", value_contract_id(&account_state_cid)),
                record_field("offerInstructionCid", value_contract_id(&offer.contract_id)),
            ],
        };

        let cmd = lapi::Command {
            command: Some(lapi::command::Command::CreateAndExercise(
                lapi::CreateAndExerciseCommand {
                    template_id: Some(deposit_request_template_id),
                    create_arguments: Some(create_args),
                    choice: "AcceptOffer".to_string(),
                    choice_argument: Some(value_record_empty()),
                },
            )),
        };

        let next_step_seq = next_command_step(op_head.as_ref(), 0);
        let command_id = deposit_command_id(&op_id, "accept-offer", next_step_seq);
        let prepared_step_seq = next_step_seq.saturating_sub(1);

        let prepared_update = TreasuryOpUpdate {
            op_id: op_id.clone(),
            op_type: TREASURY_OP_TYPE_DEPOSIT.to_string(),
            owner_party: Some(offer.sender_party.clone()),
            account_id: Some(account_id.clone()),
            instrument_admin: offer.instrument_admin.clone(),
            instrument_id: offer.instrument_id.clone(),
            amount_minor: Some(offer.amount_minor),
            state: TREASURY_OP_STATE_PREPARED.to_string(),
            step_seq: prepared_step_seq,
            command_id: command_id.clone(),
            observed_offset: None,
            observed_update_id: None,
            terminal_observed_offset: None,
            terminal_update_id: None,
            lineage_root_instruction_cid: Some(offer.contract_id.clone()),
            current_instruction_cid: None,
            input_holding_cids: vec![],
            receiver_holding_cids: vec![],
            extra: serde_json::json!({
                "phase": "accept",
                "offer_instruction_cid": offer.contract_id,
            }),
            last_error: None,
        };
        upsert_treasury_op(state, &prepared_update).await?;
        insert_treasury_op_event(
            state,
            &op_id,
            TREASURY_OP_STATE_PREPARED,
            serde_json::json!({
                "step_seq": prepared_step_seq,
                "command_id": command_id,
                "action": "AcceptOffer",
                "offer_instruction_cid": offer.contract_id,
            }),
        )
        .await?;

        let submitted_update = TreasuryOpUpdate {
            op_id: op_id.clone(),
            op_type: TREASURY_OP_TYPE_DEPOSIT.to_string(),
            owner_party: Some(offer.sender_party.clone()),
            account_id: Some(account_id.clone()),
            instrument_admin: offer.instrument_admin.clone(),
            instrument_id: offer.instrument_id.clone(),
            amount_minor: Some(offer.amount_minor),
            state: TREASURY_OP_STATE_SUBMITTED.to_string(),
            step_seq: next_step_seq,
            command_id: command_id.clone(),
            observed_offset: None,
            observed_update_id: None,
            terminal_observed_offset: None,
            terminal_update_id: None,
            lineage_root_instruction_cid: Some(offer.contract_id.clone()),
            current_instruction_cid: None,
            input_holding_cids: vec![],
            receiver_holding_cids: vec![],
            extra: serde_json::json!({
                "phase": "accept",
                "offer_instruction_cid": offer.contract_id,
            }),
            last_error: None,
        };
        upsert_treasury_op(state, &submitted_update).await?;
        insert_treasury_op_event(
            state,
            &op_id,
            TREASURY_OP_STATE_SUBMITTED,
            serde_json::json!({
                "step_seq": next_step_seq,
                "command_id": command_id,
                "action": "AcceptOffer",
                "offer_instruction_cid": offer.contract_id,
            }),
        )
        .await?;

        let act_as = vec![state.cfg.committee_party.clone()];

        let tx = match submit_and_wait_for_transaction(
            &state.cfg,
            command_id.clone(),
            act_as,
            vec![cmd],
        )
        .await
        {
            Ok(tx) => tx,
            Err(err) => {
                let err_msg = error_string(&err);
                let mut failed = submitted_update.clone();
                failed.last_error = Some(err_msg.clone());
                upsert_treasury_op(state, &failed).await?;
                insert_treasury_op_event(
                    state,
                    &op_id,
                    TREASURY_OP_STATE_SUBMITTED,
                    serde_json::json!({
                        "step_seq": next_step_seq,
                        "command_id": command_id,
                        "action": "AcceptOffer",
                        "error": err_msg,
                    }),
                )
                .await?;
                tracing::warn!(
                    error = ?err,
                    op_id = %op_id,
                    command_id = %command_id,
                    contract_id = %offer.contract_id,
                    "deposit accept failed"
                );
                continue;
            }
        };

        let pendings = extract_created_contract_ids(&tx, "Pebble.Treasury", "DepositPending");
        let receipts = extract_created_contract_ids(&tx, "Pebble.Treasury", "DepositReceipt");
        let committed_update = TreasuryOpUpdate {
            op_id: op_id.clone(),
            op_type: TREASURY_OP_TYPE_DEPOSIT.to_string(),
            owner_party: Some(offer.sender_party.clone()),
            account_id: Some(account_id.clone()),
            instrument_admin: offer.instrument_admin.clone(),
            instrument_id: offer.instrument_id.clone(),
            amount_minor: Some(offer.amount_minor),
            state: TREASURY_OP_STATE_COMMITTED.to_string(),
            step_seq: next_step_seq,
            command_id: command_id.clone(),
            observed_offset: Some(tx.offset),
            observed_update_id: Some(tx.update_id.clone()),
            terminal_observed_offset: None,
            terminal_update_id: None,
            lineage_root_instruction_cid: Some(offer.contract_id.clone()),
            current_instruction_cid: None,
            input_holding_cids: vec![],
            receiver_holding_cids: vec![],
            extra: serde_json::json!({
                "phase": "accept",
                "offer_instruction_cid": offer.contract_id,
                "deposit_pending_count": pendings.len(),
                "deposit_receipt_count": receipts.len(),
            }),
            last_error: None,
        };
        upsert_treasury_op(state, &committed_update).await?;
        insert_treasury_op_event(
            state,
            &op_id,
            TREASURY_OP_STATE_COMMITTED,
            serde_json::json!({
                "step_seq": next_step_seq,
                "command_id": command_id,
                "action": "AcceptOffer",
                "offset": tx.offset,
                "update_id": tx.update_id,
                "deposit_pending_count": pendings.len(),
                "deposit_receipt_count": receipts.len(),
            }),
        )
        .await?;
        tracing::info!(
            op_id = %op_id,
            command_id = %command_id,
            account_id = %account_id,
            deposit_id = %deposit_id,
            offer_instruction_cid = %offer.contract_id,
            amount_minor = offer.amount_minor,
            deposit_pending_count = pendings.len(),
            deposit_receipt_count = receipts.len(),
            offset = tx.offset,
            update_id = %tx.update_id,
            "deposit accepted"
        );
    }

    Ok(())
}

async fn deposit_reconcile_loop(state: AppState) -> Result<()> {
    let poll_ms = std::env::var("PEBBLE_TREASURY_DEPOSIT_RECONCILE_POLL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1000);

    loop {
        if let Err(err) = deposit_reconcile_tick(&state).await {
            tracing::warn!(error = ?err, "deposit reconcile tick failed");
        }
        tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
    }
}

async fn deposit_reconcile_tick(state: &AppState) -> Result<()> {
    #[derive(sqlx::FromRow)]
    struct PendingRow {
        deposit_pending_cid: String,
        owner_party: String,
        account_id: String,
        instrument_admin: String,
        instrument_id: String,
        deposit_id: String,
        expected_amount_minor: i64,
        lineage_root_instruction_cid: String,
        current_instruction_cid: String,
        step_seq: i64,
        pending_offset: i64,
        latest_instruction_cid: String,
        latest_instruction_offset: i64,
        latest_instruction_update_id: Option<String>,
        latest_output: String,
    }

    let pendings: Vec<PendingRow> = sqlx::query_as(
        r#"
        SELECT
          p.contract_id AS deposit_pending_cid,
          p.owner_party,
          p.account_id,
          p.instrument_admin,
          p.instrument_id,
          p.deposit_id,
          p.expected_amount_minor,
          p.lineage_root_instruction_cid,
          p.current_instruction_cid,
          p.step_seq,
          p.last_offset AS pending_offset,
          l.contract_id AS latest_instruction_cid,
          ti.last_offset AS latest_instruction_offset,
          lu.update_id AS latest_instruction_update_id,
          ti.output AS latest_output
        FROM deposit_pendings p
        JOIN token_transfer_instruction_latest l
          ON l.lineage_root_instruction_cid = p.lineage_root_instruction_cid
        JOIN token_transfer_instructions ti
          ON ti.contract_id = l.contract_id
        LEFT JOIN ledger_updates lu
          ON lu.ledger_offset = ti.last_offset
        WHERE p.active = TRUE
          AND p.committee_party = $1
          AND ti.active = TRUE
        ORDER BY p.created_at ASC
        LIMIT 50
        "#,
    )
    .bind(&state.cfg.committee_party)
    .fetch_all(&state.db)
    .await
    .context("query deposit pendings")?;

    let template_id = lapi::Identifier {
        package_id: "#pebble".to_string(),
        module_name: "Pebble.Treasury".to_string(),
        entity_name: "DepositPending".to_string(),
    };

    for p in pendings {
        let op_id = canonical_deposit_op_id(
            &p.owner_party,
            &p.instrument_admin,
            &p.instrument_id,
            &p.deposit_id,
        );
        let op_head = get_treasury_op_head(state, &op_id).await?;
        let reconcile_step_seq = op_head
            .as_ref()
            .map(|op| op.step_seq.max(p.step_seq))
            .unwrap_or(p.step_seq);
        let reconcile_command_id = op_head
            .as_ref()
            .map(|op| op.command_id.clone())
            .unwrap_or_else(|| {
                deposit_command_id(&op_id, "reconcile-observe", reconcile_step_seq.max(1))
            });
        let reconciled_update = TreasuryOpUpdate {
            op_id: op_id.clone(),
            op_type: TREASURY_OP_TYPE_DEPOSIT.to_string(),
            owner_party: Some(p.owner_party.clone()),
            account_id: Some(p.account_id.clone()),
            instrument_admin: p.instrument_admin.clone(),
            instrument_id: p.instrument_id.clone(),
            amount_minor: Some(p.expected_amount_minor),
            state: TREASURY_OP_STATE_RECONCILED.to_string(),
            step_seq: reconcile_step_seq,
            command_id: reconcile_command_id.clone(),
            observed_offset: Some(p.latest_instruction_offset),
            observed_update_id: p.latest_instruction_update_id.clone(),
            terminal_observed_offset: None,
            terminal_update_id: None,
            lineage_root_instruction_cid: Some(p.lineage_root_instruction_cid.clone()),
            current_instruction_cid: Some(p.latest_instruction_cid.clone()),
            input_holding_cids: vec![],
            receiver_holding_cids: vec![],
            extra: serde_json::json!({
                "phase": "reconcile",
                "deposit_pending_cid": p.deposit_pending_cid,
                "pending_offset": p.pending_offset,
                "latest_instruction_output": p.latest_output,
            }),
            last_error: None,
        };
        upsert_treasury_op(state, &reconciled_update).await?;
        let should_emit_reconciled = match op_head.as_ref() {
            Some(op) => {
                op.state != TREASURY_OP_STATE_RECONCILED
                    || op.current_instruction_cid.as_deref()
                        != Some(p.latest_instruction_cid.as_str())
                    || op.observed_offset != Some(p.latest_instruction_offset)
            }
            None => true,
        };
        if should_emit_reconciled {
            insert_treasury_op_event(
                state,
                &op_id,
                TREASURY_OP_STATE_RECONCILED,
                serde_json::json!({
                    "step_seq": reconcile_step_seq,
                    "command_id": reconcile_command_id,
                    "deposit_pending_cid": p.deposit_pending_cid,
                    "latest_instruction_cid": p.latest_instruction_cid,
                    "latest_output": p.latest_output,
                    "offset": p.latest_instruction_offset,
                    "update_id": p.latest_instruction_update_id,
                }),
            )
            .await?;
        }

        let latest_output = p.latest_output.as_str();
        match latest_output {
            "Pending" => {
                if p.latest_instruction_cid == p.current_instruction_cid {
                    continue;
                }

                let args = value_record(vec![record_field(
                    "nextInstructionCid",
                    value_contract_id(&p.latest_instruction_cid),
                )]);

                let cmd = lapi::Command {
                    command: Some(lapi::command::Command::Exercise(lapi::ExerciseCommand {
                        template_id: Some(template_id.clone()),
                        contract_id: p.deposit_pending_cid.clone(),
                        choice: "AdvanceDepositPending".to_string(),
                        choice_argument: Some(args),
                    })),
                };

                let next_step_seq = next_command_step(op_head.as_ref(), reconcile_step_seq);
                let command_id = deposit_command_id(&op_id, "advance-pending", next_step_seq);
                let submitted_update = TreasuryOpUpdate {
                    op_id: op_id.clone(),
                    op_type: TREASURY_OP_TYPE_DEPOSIT.to_string(),
                    owner_party: Some(p.owner_party.clone()),
                    account_id: Some(p.account_id.clone()),
                    instrument_admin: p.instrument_admin.clone(),
                    instrument_id: p.instrument_id.clone(),
                    amount_minor: Some(p.expected_amount_minor),
                    state: TREASURY_OP_STATE_SUBMITTED.to_string(),
                    step_seq: next_step_seq,
                    command_id: command_id.clone(),
                    observed_offset: Some(p.latest_instruction_offset),
                    observed_update_id: p.latest_instruction_update_id.clone(),
                    terminal_observed_offset: None,
                    terminal_update_id: None,
                    lineage_root_instruction_cid: Some(p.lineage_root_instruction_cid.clone()),
                    current_instruction_cid: Some(p.latest_instruction_cid.clone()),
                    input_holding_cids: vec![],
                    receiver_holding_cids: vec![],
                    extra: serde_json::json!({
                        "phase": "reconcile",
                        "action": "AdvanceDepositPending",
                        "deposit_pending_cid": p.deposit_pending_cid,
                    }),
                    last_error: None,
                };
                upsert_treasury_op(state, &submitted_update).await?;
                insert_treasury_op_event(
                    state,
                    &op_id,
                    TREASURY_OP_STATE_SUBMITTED,
                    serde_json::json!({
                        "step_seq": next_step_seq,
                        "command_id": command_id,
                        "action": "AdvanceDepositPending",
                        "deposit_pending_cid": p.deposit_pending_cid,
                        "next_instruction_cid": p.latest_instruction_cid,
                    }),
                )
                .await?;

                let act_as = vec![state.cfg.committee_party.clone()];
                match submit_and_wait_for_transaction(
                    &state.cfg,
                    command_id.clone(),
                    act_as,
                    vec![cmd],
                )
                .await
                {
                    Ok(tx) => {
                        let committed_update = TreasuryOpUpdate {
                            op_id: op_id.clone(),
                            op_type: TREASURY_OP_TYPE_DEPOSIT.to_string(),
                            owner_party: Some(p.owner_party.clone()),
                            account_id: Some(p.account_id.clone()),
                            instrument_admin: p.instrument_admin.clone(),
                            instrument_id: p.instrument_id.clone(),
                            amount_minor: Some(p.expected_amount_minor),
                            state: TREASURY_OP_STATE_COMMITTED.to_string(),
                            step_seq: next_step_seq,
                            command_id: command_id.clone(),
                            observed_offset: Some(tx.offset),
                            observed_update_id: Some(tx.update_id.clone()),
                            terminal_observed_offset: None,
                            terminal_update_id: None,
                            lineage_root_instruction_cid: Some(
                                p.lineage_root_instruction_cid.clone(),
                            ),
                            current_instruction_cid: Some(p.latest_instruction_cid.clone()),
                            input_holding_cids: vec![],
                            receiver_holding_cids: vec![],
                            extra: serde_json::json!({
                                "phase": "reconcile",
                                "action": "AdvanceDepositPending",
                                "deposit_pending_cid": p.deposit_pending_cid,
                            }),
                            last_error: None,
                        };
                        upsert_treasury_op(state, &committed_update).await?;
                        insert_treasury_op_event(
                            state,
                            &op_id,
                            TREASURY_OP_STATE_COMMITTED,
                            serde_json::json!({
                                "step_seq": next_step_seq,
                                "command_id": command_id,
                                "action": "AdvanceDepositPending",
                                "offset": tx.offset,
                                "update_id": tx.update_id,
                            }),
                        )
                        .await?;
                        tracing::info!(
                            op_id = %op_id,
                            command_id = %command_id,
                            account_id = %p.account_id,
                            deposit_id = %p.deposit_id,
                            offset = tx.offset,
                            "deposit pending advanced"
                        );
                    }
                    Err(err) => {
                        let err_msg = error_string(&err);
                        let mut failed = submitted_update.clone();
                        failed.last_error = Some(err_msg.clone());
                        upsert_treasury_op(state, &failed).await?;
                        insert_treasury_op_event(
                            state,
                            &op_id,
                            TREASURY_OP_STATE_SUBMITTED,
                            serde_json::json!({
                                "step_seq": next_step_seq,
                                "command_id": command_id,
                                "action": "AdvanceDepositPending",
                                "error": err_msg,
                            }),
                        )
                        .await?;
                        tracing::warn!(
                            error = ?err,
                            op_id = %op_id,
                            command_id = %command_id,
                            deposit_pending_cid = %p.deposit_pending_cid,
                            "deposit pending advance failed"
                        );
                    }
                }
            }
            "Completed" => {
                // Need the latest AccountState CID and the set of receiver holdings attributed to the deposit root.
                let account_state_cid: Option<(String,)> = sqlx::query_as(
                    r#"
                    SELECT contract_id
                    FROM account_state_latest
                    WHERE account_id = $1
                    LIMIT 1
                    "#,
                )
                .bind(&p.account_id)
                .fetch_optional(&state.db)
                .await
                .context("query latest account state")?;
                let Some((account_state_cid,)) = account_state_cid else {
                    tracing::warn!(account_id = %p.account_id, "deposit finalize skipped (no AccountState)");
                    continue;
                };

                let receiver_holdings: Vec<(String,)> = sqlx::query_as(
                    r#"
                    SELECT contract_id
                    FROM token_holdings
                    WHERE active = TRUE
                      AND owner_party = $1
                      AND instrument_admin = $2
                      AND instrument_id = $3
                      AND origin_instruction_cid = $4
                      AND lock_status_class IS NULL
                    ORDER BY amount_minor DESC, contract_id ASC
                    "#,
                )
                .bind(&state.cfg.committee_party)
                .bind(&p.instrument_admin)
                .bind(&p.instrument_id)
                .bind(&p.lineage_root_instruction_cid)
                .fetch_all(&state.db)
                .await
                .context("query receiver holdings")?;

                if receiver_holdings.is_empty() {
                    // Likely waiting for HoldingClaim -> Holding conversion.
                    continue;
                }

                let receiver_holding_values = receiver_holdings
                    .iter()
                    .map(|(cid,)| value_contract_id(cid))
                    .collect::<Vec<_>>();

                let args = value_record(vec![
                    record_field("accountStateCid", value_contract_id(&account_state_cid)),
                    record_field(
                        "terminalInstructionCid",
                        value_contract_id(&p.latest_instruction_cid),
                    ),
                    record_field("receiverHoldingCids", value_list(receiver_holding_values)),
                ]);

                let cmd = lapi::Command {
                    command: Some(lapi::command::Command::Exercise(lapi::ExerciseCommand {
                        template_id: Some(template_id.clone()),
                        contract_id: p.deposit_pending_cid.clone(),
                        choice: "FinalizeCompleted".to_string(),
                        choice_argument: Some(args),
                    })),
                };

                let receiver_holding_cids = receiver_holdings
                    .into_iter()
                    .map(|(cid,)| cid)
                    .collect::<Vec<_>>();
                let next_step_seq = next_command_step(op_head.as_ref(), reconcile_step_seq);
                let command_id = deposit_command_id(&op_id, "finalize-completed", next_step_seq);
                let submitted_update = TreasuryOpUpdate {
                    op_id: op_id.clone(),
                    op_type: TREASURY_OP_TYPE_DEPOSIT.to_string(),
                    owner_party: Some(p.owner_party.clone()),
                    account_id: Some(p.account_id.clone()),
                    instrument_admin: p.instrument_admin.clone(),
                    instrument_id: p.instrument_id.clone(),
                    amount_minor: Some(p.expected_amount_minor),
                    state: TREASURY_OP_STATE_SUBMITTED.to_string(),
                    step_seq: next_step_seq,
                    command_id: command_id.clone(),
                    observed_offset: Some(p.latest_instruction_offset),
                    observed_update_id: p.latest_instruction_update_id.clone(),
                    terminal_observed_offset: None,
                    terminal_update_id: None,
                    lineage_root_instruction_cid: Some(p.lineage_root_instruction_cid.clone()),
                    current_instruction_cid: Some(p.latest_instruction_cid.clone()),
                    input_holding_cids: vec![],
                    receiver_holding_cids: receiver_holding_cids.clone(),
                    extra: serde_json::json!({
                        "phase": "reconcile",
                        "action": "FinalizeCompleted",
                        "deposit_pending_cid": p.deposit_pending_cid,
                    }),
                    last_error: None,
                };
                upsert_treasury_op(state, &submitted_update).await?;
                insert_treasury_op_event(
                    state,
                    &op_id,
                    TREASURY_OP_STATE_SUBMITTED,
                    serde_json::json!({
                        "step_seq": next_step_seq,
                        "command_id": command_id,
                        "action": "FinalizeCompleted",
                        "deposit_pending_cid": p.deposit_pending_cid,
                        "terminal_instruction_cid": p.latest_instruction_cid,
                        "receiver_holding_count": submitted_update.receiver_holding_cids.len(),
                    }),
                )
                .await?;

                let act_as = vec![state.cfg.committee_party.clone()];
                match submit_and_wait_for_transaction(
                    &state.cfg,
                    command_id.clone(),
                    act_as,
                    vec![cmd],
                )
                .await
                {
                    Ok(tx) => {
                        let committed_update = TreasuryOpUpdate {
                            op_id: op_id.clone(),
                            op_type: TREASURY_OP_TYPE_DEPOSIT.to_string(),
                            owner_party: Some(p.owner_party.clone()),
                            account_id: Some(p.account_id.clone()),
                            instrument_admin: p.instrument_admin.clone(),
                            instrument_id: p.instrument_id.clone(),
                            amount_minor: Some(p.expected_amount_minor),
                            state: TREASURY_OP_STATE_COMMITTED.to_string(),
                            step_seq: next_step_seq,
                            command_id: command_id.clone(),
                            observed_offset: Some(tx.offset),
                            observed_update_id: Some(tx.update_id.clone()),
                            terminal_observed_offset: None,
                            terminal_update_id: None,
                            lineage_root_instruction_cid: Some(
                                p.lineage_root_instruction_cid.clone(),
                            ),
                            current_instruction_cid: Some(p.latest_instruction_cid.clone()),
                            input_holding_cids: vec![],
                            receiver_holding_cids: receiver_holding_cids.clone(),
                            extra: serde_json::json!({
                                "phase": "reconcile",
                                "action": "FinalizeCompleted",
                                "deposit_pending_cid": p.deposit_pending_cid,
                            }),
                            last_error: None,
                        };
                        upsert_treasury_op(state, &committed_update).await?;
                        insert_treasury_op_event(
                            state,
                            &op_id,
                            TREASURY_OP_STATE_COMMITTED,
                            serde_json::json!({
                                "step_seq": next_step_seq,
                                "command_id": command_id,
                                "action": "FinalizeCompleted",
                                "offset": tx.offset,
                                "update_id": tx.update_id,
                            }),
                        )
                        .await?;
                        tracing::info!(
                            op_id = %op_id,
                            command_id = %command_id,
                            account_id = %p.account_id,
                            deposit_id = %p.deposit_id,
                            offset = tx.offset,
                            "deposit finalized (completed)"
                        );
                    }
                    Err(err) => {
                        let err_msg = error_string(&err);
                        let mut failed = submitted_update.clone();
                        failed.last_error = Some(err_msg.clone());
                        upsert_treasury_op(state, &failed).await?;
                        insert_treasury_op_event(
                            state,
                            &op_id,
                            TREASURY_OP_STATE_SUBMITTED,
                            serde_json::json!({
                                "step_seq": next_step_seq,
                                "command_id": command_id,
                                "action": "FinalizeCompleted",
                                "error": err_msg,
                            }),
                        )
                        .await?;
                        tracing::warn!(
                            error = ?err,
                            op_id = %op_id,
                            command_id = %command_id,
                            deposit_pending_cid = %p.deposit_pending_cid,
                            "deposit finalize failed"
                        );
                    }
                }
            }
            "Failed" => {
                let args = value_record(vec![record_field(
                    "terminalInstructionCid",
                    value_contract_id(&p.latest_instruction_cid),
                )]);

                let cmd = lapi::Command {
                    command: Some(lapi::command::Command::Exercise(lapi::ExerciseCommand {
                        template_id: Some(template_id.clone()),
                        contract_id: p.deposit_pending_cid.clone(),
                        choice: "FinalizeFailed".to_string(),
                        choice_argument: Some(args),
                    })),
                };

                let next_step_seq = next_command_step(op_head.as_ref(), reconcile_step_seq);
                let command_id = deposit_command_id(&op_id, "finalize-failed", next_step_seq);
                let submitted_update = TreasuryOpUpdate {
                    op_id: op_id.clone(),
                    op_type: TREASURY_OP_TYPE_DEPOSIT.to_string(),
                    owner_party: Some(p.owner_party.clone()),
                    account_id: Some(p.account_id.clone()),
                    instrument_admin: p.instrument_admin.clone(),
                    instrument_id: p.instrument_id.clone(),
                    amount_minor: Some(p.expected_amount_minor),
                    state: TREASURY_OP_STATE_SUBMITTED.to_string(),
                    step_seq: next_step_seq,
                    command_id: command_id.clone(),
                    observed_offset: Some(p.latest_instruction_offset),
                    observed_update_id: p.latest_instruction_update_id.clone(),
                    terminal_observed_offset: None,
                    terminal_update_id: None,
                    lineage_root_instruction_cid: Some(p.lineage_root_instruction_cid.clone()),
                    current_instruction_cid: Some(p.latest_instruction_cid.clone()),
                    input_holding_cids: vec![],
                    receiver_holding_cids: vec![],
                    extra: serde_json::json!({
                        "phase": "reconcile",
                        "action": "FinalizeFailed",
                        "deposit_pending_cid": p.deposit_pending_cid,
                    }),
                    last_error: None,
                };
                upsert_treasury_op(state, &submitted_update).await?;
                insert_treasury_op_event(
                    state,
                    &op_id,
                    TREASURY_OP_STATE_SUBMITTED,
                    serde_json::json!({
                        "step_seq": next_step_seq,
                        "command_id": command_id,
                        "action": "FinalizeFailed",
                        "deposit_pending_cid": p.deposit_pending_cid,
                        "terminal_instruction_cid": p.latest_instruction_cid,
                    }),
                )
                .await?;

                let act_as = vec![state.cfg.committee_party.clone()];
                match submit_and_wait_for_transaction(
                    &state.cfg,
                    command_id.clone(),
                    act_as,
                    vec![cmd],
                )
                .await
                {
                    Ok(tx) => {
                        let committed_update = TreasuryOpUpdate {
                            op_id: op_id.clone(),
                            op_type: TREASURY_OP_TYPE_DEPOSIT.to_string(),
                            owner_party: Some(p.owner_party.clone()),
                            account_id: Some(p.account_id.clone()),
                            instrument_admin: p.instrument_admin.clone(),
                            instrument_id: p.instrument_id.clone(),
                            amount_minor: Some(p.expected_amount_minor),
                            state: TREASURY_OP_STATE_COMMITTED.to_string(),
                            step_seq: next_step_seq,
                            command_id: command_id.clone(),
                            observed_offset: Some(tx.offset),
                            observed_update_id: Some(tx.update_id.clone()),
                            terminal_observed_offset: None,
                            terminal_update_id: None,
                            lineage_root_instruction_cid: Some(
                                p.lineage_root_instruction_cid.clone(),
                            ),
                            current_instruction_cid: Some(p.latest_instruction_cid.clone()),
                            input_holding_cids: vec![],
                            receiver_holding_cids: vec![],
                            extra: serde_json::json!({
                                "phase": "reconcile",
                                "action": "FinalizeFailed",
                                "deposit_pending_cid": p.deposit_pending_cid,
                            }),
                            last_error: None,
                        };
                        upsert_treasury_op(state, &committed_update).await?;
                        insert_treasury_op_event(
                            state,
                            &op_id,
                            TREASURY_OP_STATE_COMMITTED,
                            serde_json::json!({
                                "step_seq": next_step_seq,
                                "command_id": command_id,
                                "action": "FinalizeFailed",
                                "offset": tx.offset,
                                "update_id": tx.update_id,
                            }),
                        )
                        .await?;
                        tracing::info!(
                            op_id = %op_id,
                            command_id = %command_id,
                            account_id = %p.account_id,
                            deposit_id = %p.deposit_id,
                            offset = tx.offset,
                            "deposit finalized (failed)"
                        );
                    }
                    Err(err) => {
                        let err_msg = error_string(&err);
                        let mut failed = submitted_update.clone();
                        failed.last_error = Some(err_msg.clone());
                        upsert_treasury_op(state, &failed).await?;
                        insert_treasury_op_event(
                            state,
                            &op_id,
                            TREASURY_OP_STATE_SUBMITTED,
                            serde_json::json!({
                                "step_seq": next_step_seq,
                                "command_id": command_id,
                                "action": "FinalizeFailed",
                                "error": err_msg,
                            }),
                        )
                        .await?;
                        tracing::warn!(
                            error = ?err,
                            op_id = %op_id,
                            command_id = %command_id,
                            deposit_pending_cid = %p.deposit_pending_cid,
                            "deposit finalize-failed failed"
                        );
                    }
                }
            }
            other => {
                tracing::warn!(
                    op_id = %op_id,
                    deposit_pending_cid = %p.deposit_pending_cid,
                    latest_output = %other,
                    "unexpected deposit latest output"
                );
            }
        }
    }

    #[derive(sqlx::FromRow)]
    struct ReceiptRow {
        deposit_receipt_cid: String,
        owner_party: String,
        account_id: String,
        instrument_admin: String,
        instrument_id: String,
        deposit_id: String,
        credited_minor: i64,
        lineage_root_instruction_cid: String,
        terminal_instruction_cid: String,
        receiver_holding_cids: serde_json::Value,
        receipt_offset: i64,
        receipt_update_id: Option<String>,
    }

    let receipts: Vec<ReceiptRow> = sqlx::query_as(
        r#"
        SELECT
          dr.contract_id AS deposit_receipt_cid,
          dr.owner_party,
          dr.account_id,
          dr.instrument_admin,
          dr.instrument_id,
          dr.deposit_id,
          dr.credited_minor,
          dr.lineage_root_instruction_cid,
          dr.terminal_instruction_cid,
          dr.receiver_holding_cids,
          dr.last_offset AS receipt_offset,
          lu.update_id AS receipt_update_id
        FROM deposit_receipts dr
        LEFT JOIN ledger_updates lu
          ON lu.ledger_offset = dr.last_offset
        LEFT JOIN treasury_ops o
          ON o.op_id = (
            'deposit:'
            || dr.owner_party
            || ':'
            || dr.instrument_admin
            || ':'
            || dr.instrument_id
            || ':'
            || dr.deposit_id
          )
        WHERE dr.active = TRUE
          AND dr.committee_party = $1
          AND (o.op_id IS NULL OR o.state <> $2)
        ORDER BY dr.created_at ASC
        LIMIT 100
        "#,
    )
    .bind(&state.cfg.committee_party)
    .bind(TREASURY_OP_STATE_DONE)
    .fetch_all(&state.db)
    .await
    .context("query unresolved deposit receipts for treasury ops")?;

    for r in receipts {
        let op_id = canonical_deposit_op_id(
            &r.owner_party,
            &r.instrument_admin,
            &r.instrument_id,
            &r.deposit_id,
        );

        let op_head = get_treasury_op_head(state, &op_id).await?;
        let done_step_seq = op_head.as_ref().map(|op| op.step_seq.max(1)).unwrap_or(1);
        let done_command_id = op_head
            .as_ref()
            .map(|op| op.command_id.clone())
            .unwrap_or_else(|| deposit_command_id(&op_id, "done", done_step_seq));
        let should_emit_done = match op_head.as_ref() {
            Some(op) => {
                op.state != TREASURY_OP_STATE_DONE
                    || op.terminal_observed_offset != Some(r.receipt_offset)
            }
            None => true,
        };
        let receiver_holding_cids = r
            .receiver_holding_cids
            .as_array()
            .map(|values| {
                values
                    .iter()
                    .filter_map(|value| value.as_str().map(std::string::ToString::to_string))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let done_update = TreasuryOpUpdate {
            op_id: op_id.clone(),
            op_type: TREASURY_OP_TYPE_DEPOSIT.to_string(),
            owner_party: Some(r.owner_party.clone()),
            account_id: Some(r.account_id.clone()),
            instrument_admin: r.instrument_admin.clone(),
            instrument_id: r.instrument_id.clone(),
            amount_minor: Some(r.credited_minor),
            state: TREASURY_OP_STATE_DONE.to_string(),
            step_seq: done_step_seq,
            command_id: done_command_id.clone(),
            observed_offset: Some(r.receipt_offset),
            observed_update_id: r.receipt_update_id.clone(),
            terminal_observed_offset: Some(r.receipt_offset),
            terminal_update_id: r.receipt_update_id.clone(),
            lineage_root_instruction_cid: Some(r.lineage_root_instruction_cid.clone()),
            current_instruction_cid: Some(r.terminal_instruction_cid.clone()),
            input_holding_cids: vec![],
            receiver_holding_cids: receiver_holding_cids.clone(),
            extra: serde_json::json!({
                "phase": "reconcile",
                "action": "ReceiptObserved",
                "deposit_receipt_cid": r.deposit_receipt_cid,
            }),
            last_error: None,
        };
        upsert_treasury_op(state, &done_update).await?;
        if should_emit_done {
            insert_treasury_op_event(
                state,
                &op_id,
                TREASURY_OP_STATE_DONE,
                serde_json::json!({
                    "step_seq": done_step_seq,
                    "command_id": done_command_id,
                    "action": "ReceiptObserved",
                    "deposit_receipt_cid": r.deposit_receipt_cid,
                    "terminal_instruction_cid": r.terminal_instruction_cid,
                    "offset": r.receipt_offset,
                    "update_id": r.receipt_update_id,
                    "receiver_holding_count": receiver_holding_cids.len(),
                }),
            )
            .await?;
        }

        tracing::info!(
            op_id = %op_id,
            command_id = %done_update.command_id,
            account_id = %r.account_id,
            deposit_id = %r.deposit_id,
            credited_minor = r.credited_minor,
            offset = r.receipt_offset,
            "deposit op done"
        );
    }

    Ok(())
}

async fn withdrawal_process_loop(state: AppState) -> Result<()> {
    let poll_ms = std::env::var("PEBBLE_TREASURY_WITHDRAWAL_PROCESS_POLL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1000);

    loop {
        if let Err(err) = withdrawal_process_tick(&state).await {
            tracing::warn!(error = ?err, "withdrawal process tick failed");
        }
        tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
    }
}

async fn withdrawal_reconcile_loop(state: AppState) -> Result<()> {
    let poll_ms = std::env::var("PEBBLE_TREASURY_WITHDRAWAL_RECONCILE_POLL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1000);

    loop {
        if let Err(err) = withdrawal_reconcile_tick(&state).await {
            tracing::warn!(error = ?err, "withdrawal reconcile tick failed");
        }
        tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
    }
}

async fn watermark_offset(
    dbtx: &mut Transaction<'_, Postgres>,
    submission_path: &str,
) -> Result<i64> {
    let row: Option<(i64,)> = sqlx::query_as(
        r#"
        SELECT watermark_offset
        FROM submission_path_watermarks
        WHERE submission_path = $1
        "#,
    )
    .bind(submission_path)
    .fetch_optional(&mut **dbtx)
    .await
    .context("query submission path watermark")?;

    Ok(row.map(|(x,)| x).unwrap_or(0))
}

async fn get_or_create_committee_wallet(
    state: &AppState,
    instrument_admin: &str,
    instrument_id: &str,
) -> Result<String> {
    let row: Option<(String,)> = sqlx::query_as(
        r#"
        SELECT contract_id
        FROM token_wallets
        WHERE active = TRUE
          AND owner_party = $1
          AND instrument_admin = $2
          AND instrument_id = $3
        ORDER BY last_offset DESC
        LIMIT 1
        "#,
    )
    .bind(&state.cfg.committee_party)
    .bind(instrument_admin)
    .bind(instrument_id)
    .fetch_optional(&state.db)
    .await
    .context("query committee wallet")?;

    if let Some((cid,)) = row {
        return Ok(cid);
    }

    let template_id = lapi::Identifier {
        package_id: "#wizardcat-token-standard".to_string(),
        module_name: "Wizardcat.Token.Standard".to_string(),
        entity_name: "Wallet".to_string(),
    };

    let create_args = lapi::Record {
        record_id: None,
        fields: vec![
            record_field("owner", value_party(&state.cfg.committee_party)),
            record_field("instrumentAdmin", value_party(instrument_admin)),
            record_field("instrumentId", value_text(instrument_id)),
        ],
    };

    let cmd = lapi::Command {
        command: Some(lapi::command::Command::Create(lapi::CreateCommand {
            template_id: Some(template_id),
            create_arguments: Some(create_args),
        })),
    };

    let command_id = format!(
        "wallet:create:{}:{}:{}",
        state.cfg.committee_party, instrument_admin, instrument_id
    );
    let act_as = vec![state.cfg.committee_party.clone()];
    let tx = submit_and_wait_for_transaction(&state.cfg, command_id, act_as, vec![cmd]).await?;

    let created = extract_created_contract_ids(&tx, "Wizardcat.Token.Standard", "Wallet");
    let wallet_cid = created
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("expected Wallet create event in tx"))?;
    Ok(wallet_cid)
}

async fn reserve_holdings_for_op(
    state: &AppState,
    op_id: &str,
    instrument_admin: &str,
    instrument_id: &str,
    amount_minor: i64,
    max_inputs: i64,
) -> Result<Vec<String>> {
    #[derive(sqlx::FromRow)]
    struct HoldingRow {
        contract_id: String,
        amount_minor: i64,
    }

    let mut dbtx = state.db.begin().await.context("begin db transaction")?;

    let watermark = watermark_offset(&mut dbtx, &state.cfg.submission_path).await?;
    if watermark <= 0 {
        // Avoid selecting outputs the indexer hasn't ingested yet.
        dbtx.rollback().await.ok();
        return Ok(vec![]);
    }

    let candidates: Vec<HoldingRow> = sqlx::query_as(
        r#"
        SELECT h.contract_id, h.amount_minor
        FROM token_holdings h
        WHERE h.active = TRUE
          AND h.owner_party = $1
          AND h.instrument_admin = $2
          AND h.instrument_id = $3
          AND h.lock_status_class IS NULL
          AND h.last_offset <= $4
          AND NOT EXISTS (
            SELECT 1
            FROM treasury_holding_reservations r
            WHERE r.holding_cid = h.contract_id
              AND r.state IN ('Reserved', 'Submitting')
          )
          AND NOT EXISTS (
            SELECT 1
            FROM deposit_pendings p
            WHERE p.active = TRUE
              AND p.lineage_root_instruction_cid = h.origin_instruction_cid
          )
        ORDER BY h.amount_minor DESC, h.contract_id ASC
        LIMIT $5
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(&state.cfg.committee_party)
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(watermark)
    .bind(max_inputs)
    .fetch_all(&mut *dbtx)
    .await
    .context("query candidate holdings")?;

    let mut selected = Vec::new();
    let mut sum: i64 = 0;
    for h in candidates {
        selected.push(h.contract_id);
        sum = sum.saturating_add(h.amount_minor);
        if sum >= amount_minor {
            break;
        }
    }

    if sum < amount_minor {
        dbtx.rollback().await.ok();
        return Ok(vec![]);
    }

    for cid in &selected {
        let res = sqlx::query(
            r#"
            INSERT INTO treasury_holding_reservations (
              holding_cid,
              op_id,
              instrument_admin,
              instrument_id,
              state
            )
            VALUES ($1,$2,$3,$4,'Reserved')
            ON CONFLICT (holding_cid, op_id) DO UPDATE
            SET
              instrument_admin = EXCLUDED.instrument_admin,
              instrument_id = EXCLUDED.instrument_id,
              state = 'Reserved',
              updated_at = now()
            "#,
        )
        .bind(cid)
        .bind(op_id)
        .bind(instrument_admin)
        .bind(instrument_id)
        .execute(&mut *dbtx)
        .await;

        if let Err(err) = res {
            dbtx.rollback().await.ok();
            return Err(anyhow!(err).context("insert holding reservation"));
        }
    }

    dbtx.commit().await.context("commit reservation tx")?;
    Ok(selected)
}

async fn reserve_smallest_holdings_for_op(
    state: &AppState,
    op_id: &str,
    instrument_admin: &str,
    instrument_id: &str,
    max_inputs: i64,
) -> Result<Vec<String>> {
    #[derive(sqlx::FromRow)]
    struct HoldingRow {
        contract_id: String,
    }

    let mut dbtx = state.db.begin().await.context("begin db transaction")?;

    let watermark = watermark_offset(&mut dbtx, &state.cfg.submission_path).await?;
    if watermark <= 0 {
        dbtx.rollback().await.ok();
        return Ok(vec![]);
    }

    let candidates: Vec<HoldingRow> = sqlx::query_as(
        r#"
        SELECT h.contract_id
        FROM token_holdings h
        WHERE h.active = TRUE
          AND h.owner_party = $1
          AND h.instrument_admin = $2
          AND h.instrument_id = $3
          AND h.lock_status_class IS NULL
          AND h.last_offset <= $4
          AND NOT EXISTS (
            SELECT 1
            FROM treasury_holding_reservations r
            WHERE r.holding_cid = h.contract_id
              AND r.state IN ('Reserved', 'Submitting')
          )
          AND NOT EXISTS (
            SELECT 1
            FROM deposit_pendings p
            WHERE p.active = TRUE
              AND p.lineage_root_instruction_cid = h.origin_instruction_cid
          )
        ORDER BY h.amount_minor ASC, h.contract_id ASC
        LIMIT $5
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(&state.cfg.committee_party)
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(watermark)
    .bind(max_inputs)
    .fetch_all(&mut *dbtx)
    .await
    .context("query candidate holdings for consolidation")?;

    if candidates.len() < 2 {
        dbtx.rollback().await.ok();
        return Ok(vec![]);
    }

    let selected = candidates
        .into_iter()
        .map(|h| h.contract_id)
        .collect::<Vec<_>>();

    for cid in &selected {
        let res = sqlx::query(
            r#"
            INSERT INTO treasury_holding_reservations (
              holding_cid,
              op_id,
              instrument_admin,
              instrument_id,
              state
            )
            VALUES ($1,$2,$3,$4,'Reserved')
            ON CONFLICT (holding_cid, op_id) DO UPDATE
            SET
              instrument_admin = EXCLUDED.instrument_admin,
              instrument_id = EXCLUDED.instrument_id,
              state = 'Reserved',
              updated_at = now()
            "#,
        )
        .bind(cid)
        .bind(op_id)
        .bind(instrument_admin)
        .bind(instrument_id)
        .execute(&mut *dbtx)
        .await;

        if let Err(err) = res {
            dbtx.rollback().await.ok();
            return Err(anyhow::Error::new(err).context("insert consolidation reservation"));
        }
    }

    dbtx.commit()
        .await
        .context("commit consolidation reservation tx")?;
    Ok(selected)
}

async fn mark_reservations_state(state: &AppState, op_id: &str, new_state: &str) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE treasury_holding_reservations
        SET state = $2, updated_at = now()
        WHERE op_id = $1
          AND state IN ('Reserved', 'Submitting')
        "#,
    )
    .bind(op_id)
    .bind(new_state)
    .execute(&state.db)
    .await
    .context("update reservation state")?;
    Ok(())
}

async fn advisory_lock_account(
    tx: &mut Transaction<'_, Postgres>,
    namespace: i32,
    account_id: &str,
) -> Result<()> {
    sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
        .bind(namespace)
        .bind(account_id)
        .execute(&mut **tx)
        .await
        .context("acquire account advisory lock")?;
    Ok(())
}

struct WithdrawalRequestStateTransition<'a> {
    account_id: &'a str,
    withdrawal_id: &'a str,
    next_state: &'a str,
    reason: Option<&'a str>,
    eligibility_snapshot: Option<serde_json::Value>,
    payload: serde_json::Value,
    allowed_from_states: &'a [&'a str],
}

async fn transition_withdrawal_request_state(
    state: &AppState,
    transition: &WithdrawalRequestStateTransition<'_>,
) -> Result<bool> {
    if !state.cfg.withdrawal_request_state_enabled {
        return Ok(false);
    }

    let mut tx = state
        .db
        .begin()
        .await
        .context("begin withdrawal request state transition tx")?;

    let current: Option<(String, String)> = sqlx::query_as(
        r#"
        SELECT contract_id, request_state
        FROM withdrawal_requests
        WHERE account_id = $1
          AND withdrawal_id = $2
        ORDER BY active DESC, created_at DESC, contract_id DESC
        LIMIT 1
        FOR UPDATE
        "#,
    )
    .bind(transition.account_id)
    .bind(transition.withdrawal_id)
    .fetch_optional(&mut *tx)
    .await
    .context("load withdrawal request state for transition")?;

    let Some((request_contract_id, from_state)) = current else {
        sqlx::query(
            r#"
            INSERT INTO withdrawal_request_events (
              contract_id,
              withdrawal_id,
              account_id,
              from_state,
              to_state,
              reason,
              payload
            )
            VALUES (NULL,$1,$2,NULL,$3,$4,$5)
            "#,
        )
        .bind(transition.withdrawal_id)
        .bind(transition.account_id)
        .bind(transition.next_state)
        .bind(transition.reason)
        .bind(transition.payload.clone())
        .execute(&mut *tx)
        .await
        .context("insert withdrawal request state event (row missing)")?;

        tx.commit()
            .await
            .context("commit withdrawal request state transition tx (row missing)")?;
        return Ok(false);
    };

    if !transition.allowed_from_states.is_empty()
        && !transition
            .allowed_from_states
            .iter()
            .any(|s| *s == from_state)
    {
        tx.commit()
            .await
            .context("commit withdrawal request state transition tx (from-state mismatch)")?;
        return Ok(false);
    }

    if from_state == transition.next_state {
        tx.commit()
            .await
            .context("commit withdrawal request state transition tx (already in state)")?;
        return Ok(false);
    }

    sqlx::query(
        r#"
        UPDATE withdrawal_requests
        SET
          request_state = $2,
          request_state_reason = $3,
          eligibility_snapshot = COALESCE($4::jsonb, eligibility_snapshot),
          last_evaluated_at = now(),
          processed_at = CASE
            WHEN $2 IN ('Processed', 'RejectedIneligible', 'Failed', 'Superseded')
              THEN COALESCE(processed_at, now())
            ELSE processed_at
          END
        WHERE contract_id = $1
        "#,
    )
    .bind(&request_contract_id)
    .bind(transition.next_state)
    .bind(transition.reason)
    .bind(transition.eligibility_snapshot.clone())
    .execute(&mut *tx)
    .await
    .context("update withdrawal request state")?;

    sqlx::query(
        r#"
        INSERT INTO withdrawal_request_events (
          contract_id,
          withdrawal_id,
          account_id,
          from_state,
          to_state,
          reason,
          payload
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        "#,
    )
    .bind(&request_contract_id)
    .bind(transition.withdrawal_id)
    .bind(transition.account_id)
    .bind(from_state)
    .bind(transition.next_state)
    .bind(transition.reason)
    .bind(transition.payload.clone())
    .execute(&mut *tx)
    .await
    .context("insert withdrawal request state event")?;

    tx.commit()
        .await
        .context("commit withdrawal request state transition tx")?;

    Ok(true)
}

async fn release_account_withdrawal_reservation(
    state: &AppState,
    account_id: &str,
    withdrawal_id: &str,
    amount_minor: i64,
    terminal_ref: &str,
    release_reason: &str,
) -> Result<bool> {
    if amount_minor <= 0 {
        return Ok(false);
    }

    let mut tx = state
        .db
        .begin()
        .await
        .context("begin withdrawal reservation release tx")?;
    advisory_lock_account(&mut tx, state.cfg.account_lock_namespace, account_id).await?;

    let marker_inserted = sqlx::query_scalar::<_, String>(
        r#"
        INSERT INTO withdrawal_reservation_releases (
          account_id,
          withdrawal_id,
          terminal_ref,
          release_reason,
          amount_minor
        )
        VALUES ($1,$2,$3,$4,$5)
        ON CONFLICT (account_id, withdrawal_id) DO NOTHING
        RETURNING account_id
        "#,
    )
    .bind(account_id)
    .bind(withdrawal_id)
    .bind(terminal_ref)
    .bind(release_reason)
    .bind(amount_minor)
    .fetch_optional(&mut *tx)
    .await
    .context("insert withdrawal reservation release marker")?
    .is_some();

    if !marker_inserted {
        tx.commit()
            .await
            .context("commit withdrawal reservation release tx (marker conflict)")?;
        return Ok(false);
    }

    let result = sqlx::query(
        r#"
        UPDATE account_liquidity_state
        SET
          pending_withdrawals_reserved_minor = pending_withdrawals_reserved_minor - $2,
          version = version + 1,
          updated_at = now()
        WHERE account_id = $1
          AND pending_withdrawals_reserved_minor >= $2
        "#,
    )
    .bind(account_id)
    .bind(amount_minor)
    .execute(&mut *tx)
    .await
    .context("release account withdrawal reservation")?;

    if result.rows_affected() == 0 {
        tracing::warn!(
            account_id = %account_id,
            withdrawal_id = %withdrawal_id,
            terminal_ref = %terminal_ref,
            amount_minor,
            "withdrawal reservation release marker inserted but liquidity row was not updated"
        );
    }

    tx.commit()
        .await
        .context("commit withdrawal reservation release tx")?;

    Ok(result.rows_affected() > 0)
}

async fn reconcile_withdrawal_create_intents(state: &AppState) -> Result<()> {
    #[derive(sqlx::FromRow)]
    struct IntentRow {
        intent_id: String,
        intent_state: String,
        account_id: String,
        withdrawal_id: String,
        amount_minor: i64,
    }

    let intents: Vec<IntentRow> = sqlx::query_as(
        r#"
        SELECT
          intent_id::text AS intent_id,
          state AS intent_state,
          account_id,
          withdrawal_id,
          amount_minor
        FROM withdrawal_create_intents
        WHERE state IN ('Prepared', 'Submitted', 'FailedSubmitUnknown')
          AND updated_at <= now() - interval '30 seconds'
        ORDER BY updated_at ASC
        LIMIT 50
        "#,
    )
    .fetch_all(&state.db)
    .await
    .context("query unresolved withdrawal create intents")?;

    for intent in intents {
        let existing_request_cid: Option<String> = sqlx::query_scalar(
            r#"
            SELECT contract_id
            FROM withdrawal_requests
            WHERE account_id = $1
              AND withdrawal_id = $2
            ORDER BY active DESC, created_at DESC, contract_id DESC
            LIMIT 1
            "#,
        )
        .bind(&intent.account_id)
        .bind(&intent.withdrawal_id)
        .fetch_optional(&state.db)
        .await
        .context("query withdrawal request by intent lineage")?;

        match existing_request_cid {
            Some(contract_id) => {
                sqlx::query(
                    r#"
                    UPDATE withdrawal_create_intents
                    SET
                      state = 'Committed',
                      request_contract_id = COALESCE(request_contract_id, $2),
                      last_error = NULL,
                      updated_at = now()
                    WHERE intent_id = $1::uuid
                    "#,
                )
                .bind(&intent.intent_id)
                .bind(contract_id)
                .execute(&state.db)
                .await
                .context("mark unresolved create intent committed via reconcile")?;
            }
            None => {
                let failure_reason = format!(
                    "reconcile: {} create absent after timeout",
                    intent.intent_state
                );
                sqlx::query(
                    r#"
                    UPDATE withdrawal_create_intents
                    SET
                      state = 'Failed',
                      last_error = $2,
                      updated_at = now()
                    WHERE intent_id = $1::uuid
                    "#,
                )
                .bind(&intent.intent_id)
                .bind(&failure_reason)
                .execute(&state.db)
                .await
                .context("mark unresolved create intent as terminal failed")?;
                let _ = release_account_withdrawal_reservation(
                    state,
                    &intent.account_id,
                    &intent.withdrawal_id,
                    intent.amount_minor,
                    &format!("create-intent:{}", intent.intent_id),
                    "CREATE_INTENT_RECONCILE_ABSENT",
                )
                .await?;
            }
        }
    }

    Ok(())
}

async fn withdrawal_process_tick(state: &AppState) -> Result<()> {
    #[derive(sqlx::FromRow)]
    struct WithdrawalReqRow {
        contract_id: String,
        owner_party: String,
        account_id: String,
        instrument_admin: String,
        instrument_id: String,
        withdrawal_id: String,
        amount_minor: i64,
    }

    let mut queue_tx = state
        .db
        .begin()
        .await
        .context("begin withdrawal queue selection tx")?;
    let reqs: Vec<WithdrawalReqRow> = if state.cfg.withdrawal_request_state_enabled {
        sqlx::query_as(
            r#"
            SELECT
              contract_id,
              owner_party,
              account_id,
              instrument_admin,
              instrument_id,
              withdrawal_id,
              amount_minor
            FROM withdrawal_requests
            WHERE active = TRUE
              AND committee_party = $1
              AND request_state = 'Queued'
            ORDER BY created_at ASC
            LIMIT 25
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(&state.cfg.committee_party)
        .fetch_all(&mut *queue_tx)
        .await
        .context("query queued withdrawal requests")?
    } else {
        sqlx::query_as(
            r#"
            SELECT
              contract_id,
              owner_party,
              account_id,
              instrument_admin,
              instrument_id,
              withdrawal_id,
              amount_minor
            FROM withdrawal_requests
            WHERE active = TRUE
              AND committee_party = $1
            ORDER BY created_at ASC
            LIMIT 25
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(&state.cfg.committee_party)
        .fetch_all(&mut *queue_tx)
        .await
        .context("query active withdrawal requests")?
    };
    queue_tx
        .commit()
        .await
        .context("commit withdrawal queue selection tx")?;

    if reqs.is_empty() {
        return Ok(());
    }

    for req in reqs {
        if let Err(err) = parse_account_id(&req.account_id) {
            tracing::warn!(
                contract_id = %req.contract_id,
                account_id = %req.account_id,
                error = %err,
                "skipping withdrawal request with non-canonical account_id"
            );
            let _ = transition_withdrawal_request_state(
                state,
                &WithdrawalRequestStateTransition {
                    account_id: &req.account_id,
                    withdrawal_id: &req.withdrawal_id,
                    next_state: "Failed",
                    reason: Some("WITHDRAWAL_ACCOUNT_ID_INVALID"),
                    eligibility_snapshot: None,
                    payload: serde_json::json!({
                        "error": err.to_string(),
                    }),
                    allowed_from_states: &["Queued", "Processing"],
                },
            )
            .await?;
            continue;
        }
        if let Err(err) = parse_withdrawal_id(&req.withdrawal_id) {
            tracing::warn!(
                contract_id = %req.contract_id,
                withdrawal_id = %req.withdrawal_id,
                error = %err,
                "skipping withdrawal request with non-canonical withdrawal_id"
            );
            let _ = transition_withdrawal_request_state(
                state,
                &WithdrawalRequestStateTransition {
                    account_id: &req.account_id,
                    withdrawal_id: &req.withdrawal_id,
                    next_state: "Failed",
                    reason: Some("WITHDRAWAL_ID_INVALID"),
                    eligibility_snapshot: None,
                    payload: serde_json::json!({
                        "error": err.to_string(),
                    }),
                    allowed_from_states: &["Queued", "Processing"],
                },
            )
            .await?;
            continue;
        }

        let op_id = canonical_withdrawal_op_id(
            &req.owner_party,
            &req.instrument_admin,
            &req.instrument_id,
            &req.withdrawal_id,
        );

        let mut account_lock_tx = state
            .db
            .begin()
            .await
            .context("begin withdrawal account advisory lock tx")?;
        advisory_lock_account(
            &mut account_lock_tx,
            state.cfg.account_lock_namespace,
            &req.account_id,
        )
        .await?;

        let existing_pending: Option<(String,)> = sqlx::query_as(
            r#"
            SELECT contract_id
            FROM withdrawal_pendings
            WHERE active = TRUE
              AND owner_party = $1
              AND instrument_admin = $2
              AND instrument_id = $3
              AND withdrawal_id = $4
            LIMIT 1
            "#,
        )
        .bind(&req.owner_party)
        .bind(&req.instrument_admin)
        .bind(&req.instrument_id)
        .bind(&req.withdrawal_id)
        .fetch_optional(&mut *account_lock_tx)
        .await
        .context("query existing withdrawal pending")?;
        if existing_pending.is_some() {
            let _ = transition_withdrawal_request_state(
                state,
                &WithdrawalRequestStateTransition {
                    account_id: &req.account_id,
                    withdrawal_id: &req.withdrawal_id,
                    next_state: "Processing",
                    reason: Some("WITHDRAWAL_PENDING_ALREADY_EXISTS"),
                    eligibility_snapshot: None,
                    payload: serde_json::json!({
                        "reason": "pending already exists",
                    }),
                    allowed_from_states: &["Queued", "Processing"],
                },
            )
            .await?;
            continue;
        }

        let existing_receipt: Option<(String,)> = sqlx::query_as(
            r#"
            SELECT contract_id
            FROM withdrawal_receipts
            WHERE active = TRUE
              AND owner_party = $1
              AND instrument_admin = $2
              AND instrument_id = $3
              AND withdrawal_id = $4
            LIMIT 1
            "#,
        )
        .bind(&req.owner_party)
        .bind(&req.instrument_admin)
        .bind(&req.instrument_id)
        .bind(&req.withdrawal_id)
        .fetch_optional(&mut *account_lock_tx)
        .await
        .context("query existing withdrawal receipt")?;
        if let Some((existing_receipt_cid,)) = existing_receipt {
            let _ = transition_withdrawal_request_state(
                state,
                &WithdrawalRequestStateTransition {
                    account_id: &req.account_id,
                    withdrawal_id: &req.withdrawal_id,
                    next_state: "Processed",
                    reason: Some("WITHDRAWAL_RECEIPT_ALREADY_EXISTS"),
                    eligibility_snapshot: None,
                    payload: serde_json::json!({
                        "reason": "receipt already exists",
                        "receipt_contract_id": existing_receipt_cid,
                    }),
                    allowed_from_states: &["Queued", "Processing", "Failed", "RejectedIneligible"],
                },
            )
            .await?;
            let _ = release_account_withdrawal_reservation(
                state,
                &req.account_id,
                &req.withdrawal_id,
                req.amount_minor,
                &format!("existing-receipt:{}", req.contract_id),
                "WITHDRAWAL_RECEIPT_ALREADY_EXISTS",
            )
            .await?;
            continue;
        }

        let op_head = get_treasury_op_head(state, &op_id).await?;
        if let Some(existing_op) = op_head.as_ref() {
            if matches!(
                existing_op.state.as_str(),
                TREASURY_OP_STATE_SUBMITTED
                    | TREASURY_OP_STATE_COMMITTED
                    | TREASURY_OP_STATE_RECONCILED
                    | TREASURY_OP_STATE_DONE
            ) {
                continue;
            }
        }

        #[derive(sqlx::FromRow)]
        struct AccountCheckRow {
            account_ref_cid: String,
            account_status: String,
            finalized_epoch: Option<i64>,
            account_state_cid: String,
            cleared_cash_minor: i64,
            last_applied_epoch: i64,
        }

        let acct: Option<AccountCheckRow> = sqlx::query_as(
            r#"
            SELECT
              ar.contract_id AS account_ref_cid,
              ar.status AS account_status,
              ar.finalized_epoch,
              st.contract_id AS account_state_cid,
              st.cleared_cash_minor,
              st.last_applied_epoch
            FROM account_ref_latest lref
            JOIN account_refs ar ON ar.contract_id = lref.contract_id
            JOIN account_state_latest lst ON lst.account_id = lref.account_id
            JOIN account_states st ON st.contract_id = lst.contract_id
            WHERE lref.account_id = $1
              AND ar.active = TRUE
              AND st.active = TRUE
            LIMIT 1
            "#,
        )
        .bind(&req.account_id)
        .fetch_optional(&mut *account_lock_tx)
        .await
        .context("query account state/ref")?;

        let Some(acct) = acct else {
            let _ = transition_withdrawal_request_state(
                state,
                &WithdrawalRequestStateTransition {
                    account_id: &req.account_id,
                    withdrawal_id: &req.withdrawal_id,
                    next_state: "RejectedIneligible",
                    reason: Some("WITHDRAWAL_ACCOUNT_STATE_STALE"),
                    eligibility_snapshot: None,
                    payload: serde_json::json!({
                        "reason": "account state/ref missing",
                    }),
                    allowed_from_states: &["Queued", "Processing"],
                },
            )
            .await?;
            let _ = release_account_withdrawal_reservation(
                state,
                &req.account_id,
                &req.withdrawal_id,
                req.amount_minor,
                &format!(
                    "process-reject:{}:WITHDRAWAL_ACCOUNT_STATE_STALE",
                    req.contract_id
                ),
                "WITHDRAWAL_ACCOUNT_STATE_STALE",
            )
            .await?;
            continue;
        };

        let status_allowed = match acct.account_status.as_str() {
            "Active" => state.cfg.active_withdrawals_enabled,
            "Suspended" => true,
            _ => false,
        };
        if !status_allowed {
            let _ = transition_withdrawal_request_state(
                state,
                &WithdrawalRequestStateTransition {
                    account_id: &req.account_id,
                    withdrawal_id: &req.withdrawal_id,
                    next_state: "RejectedIneligible",
                    reason: Some("WITHDRAWAL_INELIGIBLE_STATUS"),
                    eligibility_snapshot: Some(serde_json::json!({
                        "account_status": acct.account_status,
                        "active_withdrawals_enabled": state.cfg.active_withdrawals_enabled,
                    })),
                    payload: serde_json::json!({
                        "reason": "account status not allowed by policy",
                    }),
                    allowed_from_states: &["Queued", "Processing"],
                },
            )
            .await?;
            let _ = release_account_withdrawal_reservation(
                state,
                &req.account_id,
                &req.withdrawal_id,
                req.amount_minor,
                &format!(
                    "process-reject:{}:WITHDRAWAL_INELIGIBLE_STATUS",
                    req.contract_id
                ),
                "WITHDRAWAL_INELIGIBLE_STATUS",
            )
            .await?;
            continue;
        }

        if acct.account_status == "Suspended" {
            let Some(finalized_epoch) = acct.finalized_epoch else {
                let _ = transition_withdrawal_request_state(
                    state,
                    &WithdrawalRequestStateTransition {
                        account_id: &req.account_id,
                        withdrawal_id: &req.withdrawal_id,
                        next_state: "RejectedIneligible",
                        reason: Some("WITHDRAWAL_ACCOUNT_STATE_STALE"),
                        eligibility_snapshot: Some(serde_json::json!({
                            "account_status": acct.account_status,
                            "finalized_epoch": null,
                            "last_applied_epoch": acct.last_applied_epoch,
                        })),
                        payload: serde_json::json!({
                            "reason": "suspended account missing finalized_epoch",
                        }),
                        allowed_from_states: &["Queued", "Processing"],
                    },
                )
                .await?;
                let _ = release_account_withdrawal_reservation(
                    state,
                    &req.account_id,
                    &req.withdrawal_id,
                    req.amount_minor,
                    &format!(
                        "process-reject:{}:WITHDRAWAL_ACCOUNT_STATE_STALE",
                        req.contract_id
                    ),
                    "WITHDRAWAL_ACCOUNT_STATE_STALE",
                )
                .await?;
                continue;
            };
            if acct.last_applied_epoch < finalized_epoch {
                let _ = transition_withdrawal_request_state(
                    state,
                    &WithdrawalRequestStateTransition {
                        account_id: &req.account_id,
                        withdrawal_id: &req.withdrawal_id,
                        next_state: "RejectedIneligible",
                        reason: Some("WITHDRAWAL_ACCOUNT_STATE_STALE"),
                        eligibility_snapshot: Some(serde_json::json!({
                            "account_status": acct.account_status,
                            "finalized_epoch": finalized_epoch,
                            "last_applied_epoch": acct.last_applied_epoch,
                        })),
                        payload: serde_json::json!({
                            "reason": "suspended account not caught up to finalized_epoch",
                        }),
                        allowed_from_states: &["Queued", "Processing"],
                    },
                )
                .await?;
                let _ = release_account_withdrawal_reservation(
                    state,
                    &req.account_id,
                    &req.withdrawal_id,
                    req.amount_minor,
                    &format!(
                        "process-reject:{}:WITHDRAWAL_ACCOUNT_STATE_STALE",
                        req.contract_id
                    ),
                    "WITHDRAWAL_ACCOUNT_STATE_STALE",
                )
                .await?;
                continue;
            }
        }

        if acct.cleared_cash_minor < req.amount_minor {
            let _ = transition_withdrawal_request_state(
                state,
                &WithdrawalRequestStateTransition {
                    account_id: &req.account_id,
                    withdrawal_id: &req.withdrawal_id,
                    next_state: "RejectedIneligible",
                    reason: Some("WITHDRAWAL_INSUFFICIENT_WITHDRAWABLE"),
                    eligibility_snapshot: Some(serde_json::json!({
                        "cleared_cash_minor": acct.cleared_cash_minor,
                        "requested_amount_minor": req.amount_minor,
                    })),
                    payload: serde_json::json!({
                        "reason": "insufficient cleared cash at processing time",
                    }),
                    allowed_from_states: &["Queued", "Processing"],
                },
            )
            .await?;
            let _ = release_account_withdrawal_reservation(
                state,
                &req.account_id,
                &req.withdrawal_id,
                req.amount_minor,
                &format!(
                    "process-reject:{}:WITHDRAWAL_INSUFFICIENT_WITHDRAWABLE",
                    req.contract_id
                ),
                "WITHDRAWAL_INSUFFICIENT_WITHDRAWABLE",
            )
            .await?;
            continue;
        }

        #[derive(sqlx::FromRow)]
        struct TokenCfgRow {
            contract_id: String,
            operational_max_inputs_per_transfer: i64,
        }

        let token_cfg: Option<TokenCfgRow> = sqlx::query_as(
            r#"
            SELECT contract_id, operational_max_inputs_per_transfer
            FROM token_configs
            WHERE active = TRUE
              AND instrument_admin = $1
              AND instrument_id = $2
            ORDER BY last_offset DESC
            LIMIT 1
            "#,
        )
        .bind(&req.instrument_admin)
        .bind(&req.instrument_id)
        .fetch_optional(&mut *account_lock_tx)
        .await
        .context("query token config")?;
        let Some(token_cfg) = token_cfg else {
            let _ = transition_withdrawal_request_state(
                state,
                &WithdrawalRequestStateTransition {
                    account_id: &req.account_id,
                    withdrawal_id: &req.withdrawal_id,
                    next_state: "Failed",
                    reason: Some("WITHDRAWAL_TOKEN_CONFIG_MISSING"),
                    eligibility_snapshot: None,
                    payload: serde_json::json!({
                        "reason": "token config missing for instrument",
                    }),
                    allowed_from_states: &["Queued", "Processing"],
                },
            )
            .await?;
            let _ = release_account_withdrawal_reservation(
                state,
                &req.account_id,
                &req.withdrawal_id,
                req.amount_minor,
                &format!(
                    "process-failed:{}:WITHDRAWAL_TOKEN_CONFIG_MISSING",
                    req.contract_id
                ),
                "WITHDRAWAL_TOKEN_CONFIG_MISSING",
            )
            .await?;
            continue;
        };

        account_lock_tx
            .commit()
            .await
            .context("commit withdrawal account advisory lock tx")?;

        let committee_wallet_cid =
            get_or_create_committee_wallet(state, &req.instrument_admin, &req.instrument_id)
                .await?;

        let inputs = reserve_holdings_for_op(
            state,
            &op_id,
            &req.instrument_admin,
            &req.instrument_id,
            req.amount_minor,
            token_cfg.operational_max_inputs_per_transfer,
        )
        .await?;

        if inputs.is_empty() {
            let _ = transition_withdrawal_request_state(
                state,
                &WithdrawalRequestStateTransition {
                    account_id: &req.account_id,
                    withdrawal_id: &req.withdrawal_id,
                    next_state: "Failed",
                    reason: Some("WITHDRAWAL_TREASURY_LIQUIDITY_UNAVAILABLE"),
                    eligibility_snapshot: None,
                    payload: serde_json::json!({
                        "reason": "no available treasury holdings for withdrawal",
                    }),
                    allowed_from_states: &["Queued", "Processing"],
                },
            )
            .await?;
            let _ = release_account_withdrawal_reservation(
                state,
                &req.account_id,
                &req.withdrawal_id,
                req.amount_minor,
                &format!(
                    "process-failed:{}:WITHDRAWAL_TREASURY_LIQUIDITY_UNAVAILABLE",
                    req.contract_id
                ),
                "WITHDRAWAL_TREASURY_LIQUIDITY_UNAVAILABLE",
            )
            .await?;
            continue;
        }

        let next_step_seq = next_command_step(op_head.as_ref(), 0);
        let command_id = withdrawal_command_id(&op_id, "process", next_step_seq);
        let prepared_step_seq = next_step_seq.saturating_sub(1);

        let prepared_update = WithdrawalOpUpdate {
            op_id: op_id.clone(),
            owner_party: req.owner_party.clone(),
            account_id: req.account_id.clone(),
            instrument_admin: req.instrument_admin.clone(),
            instrument_id: req.instrument_id.clone(),
            amount_minor: req.amount_minor,
            state: TREASURY_OP_STATE_PREPARED.to_string(),
            step_seq: prepared_step_seq,
            command_id: command_id.clone(),
            observed_offset: None,
            observed_update_id: None,
            terminal_observed_offset: None,
            terminal_update_id: None,
            lineage_root_instruction_cid: None,
            current_instruction_cid: None,
            input_holding_cids: inputs.clone(),
            receiver_holding_cids: vec![],
            extra: serde_json::json!({
                "phase": "process",
                "withdrawal_request_cid": req.contract_id,
            }),
            last_error: None,
        };
        upsert_withdrawal_treasury_op(state, &prepared_update).await?;
        insert_treasury_op_event(
            state,
            &op_id,
            TREASURY_OP_STATE_PREPARED,
            serde_json::json!({
                "step_seq": prepared_step_seq,
                "command_id": command_id,
                "input_count": inputs.len(),
            }),
        )
        .await?;

        let mut md = serde_json::Map::new();
        md.insert(
            METADATA_ACCOUNT_ID_KEY.to_string(),
            serde_json::Value::String(req.account_id.clone()),
        );
        md.insert(
            METADATA_WITHDRAWAL_ID_KEY.to_string(),
            serde_json::Value::String(req.withdrawal_id.clone()),
        );
        md.insert(
            METADATA_FLOW_KEY.to_string(),
            serde_json::Value::String(METADATA_FLOW_WITHDRAWAL.to_string()),
        );
        let md_val = value_map_text(&md)?;

        let lock_expires_micros = (Utc::now()
            + chrono::Duration::seconds(state.cfg.withdrawal_lock_expires_seconds))
        .timestamp_micros();
        let input_vals = inputs
            .iter()
            .map(|cid| value_contract_id(cid))
            .collect::<Vec<_>>();

        let args = value_record(vec![
            record_field("tokenConfigCid", value_contract_id(&token_cfg.contract_id)),
            record_field("accountRefCid", value_contract_id(&acct.account_ref_cid)),
            record_field(
                "accountStateCid",
                value_contract_id(&acct.account_state_cid),
            ),
            record_field(
                "committeeWalletCid",
                value_contract_id(&committee_wallet_cid),
            ),
            record_field("inputHoldingCids", value_list(input_vals)),
            record_field("metadata", md_val),
            record_field("reason", value_text(&req.account_id)),
            record_field("lockExpiresAt", value_time_micros(lock_expires_micros)),
        ]);

        let template_id = lapi::Identifier {
            package_id: "#pebble".to_string(),
            module_name: "Pebble.Treasury".to_string(),
            entity_name: "WithdrawalRequest".to_string(),
        };

        let cmd = lapi::Command {
            command: Some(lapi::command::Command::Exercise(lapi::ExerciseCommand {
                template_id: Some(template_id),
                contract_id: req.contract_id.clone(),
                choice: "Process".to_string(),
                choice_argument: Some(args),
            })),
        };

        let submitted_update = WithdrawalOpUpdate {
            op_id: op_id.clone(),
            owner_party: req.owner_party.clone(),
            account_id: req.account_id.clone(),
            instrument_admin: req.instrument_admin.clone(),
            instrument_id: req.instrument_id.clone(),
            amount_minor: req.amount_minor,
            state: TREASURY_OP_STATE_SUBMITTED.to_string(),
            step_seq: next_step_seq,
            command_id: command_id.clone(),
            observed_offset: None,
            observed_update_id: None,
            terminal_observed_offset: None,
            terminal_update_id: None,
            lineage_root_instruction_cid: None,
            current_instruction_cid: None,
            input_holding_cids: inputs.clone(),
            receiver_holding_cids: vec![],
            extra: serde_json::json!({
                "phase": "process",
                "withdrawal_request_cid": req.contract_id,
            }),
            last_error: None,
        };
        upsert_withdrawal_treasury_op(state, &submitted_update).await?;
        insert_treasury_op_event(
            state,
            &op_id,
            TREASURY_OP_STATE_SUBMITTED,
            serde_json::json!({
                "step_seq": next_step_seq,
                "command_id": command_id,
                "action": "Process",
                "withdrawal_request_cid": req.contract_id,
            }),
        )
        .await?;

        if let Err(err) = mark_reservations_state(state, &op_id, "Submitting").await {
            let err_msg = error_string(&err);
            let mut failed = submitted_update.clone();
            failed.state = TREASURY_OP_STATE_PREPARED.to_string();
            failed.last_error = Some(err_msg.clone());
            upsert_withdrawal_treasury_op(state, &failed).await?;
            insert_treasury_op_event(
                state,
                &op_id,
                TREASURY_OP_STATE_PREPARED,
                serde_json::json!({
                    "step_seq": next_step_seq,
                    "command_id": command_id,
                    "action": "Process",
                    "retry_scheduled": true,
                    "error": err_msg,
                }),
            )
            .await?;
            tracing::warn!(
                error = ?err,
                op_id = %op_id,
                command_id = %command_id,
                contract_id = %req.contract_id,
                "withdrawal process reservation mark failed"
            );
            let _ = transition_withdrawal_request_state(
                state,
                &WithdrawalRequestStateTransition {
                    account_id: &req.account_id,
                    withdrawal_id: &req.withdrawal_id,
                    next_state: "Failed",
                    reason: Some("WITHDRAWAL_RESERVATION_STATE_FAILURE"),
                    eligibility_snapshot: None,
                    payload: serde_json::json!({
                        "error": err_msg,
                    }),
                    allowed_from_states: &["Queued", "Processing"],
                },
            )
            .await?;
            let _ = release_account_withdrawal_reservation(
                state,
                &req.account_id,
                &req.withdrawal_id,
                req.amount_minor,
                &format!(
                    "process-failed:{}:WITHDRAWAL_RESERVATION_STATE_FAILURE",
                    req.contract_id
                ),
                "WITHDRAWAL_RESERVATION_STATE_FAILURE",
            )
            .await?;
            continue;
        }

        let _ = transition_withdrawal_request_state(
            state,
            &WithdrawalRequestStateTransition {
                account_id: &req.account_id,
                withdrawal_id: &req.withdrawal_id,
                next_state: "Processing",
                reason: None,
                eligibility_snapshot: None,
                payload: serde_json::json!({
                    "phase": "submit",
                    "command_id": command_id,
                }),
                allowed_from_states: &["Queued", "Processing"],
            },
        )
        .await?;

        let act_as = vec![state.cfg.committee_party.clone()];
        let tx = match submit_and_wait_for_transaction(
            &state.cfg,
            command_id.clone(),
            act_as,
            vec![cmd],
        )
        .await
        {
            Ok(tx) => tx,
            Err(err) => {
                let unknown_submit = is_unknown_submission_error(&err);
                let err_msg = error_string(&err);
                let mut failed = submitted_update.clone();
                failed.state = if unknown_submit {
                    TREASURY_OP_STATE_SUBMITTED.to_string()
                } else {
                    TREASURY_OP_STATE_PREPARED.to_string()
                };
                failed.last_error = Some(err_msg.clone());
                upsert_withdrawal_treasury_op(state, &failed).await?;
                insert_treasury_op_event(
                    state,
                    &op_id,
                    if unknown_submit {
                        TREASURY_OP_STATE_SUBMITTED
                    } else {
                        TREASURY_OP_STATE_PREPARED
                    },
                    serde_json::json!({
                        "step_seq": next_step_seq,
                        "command_id": command_id,
                        "action": "Process",
                        "retry_scheduled": !unknown_submit,
                        "submission_outcome_unknown": unknown_submit,
                        "error": err_msg,
                    }),
                )
                .await?;
                if !unknown_submit {
                    if let Err(release_err) =
                        mark_reservations_state(state, &op_id, "Released").await
                    {
                        tracing::warn!(
                            error = ?release_err,
                            op_id = %op_id,
                            command_id = %command_id,
                            "withdrawal process reservation release failed after submit error"
                        );
                    }
                }
                tracing::warn!(
                    error = ?err,
                    op_id = %op_id,
                    command_id = %command_id,
                    contract_id = %req.contract_id,
                    "withdrawal process submit failed"
                );
                let reason_code = if unknown_submit {
                    "WITHDRAWAL_SUBMISSION_UNKNOWN"
                } else {
                    "WITHDRAWAL_SUBMISSION_FAILED"
                };
                let _ = transition_withdrawal_request_state(
                    state,
                    &WithdrawalRequestStateTransition {
                        account_id: &req.account_id,
                        withdrawal_id: &req.withdrawal_id,
                        next_state: "Failed",
                        reason: Some(reason_code),
                        eligibility_snapshot: None,
                        payload: serde_json::json!({
                            "error": err_msg,
                            "submission_outcome_unknown": unknown_submit,
                        }),
                        allowed_from_states: &["Queued", "Processing"],
                    },
                )
                .await?;
                if !unknown_submit {
                    let _ = release_account_withdrawal_reservation(
                        state,
                        &req.account_id,
                        &req.withdrawal_id,
                        req.amount_minor,
                        &format!("process-failed:{}:{}", req.contract_id, reason_code),
                        reason_code,
                    )
                    .await?;
                }
                continue;
            }
        };

        let pendings = extract_created_contract_ids(&tx, "Pebble.Treasury", "WithdrawalPending");
        let committed_update = WithdrawalOpUpdate {
            op_id: op_id.clone(),
            owner_party: req.owner_party.clone(),
            account_id: req.account_id.clone(),
            instrument_admin: req.instrument_admin.clone(),
            instrument_id: req.instrument_id.clone(),
            amount_minor: req.amount_minor,
            state: TREASURY_OP_STATE_COMMITTED.to_string(),
            step_seq: next_step_seq,
            command_id: command_id.clone(),
            observed_offset: Some(tx.offset),
            observed_update_id: Some(tx.update_id.clone()),
            terminal_observed_offset: None,
            terminal_update_id: None,
            lineage_root_instruction_cid: None,
            current_instruction_cid: None,
            input_holding_cids: inputs.clone(),
            receiver_holding_cids: vec![],
            extra: serde_json::json!({
                "phase": "process",
                "withdrawal_request_cid": req.contract_id,
                "pending_count": pendings.len(),
            }),
            last_error: None,
        };
        upsert_withdrawal_treasury_op(state, &committed_update).await?;
        insert_treasury_op_event(
            state,
            &op_id,
            TREASURY_OP_STATE_COMMITTED,
            serde_json::json!({
                "step_seq": next_step_seq,
                "command_id": command_id,
                "action": "Process",
                "offset": tx.offset,
                "update_id": tx.update_id,
                "pending_count": pendings.len(),
            }),
        )
        .await?;

        tracing::info!(
            op_id = %op_id,
            command_id = %command_id,
            account_id = %req.account_id,
            withdrawal_id = %req.withdrawal_id,
            amount_minor = req.amount_minor,
            pending_count = pendings.len(),
            offset = tx.offset,
            update_id = %tx.update_id,
            "withdrawal processed"
        );
    }

    Ok(())
}

async fn withdrawal_reconcile_tick(state: &AppState) -> Result<()> {
    if state.cfg.withdrawal_submission_saga_enabled {
        reconcile_withdrawal_create_intents(state).await?;
    }

    #[derive(sqlx::FromRow)]
    struct PendingRow {
        withdrawal_pending_cid: String,
        owner_party: String,
        account_id: String,
        instrument_admin: String,
        instrument_id: String,
        withdrawal_id: String,
        amount_minor: i64,
        lineage_root_instruction_cid: String,
        current_instruction_cid: String,
        step_seq: i64,
        pending_state: String,
        pending_offset: i64,
        latest_instruction_cid: String,
        latest_instruction_offset: i64,
        latest_instruction_update_id: Option<String>,
        latest_sender_party: String,
        latest_output: String,
        latest_status_class: Option<String>,
        latest_metadata: serde_json::Value,
    }

    let pendings: Vec<PendingRow> = sqlx::query_as(
        r#"
        SELECT
          w.contract_id AS withdrawal_pending_cid,
          w.owner_party,
          w.account_id,
          w.instrument_admin,
          w.instrument_id,
          w.withdrawal_id,
          w.amount_minor,
          w.lineage_root_instruction_cid,
          w.current_instruction_cid,
          w.step_seq,
          w.pending_state,
          w.last_offset AS pending_offset,
          l.contract_id AS latest_instruction_cid,
          ti.last_offset AS latest_instruction_offset,
          lu.update_id AS latest_instruction_update_id,
          ti.sender_party AS latest_sender_party,
          ti.output AS latest_output,
          ti.status_class AS latest_status_class,
          ti.metadata AS latest_metadata
        FROM withdrawal_pendings w
        JOIN token_transfer_instruction_latest l
          ON l.lineage_root_instruction_cid = w.lineage_root_instruction_cid
        JOIN token_transfer_instructions ti
          ON ti.contract_id = l.contract_id
        LEFT JOIN ledger_updates lu
          ON lu.ledger_offset = ti.last_offset
        WHERE w.active = TRUE
          AND w.committee_party = $1
          AND ti.active = TRUE
        ORDER BY w.created_at ASC
        LIMIT 50
        "#,
    )
    .bind(&state.cfg.committee_party)
    .fetch_all(&state.db)
    .await
    .context("query withdrawal pendings")?;

    let template_id = lapi::Identifier {
        package_id: "#pebble".to_string(),
        module_name: "Pebble.Treasury".to_string(),
        entity_name: "WithdrawalPending".to_string(),
    };
    let transfer_instruction_template_id = lapi::Identifier {
        package_id: "#wizardcat-token-standard".to_string(),
        module_name: "Wizardcat.Token.Standard".to_string(),
        entity_name: "TransferInstruction".to_string(),
    };

    for p in pendings {
        let op_id = canonical_withdrawal_op_id(
            &p.owner_party,
            &p.instrument_admin,
            &p.instrument_id,
            &p.withdrawal_id,
        );
        let op_head = get_treasury_op_head(state, &op_id).await?;
        let input_holding_cids = list_op_input_holding_cids(state, &op_id).await?;
        let reconcile_step_seq = op_head
            .as_ref()
            .map(|op| op.step_seq.max(p.step_seq))
            .unwrap_or(p.step_seq);
        let reconcile_command_id = op_head
            .as_ref()
            .map(|op| op.command_id.clone())
            .unwrap_or_else(|| {
                withdrawal_command_id(&op_id, "reconcile-observe", reconcile_step_seq.max(1))
            });
        let escalate_message =
            "withdrawal pending in CancelEscalated state requires manual intervention";
        let reconciled_update = WithdrawalOpUpdate {
            op_id: op_id.clone(),
            owner_party: p.owner_party.clone(),
            account_id: p.account_id.clone(),
            instrument_admin: p.instrument_admin.clone(),
            instrument_id: p.instrument_id.clone(),
            amount_minor: p.amount_minor,
            state: TREASURY_OP_STATE_RECONCILED.to_string(),
            step_seq: reconcile_step_seq,
            command_id: reconcile_command_id.clone(),
            observed_offset: Some(p.latest_instruction_offset),
            observed_update_id: p.latest_instruction_update_id.clone(),
            terminal_observed_offset: None,
            terminal_update_id: None,
            lineage_root_instruction_cid: Some(p.lineage_root_instruction_cid.clone()),
            current_instruction_cid: Some(p.latest_instruction_cid.clone()),
            input_holding_cids: input_holding_cids.clone(),
            receiver_holding_cids: vec![],
            extra: serde_json::json!({
                "phase": "reconcile",
                "withdrawal_pending_cid": p.withdrawal_pending_cid,
                "pending_state": p.pending_state,
                "pending_offset": p.pending_offset,
                "latest_instruction_output": p.latest_output,
                "latest_status_class": p.latest_status_class,
            }),
            last_error: if p.pending_state == "CancelEscalated" {
                Some(escalate_message.to_string())
            } else {
                None
            },
        };
        upsert_withdrawal_treasury_op(state, &reconciled_update).await?;
        let should_emit_reconciled = match op_head.as_ref() {
            Some(op) => {
                op.state != TREASURY_OP_STATE_RECONCILED
                    || op.current_instruction_cid.as_deref()
                        != Some(p.latest_instruction_cid.as_str())
                    || op.observed_offset != Some(p.latest_instruction_offset)
            }
            None => true,
        };
        if should_emit_reconciled {
            insert_treasury_op_event(
                state,
                &op_id,
                TREASURY_OP_STATE_RECONCILED,
                serde_json::json!({
                    "step_seq": reconcile_step_seq,
                    "command_id": reconcile_command_id,
                    "withdrawal_pending_cid": p.withdrawal_pending_cid,
                    "pending_state": p.pending_state,
                    "latest_instruction_cid": p.latest_instruction_cid,
                    "latest_output": p.latest_output,
                    "latest_status_class": p.latest_status_class,
                    "offset": p.latest_instruction_offset,
                    "update_id": p.latest_instruction_update_id,
                }),
            )
            .await?;
        }

        let _ = transition_withdrawal_request_state(
            state,
            &WithdrawalRequestStateTransition {
                account_id: &p.account_id,
                withdrawal_id: &p.withdrawal_id,
                next_state: "Processing",
                reason: Some("WITHDRAWAL_PENDING_OBSERVED"),
                eligibility_snapshot: None,
                payload: serde_json::json!({
                    "pending_state": p.pending_state.clone(),
                    "latest_output": p.latest_output.clone(),
                    "latest_status_class": p.latest_status_class.clone(),
                    "automation_mode": if state.cfg.withdrawal_one_shot_auto_accept_enabled {
                        "OneShotAuto"
                    } else {
                        "LegacyManual"
                    },
                    "auto_accept_attempted": false,
                }),
                allowed_from_states: &["Queued", "Processing", "Failed", "RejectedIneligible"],
            },
        )
        .await?;

        if p.pending_state == "CancelEscalated" {
            // Manual path: do not automate further without external evidence.
            tracing::error!(
                op_id = %op_id,
                account_id = %p.account_id,
                instrument_admin = %p.instrument_admin,
                instrument_id = %p.instrument_id,
                withdrawal_id = %p.withdrawal_id,
                lineage_root_instruction_cid = %p.lineage_root_instruction_cid,
                escalate_message = %escalate_message,
                "withdrawal pending in CancelEscalated state requires manual intervention"
            );
            continue;
        }

        match p.latest_output.as_str() {
            "Pending" => {
                if p.latest_status_class.as_deref() == Some(TRANSFER_PENDING_RECEIVER_ACCEPTANCE) {
                    if p.pending_state != "OfferPending" {
                        tracing::warn!(
                            op_id = %op_id,
                            withdrawal_pending_cid = %p.withdrawal_pending_cid,
                            pending_state = %p.pending_state,
                            latest_instruction_cid = %p.latest_instruction_cid,
                            "receiver-acceptance pending ignored because pending_state is not OfferPending"
                        );
                        continue;
                    }
                    if !state.cfg.withdrawal_one_shot_auto_accept_enabled {
                        continue;
                    }
                    if p.latest_sender_party != state.cfg.committee_party {
                        tracing::warn!(
                            op_id = %op_id,
                            withdrawal_pending_cid = %p.withdrawal_pending_cid,
                            latest_sender_party = %p.latest_sender_party,
                            expected_sender_party = %state.cfg.committee_party,
                            "auto-accept skipped due to sender mismatch"
                        );
                        continue;
                    }

                    let flow = metadata_text(&p.latest_metadata, METADATA_FLOW_KEY);
                    let meta_account_id =
                        metadata_text(&p.latest_metadata, METADATA_ACCOUNT_ID_KEY);
                    let meta_withdrawal_id =
                        metadata_text(&p.latest_metadata, METADATA_WITHDRAWAL_ID_KEY);
                    if flow != Some(METADATA_FLOW_WITHDRAWAL)
                        || meta_account_id != Some(p.account_id.as_str())
                        || meta_withdrawal_id != Some(p.withdrawal_id.as_str())
                    {
                        tracing::warn!(
                            op_id = %op_id,
                            withdrawal_pending_cid = %p.withdrawal_pending_cid,
                            flow = ?flow,
                            meta_account_id = ?meta_account_id,
                            meta_withdrawal_id = ?meta_withdrawal_id,
                            "auto-accept skipped due to invalid withdrawal metadata markers"
                        );
                        continue;
                    }

                    let policy_cid = match find_operator_accept_policy_contract_id(
                        state,
                        &p.instrument_admin,
                        &p.instrument_id,
                        &p.latest_sender_party,
                        &state.cfg.committee_party,
                    )
                    .await?
                    {
                        Some(cid) => cid,
                        None => {
                            if state.cfg.withdrawal_one_shot_require_policy {
                                tracing::warn!(
                                    op_id = %op_id,
                                    withdrawal_pending_cid = %p.withdrawal_pending_cid,
                                    instrument_admin = %p.instrument_admin,
                                    instrument_id = %p.instrument_id,
                                    authorizer_party = %p.latest_sender_party,
                                    operator_party = %state.cfg.committee_party,
                                    "auto-accept skipped: required operator-accept policy missing"
                                );
                                insert_treasury_op_event(
                                    state,
                                    &op_id,
                                    TREASURY_OP_STATE_RECONCILED,
                                    serde_json::json!({
                                        "step_seq": reconcile_step_seq,
                                        "command_id": reconcile_command_id,
                                        "action": "AutoAcceptSkipped",
                                        "reason": "OPERATOR_ACCEPT_POLICY_MISSING",
                                        "withdrawal_pending_cid": p.withdrawal_pending_cid,
                                        "latest_instruction_cid": p.latest_instruction_cid,
                                    }),
                                )
                                .await?;
                            }
                            continue;
                        }
                    };

                    let args = value_record(vec![
                        record_field(
                            "authorizedOperator",
                            value_party(&state.cfg.committee_party),
                        ),
                        record_field("policyCid", value_contract_id(&policy_cid)),
                    ]);

                    let cmd = lapi::Command {
                        command: Some(lapi::command::Command::Exercise(lapi::ExerciseCommand {
                            template_id: Some(transfer_instruction_template_id.clone()),
                            contract_id: p.latest_instruction_cid.clone(),
                            choice: "AcceptByAuthorizedOperator".to_string(),
                            choice_argument: Some(args),
                        })),
                    };

                    let next_step_seq = next_command_step(op_head.as_ref(), reconcile_step_seq);
                    let command_id = withdrawal_command_id(&op_id, "auto-accept", next_step_seq);
                    let submitted_update = WithdrawalOpUpdate {
                        op_id: op_id.clone(),
                        owner_party: p.owner_party.clone(),
                        account_id: p.account_id.clone(),
                        instrument_admin: p.instrument_admin.clone(),
                        instrument_id: p.instrument_id.clone(),
                        amount_minor: p.amount_minor,
                        state: TREASURY_OP_STATE_SUBMITTED.to_string(),
                        step_seq: next_step_seq,
                        command_id: command_id.clone(),
                        observed_offset: Some(p.latest_instruction_offset),
                        observed_update_id: p.latest_instruction_update_id.clone(),
                        terminal_observed_offset: None,
                        terminal_update_id: None,
                        lineage_root_instruction_cid: Some(p.lineage_root_instruction_cid.clone()),
                        current_instruction_cid: Some(p.latest_instruction_cid.clone()),
                        input_holding_cids: input_holding_cids.clone(),
                        receiver_holding_cids: vec![],
                        extra: serde_json::json!({
                            "phase": "reconcile",
                            "action": "AcceptByAuthorizedOperator",
                            "withdrawal_pending_cid": p.withdrawal_pending_cid,
                            "policy_cid": policy_cid,
                        }),
                        last_error: None,
                    };
                    upsert_withdrawal_treasury_op(state, &submitted_update).await?;
                    insert_treasury_op_event(
                        state,
                        &op_id,
                        TREASURY_OP_STATE_SUBMITTED,
                        serde_json::json!({
                            "step_seq": next_step_seq,
                            "command_id": command_id,
                            "action": "AcceptByAuthorizedOperator",
                            "withdrawal_pending_cid": p.withdrawal_pending_cid,
                            "instruction_cid": p.latest_instruction_cid,
                            "policy_cid": policy_cid,
                        }),
                    )
                    .await?;

                    let act_as = vec![state.cfg.committee_party.clone()];
                    match submit_and_wait_for_transaction(
                        &state.cfg,
                        command_id.clone(),
                        act_as,
                        vec![cmd],
                    )
                    .await
                    {
                        Ok(tx) => {
                            let committed_update = WithdrawalOpUpdate {
                                op_id: op_id.clone(),
                                owner_party: p.owner_party.clone(),
                                account_id: p.account_id.clone(),
                                instrument_admin: p.instrument_admin.clone(),
                                instrument_id: p.instrument_id.clone(),
                                amount_minor: p.amount_minor,
                                state: TREASURY_OP_STATE_COMMITTED.to_string(),
                                step_seq: next_step_seq,
                                command_id: command_id.clone(),
                                observed_offset: Some(tx.offset),
                                observed_update_id: Some(tx.update_id.clone()),
                                terminal_observed_offset: None,
                                terminal_update_id: None,
                                lineage_root_instruction_cid: Some(
                                    p.lineage_root_instruction_cid.clone(),
                                ),
                                current_instruction_cid: Some(p.latest_instruction_cid.clone()),
                                input_holding_cids: input_holding_cids.clone(),
                                receiver_holding_cids: vec![],
                                extra: serde_json::json!({
                                    "phase": "reconcile",
                                    "action": "AcceptByAuthorizedOperator",
                                    "withdrawal_pending_cid": p.withdrawal_pending_cid,
                                }),
                                last_error: None,
                            };
                            upsert_withdrawal_treasury_op(state, &committed_update).await?;
                            insert_treasury_op_event(
                                state,
                                &op_id,
                                TREASURY_OP_STATE_COMMITTED,
                                serde_json::json!({
                                    "step_seq": next_step_seq,
                                    "command_id": command_id,
                                    "action": "AcceptByAuthorizedOperator",
                                    "offset": tx.offset,
                                    "update_id": tx.update_id,
                                }),
                            )
                            .await?;
                        }
                        Err(err) => {
                            if is_idempotent_contract_advance_error(&err) {
                                tracing::info!(
                                    error = ?err,
                                    op_id = %op_id,
                                    command_id = %command_id,
                                    withdrawal_pending_cid = %p.withdrawal_pending_cid,
                                    "auto-accept treated as idempotent already-advanced outcome"
                                );
                                continue;
                            }
                            let err_msg = error_string(&err);
                            let mut failed = submitted_update.clone();
                            failed.last_error = Some(err_msg.clone());
                            upsert_withdrawal_treasury_op(state, &failed).await?;
                            insert_treasury_op_event(
                                state,
                                &op_id,
                                TREASURY_OP_STATE_SUBMITTED,
                                serde_json::json!({
                                    "step_seq": next_step_seq,
                                    "command_id": command_id,
                                    "action": "AcceptByAuthorizedOperator",
                                    "error": err_msg,
                                }),
                            )
                            .await?;
                            tracing::warn!(
                                error = ?err,
                                op_id = %op_id,
                                command_id = %command_id,
                                withdrawal_pending_cid = %p.withdrawal_pending_cid,
                                "withdrawal auto-accept submit failed"
                            );
                        }
                    }
                    continue;
                }

                if p.latest_status_class.as_deref() != Some(TRANSFER_PENDING_INTERNAL_WORKFLOW) {
                    tracing::warn!(
                        op_id = %op_id,
                        withdrawal_pending_cid = %p.withdrawal_pending_cid,
                        latest_instruction_cid = %p.latest_instruction_cid,
                        latest_status_class = ?p.latest_status_class,
                        "pending withdrawal instruction ignored due to unsupported status class"
                    );
                    continue;
                }

                if p.latest_instruction_cid == p.current_instruction_cid {
                    continue;
                }

                let args = value_record(vec![record_field(
                    "nextInstructionCid",
                    value_contract_id(&p.latest_instruction_cid),
                )]);

                let cmd = lapi::Command {
                    command: Some(lapi::command::Command::Exercise(lapi::ExerciseCommand {
                        template_id: Some(template_id.clone()),
                        contract_id: p.withdrawal_pending_cid.clone(),
                        choice: "AdvanceWithdrawalPending".to_string(),
                        choice_argument: Some(args),
                    })),
                };

                let next_step_seq = next_command_step(op_head.as_ref(), reconcile_step_seq);
                let command_id = withdrawal_command_id(&op_id, "advance-pending", next_step_seq);
                let submitted_update = WithdrawalOpUpdate {
                    op_id: op_id.clone(),
                    owner_party: p.owner_party.clone(),
                    account_id: p.account_id.clone(),
                    instrument_admin: p.instrument_admin.clone(),
                    instrument_id: p.instrument_id.clone(),
                    amount_minor: p.amount_minor,
                    state: TREASURY_OP_STATE_SUBMITTED.to_string(),
                    step_seq: next_step_seq,
                    command_id: command_id.clone(),
                    observed_offset: Some(p.latest_instruction_offset),
                    observed_update_id: p.latest_instruction_update_id.clone(),
                    terminal_observed_offset: None,
                    terminal_update_id: None,
                    lineage_root_instruction_cid: Some(p.lineage_root_instruction_cid.clone()),
                    current_instruction_cid: Some(p.latest_instruction_cid.clone()),
                    input_holding_cids: input_holding_cids.clone(),
                    receiver_holding_cids: vec![],
                    extra: serde_json::json!({
                        "phase": "reconcile",
                        "action": "AdvanceWithdrawalPending",
                        "withdrawal_pending_cid": p.withdrawal_pending_cid,
                    }),
                    last_error: None,
                };
                upsert_withdrawal_treasury_op(state, &submitted_update).await?;
                insert_treasury_op_event(
                    state,
                    &op_id,
                    TREASURY_OP_STATE_SUBMITTED,
                    serde_json::json!({
                        "step_seq": next_step_seq,
                        "command_id": command_id,
                        "action": "AdvanceWithdrawalPending",
                        "withdrawal_pending_cid": p.withdrawal_pending_cid,
                        "next_instruction_cid": p.latest_instruction_cid,
                    }),
                )
                .await?;

                let act_as = vec![state.cfg.committee_party.clone()];
                match submit_and_wait_for_transaction(
                    &state.cfg,
                    command_id.clone(),
                    act_as,
                    vec![cmd],
                )
                .await
                {
                    Ok(tx) => {
                        let committed_update = WithdrawalOpUpdate {
                            op_id: op_id.clone(),
                            owner_party: p.owner_party.clone(),
                            account_id: p.account_id.clone(),
                            instrument_admin: p.instrument_admin.clone(),
                            instrument_id: p.instrument_id.clone(),
                            amount_minor: p.amount_minor,
                            state: TREASURY_OP_STATE_COMMITTED.to_string(),
                            step_seq: next_step_seq,
                            command_id: command_id.clone(),
                            observed_offset: Some(tx.offset),
                            observed_update_id: Some(tx.update_id.clone()),
                            terminal_observed_offset: None,
                            terminal_update_id: None,
                            lineage_root_instruction_cid: Some(
                                p.lineage_root_instruction_cid.clone(),
                            ),
                            current_instruction_cid: Some(p.latest_instruction_cid.clone()),
                            input_holding_cids: input_holding_cids.clone(),
                            receiver_holding_cids: vec![],
                            extra: serde_json::json!({
                                "phase": "reconcile",
                                "action": "AdvanceWithdrawalPending",
                                "withdrawal_pending_cid": p.withdrawal_pending_cid,
                            }),
                            last_error: None,
                        };
                        upsert_withdrawal_treasury_op(state, &committed_update).await?;
                        insert_treasury_op_event(
                            state,
                            &op_id,
                            TREASURY_OP_STATE_COMMITTED,
                            serde_json::json!({
                                "step_seq": next_step_seq,
                                "command_id": command_id,
                                "action": "AdvanceWithdrawalPending",
                                "offset": tx.offset,
                                "update_id": tx.update_id,
                            }),
                        )
                        .await?;
                    }
                    Err(err) => {
                        let err_msg = error_string(&err);
                        let mut failed = submitted_update.clone();
                        failed.last_error = Some(err_msg.clone());
                        upsert_withdrawal_treasury_op(state, &failed).await?;
                        insert_treasury_op_event(
                            state,
                            &op_id,
                            TREASURY_OP_STATE_SUBMITTED,
                            serde_json::json!({
                                "step_seq": next_step_seq,
                                "command_id": command_id,
                                "action": "AdvanceWithdrawalPending",
                                "error": err_msg,
                            }),
                        )
                        .await?;
                        tracing::warn!(
                            error = ?err,
                            op_id = %op_id,
                            command_id = %command_id,
                            withdrawal_pending_cid = %p.withdrawal_pending_cid,
                            "withdrawal advance submit failed"
                        );
                    }
                }
            }
            "Completed" => {
                let args = value_record(vec![record_field(
                    "terminalInstructionCid",
                    value_contract_id(&p.latest_instruction_cid),
                )]);

                let cmd = lapi::Command {
                    command: Some(lapi::command::Command::Exercise(lapi::ExerciseCommand {
                        template_id: Some(template_id.clone()),
                        contract_id: p.withdrawal_pending_cid.clone(),
                        choice: "FinalizeAccepted".to_string(),
                        choice_argument: Some(args),
                    })),
                };

                let next_step_seq = next_command_step(op_head.as_ref(), reconcile_step_seq);
                let command_id = withdrawal_command_id(&op_id, "finalize-accepted", next_step_seq);
                let submitted_update = WithdrawalOpUpdate {
                    op_id: op_id.clone(),
                    owner_party: p.owner_party.clone(),
                    account_id: p.account_id.clone(),
                    instrument_admin: p.instrument_admin.clone(),
                    instrument_id: p.instrument_id.clone(),
                    amount_minor: p.amount_minor,
                    state: TREASURY_OP_STATE_SUBMITTED.to_string(),
                    step_seq: next_step_seq,
                    command_id: command_id.clone(),
                    observed_offset: Some(p.latest_instruction_offset),
                    observed_update_id: p.latest_instruction_update_id.clone(),
                    terminal_observed_offset: None,
                    terminal_update_id: None,
                    lineage_root_instruction_cid: Some(p.lineage_root_instruction_cid.clone()),
                    current_instruction_cid: Some(p.latest_instruction_cid.clone()),
                    input_holding_cids: input_holding_cids.clone(),
                    receiver_holding_cids: vec![],
                    extra: serde_json::json!({
                        "phase": "reconcile",
                        "action": "FinalizeAccepted",
                        "withdrawal_pending_cid": p.withdrawal_pending_cid,
                    }),
                    last_error: None,
                };
                upsert_withdrawal_treasury_op(state, &submitted_update).await?;
                insert_treasury_op_event(
                    state,
                    &op_id,
                    TREASURY_OP_STATE_SUBMITTED,
                    serde_json::json!({
                        "step_seq": next_step_seq,
                        "command_id": command_id,
                        "action": "FinalizeAccepted",
                        "withdrawal_pending_cid": p.withdrawal_pending_cid,
                        "terminal_instruction_cid": p.latest_instruction_cid,
                    }),
                )
                .await?;

                let act_as = vec![state.cfg.committee_party.clone()];
                match submit_and_wait_for_transaction(
                    &state.cfg,
                    command_id.clone(),
                    act_as,
                    vec![cmd],
                )
                .await
                {
                    Ok(tx) => {
                        let committed_update = WithdrawalOpUpdate {
                            op_id: op_id.clone(),
                            owner_party: p.owner_party.clone(),
                            account_id: p.account_id.clone(),
                            instrument_admin: p.instrument_admin.clone(),
                            instrument_id: p.instrument_id.clone(),
                            amount_minor: p.amount_minor,
                            state: TREASURY_OP_STATE_COMMITTED.to_string(),
                            step_seq: next_step_seq,
                            command_id: command_id.clone(),
                            observed_offset: Some(tx.offset),
                            observed_update_id: Some(tx.update_id.clone()),
                            terminal_observed_offset: None,
                            terminal_update_id: None,
                            lineage_root_instruction_cid: Some(
                                p.lineage_root_instruction_cid.clone(),
                            ),
                            current_instruction_cid: Some(p.latest_instruction_cid.clone()),
                            input_holding_cids: input_holding_cids.clone(),
                            receiver_holding_cids: vec![],
                            extra: serde_json::json!({
                                "phase": "reconcile",
                                "action": "FinalizeAccepted",
                                "withdrawal_pending_cid": p.withdrawal_pending_cid,
                            }),
                            last_error: None,
                        };
                        upsert_withdrawal_treasury_op(state, &committed_update).await?;
                        insert_treasury_op_event(
                            state,
                            &op_id,
                            TREASURY_OP_STATE_COMMITTED,
                            serde_json::json!({
                                "step_seq": next_step_seq,
                                "command_id": command_id,
                                "action": "FinalizeAccepted",
                                "offset": tx.offset,
                                "update_id": tx.update_id,
                            }),
                        )
                        .await?;
                    }
                    Err(err) => {
                        let err_msg = error_string(&err);
                        let mut failed = submitted_update.clone();
                        failed.last_error = Some(err_msg.clone());
                        upsert_withdrawal_treasury_op(state, &failed).await?;
                        insert_treasury_op_event(
                            state,
                            &op_id,
                            TREASURY_OP_STATE_SUBMITTED,
                            serde_json::json!({
                                "step_seq": next_step_seq,
                                "command_id": command_id,
                                "action": "FinalizeAccepted",
                                "error": err_msg,
                            }),
                        )
                        .await?;
                        tracing::warn!(
                            error = ?err,
                            op_id = %op_id,
                            command_id = %command_id,
                            withdrawal_pending_cid = %p.withdrawal_pending_cid,
                            "withdrawal finalize-accepted submit failed"
                        );
                    }
                }
            }
            "Failed" => {
                let account_state_cid: Option<(String,)> = sqlx::query_as(
                    r#"
                    SELECT contract_id
                    FROM account_state_latest
                    WHERE account_id = $1
                    LIMIT 1
                    "#,
                )
                .bind(&p.account_id)
                .fetch_optional(&state.db)
                .await
                .context("query latest account state")?;
                let Some((account_state_cid,)) = account_state_cid else {
                    continue;
                };

                let args = value_record(vec![
                    record_field("accountStateCid", value_contract_id(&account_state_cid)),
                    record_field(
                        "terminalInstructionCid",
                        value_contract_id(&p.latest_instruction_cid),
                    ),
                ]);

                let cmd = lapi::Command {
                    command: Some(lapi::command::Command::Exercise(lapi::ExerciseCommand {
                        template_id: Some(template_id.clone()),
                        contract_id: p.withdrawal_pending_cid.clone(),
                        choice: "FinalizeRejected".to_string(),
                        choice_argument: Some(args),
                    })),
                };

                let next_step_seq = next_command_step(op_head.as_ref(), reconcile_step_seq);
                let command_id = withdrawal_command_id(&op_id, "finalize-rejected", next_step_seq);
                let submitted_update = WithdrawalOpUpdate {
                    op_id: op_id.clone(),
                    owner_party: p.owner_party.clone(),
                    account_id: p.account_id.clone(),
                    instrument_admin: p.instrument_admin.clone(),
                    instrument_id: p.instrument_id.clone(),
                    amount_minor: p.amount_minor,
                    state: TREASURY_OP_STATE_SUBMITTED.to_string(),
                    step_seq: next_step_seq,
                    command_id: command_id.clone(),
                    observed_offset: Some(p.latest_instruction_offset),
                    observed_update_id: p.latest_instruction_update_id.clone(),
                    terminal_observed_offset: None,
                    terminal_update_id: None,
                    lineage_root_instruction_cid: Some(p.lineage_root_instruction_cid.clone()),
                    current_instruction_cid: Some(p.latest_instruction_cid.clone()),
                    input_holding_cids: input_holding_cids.clone(),
                    receiver_holding_cids: vec![],
                    extra: serde_json::json!({
                        "phase": "reconcile",
                        "action": "FinalizeRejected",
                        "withdrawal_pending_cid": p.withdrawal_pending_cid,
                    }),
                    last_error: None,
                };
                upsert_withdrawal_treasury_op(state, &submitted_update).await?;
                insert_treasury_op_event(
                    state,
                    &op_id,
                    TREASURY_OP_STATE_SUBMITTED,
                    serde_json::json!({
                        "step_seq": next_step_seq,
                        "command_id": command_id,
                        "action": "FinalizeRejected",
                        "withdrawal_pending_cid": p.withdrawal_pending_cid,
                        "terminal_instruction_cid": p.latest_instruction_cid,
                    }),
                )
                .await?;

                let act_as = vec![state.cfg.committee_party.clone()];
                match submit_and_wait_for_transaction(
                    &state.cfg,
                    command_id.clone(),
                    act_as,
                    vec![cmd],
                )
                .await
                {
                    Ok(tx) => {
                        let committed_update = WithdrawalOpUpdate {
                            op_id: op_id.clone(),
                            owner_party: p.owner_party.clone(),
                            account_id: p.account_id.clone(),
                            instrument_admin: p.instrument_admin.clone(),
                            instrument_id: p.instrument_id.clone(),
                            amount_minor: p.amount_minor,
                            state: TREASURY_OP_STATE_COMMITTED.to_string(),
                            step_seq: next_step_seq,
                            command_id: command_id.clone(),
                            observed_offset: Some(tx.offset),
                            observed_update_id: Some(tx.update_id.clone()),
                            terminal_observed_offset: None,
                            terminal_update_id: None,
                            lineage_root_instruction_cid: Some(
                                p.lineage_root_instruction_cid.clone(),
                            ),
                            current_instruction_cid: Some(p.latest_instruction_cid.clone()),
                            input_holding_cids: input_holding_cids.clone(),
                            receiver_holding_cids: vec![],
                            extra: serde_json::json!({
                                "phase": "reconcile",
                                "action": "FinalizeRejected",
                                "withdrawal_pending_cid": p.withdrawal_pending_cid,
                            }),
                            last_error: None,
                        };
                        upsert_withdrawal_treasury_op(state, &committed_update).await?;
                        insert_treasury_op_event(
                            state,
                            &op_id,
                            TREASURY_OP_STATE_COMMITTED,
                            serde_json::json!({
                                "step_seq": next_step_seq,
                                "command_id": command_id,
                                "action": "FinalizeRejected",
                                "offset": tx.offset,
                                "update_id": tx.update_id,
                            }),
                        )
                        .await?;
                    }
                    Err(err) => {
                        let err_msg = error_string(&err);
                        let mut failed = submitted_update.clone();
                        failed.last_error = Some(err_msg.clone());
                        upsert_withdrawal_treasury_op(state, &failed).await?;
                        insert_treasury_op_event(
                            state,
                            &op_id,
                            TREASURY_OP_STATE_SUBMITTED,
                            serde_json::json!({
                                "step_seq": next_step_seq,
                                "command_id": command_id,
                                "action": "FinalizeRejected",
                                "error": err_msg,
                            }),
                        )
                        .await?;
                        tracing::warn!(
                            error = ?err,
                            op_id = %op_id,
                            command_id = %command_id,
                            withdrawal_pending_cid = %p.withdrawal_pending_cid,
                            "withdrawal finalize-rejected submit failed"
                        );
                    }
                }
            }
            other => {
                tracing::warn!(
                    op_id = %op_id,
                    withdrawal_pending_cid = %p.withdrawal_pending_cid,
                    latest_output = %other,
                    "unexpected withdrawal latest output"
                );
            }
        }
    }

    #[derive(sqlx::FromRow)]
    struct ReceiptRow {
        withdrawal_receipt_cid: String,
        owner_party: String,
        account_id: String,
        instrument_admin: String,
        instrument_id: String,
        withdrawal_id: String,
        amount_minor: i64,
        status: String,
        lineage_root_instruction_cid: String,
        terminal_instruction_cid: String,
        receipt_offset: i64,
        receipt_update_id: Option<String>,
    }

    let receipts: Vec<ReceiptRow> = sqlx::query_as(
        r#"
        SELECT
          wr.contract_id AS withdrawal_receipt_cid,
          wr.owner_party,
          wr.account_id,
          wr.instrument_admin,
          wr.instrument_id,
          wr.withdrawal_id,
          wr.amount_minor,
          wr.status,
          wr.lineage_root_instruction_cid,
          wr.terminal_instruction_cid,
          wr.last_offset AS receipt_offset,
          lu.update_id AS receipt_update_id
        FROM withdrawal_receipts wr
        LEFT JOIN ledger_updates lu
          ON lu.ledger_offset = wr.last_offset
        LEFT JOIN treasury_ops o
          ON o.op_id = (
            'withdrawal:'
            || wr.owner_party
            || ':'
            || wr.instrument_admin
            || ':'
            || wr.instrument_id
            || ':'
            || wr.withdrawal_id
          )
        WHERE wr.active = TRUE
          AND wr.committee_party = $1
          AND (o.op_id IS NULL OR o.state <> $2)
        ORDER BY wr.created_at ASC
        LIMIT 100
        "#,
    )
    .bind(&state.cfg.committee_party)
    .bind(TREASURY_OP_STATE_DONE)
    .fetch_all(&state.db)
    .await
    .context("query unresolved withdrawal receipts for treasury ops")?;

    for r in receipts {
        let op_id = canonical_withdrawal_op_id(
            &r.owner_party,
            &r.instrument_admin,
            &r.instrument_id,
            &r.withdrawal_id,
        );

        let _ = transition_withdrawal_request_state(
            state,
            &WithdrawalRequestStateTransition {
                account_id: &r.account_id,
                withdrawal_id: &r.withdrawal_id,
                next_state: "Processed",
                reason: Some("WITHDRAWAL_RECEIPT_OBSERVED"),
                eligibility_snapshot: None,
                payload: serde_json::json!({
                    "receipt_status": r.status.clone(),
                    "receipt_offset": r.receipt_offset,
                    "receipt_contract_id": r.withdrawal_receipt_cid.clone(),
                }),
                allowed_from_states: &["Queued", "Processing", "Failed", "RejectedIneligible"],
            },
        )
        .await?;
        let _ = release_account_withdrawal_reservation(
            state,
            &r.account_id,
            &r.withdrawal_id,
            r.amount_minor,
            &format!("receipt:{}", r.withdrawal_receipt_cid),
            "WITHDRAWAL_RECEIPT_OBSERVED",
        )
        .await?;

        mark_reservations_state(state, &op_id, "Released").await?;

        let op_head = get_treasury_op_head(state, &op_id).await?;
        let input_holding_cids = list_op_input_holding_cids(state, &op_id).await?;
        let done_step_seq = op_head.as_ref().map(|op| op.step_seq.max(1)).unwrap_or(1);
        let done_command_id = op_head
            .as_ref()
            .map(|op| op.command_id.clone())
            .unwrap_or_else(|| withdrawal_command_id(&op_id, "done", done_step_seq));
        let should_emit_done = match op_head.as_ref() {
            Some(op) => {
                op.state != TREASURY_OP_STATE_DONE
                    || op.terminal_observed_offset != Some(r.receipt_offset)
            }
            None => true,
        };

        let done_update = WithdrawalOpUpdate {
            op_id: op_id.clone(),
            owner_party: r.owner_party.clone(),
            account_id: r.account_id.clone(),
            instrument_admin: r.instrument_admin.clone(),
            instrument_id: r.instrument_id.clone(),
            amount_minor: r.amount_minor,
            state: TREASURY_OP_STATE_DONE.to_string(),
            step_seq: done_step_seq,
            command_id: done_command_id.clone(),
            observed_offset: Some(r.receipt_offset),
            observed_update_id: r.receipt_update_id.clone(),
            terminal_observed_offset: Some(r.receipt_offset),
            terminal_update_id: r.receipt_update_id.clone(),
            lineage_root_instruction_cid: Some(r.lineage_root_instruction_cid.clone()),
            current_instruction_cid: Some(r.terminal_instruction_cid.clone()),
            input_holding_cids: input_holding_cids.clone(),
            receiver_holding_cids: vec![],
            extra: serde_json::json!({
                "phase": "reconcile",
                "action": "ReceiptObserved",
                "withdrawal_receipt_cid": r.withdrawal_receipt_cid,
                "withdrawal_receipt_status": r.status,
            }),
            last_error: None,
        };
        upsert_withdrawal_treasury_op(state, &done_update).await?;
        if should_emit_done {
            insert_treasury_op_event(
                state,
                &op_id,
                TREASURY_OP_STATE_DONE,
                serde_json::json!({
                    "step_seq": done_step_seq,
                    "command_id": done_command_id,
                    "action": "ReceiptObserved",
                    "withdrawal_receipt_cid": r.withdrawal_receipt_cid,
                    "withdrawal_receipt_status": r.status,
                    "terminal_instruction_cid": r.terminal_instruction_cid,
                    "offset": r.receipt_offset,
                    "update_id": r.receipt_update_id,
                }),
            )
            .await?;
        }

        tracing::info!(
            op_id = %op_id,
            command_id = %done_update.command_id,
            account_id = %r.account_id,
            withdrawal_id = %r.withdrawal_id,
            withdrawal_status = %r.status,
            offset = r.receipt_offset,
            "withdrawal op done"
        );
    }

    Ok(())
}

async fn withdrawal_cancel_loop(state: AppState) -> Result<()> {
    let poll_ms = std::env::var("PEBBLE_TREASURY_WITHDRAWAL_CANCEL_POLL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1000);

    loop {
        if let Err(err) = withdrawal_cancel_tick(&state).await {
            tracing::warn!(error = ?err, "withdrawal cancel tick failed");
        }
        tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
    }
}

#[derive(Debug, Clone)]
struct WithdrawalCancelCandidate {
    withdrawal_pending_cid: String,
    owner_party: String,
    account_id: String,
    instrument_admin: String,
    instrument_id: String,
    withdrawal_id: String,
    amount_minor: i64,
    step_seq: i64,
    lineage_root_instruction_cid: String,
    current_instruction_cid: String,
    evidence_offset: Option<i64>,
    evidence_update_id: Option<String>,
}

async fn submit_withdrawal_cancel_candidate(
    state: &AppState,
    candidate: &WithdrawalCancelCandidate,
    trigger: &str,
    trigger_detail: serde_json::Value,
) -> Result<()> {
    // Pre-check: only attempt cancel while the locked holding is still committee-owned.
    let locked_holding: Option<(Option<String>,)> = sqlx::query_as(
        r#"
        SELECT locked_holding_cid
        FROM token_transfer_instructions
        WHERE contract_id = $1
          AND active = TRUE
        LIMIT 1
        "#,
    )
    .bind(&candidate.current_instruction_cid)
    .fetch_optional(&state.db)
    .await
    .context("query current token instruction")?;

    let locked_holding_cid = locked_holding.and_then(|(cid,)| cid);
    let Some(locked_holding_cid) = locked_holding_cid else {
        return Ok(());
    };

    let holding_owner: Option<(String,)> = sqlx::query_as(
        r#"
        SELECT owner_party
        FROM token_holdings
        WHERE contract_id = $1
          AND active = TRUE
        LIMIT 1
        "#,
    )
    .bind(&locked_holding_cid)
    .fetch_optional(&state.db)
    .await
    .context("query locked holding owner")?;
    let Some((owner_party,)) = holding_owner else {
        return Ok(());
    };
    if owner_party != state.cfg.committee_party {
        return Ok(());
    }

    let account_state_cid: Option<(String,)> = sqlx::query_as(
        r#"
        SELECT contract_id
        FROM account_state_latest
        WHERE account_id = $1
        LIMIT 1
        "#,
    )
    .bind(&candidate.account_id)
    .fetch_optional(&state.db)
    .await
    .context("query latest account state")?;
    let Some((account_state_cid,)) = account_state_cid else {
        return Ok(());
    };

    let template_id = lapi::Identifier {
        package_id: "#pebble".to_string(),
        module_name: "Pebble.Treasury".to_string(),
        entity_name: "WithdrawalPending".to_string(),
    };

    let args = value_record(vec![record_field(
        "accountStateCid",
        value_contract_id(&account_state_cid),
    )]);

    let cmd = lapi::Command {
        command: Some(lapi::command::Command::Exercise(lapi::ExerciseCommand {
            template_id: Some(template_id),
            contract_id: candidate.withdrawal_pending_cid.clone(),
            choice: "CancelAndMaybeRefund".to_string(),
            choice_argument: Some(args),
        })),
    };

    let op_id = canonical_withdrawal_op_id(
        &candidate.owner_party,
        &candidate.instrument_admin,
        &candidate.instrument_id,
        &candidate.withdrawal_id,
    );
    let op_head = get_treasury_op_head(state, &op_id).await?;
    let input_holding_cids = list_op_input_holding_cids(state, &op_id).await?;
    let next_step_seq = next_command_step(op_head.as_ref(), candidate.step_seq);
    let command_id = withdrawal_command_id(&op_id, "cancel", next_step_seq);

    let submitted_update = WithdrawalOpUpdate {
        op_id: op_id.clone(),
        owner_party: candidate.owner_party.clone(),
        account_id: candidate.account_id.clone(),
        instrument_admin: candidate.instrument_admin.clone(),
        instrument_id: candidate.instrument_id.clone(),
        amount_minor: candidate.amount_minor,
        state: TREASURY_OP_STATE_SUBMITTED.to_string(),
        step_seq: next_step_seq,
        command_id: command_id.clone(),
        observed_offset: candidate.evidence_offset,
        observed_update_id: candidate.evidence_update_id.clone(),
        terminal_observed_offset: None,
        terminal_update_id: None,
        lineage_root_instruction_cid: Some(candidate.lineage_root_instruction_cid.clone()),
        current_instruction_cid: Some(candidate.current_instruction_cid.clone()),
        input_holding_cids: input_holding_cids.clone(),
        receiver_holding_cids: vec![],
        extra: serde_json::json!({
            "phase": "cancel",
            "action": "CancelAndMaybeRefund",
            "trigger": trigger,
            "trigger_detail": trigger_detail,
            "locked_holding_cid": locked_holding_cid,
            "withdrawal_pending_cid": candidate.withdrawal_pending_cid,
        }),
        last_error: None,
    };
    upsert_withdrawal_treasury_op(state, &submitted_update).await?;
    insert_treasury_op_event(
        state,
        &op_id,
        TREASURY_OP_STATE_SUBMITTED,
        serde_json::json!({
            "step_seq": next_step_seq,
            "command_id": command_id,
            "action": "CancelAndMaybeRefund",
            "trigger": trigger,
            "locked_holding_cid": locked_holding_cid,
            "withdrawal_pending_cid": candidate.withdrawal_pending_cid,
        }),
    )
    .await?;

    let act_as = vec![state.cfg.committee_party.clone()];
    match submit_and_wait_for_transaction(&state.cfg, command_id.clone(), act_as, vec![cmd]).await {
        Ok(tx) => {
            let committed_update = WithdrawalOpUpdate {
                op_id: op_id.clone(),
                owner_party: candidate.owner_party.clone(),
                account_id: candidate.account_id.clone(),
                instrument_admin: candidate.instrument_admin.clone(),
                instrument_id: candidate.instrument_id.clone(),
                amount_minor: candidate.amount_minor,
                state: TREASURY_OP_STATE_COMMITTED.to_string(),
                step_seq: next_step_seq,
                command_id: command_id.clone(),
                observed_offset: Some(tx.offset),
                observed_update_id: Some(tx.update_id.clone()),
                terminal_observed_offset: None,
                terminal_update_id: None,
                lineage_root_instruction_cid: Some(candidate.lineage_root_instruction_cid.clone()),
                current_instruction_cid: Some(candidate.current_instruction_cid.clone()),
                input_holding_cids,
                receiver_holding_cids: vec![],
                extra: serde_json::json!({
                    "phase": "cancel",
                    "action": "CancelAndMaybeRefund",
                    "trigger": trigger,
                    "withdrawal_pending_cid": candidate.withdrawal_pending_cid,
                }),
                last_error: None,
            };
            upsert_withdrawal_treasury_op(state, &committed_update).await?;
            insert_treasury_op_event(
                state,
                &op_id,
                TREASURY_OP_STATE_COMMITTED,
                serde_json::json!({
                    "step_seq": next_step_seq,
                    "command_id": command_id,
                    "action": "CancelAndMaybeRefund",
                    "trigger": trigger,
                    "offset": tx.offset,
                    "update_id": tx.update_id,
                }),
            )
            .await?;
            tracing::info!(
                op_id = %op_id,
                command_id = %command_id,
                account_id = %candidate.account_id,
                withdrawal_id = %candidate.withdrawal_id,
                offset = tx.offset,
                update_id = %tx.update_id,
                "withdrawal cancel submitted"
            );
        }
        Err(err) => {
            let err_msg = error_string(&err);
            let mut failed = submitted_update.clone();
            failed.last_error = Some(err_msg.clone());
            upsert_withdrawal_treasury_op(state, &failed).await?;
            insert_treasury_op_event(
                state,
                &op_id,
                TREASURY_OP_STATE_SUBMITTED,
                serde_json::json!({
                    "step_seq": next_step_seq,
                    "command_id": command_id,
                    "action": "CancelAndMaybeRefund",
                    "trigger": trigger,
                    "error": err_msg,
                }),
            )
            .await?;
            tracing::warn!(
                error = ?err,
                op_id = %op_id,
                command_id = %command_id,
                withdrawal_pending_cid = %candidate.withdrawal_pending_cid,
                "withdrawal cancel submit failed"
            );
        }
    }

    Ok(())
}

async fn withdrawal_lock_reclaim_loop(state: AppState) -> Result<()> {
    loop {
        if let Err(err) = withdrawal_lock_reclaim_tick(&state).await {
            tracing::warn!(error = ?err, "withdrawal lock reclaim tick failed");
        }
        tokio::time::sleep(std::time::Duration::from_millis(
            state.cfg.withdrawal_lock_reclaim_poll_ms,
        ))
        .await;
    }
}

async fn withdrawal_lock_reclaim_tick(state: &AppState) -> Result<()> {
    #[derive(sqlx::FromRow)]
    struct LockExpiredRow {
        withdrawal_pending_cid: String,
        owner_party: String,
        account_id: String,
        instrument_admin: String,
        instrument_id: String,
        withdrawal_id: String,
        amount_minor: i64,
        step_seq: i64,
        lineage_root_instruction_cid: String,
        current_instruction_cid: String,
        instruction_offset: i64,
        instruction_update_id: Option<String>,
        locked_holding_cid: String,
        lock_expires_at: chrono::DateTime<Utc>,
    }

    let mut dbtx = state.db.begin().await.context("begin db transaction")?;
    let watermark = watermark_offset(&mut dbtx, &state.cfg.submission_path).await?;
    dbtx.rollback().await.ok();
    if watermark <= 0 {
        return Ok(());
    }

    let candidates: Vec<LockExpiredRow> = sqlx::query_as(
        r#"
        SELECT
          w.contract_id AS withdrawal_pending_cid,
          w.owner_party,
          w.account_id,
          w.instrument_admin,
          w.instrument_id,
          w.withdrawal_id,
          w.amount_minor,
          w.step_seq,
          w.lineage_root_instruction_cid,
          w.current_instruction_cid,
          ti.last_offset AS instruction_offset,
          lu.update_id AS instruction_update_id,
          ti.locked_holding_cid,
          h.lock_expires_at
        FROM withdrawal_pendings w
        JOIN token_transfer_instruction_latest l
          ON l.lineage_root_instruction_cid = w.lineage_root_instruction_cid
        JOIN token_transfer_instructions ti
          ON ti.contract_id = w.current_instruction_cid
        LEFT JOIN ledger_updates lu
          ON lu.ledger_offset = ti.last_offset
        JOIN token_holdings h
          ON h.contract_id = ti.locked_holding_cid
        WHERE w.active = TRUE
          AND w.committee_party = $1
          AND w.pending_state = 'OfferPending'
          AND l.contract_id = w.current_instruction_cid
          AND ti.active = TRUE
          AND ti.output = 'Pending'
          AND ti.last_offset <= $2
          AND h.active = TRUE
          AND h.owner_party = $1
          AND h.lock_status_class IS NOT NULL
          AND h.lock_expires_at IS NOT NULL
          AND now() > h.lock_expires_at + make_interval(secs => $3)
        ORDER BY h.lock_expires_at ASC, w.created_at ASC
        LIMIT 25
        "#,
    )
    .bind(&state.cfg.committee_party)
    .bind(watermark)
    .bind(state.cfg.lock_expiry_skew_seconds)
    .fetch_all(&state.db)
    .await
    .context("query lock-expired withdrawal candidates")?;

    for row in candidates {
        let candidate = WithdrawalCancelCandidate {
            withdrawal_pending_cid: row.withdrawal_pending_cid.clone(),
            owner_party: row.owner_party.clone(),
            account_id: row.account_id.clone(),
            instrument_admin: row.instrument_admin.clone(),
            instrument_id: row.instrument_id.clone(),
            withdrawal_id: row.withdrawal_id.clone(),
            amount_minor: row.amount_minor,
            step_seq: row.step_seq,
            lineage_root_instruction_cid: row.lineage_root_instruction_cid.clone(),
            current_instruction_cid: row.current_instruction_cid.clone(),
            evidence_offset: Some(row.instruction_offset),
            evidence_update_id: row.instruction_update_id.clone(),
        };

        let trigger_detail = serde_json::json!({
            "locked_holding_cid": row.locked_holding_cid,
            "lock_expires_at": row.lock_expires_at,
            "lock_expiry_skew_seconds": state.cfg.lock_expiry_skew_seconds,
            "watermark_offset": watermark,
        });
        if let Err(err) = submit_withdrawal_cancel_candidate(
            state,
            &candidate,
            "LockExpiredReclaim",
            trigger_detail,
        )
        .await
        {
            tracing::warn!(
                error = ?err,
                withdrawal_pending_cid = %candidate.withdrawal_pending_cid,
                "withdrawal lock-expiry reclaim failed"
            );
        }
    }

    Ok(())
}

async fn withdrawal_cancel_tick(state: &AppState) -> Result<()> {
    #[derive(sqlx::FromRow)]
    struct CancelRow {
        withdrawal_pending_cid: String,
        owner_party: String,
        account_id: String,
        instrument_admin: String,
        instrument_id: String,
        withdrawal_id: String,
        amount_minor: i64,
        step_seq: i64,
        lineage_root_instruction_cid: String,
        current_instruction_cid: String,
        pending_offset: i64,
        pending_update_id: Option<String>,
    }

    // Dev-only behavior: treat withdrawal IDs containing "cancel" as auto-cancel requests.
    let candidates: Vec<CancelRow> = sqlx::query_as(
        r#"
        SELECT
          w.contract_id AS withdrawal_pending_cid,
          w.owner_party,
          w.account_id,
          w.instrument_admin,
          w.instrument_id,
          w.withdrawal_id,
          w.amount_minor,
          w.step_seq,
          w.lineage_root_instruction_cid,
          w.current_instruction_cid,
          w.last_offset AS pending_offset,
          lu.update_id AS pending_update_id
        FROM withdrawal_pendings w
        LEFT JOIN ledger_updates lu
          ON lu.ledger_offset = w.last_offset
        WHERE w.active = TRUE
          AND w.committee_party = $1
          AND w.pending_state = 'OfferPending'
          AND w.withdrawal_id ILIKE '%cancel%'
        ORDER BY w.created_at ASC
        LIMIT 25
        "#,
    )
    .bind(&state.cfg.committee_party)
    .fetch_all(&state.db)
    .await
    .context("query cancel candidates")?;

    if candidates.is_empty() {
        return Ok(());
    }

    for c in candidates {
        let candidate = WithdrawalCancelCandidate {
            withdrawal_pending_cid: c.withdrawal_pending_cid.clone(),
            owner_party: c.owner_party.clone(),
            account_id: c.account_id.clone(),
            instrument_admin: c.instrument_admin.clone(),
            instrument_id: c.instrument_id.clone(),
            withdrawal_id: c.withdrawal_id.clone(),
            amount_minor: c.amount_minor,
            step_seq: c.step_seq,
            lineage_root_instruction_cid: c.lineage_root_instruction_cid.clone(),
            current_instruction_cid: c.current_instruction_cid.clone(),
            evidence_offset: Some(c.pending_offset),
            evidence_update_id: c.pending_update_id.clone(),
        };
        let trigger_detail = serde_json::json!({
            "mode": "dev_auto_cancel",
            "withdrawal_pending_cid": c.withdrawal_pending_cid,
        });
        if let Err(err) =
            submit_withdrawal_cancel_candidate(state, &candidate, "DevAutoCancel", trigger_detail)
                .await
        {
            tracing::warn!(
                error = ?err,
                withdrawal_pending_cid = %candidate.withdrawal_pending_cid,
                "withdrawal dev cancel failed"
            );
        }
    }

    Ok(())
}

async fn consolidation_loop(state: AppState) -> Result<()> {
    let poll_ms = std::env::var("PEBBLE_TREASURY_CONSOLIDATION_POLL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1000);

    loop {
        if let Err(err) = consolidation_tick(&state).await {
            tracing::warn!(error = ?err, "consolidation tick failed");
        }
        tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
    }
}

async fn consolidation_tick(state: &AppState) -> Result<()> {
    #[derive(sqlx::FromRow)]
    struct TokenCfgRow {
        instrument_admin: String,
        instrument_id: String,
        operational_max_inputs_per_transfer: Option<i64>,
        target_utxo_count_max: Option<i64>,
    }

    let cfgs: Vec<TokenCfgRow> = sqlx::query_as(
        r#"
        SELECT DISTINCT ON (instrument_admin, instrument_id)
          instrument_admin,
          instrument_id,
          operational_max_inputs_per_transfer,
          target_utxo_count_max
        FROM token_configs
        WHERE active = TRUE
          AND committee_party = $1
        ORDER BY instrument_admin, instrument_id, last_offset DESC
        "#,
    )
    .bind(&state.cfg.committee_party)
    .fetch_all(&state.db)
    .await
    .context("query token configs for consolidation")?;

    for cfg in cfgs {
        let Some(op_max_inputs) = cfg.operational_max_inputs_per_transfer else {
            continue;
        };
        let Some(target_max) = cfg.target_utxo_count_max else {
            continue;
        };

        let mut dbtx = state.db.begin().await.context("begin db transaction")?;
        let watermark = watermark_offset(&mut dbtx, &state.cfg.submission_path).await?;
        dbtx.rollback().await.ok();
        if watermark <= 0 {
            continue;
        }

        let count_row: Option<(i64,)> = sqlx::query_as(
            r#"
            SELECT count(*)::bigint
            FROM token_holdings h
            WHERE h.active = TRUE
              AND h.owner_party = $1
              AND h.instrument_admin = $2
              AND h.instrument_id = $3
              AND h.lock_status_class IS NULL
              AND h.last_offset <= $4
              AND NOT EXISTS (
                SELECT 1
                FROM treasury_holding_reservations r
                WHERE r.holding_cid = h.contract_id
                  AND r.state IN ('Reserved', 'Submitting')
              )
              AND NOT EXISTS (
                SELECT 1
                FROM deposit_pendings p
                WHERE p.active = TRUE
                  AND p.lineage_root_instruction_cid = h.origin_instruction_cid
              )
            "#,
        )
        .bind(&state.cfg.committee_party)
        .bind(&cfg.instrument_admin)
        .bind(&cfg.instrument_id)
        .bind(watermark)
        .fetch_optional(&state.db)
        .await
        .context("count unlocked holdings")?;

        let count = count_row.map(|(c,)| c).unwrap_or(0);
        if count <= target_max {
            continue;
        }

        let batch_inputs = op_max_inputs.clamp(2, 50);
        let op_id = format!(
            "consolidation:{}:{}:{}",
            cfg.instrument_admin,
            cfg.instrument_id,
            uuid::Uuid::new_v4()
        );
        let op_head = get_treasury_op_head(state, &op_id).await?;

        let inputs = reserve_smallest_holdings_for_op(
            state,
            &op_id,
            &cfg.instrument_admin,
            &cfg.instrument_id,
            batch_inputs,
        )
        .await?;
        if inputs.is_empty() {
            continue;
        }

        let wallet_cid =
            get_or_create_committee_wallet(state, &cfg.instrument_admin, &cfg.instrument_id)
                .await?;

        let template_id = lapi::Identifier {
            package_id: "#wizardcat-token-standard".to_string(),
            module_name: "Wizardcat.Token.Standard".to_string(),
            entity_name: "Wallet".to_string(),
        };

        let input_vals = inputs
            .iter()
            .map(|cid| value_contract_id(cid))
            .collect::<Vec<_>>();
        let args = value_record(vec![record_field(
            "inputHoldingCids",
            value_list(input_vals),
        )]);

        let cmd = lapi::Command {
            command: Some(lapi::command::Command::Exercise(lapi::ExerciseCommand {
                template_id: Some(template_id),
                contract_id: wallet_cid,
                choice: "Consolidate".to_string(),
                choice_argument: Some(args),
            })),
        };

        let next_step_seq = next_command_step(op_head.as_ref(), 0);
        let command_id = canonical_op_command_id(&op_id, next_step_seq);
        let prepared_step_seq = next_step_seq.saturating_sub(1);

        let prepared_update = TreasuryOpUpdate {
            op_id: op_id.clone(),
            op_type: TREASURY_OP_TYPE_CONSOLIDATION.to_string(),
            owner_party: Some(state.cfg.committee_party.clone()),
            account_id: None,
            instrument_admin: cfg.instrument_admin.clone(),
            instrument_id: cfg.instrument_id.clone(),
            amount_minor: None,
            state: TREASURY_OP_STATE_PREPARED.to_string(),
            step_seq: prepared_step_seq,
            command_id: command_id.clone(),
            observed_offset: None,
            observed_update_id: None,
            terminal_observed_offset: None,
            terminal_update_id: None,
            lineage_root_instruction_cid: None,
            current_instruction_cid: None,
            input_holding_cids: inputs.clone(),
            receiver_holding_cids: vec![],
            extra: serde_json::json!({
                "phase": "consolidation",
                "holdings_before": count,
                "target_max": target_max,
            }),
            last_error: None,
        };
        upsert_treasury_op(state, &prepared_update).await?;
        insert_treasury_op_event(
            state,
            &op_id,
            TREASURY_OP_STATE_PREPARED,
            serde_json::json!({
                "step_seq": prepared_step_seq,
                "command_id": command_id,
                "action": "Consolidate",
                "input_count": inputs.len(),
                "holdings_before": count,
                "target_max": target_max,
            }),
        )
        .await?;

        let submitted_update = TreasuryOpUpdate {
            op_id: op_id.clone(),
            op_type: TREASURY_OP_TYPE_CONSOLIDATION.to_string(),
            owner_party: Some(state.cfg.committee_party.clone()),
            account_id: None,
            instrument_admin: cfg.instrument_admin.clone(),
            instrument_id: cfg.instrument_id.clone(),
            amount_minor: None,
            state: TREASURY_OP_STATE_SUBMITTED.to_string(),
            step_seq: next_step_seq,
            command_id: command_id.clone(),
            observed_offset: None,
            observed_update_id: None,
            terminal_observed_offset: None,
            terminal_update_id: None,
            lineage_root_instruction_cid: None,
            current_instruction_cid: None,
            input_holding_cids: inputs.clone(),
            receiver_holding_cids: vec![],
            extra: serde_json::json!({
                "phase": "consolidation",
                "holdings_before": count,
                "target_max": target_max,
            }),
            last_error: None,
        };
        upsert_treasury_op(state, &submitted_update).await?;
        insert_treasury_op_event(
            state,
            &op_id,
            TREASURY_OP_STATE_SUBMITTED,
            serde_json::json!({
                "step_seq": next_step_seq,
                "command_id": command_id,
                "action": "Consolidate",
                "input_count": inputs.len(),
                "holdings_before": count,
                "target_max": target_max,
            }),
        )
        .await?;

        if let Err(err) = mark_reservations_state(state, &op_id, "Submitting").await {
            let err_msg = error_string(&err);
            let mut failed = submitted_update.clone();
            failed.last_error = Some(err_msg.clone());
            upsert_treasury_op(state, &failed).await?;
            insert_treasury_op_event(
                state,
                &op_id,
                TREASURY_OP_STATE_SUBMITTED,
                serde_json::json!({
                    "step_seq": next_step_seq,
                    "command_id": command_id,
                    "action": "Consolidate",
                    "error": err_msg,
                }),
            )
            .await?;
            tracing::warn!(
                error = ?err,
                op_id = %op_id,
                command_id = %command_id,
                "consolidation reservation mark failed"
            );
            continue;
        }

        let act_as = vec![state.cfg.committee_party.clone()];

        match submit_and_wait_for_transaction(&state.cfg, command_id.clone(), act_as, vec![cmd])
            .await
        {
            Ok(tx) => {
                let committed_update = TreasuryOpUpdate {
                    op_id: op_id.clone(),
                    op_type: TREASURY_OP_TYPE_CONSOLIDATION.to_string(),
                    owner_party: Some(state.cfg.committee_party.clone()),
                    account_id: None,
                    instrument_admin: cfg.instrument_admin.clone(),
                    instrument_id: cfg.instrument_id.clone(),
                    amount_minor: None,
                    state: TREASURY_OP_STATE_COMMITTED.to_string(),
                    step_seq: next_step_seq,
                    command_id: command_id.clone(),
                    observed_offset: Some(tx.offset),
                    observed_update_id: Some(tx.update_id.clone()),
                    terminal_observed_offset: None,
                    terminal_update_id: None,
                    lineage_root_instruction_cid: None,
                    current_instruction_cid: None,
                    input_holding_cids: inputs.clone(),
                    receiver_holding_cids: vec![],
                    extra: serde_json::json!({
                        "phase": "consolidation",
                        "holdings_before": count,
                        "target_max": target_max,
                    }),
                    last_error: None,
                };
                upsert_treasury_op(state, &committed_update).await?;
                insert_treasury_op_event(
                    state,
                    &op_id,
                    TREASURY_OP_STATE_COMMITTED,
                    serde_json::json!({
                        "step_seq": next_step_seq,
                        "command_id": command_id,
                        "action": "Consolidate",
                        "offset": tx.offset,
                        "update_id": tx.update_id,
                        "input_count": inputs.len(),
                    }),
                )
                .await?;

                if let Err(err) = mark_reservations_state(state, &op_id, "Released").await {
                    let err_msg = error_string(&err);
                    let mut failed = committed_update.clone();
                    failed.last_error = Some(err_msg.clone());
                    upsert_treasury_op(state, &failed).await?;
                    insert_treasury_op_event(
                        state,
                        &op_id,
                        TREASURY_OP_STATE_COMMITTED,
                        serde_json::json!({
                            "step_seq": next_step_seq,
                            "command_id": command_id,
                            "action": "Consolidate",
                            "error": err_msg,
                        }),
                    )
                    .await?;
                    tracing::warn!(
                        error = ?err,
                        op_id = %op_id,
                        command_id = %command_id,
                        "consolidation reservation release failed"
                    );
                    continue;
                }

                let done_update = TreasuryOpUpdate {
                    op_id: op_id.clone(),
                    op_type: TREASURY_OP_TYPE_CONSOLIDATION.to_string(),
                    owner_party: Some(state.cfg.committee_party.clone()),
                    account_id: None,
                    instrument_admin: cfg.instrument_admin.clone(),
                    instrument_id: cfg.instrument_id.clone(),
                    amount_minor: None,
                    state: TREASURY_OP_STATE_DONE.to_string(),
                    step_seq: next_step_seq,
                    command_id: command_id.clone(),
                    observed_offset: Some(tx.offset),
                    observed_update_id: Some(tx.update_id.clone()),
                    terminal_observed_offset: Some(tx.offset),
                    terminal_update_id: Some(tx.update_id.clone()),
                    lineage_root_instruction_cid: None,
                    current_instruction_cid: None,
                    input_holding_cids: inputs.clone(),
                    receiver_holding_cids: vec![],
                    extra: serde_json::json!({
                        "phase": "consolidation",
                        "holdings_before": count,
                        "target_max": target_max,
                    }),
                    last_error: None,
                };
                upsert_treasury_op(state, &done_update).await?;
                insert_treasury_op_event(
                    state,
                    &op_id,
                    TREASURY_OP_STATE_DONE,
                    serde_json::json!({
                        "step_seq": next_step_seq,
                        "command_id": command_id,
                        "action": "Consolidate",
                        "offset": tx.offset,
                        "update_id": tx.update_id,
                        "input_count": inputs.len(),
                    }),
                )
                .await?;
                tracing::info!(
                    op_id = %op_id,
                    command_id = %command_id,
                    instrument_admin = %cfg.instrument_admin,
                    instrument_id = %cfg.instrument_id,
                    input_count = inputs.len(),
                    holdings_before = count,
                    target_max = target_max,
                    offset = tx.offset,
                    update_id = %tx.update_id,
                    "consolidation done"
                );
            }
            Err(err) => {
                let err_msg = error_string(&err);
                let mut failed = submitted_update.clone();
                failed.last_error = Some(err_msg.clone());
                upsert_treasury_op(state, &failed).await?;
                insert_treasury_op_event(
                    state,
                    &op_id,
                    TREASURY_OP_STATE_SUBMITTED,
                    serde_json::json!({
                        "step_seq": next_step_seq,
                        "command_id": command_id,
                        "action": "Consolidate",
                        "error": err_msg,
                    }),
                )
                .await?;
                tracing::warn!(
                    error = ?err,
                    op_id = %op_id,
                    command_id = %command_id,
                    "consolidation submission failed"
                );
                let _ = mark_reservations_state(state, &op_id, "Released").await;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_instrument_threshold_map_accepts_valid_entries() {
        let parsed =
            parse_instrument_threshold_map("PEBBLE_TEST", "AdminA|USD=5, AdminB|EUR=12").unwrap();

        assert_eq!(
            parsed.get(&InstrumentKey {
                instrument_admin: "AdminA".to_string(),
                instrument_id: "USD".to_string(),
            }),
            Some(&5)
        );
        assert_eq!(
            parsed.get(&InstrumentKey {
                instrument_admin: "AdminB".to_string(),
                instrument_id: "EUR".to_string(),
            }),
            Some(&12)
        );
    }

    #[test]
    fn parse_instrument_threshold_map_rejects_duplicate_instrument() {
        let err = parse_instrument_threshold_map("PEBBLE_TEST", "AdminA|USD=5,AdminA|USD=6")
            .unwrap_err()
            .to_string();
        assert!(err.contains("duplicate instrument entry"));
    }

    #[test]
    fn parse_instrument_threshold_map_rejects_invalid_shape() {
        assert!(parse_instrument_threshold_map("PEBBLE_TEST", "AdminA|USD").is_err());
        assert!(parse_instrument_threshold_map("PEBBLE_TEST", "AdminA=1").is_err());
    }

    #[test]
    fn withdrawal_claim_auto_command_id_is_deterministic_and_bounded() {
        let claim_cid = "00abcdef1234567890";
        let command_id_a = withdrawal_claim_auto_command_id(claim_cid);
        let command_id_b = withdrawal_claim_auto_command_id(claim_cid);

        assert_eq!(command_id_a, command_id_b);
        assert!(command_id_a.starts_with("wd-claim:auto:"));
        assert!(command_id_a.len() <= MAX_BOUNDED_COMMAND_ID_LEN);
    }
}
