use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    time::Duration,
};

use anyhow::{anyhow, Context as _, Result};
use axum::{routing::get, Json, Router};
use chrono::Utc;
use pebble_daml_grpc::com::daml::ledger::api::v2 as lapi;
use pebble_ids::{parse_account_id, parse_batch_hash, validate_positive_epoch};
use sqlx::{postgres::PgPoolOptions, PgPool, Postgres, Transaction};
use tonic::{metadata::MetadataValue, transport::Endpoint, Request};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const MARKET_LOCK_NAMESPACE: i32 = 12;

#[tokio::main]
async fn main() -> Result<()> {
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

    sqlx::migrate!("../../infra/sql/migrations")
        .run(&db)
        .await
        .context("run migrations")?;

    upsert_account_lock_capability(
        &db,
        "clearing",
        cfg.account_lock_namespace,
        cfg.active_withdrawals_enabled,
        &serde_json::json!({
            "account_writer_enabled": cfg.account_writer_enabled,
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

    let state = AppState { db, cfg };
    let addr = state.cfg.addr;

    {
        let s = state.clone();
        tokio::spawn(async move {
            if let Err(err) = clearing_loop(s).await {
                tracing::error!(error = ?err, "clearing loop crashed");
            }
        });
    }

    let app = Router::new()
        .route("/healthz", get(healthz))
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    tracing::info!(%addr, "pebble-clearing listening");

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
    committee_party: String,
    ledger_user_id: String,
    auth_header: Option<MetadataValue<tonic::metadata::Ascii>>,
    ledger_channel: tonic::transport::Channel,
    enable_apply: bool,
    poll_ms: u64,
    engine_version: String,
    apply_delay_ms: u64,
    max_applies_per_tick: usize,
    market_settlement_enable: bool,
    market_settlement_dry_run: bool,
    market_settlement_require_zero_sum: bool,
    account_lock_namespace: i32,
    active_withdrawals_enabled: bool,
    account_writer_enabled: bool,
}

impl Config {
    async fn from_env() -> Result<Self> {
        let addr: SocketAddr = std::env::var("PEBBLE_CLEARING_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:3050".to_string())
            .parse()
            .context("parse PEBBLE_CLEARING_ADDR")?;

        let db_url = std::env::var("PEBBLE_DB_URL")
            .unwrap_or_else(|_| "postgres://pebble:pebble@127.0.0.1:5432/pebble".to_string());

        let committee_party = std::env::var("PEBBLE_COMMITTEE_PARTY")
            .context("PEBBLE_COMMITTEE_PARTY is required")?;
        let ledger_user_id = std::env::var("PEBBLE_CLEARING_LEDGER_USER_ID")
            .unwrap_or_else(|_| "pebble-clearing".to_string());

        let ledger_host = std::env::var("PEBBLE_CANTON_LEDGER_API_HOST")
            .unwrap_or_else(|_| "127.0.0.1".to_string());
        let ledger_port = std::env::var("PEBBLE_CANTON_LEDGER_API_PORT")
            .unwrap_or_else(|_| "6865".to_string())
            .parse::<u16>()
            .context("parse PEBBLE_CANTON_LEDGER_API_PORT")?;

        let auth_header = match std::env::var("PEBBLE_CANTON_AUTH_TOKEN") {
            Ok(token) if !token.trim().is_empty() => Some(parse_auth_header(&token)?),
            _ => None,
        };

        let enable_apply = match std::env::var("PEBBLE_CLEARING_ENABLE_APPLY") {
            Ok(v) => parse_env_bool("PEBBLE_CLEARING_ENABLE_APPLY", &v)?,
            Err(_) => false,
        };

        let poll_ms = std::env::var("PEBBLE_CLEARING_POLL_MS")
            .unwrap_or_else(|_| "60000".to_string())
            .parse::<u64>()
            .context("parse PEBBLE_CLEARING_POLL_MS")?;
        if poll_ms == 0 {
            return Err(anyhow!("PEBBLE_CLEARING_POLL_MS must be > 0"));
        }

        let engine_version = std::env::var("PEBBLE_CLEARING_ENGINE_VERSION")
            .unwrap_or_else(|_| "m3-dev-v1".to_string());
        if engine_version.trim().is_empty() {
            return Err(anyhow!("PEBBLE_CLEARING_ENGINE_VERSION must be non-empty"));
        }

        let apply_delay_ms = std::env::var("PEBBLE_CLEARING_APPLY_DELAY_MS")
            .unwrap_or_else(|_| "0".to_string())
            .parse::<u64>()
            .context("parse PEBBLE_CLEARING_APPLY_DELAY_MS")?;

        let max_applies_per_tick = std::env::var("PEBBLE_CLEARING_MAX_APPLIES_PER_TICK")
            .unwrap_or_else(|_| "0".to_string())
            .parse::<usize>()
            .context("parse PEBBLE_CLEARING_MAX_APPLIES_PER_TICK")?;

        let market_settlement_enable = match std::env::var("PEBBLE_MARKET_SETTLEMENT_ENABLE") {
            Ok(v) => parse_env_bool("PEBBLE_MARKET_SETTLEMENT_ENABLE", &v)?,
            Err(_) => true,
        };
        let market_settlement_dry_run = match std::env::var("PEBBLE_MARKET_SETTLEMENT_DRY_RUN") {
            Ok(v) => parse_env_bool("PEBBLE_MARKET_SETTLEMENT_DRY_RUN", &v)?,
            Err(_) => false,
        };
        let market_settlement_require_zero_sum =
            match std::env::var("PEBBLE_MARKET_SETTLEMENT_REQUIRE_ZERO_SUM") {
                Ok(v) => parse_env_bool("PEBBLE_MARKET_SETTLEMENT_REQUIRE_ZERO_SUM", &v)?,
                Err(_) => true,
            };
        let account_lock_namespace = parse_account_lock_namespace_env()?;
        let active_withdrawals_disabled = match std::env::var("PEBBLE_ACTIVE_WITHDRAWALS_DISABLED")
        {
            Ok(v) => parse_env_bool("PEBBLE_ACTIVE_WITHDRAWALS_DISABLED", &v)?,
            Err(_) => false,
        };
        let active_withdrawals_enabled = !active_withdrawals_disabled;
        let account_writer_enabled = match std::env::var("PEBBLE_ACCOUNT_WRITER_ENABLE") {
            Ok(v) => parse_env_bool("PEBBLE_ACCOUNT_WRITER_ENABLE", &v)?,
            Err(_) => false,
        };

        let endpoint = Endpoint::from_shared(format!("http://{}:{}", ledger_host, ledger_port))
            .context("build ledger endpoint")?;
        let ledger_channel = endpoint.connect().await.context("connect to ledger")?;

        Ok(Self {
            addr,
            db_url,
            committee_party,
            ledger_user_id,
            auth_header,
            ledger_channel,
            enable_apply,
            poll_ms,
            engine_version,
            apply_delay_ms,
            max_applies_per_tick,
            market_settlement_enable,
            market_settlement_dry_run,
            market_settlement_require_zero_sum,
            account_lock_namespace,
            active_withdrawals_enabled,
            account_writer_enabled,
        })
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

async fn healthz(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({
      "status": "ok",
      "now": Utc::now(),
      "enable_apply": state.cfg.enable_apply,
      "max_applies_per_tick": state.cfg.max_applies_per_tick,
      "market_settlement_enable": state.cfg.market_settlement_enable,
      "market_settlement_dry_run": state.cfg.market_settlement_dry_run,
      "market_settlement_require_zero_sum": state.cfg.market_settlement_require_zero_sum,
      "account_lock_namespace": state.cfg.account_lock_namespace,
      "active_withdrawals_enabled": state.cfg.active_withdrawals_enabled,
      "account_writer_enabled": state.cfg.account_writer_enabled,
    }))
}

async fn clearing_loop(state: AppState) -> Result<()> {
    let period = Duration::from_millis(state.cfg.poll_ms);

    loop {
        if let Err(err) = clearing_tick(&state).await {
            tracing::warn!(error = ?err, "clearing tick failed");
        }
        tokio::time::sleep(period).await;
    }
}

#[derive(sqlx::FromRow)]
struct ClearingHeadRow {
    instrument_admin: String,
    instrument_id: String,
    clearing_head_cid: String,
    next_epoch: i64,
    prev_batch_hash: String,
    engine_version: String,
}

#[derive(sqlx::FromRow)]
struct DeltaRow {
    delta_minor: i64,
    source_fill_count: i32,
}

#[derive(sqlx::FromRow)]
struct ExistingEpochRow {
    status: String,
    anchor_contract_id: Option<String>,
}

#[derive(sqlx::FromRow)]
struct CatchupDeltaRow {
    account_id: String,
    delta_minor: i64,
}

#[derive(sqlx::FromRow)]
struct UnsettledFillRow {
    fill_id: String,
    maker_account_id: String,
    taker_account_id: String,
    price_ticks: i64,
    quantity_minor: i64,
    taker_side: String,
}

#[derive(sqlx::FromRow)]
struct PendingMarketSettlementJobRow {
    market_id: String,
}

#[derive(sqlx::FromRow)]
struct MarketSettlementJobComputeRow {
    market_contract_id: String,
    instrument_admin: String,
    instrument_id: String,
    resolved_outcome: String,
    payout_per_share_minor: i64,
    state: String,
}

#[derive(sqlx::FromRow)]
struct MarketSettlementMarketRow {
    status: String,
    resolved_outcome: Option<String>,
}

#[derive(sqlx::FromRow)]
struct OpenOrderForSettlementRow {
    order_id: String,
    account_id: String,
    instrument_admin: String,
    instrument_id: String,
    locked_minor: i64,
}

#[derive(sqlx::FromRow, Clone)]
struct SettlementPositionRow {
    account_id: String,
    outcome: String,
    net_quantity_minor: i64,
}

#[derive(Clone)]
struct ComputedSettlementDelta {
    account_id: String,
    delta_minor: i64,
    source_position_count: i32,
}

#[derive(sqlx::FromRow)]
struct SettlementDeltaForEpochRow {
    account_id: String,
    delta_minor: i64,
}

#[derive(sqlx::FromRow)]
struct QueuedSettlementJobRow {
    market_id: String,
}

#[derive(sqlx::FromRow)]
struct AwaitingApplyJobRow {
    market_id: String,
    instrument_admin: String,
    instrument_id: String,
    target_epoch: i64,
}

#[derive(sqlx::FromRow)]
struct JobAccountDeltaRow {
    account_id: String,
}

#[derive(sqlx::FromRow)]
struct AccountApplyStatusRow {
    status: String,
}

#[derive(sqlx::FromRow)]
struct AccountStateEpochRow {
    last_applied_epoch: i64,
}

#[derive(sqlx::FromRow)]
struct RecoveryCandidateRow {
    account_id: String,
}

#[derive(sqlx::FromRow)]
struct AccountStateRecoveryRow {
    contract_id: String,
    last_applied_epoch: i64,
    last_applied_batch_anchor: Option<String>,
}

#[derive(sqlx::FromRow)]
struct PendingTradeDriftSummaryRow {
    drifted_accounts: i64,
    total_drift_minor: i64,
    max_abs_drift_minor: i64,
}

#[derive(sqlx::FromRow)]
struct PendingTradeDriftDetailRow {
    account_id: String,
    observed_delta_pending_trades_minor: i64,
    expected_pending_fill_delta_minor: i64,
    drift_minor: i64,
}

#[derive(sqlx::FromRow)]
struct AppliedJobRow {
    market_id: String,
    resolved_outcome: String,
}

async fn clearing_tick(state: &AppState) -> Result<()> {
    if state.cfg.market_settlement_enable {
        process_pending_market_settlement_jobs(state).await?;
    }

    let heads: Vec<ClearingHeadRow> = sqlx::query_as(
        r#"
        SELECT
          ch.instrument_admin,
          ch.instrument_id,
          ch.contract_id AS clearing_head_cid,
          ch.next_epoch,
          ch.prev_batch_hash,
          ch.engine_version
        FROM clearing_head_latest latest
        JOIN clearing_heads ch ON ch.contract_id = latest.contract_id
        WHERE ch.active = TRUE
        ORDER BY ch.instrument_admin, ch.instrument_id
        "#,
    )
    .fetch_all(&state.db)
    .await
    .context("query active clearing heads")?;

    for head in heads {
        if let Err(err) = process_instrument_epoch(state, &head).await {
            tracing::warn!(
                error = ?err,
                instrument_admin = %head.instrument_admin,
                instrument_id = %head.instrument_id,
                epoch = head.next_epoch,
                "instrument clearing tick failed"
            );
        }
    }

    if state.cfg.market_settlement_enable {
        process_awaiting_apply_market_settlement_jobs(state).await?;
        finalize_applied_market_settlement_jobs(state).await?;
    }

    Ok(())
}

async fn process_pending_market_settlement_jobs(state: &AppState) -> Result<()> {
    let rows: Vec<PendingMarketSettlementJobRow> = sqlx::query_as(
        r#"
        SELECT market_id
        FROM market_settlement_jobs
        WHERE state = 'Pending'
        ORDER BY created_at ASC, market_id ASC
        LIMIT 64
        "#,
    )
    .fetch_all(&state.db)
    .await
    .context("query pending market settlement jobs")?;

    for row in rows {
        if let Err(err) = compute_pending_market_settlement_job(state, &row.market_id).await {
            tracing::warn!(
                error = ?err,
                market_id = %row.market_id,
                "failed to compute market settlement job"
            );
        }
    }

    Ok(())
}

async fn compute_pending_market_settlement_job(state: &AppState, market_id: &str) -> Result<()> {
    let compute_result = compute_pending_market_settlement_job_tx(state, market_id).await;
    if let Err(err) = compute_result {
        mark_market_settlement_job_failed(&state.db, market_id, &err.to_string()).await?;
    }
    Ok(())
}

async fn compute_pending_market_settlement_job_tx(state: &AppState, market_id: &str) -> Result<()> {
    let mut tx = state
        .db
        .begin()
        .await
        .context("begin market settlement compute tx")?;

    lock_market(&mut tx, market_id).await?;

    let job: Option<MarketSettlementJobComputeRow> = sqlx::query_as(
        r#"
        SELECT
          market_contract_id,
          instrument_admin,
          instrument_id,
          resolved_outcome,
          payout_per_share_minor,
          state
        FROM market_settlement_jobs
        WHERE market_id = $1
        FOR UPDATE
        "#,
    )
    .bind(market_id)
    .fetch_optional(&mut *tx)
    .await
    .context("lock market settlement job for compute")?;
    let Some(job) = job else {
        tx.commit()
            .await
            .context("commit market settlement compute tx (job missing)")?;
        return Ok(());
    };
    if job.state != "Pending" {
        tx.commit()
            .await
            .context("commit market settlement compute tx (state not pending)")?;
        return Ok(());
    }
    if job.payout_per_share_minor <= 0 {
        return Err(anyhow!(
            "market settlement payout_per_share_minor must be > 0"
        ));
    }

    sqlx::query(
        r#"
        UPDATE market_settlement_jobs
        SET state = 'Computing',
            updated_at = now(),
            error = NULL
        WHERE market_id = $1
        "#,
    )
    .bind(market_id)
    .execute(&mut *tx)
    .await
    .context("mark settlement job computing")?;

    insert_market_settlement_event_tx(
        &mut tx,
        market_id,
        "ComputingStarted",
        serde_json::json!({
          "market_contract_id": job.market_contract_id,
          "instrument_admin": job.instrument_admin,
          "instrument_id": job.instrument_id,
          "resolved_outcome": job.resolved_outcome,
          "payout_per_share_minor": job.payout_per_share_minor,
        }),
    )
    .await?;

    let market: Option<MarketSettlementMarketRow> = sqlx::query_as(
        r#"
        SELECT status, resolved_outcome
        FROM markets
        WHERE market_id = $1
          AND active = TRUE
        ORDER BY created_at DESC, contract_id DESC
        LIMIT 1
        "#,
    )
    .bind(market_id)
    .fetch_optional(&mut *tx)
    .await
    .context("query latest market row for settlement compute")?;
    let Some(market) = market else {
        return Err(anyhow!(
            "cannot compute settlement for market {}: active market row missing",
            market_id
        ));
    };
    if market.status != "Resolved" {
        return Err(anyhow!(
            "cannot compute settlement for market {}: status is {}, expected Resolved",
            market_id,
            market.status
        ));
    }
    match market.resolved_outcome.as_deref() {
        Some(outcome) if outcome == job.resolved_outcome => {}
        Some(outcome) => {
            return Err(anyhow!(
                "resolved outcome mismatch for market {}: job={} market={}",
                market_id,
                job.resolved_outcome,
                outcome
            ))
        }
        None => {
            return Err(anyhow!(
                "resolved market {} missing resolved_outcome",
                market_id
            ))
        }
    }

    cancel_residual_open_orders_for_market(&mut tx, market_id).await?;

    let positions: Vec<SettlementPositionRow> = sqlx::query_as(
        r#"
        SELECT
          account_id,
          outcome,
          net_quantity_minor
        FROM positions
        WHERE market_id = $1
          AND settled = FALSE
        ORDER BY account_id ASC, outcome ASC
        FOR UPDATE
        "#,
    )
    .bind(market_id)
    .fetch_all(&mut *tx)
    .await
    .context("query unsettled positions for market settlement")?;

    let (deltas, total_delta_minor) = compute_market_settlement_account_deltas(
        &positions,
        &job.resolved_outcome,
        job.payout_per_share_minor,
    )?;
    if state.cfg.market_settlement_require_zero_sum && total_delta_minor != 0 {
        return Err(anyhow!(
            "market settlement zero-sum check failed for market {}: total_delta_minor={}",
            market_id,
            total_delta_minor
        ));
    }

    let assigned_rows: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)::BIGINT
        FROM market_settlement_deltas
        WHERE market_id = $1
          AND assigned_epoch IS NOT NULL
        "#,
    )
    .bind(market_id)
    .fetch_one(&mut *tx)
    .await
    .context("count assigned settlement deltas before compute")?;
    if assigned_rows > 0 {
        return Err(anyhow!(
            "cannot recompute settlement for market {}: {} deltas already assigned to epochs",
            market_id,
            assigned_rows
        ));
    }

    sqlx::query(
        r#"
        DELETE FROM market_settlement_deltas
        WHERE market_id = $1
          AND assigned_epoch IS NULL
        "#,
    )
    .bind(market_id)
    .execute(&mut *tx)
    .await
    .context("clear previous unassigned settlement deltas")?;

    for delta in &deltas {
        sqlx::query(
            r#"
            INSERT INTO market_settlement_deltas (
              market_id,
              account_id,
              instrument_admin,
              instrument_id,
              delta_minor,
              source_position_count,
              assigned_epoch,
              created_at,
              updated_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,NULL,now(),now())
            ON CONFLICT (market_id, account_id) DO UPDATE SET
              instrument_admin = EXCLUDED.instrument_admin,
              instrument_id = EXCLUDED.instrument_id,
              delta_minor = EXCLUDED.delta_minor,
              source_position_count = EXCLUDED.source_position_count,
              assigned_epoch = EXCLUDED.assigned_epoch,
              updated_at = now()
            "#,
        )
        .bind(market_id)
        .bind(&delta.account_id)
        .bind(&job.instrument_admin)
        .bind(&job.instrument_id)
        .bind(delta.delta_minor)
        .bind(delta.source_position_count)
        .execute(&mut *tx)
        .await
        .context("upsert market settlement delta")?;
    }

    sqlx::query(
        r#"
        UPDATE market_settlement_jobs
        SET
          state = 'QueuedForClearing',
          target_epoch = NULL,
          error = NULL,
          updated_at = now()
        WHERE market_id = $1
        "#,
    )
    .bind(market_id)
    .execute(&mut *tx)
    .await
    .context("mark market settlement job queued for clearing")?;

    insert_market_settlement_event_tx(
        &mut tx,
        market_id,
        "QueuedForClearing",
        serde_json::json!({
          "account_count": deltas.len(),
          "total_delta_minor": total_delta_minor,
          "require_zero_sum": state.cfg.market_settlement_require_zero_sum,
        }),
    )
    .await?;

    tx.commit()
        .await
        .context("commit market settlement compute tx")?;

    Ok(())
}

async fn cancel_residual_open_orders_for_market(
    tx: &mut Transaction<'_, Postgres>,
    market_id: &str,
) -> Result<()> {
    let rows: Vec<OpenOrderForSettlementRow> = sqlx::query_as(
        r#"
        SELECT
          order_id,
          account_id,
          instrument_admin,
          instrument_id,
          locked_minor
        FROM orders
        WHERE market_id = $1
          AND status IN ('Open', 'PartiallyFilled')
          AND remaining_minor > 0
        ORDER BY submitted_at ASC, order_id ASC
        FOR UPDATE
        "#,
    )
    .bind(market_id)
    .fetch_all(&mut **tx)
    .await
    .context("query open orders for settlement cancellation")?;

    for row in rows {
        sqlx::query(
            r#"
            UPDATE orders
            SET
              status = 'Cancelled',
              remaining_minor = 0,
              locked_minor = 0,
              updated_at = now()
            WHERE order_id = $1
            "#,
        )
        .bind(&row.order_id)
        .execute(&mut **tx)
        .await
        .context("cancel residual order before settlement snapshot")?;

        if row.locked_minor == 0 {
            continue;
        }

        let updated = sqlx::query(
            r#"
            UPDATE account_risk_state
            SET
              locked_open_orders_minor = locked_open_orders_minor - $4,
              updated_at = now()
            WHERE account_id = $1
              AND instrument_admin = $2
              AND instrument_id = $3
            "#,
        )
        .bind(&row.account_id)
        .bind(&row.instrument_admin)
        .bind(&row.instrument_id)
        .bind(row.locked_minor)
        .execute(&mut **tx)
        .await
        .context("release locked balance for residual order cancellation")?;
        if updated.rows_affected() == 0 {
            return Err(anyhow!(
                "missing account_risk_state while cancelling residual order {}",
                row.order_id
            ));
        }
    }

    Ok(())
}

async fn mark_market_settlement_job_failed(
    db: &PgPool,
    market_id: &str,
    error: &str,
) -> Result<()> {
    let truncated_error = truncate_error(error, 2048);

    let mut tx = db.begin().await.context("begin settlement failure tx")?;
    let updated = sqlx::query(
        r#"
        UPDATE market_settlement_jobs
        SET
          state = 'Failed',
          error = $2,
          updated_at = now()
        WHERE market_id = $1
          AND state IN ('Pending', 'Computing')
        "#,
    )
    .bind(market_id)
    .bind(&truncated_error)
    .execute(&mut *tx)
    .await
    .context("mark market settlement job failed")?;
    if updated.rows_affected() == 0 {
        tx.commit()
            .await
            .context("commit settlement failure tx (no rows updated)")?;
        return Ok(());
    }

    insert_market_settlement_event_tx(
        &mut tx,
        market_id,
        "Failed",
        serde_json::json!({
          "error": truncated_error,
        }),
    )
    .await?;

    tx.commit().await.context("commit settlement failure tx")?;
    Ok(())
}

async fn process_awaiting_apply_market_settlement_jobs(state: &AppState) -> Result<()> {
    let jobs: Vec<AwaitingApplyJobRow> = sqlx::query_as(
        r#"
        SELECT
          market_id,
          instrument_admin,
          instrument_id,
          target_epoch
        FROM market_settlement_jobs
        WHERE state = 'AwaitingApply'
          AND target_epoch IS NOT NULL
        ORDER BY updated_at ASC, market_id ASC
        LIMIT 128
        "#,
    )
    .fetch_all(&state.db)
    .await
    .context("query awaiting-apply market settlement jobs")?;

    for job in jobs {
        match is_market_settlement_job_applied(state, &job).await {
            Ok(true) => {
                if let Err(err) = mark_market_settlement_job_applied(state, &job.market_id).await {
                    tracing::warn!(
                        error = ?err,
                        market_id = %job.market_id,
                        "failed to mark settlement job applied"
                    );
                }
            }
            Ok(false) => {}
            Err(err) => {
                tracing::warn!(
                    error = ?err,
                    market_id = %job.market_id,
                    "failed to evaluate settlement apply completion"
                );
            }
        }
    }

    Ok(())
}

async fn is_market_settlement_job_applied(
    state: &AppState,
    job: &AwaitingApplyJobRow,
) -> Result<bool> {
    let account_deltas: Vec<JobAccountDeltaRow> = sqlx::query_as(
        r#"
        SELECT account_id
        FROM market_settlement_deltas
        WHERE market_id = $1
        ORDER BY account_id ASC
        "#,
    )
    .bind(&job.market_id)
    .fetch_all(&state.db)
    .await
    .context("query settlement job account deltas")?;

    for account in account_deltas {
        let apply: Option<AccountApplyStatusRow> = sqlx::query_as(
            r#"
            SELECT status
            FROM clearing_applies
            WHERE instrument_admin = $1
              AND instrument_id = $2
              AND epoch = $3
              AND account_id = $4
            LIMIT 1
            "#,
        )
        .bind(&job.instrument_admin)
        .bind(&job.instrument_id)
        .bind(job.target_epoch)
        .bind(&account.account_id)
        .fetch_optional(&state.db)
        .await
        .context("query clearing apply for settlement job account")?;
        if matches!(
            apply.as_ref().map(|row| row.status.as_str()),
            Some("Applied")
        ) {
            continue;
        }

        let account_state: Option<AccountStateEpochRow> = sqlx::query_as(
            r#"
            SELECT st.last_applied_epoch
            FROM account_state_latest latest
            JOIN account_states st
              ON st.contract_id = latest.contract_id
            WHERE latest.account_id = $1
              AND st.active = TRUE
              AND st.instrument_admin = $2
              AND st.instrument_id = $3
            LIMIT 1
            "#,
        )
        .bind(&account.account_id)
        .bind(&job.instrument_admin)
        .bind(&job.instrument_id)
        .fetch_optional(&state.db)
        .await
        .context("query latest account state for settlement apply completion")?;
        let Some(account_state) = account_state else {
            return Ok(false);
        };
        if account_state.last_applied_epoch < job.target_epoch {
            return Ok(false);
        }

        let pending_delta_minor: i64 = sqlx::query_scalar(
            r#"
            SELECT COALESCE(SUM(delta_minor), 0)::BIGINT
            FROM clearing_epoch_deltas
            WHERE instrument_admin = $1
              AND instrument_id = $2
              AND account_id = $3
              AND epoch > $4
              AND epoch <= $5
            "#,
        )
        .bind(&job.instrument_admin)
        .bind(&job.instrument_id)
        .bind(&account.account_id)
        .bind(account_state.last_applied_epoch)
        .bind(job.target_epoch)
        .fetch_one(&state.db)
        .await
        .context("query pending cumulative delta for settlement apply completion")?;
        if pending_delta_minor != 0 {
            return Ok(false);
        }
    }

    Ok(true)
}

async fn mark_market_settlement_job_applied(state: &AppState, market_id: &str) -> Result<()> {
    let mut tx = state
        .db
        .begin()
        .await
        .context("begin settlement mark-applied tx")?;
    let updated = sqlx::query(
        r#"
        UPDATE market_settlement_jobs
        SET
          state = 'Applied',
          error = NULL,
          updated_at = now()
        WHERE market_id = $1
          AND state = 'AwaitingApply'
        "#,
    )
    .bind(market_id)
    .execute(&mut *tx)
    .await
    .context("mark settlement job applied")?;
    if updated.rows_affected() == 0 {
        tx.commit()
            .await
            .context("commit settlement mark-applied tx (no rows updated)")?;
        return Ok(());
    }

    insert_market_settlement_event_tx(&mut tx, market_id, "Applied", serde_json::json!({})).await?;

    tx.commit()
        .await
        .context("commit settlement mark-applied tx")?;
    Ok(())
}

async fn finalize_applied_market_settlement_jobs(state: &AppState) -> Result<()> {
    let jobs: Vec<AppliedJobRow> = sqlx::query_as(
        r#"
        SELECT market_id, resolved_outcome
        FROM market_settlement_jobs
        WHERE state = 'Applied'
        ORDER BY updated_at ASC, market_id ASC
        LIMIT 64
        "#,
    )
    .fetch_all(&state.db)
    .await
    .context("query applied market settlement jobs")?;

    for job in jobs {
        if let Err(err) = finalize_market_settlement_job(state, &job).await {
            tracing::warn!(
                error = ?err,
                market_id = %job.market_id,
                "failed to finalize settlement job"
            );
        }
    }

    Ok(())
}

async fn finalize_market_settlement_job(state: &AppState, job: &AppliedJobRow) -> Result<()> {
    let mut tx = state
        .db
        .begin()
        .await
        .context("begin settlement finalization tx")?;
    lock_market(&mut tx, &job.market_id).await?;

    let live: Option<AppliedJobRow> = sqlx::query_as(
        r#"
        SELECT market_id, resolved_outcome
        FROM market_settlement_jobs
        WHERE market_id = $1
          AND state = 'Applied'
        FOR UPDATE
        "#,
    )
    .bind(&job.market_id)
    .fetch_optional(&mut *tx)
    .await
    .context("lock applied settlement job for finalization")?;
    let Some(live) = live else {
        tx.commit()
            .await
            .context("commit settlement finalization tx (state changed)")?;
        return Ok(());
    };

    let snapshot_insert = sqlx::query(
        r#"
        INSERT INTO position_settlement_snapshots (
          market_id,
          account_id,
          outcome,
          net_quantity_minor_before,
          avg_entry_price_ticks_before,
          realized_pnl_minor_before,
          resolved_outcome,
          settlement_delta_minor,
          created_at
        )
        SELECT
          p.market_id,
          p.account_id,
          p.outcome,
          p.net_quantity_minor,
          p.avg_entry_price_ticks,
          p.realized_pnl_minor,
          $2,
          COALESCE(d.delta_minor, 0),
          now()
        FROM positions p
        LEFT JOIN market_settlement_deltas d
          ON d.market_id = p.market_id
         AND d.account_id = p.account_id
        WHERE p.market_id = $1
          AND p.settled = FALSE
        ON CONFLICT (market_id, account_id, outcome) DO NOTHING
        "#,
    )
    .bind(&live.market_id)
    .bind(&live.resolved_outcome)
    .execute(&mut *tx)
    .await
    .context("insert position settlement snapshots")?;
    let snapshot_rows = i64::try_from(snapshot_insert.rows_affected())
        .context("snapshot rows_affected conversion overflow")?;

    let settled_update = sqlx::query(
        r#"
        UPDATE positions
        SET
          net_quantity_minor = 0,
          settled = TRUE,
          settled_outcome = $2,
          settled_at = COALESCE(settled_at, now()),
          updated_at = now()
        WHERE market_id = $1
          AND settled = FALSE
        "#,
    )
    .bind(&live.market_id)
    .bind(&live.resolved_outcome)
    .execute(&mut *tx)
    .await
    .context("terminalize market positions")?;
    let settled_rows = i64::try_from(settled_update.rows_affected())
        .context("settled rows_affected conversion overflow")?;

    sqlx::query(
        r#"
        UPDATE market_settlement_jobs
        SET
          state = 'Finalized',
          error = NULL,
          updated_at = now()
        WHERE market_id = $1
          AND state = 'Applied'
        "#,
    )
    .bind(&live.market_id)
    .execute(&mut *tx)
    .await
    .context("mark market settlement job finalized")?;

    insert_market_settlement_event_tx(
        &mut tx,
        &live.market_id,
        "Finalized",
        serde_json::json!({
          "snapshot_rows": snapshot_rows,
          "settled_rows": settled_rows,
        }),
    )
    .await?;

    tx.commit()
        .await
        .context("commit settlement finalization tx")?;

    Ok(())
}

fn truncate_error(error: &str, max_len: usize) -> String {
    if error.chars().count() <= max_len {
        return error.to_string();
    }
    let truncated: String = error.chars().take(max_len).collect();
    format!("{truncated}...")
}

async fn process_instrument_epoch(state: &AppState, head: &ClearingHeadRow) -> Result<()> {
    validate_positive_epoch(head.next_epoch).context("validate clearing epoch")?;
    parse_batch_hash(&head.prev_batch_hash).context("validate prev_batch_hash")?;

    let existing_epoch: Option<ExistingEpochRow> = sqlx::query_as(
        r#"
        SELECT status, anchor_contract_id
        FROM clearing_epochs
        WHERE instrument_admin = $1
          AND instrument_id = $2
          AND epoch = $3
        LIMIT 1
        "#,
    )
    .bind(&head.instrument_admin)
    .bind(&head.instrument_id)
    .bind(head.next_epoch)
    .fetch_optional(&state.db)
    .await
    .context("query clearing epoch status")?;
    if state.cfg.enable_apply {
        recover_missing_post_apply_local_state_for_instrument(
            state,
            &head.instrument_admin,
            &head.instrument_id,
        )
        .await
        .context("recover missing post-apply local state")?;
    }
    if let Err(err) = log_pending_trade_delta_drift_for_instrument(
        state,
        &head.instrument_admin,
        &head.instrument_id,
    )
    .await
    {
        tracing::warn!(
            error = ?err,
            instrument_admin = %head.instrument_admin,
            instrument_id = %head.instrument_id,
            "pending-trade drift check failed"
        );
    }
    if !state.cfg.enable_apply && existing_epoch.is_some() {
        // Dry-run mode is read-only; once an epoch marker exists, don't keep re-upserting/logging it.
        return Ok(());
    }
    if matches!(existing_epoch.as_ref(), Some(row) if row.status == "Applied") {
        return Ok(());
    }

    materialize_epoch_deltas_from_fills(state, head)
        .await
        .context("materialize clearing deltas from unsettled fills")?;

    let epoch_deltas: Vec<DeltaRow> = sqlx::query_as(
        r#"
        SELECT delta_minor, source_fill_count
        FROM clearing_epoch_deltas
        WHERE instrument_admin = $1
          AND instrument_id = $2
          AND epoch = $3
        ORDER BY account_id ASC
        "#,
    )
    .bind(&head.instrument_admin)
    .bind(&head.instrument_id)
    .bind(head.next_epoch)
    .fetch_all(&state.db)
    .await
    .context("query clearing epoch deltas")?;

    if epoch_deltas.is_empty() {
        return Ok(());
    }

    let batch_hash = compute_batch_hash(
        &head.instrument_admin,
        &head.instrument_id,
        head.next_epoch,
        &state.cfg.engine_version,
        &epoch_deltas,
    );
    let batch_hash = parse_batch_hash(&batch_hash).context("validate computed batch_hash")?;
    let delta_count = i64::try_from(epoch_deltas.len()).context("delta count overflow")?;
    let delta_total: i64 = epoch_deltas.iter().map(|x| x.delta_minor).sum();
    let source_fill_count: i64 = epoch_deltas
        .iter()
        .map(|x| i64::from(x.source_fill_count))
        .sum();
    let details = serde_json::json!({
      "delta_count": delta_count,
      "delta_total": delta_total,
      "source_fill_count": source_fill_count,
      "mode": if state.cfg.enable_apply { "apply" } else { "dry-run" },
      "head_engine_version": head.engine_version,
      "market_settlement_enabled": state.cfg.market_settlement_enable,
      "market_settlement_dry_run": state.cfg.market_settlement_dry_run,
    });
    let initial_status = if state.cfg.enable_apply {
        "Anchoring"
    } else {
        "DryRunReady"
    };
    let initial_finalized_at = if state.cfg.enable_apply {
        None
    } else {
        Some(Utc::now())
    };

    if existing_epoch.is_none() {
        sqlx::query(
            r#"
            INSERT INTO clearing_epochs (
              instrument_admin,
              instrument_id,
              epoch,
              prev_batch_hash,
              batch_hash,
              engine_version,
              status,
              finalized_at,
              details
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (instrument_admin, instrument_id, epoch) DO NOTHING
            "#,
        )
        .bind(&head.instrument_admin)
        .bind(&head.instrument_id)
        .bind(head.next_epoch)
        .bind(&head.prev_batch_hash)
        .bind(&batch_hash)
        .bind(&state.cfg.engine_version)
        .bind(initial_status)
        .bind(initial_finalized_at)
        .bind(&details)
        .execute(&state.db)
        .await
        .context("insert clearing_epochs row")?;
    } else {
        sqlx::query(
            r#"
            UPDATE clearing_epochs
            SET
              prev_batch_hash = $4,
              batch_hash = $5,
              engine_version = $6,
              details = $7
            WHERE instrument_admin = $1
              AND instrument_id = $2
              AND epoch = $3
            "#,
        )
        .bind(&head.instrument_admin)
        .bind(&head.instrument_id)
        .bind(head.next_epoch)
        .bind(&head.prev_batch_hash)
        .bind(&batch_hash)
        .bind(&state.cfg.engine_version)
        .bind(&details)
        .execute(&state.db)
        .await
        .context("refresh clearing_epochs hash/details")?;
    }

    if !state.cfg.enable_apply {
        tracing::info!(
            instrument_admin = %head.instrument_admin,
            instrument_id = %head.instrument_id,
            epoch = head.next_epoch,
            delta_count,
            delta_total,
            source_fill_count,
            "clearing epoch ready (dry-run mode, ledger apply disabled)"
        );
        return Ok(());
    }

    let mut batch_anchor_cid = existing_epoch
        .as_ref()
        .and_then(|row| row.anchor_contract_id.clone());
    if batch_anchor_cid.is_none() {
        let create_batch_tx = create_next_batch(
            state,
            &head.clearing_head_cid,
            &batch_hash,
            &head.instrument_admin,
            &head.instrument_id,
            head.next_epoch,
        )
        .await?;
        let anchored_cid =
            extract_created_contract_id(&create_batch_tx, "Pebble.Clearing", "BatchAnchor")
                .ok_or_else(|| anyhow!("CreateNextBatch tx missing BatchAnchor create event"))?;

        sqlx::query(
            r#"
            UPDATE clearing_epochs
            SET anchor_contract_id = $4, status = 'Anchored'
            WHERE instrument_admin = $1
              AND instrument_id = $2
              AND epoch = $3
            "#,
        )
        .bind(&head.instrument_admin)
        .bind(&head.instrument_id)
        .bind(head.next_epoch)
        .bind(&anchored_cid)
        .execute(&state.db)
        .await
        .context("mark clearing epoch anchored")?;

        batch_anchor_cid = Some(anchored_cid);
    }
    let batch_anchor_cid =
        batch_anchor_cid.ok_or_else(|| anyhow!("missing batch anchor cid after anchoring"))?;

    sqlx::query(
        r#"
        UPDATE clearing_epochs
        SET status = 'Applying'
        WHERE instrument_admin = $1
          AND instrument_id = $2
          AND epoch = $3
          AND status IN ('Anchoring', 'Anchored')
        "#,
    )
    .bind(&head.instrument_admin)
    .bind(&head.instrument_id)
    .bind(head.next_epoch)
    .execute(&state.db)
    .await
    .context("mark clearing epoch applying")?;

    let mut any_failed = false;
    let mut applied_count: i64 = 0;
    let mut failed_count: i64 = 0;
    let mut deferred_count: i64 = 0;

    let catchup_deltas: Vec<CatchupDeltaRow> = sqlx::query_as(
        r#"
        SELECT
          st.account_id,
          SUM(d.delta_minor)::BIGINT AS delta_minor
        FROM account_state_latest latest
        JOIN account_states st ON st.contract_id = latest.contract_id
        JOIN clearing_epoch_deltas d
          ON d.instrument_admin = st.instrument_admin
         AND d.instrument_id = st.instrument_id
         AND d.account_id = st.account_id
        WHERE st.active = TRUE
          AND st.instrument_admin = $1
          AND st.instrument_id = $2
          AND d.epoch > st.last_applied_epoch
          AND d.epoch <= $3
        GROUP BY st.account_id, st.last_applied_epoch
        ORDER BY st.last_applied_epoch ASC, st.account_id ASC
        "#,
    )
    .bind(&head.instrument_admin)
    .bind(&head.instrument_id)
    .bind(head.next_epoch)
    .fetch_all(&state.db)
    .await
    .context("query catch-up clearing deltas")?;

    let apply_budget = state.cfg.max_applies_per_tick;
    let mut applied_attempts: usize = 0;

    for d in catchup_deltas {
        parse_account_id(&d.account_id).with_context(|| {
            format!(
                "validate canonical account_id for clearing epoch {}",
                head.next_epoch
            )
        })?;

        let existing_apply_status: Option<AccountApplyStatusRow> = sqlx::query_as(
            r#"
            SELECT status
            FROM clearing_applies
            WHERE instrument_admin = $1
              AND instrument_id = $2
              AND epoch = $3
              AND account_id = $4
            LIMIT 1
            "#,
        )
        .bind(&head.instrument_admin)
        .bind(&head.instrument_id)
        .bind(head.next_epoch)
        .bind(&d.account_id)
        .fetch_optional(&state.db)
        .await
        .context("query clearing_applies status")?;
        if matches!(
            existing_apply_status
                .as_ref()
                .map(|row| row.status.as_str()),
            Some("Applied")
        ) {
            continue;
        }

        if apply_budget > 0 && applied_attempts >= apply_budget {
            deferred_count = deferred_count
                .checked_add(1)
                .ok_or_else(|| anyhow!("deferred count overflow"))?;
            upsert_clearing_apply(
                &state.db,
                &ClearingApplyUpdate {
                    instrument_admin: &head.instrument_admin,
                    instrument_id: &head.instrument_id,
                    epoch: head.next_epoch,
                    account_id: &d.account_id,
                    batch_anchor_cid: &batch_anchor_cid,
                    account_state_before_cid: None,
                    account_state_after_cid: None,
                    delta_minor: d.delta_minor,
                    status: "Skipped",
                    applied_offset: None,
                    applied_update_id: None,
                    error: Some(format!(
                        "deferred by apply budget PEBBLE_CLEARING_MAX_APPLIES_PER_TICK={}",
                        apply_budget
                    )),
                },
            )
            .await?;
            continue;
        }

        if d.delta_minor == 0 {
            upsert_clearing_apply(
                &state.db,
                &ClearingApplyUpdate {
                    instrument_admin: &head.instrument_admin,
                    instrument_id: &head.instrument_id,
                    epoch: head.next_epoch,
                    account_id: &d.account_id,
                    batch_anchor_cid: &batch_anchor_cid,
                    account_state_before_cid: None,
                    account_state_after_cid: None,
                    delta_minor: d.delta_minor,
                    status: "Skipped",
                    applied_offset: None,
                    applied_update_id: None,
                    error: None,
                },
            )
            .await?;
            continue;
        }
        applied_attempts = applied_attempts
            .checked_add(1)
            .ok_or_else(|| anyhow!("apply attempt counter overflow"))?;

        let mut account_writer_tx = state
            .db
            .begin()
            .await
            .context("begin account writer lock tx")?;
        sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
            .bind(state.cfg.account_lock_namespace)
            .bind(&d.account_id)
            .execute(&mut *account_writer_tx)
            .await
            .context("acquire account writer lock")?;

        let account_state_cid: Option<(String, i64)> = sqlx::query_as(
            r#"
            SELECT st.contract_id, st.last_applied_epoch
            FROM account_state_latest latest
            JOIN account_states st ON st.contract_id = latest.contract_id
            WHERE latest.account_id = $1
              AND st.active = TRUE
              AND st.instrument_admin = $2
              AND st.instrument_id = $3
            LIMIT 1
            "#,
        )
        .bind(&d.account_id)
        .bind(&head.instrument_admin)
        .bind(&head.instrument_id)
        .fetch_optional(&mut *account_writer_tx)
        .await
        .context("query latest account state under writer lock")?;

        let Some((account_state_cid, last_applied_epoch)) = account_state_cid else {
            account_writer_tx
                .commit()
                .await
                .context("commit account writer lock tx (missing state)")?;
            any_failed = true;
            failed_count = failed_count
                .checked_add(1)
                .ok_or_else(|| anyhow!("failed count overflow"))?;
            upsert_clearing_apply(
                &state.db,
                &ClearingApplyUpdate {
                    instrument_admin: &head.instrument_admin,
                    instrument_id: &head.instrument_id,
                    epoch: head.next_epoch,
                    account_id: &d.account_id,
                    batch_anchor_cid: &batch_anchor_cid,
                    account_state_before_cid: None,
                    account_state_after_cid: None,
                    delta_minor: d.delta_minor,
                    status: "Failed",
                    applied_offset: None,
                    applied_update_id: None,
                    error: Some("missing active AccountState".to_string()),
                },
            )
            .await?;
            continue;
        };

        if last_applied_epoch >= head.next_epoch {
            account_writer_tx
                .commit()
                .await
                .context("commit account writer lock tx (already applied)")?;
            upsert_clearing_apply(
                &state.db,
                &ClearingApplyUpdate {
                    instrument_admin: &head.instrument_admin,
                    instrument_id: &head.instrument_id,
                    epoch: head.next_epoch,
                    account_id: &d.account_id,
                    batch_anchor_cid: &batch_anchor_cid,
                    account_state_before_cid: Some(&account_state_cid),
                    account_state_after_cid: Some(&account_state_cid),
                    delta_minor: d.delta_minor,
                    status: "Skipped",
                    applied_offset: None,
                    applied_update_id: None,
                    error: Some(format!(
                        "account already at epoch {} (target {})",
                        last_applied_epoch, head.next_epoch
                    )),
                },
            )
            .await?;
            continue;
        }

        let (account_delta_minor, _account_source_fill_count): (i64, i64) = sqlx::query_as(
            r#"
            SELECT
              COALESCE(SUM(delta_minor), 0)::BIGINT AS delta_minor,
              COALESCE(SUM(source_fill_count), 0)::BIGINT AS source_fill_count
            FROM clearing_epoch_deltas
            WHERE instrument_admin = $1
              AND instrument_id = $2
              AND account_id = $3
              AND epoch > $4
              AND epoch <= $5
            "#,
        )
        .bind(&head.instrument_admin)
        .bind(&head.instrument_id)
        .bind(&d.account_id)
        .bind(last_applied_epoch)
        .bind(head.next_epoch)
        .fetch_one(&mut *account_writer_tx)
        .await
        .context("query account cumulative clearing delta")?;
        let account_fill_delta_minor = load_account_cumulative_fill_delta(
            &mut account_writer_tx,
            &head.instrument_admin,
            &head.instrument_id,
            &d.account_id,
            last_applied_epoch,
            head.next_epoch,
        )
        .await?;

        if account_delta_minor == 0 {
            account_writer_tx
                .commit()
                .await
                .context("commit account writer lock tx (zero cumulative delta)")?;
            upsert_clearing_apply(
                &state.db,
                &ClearingApplyUpdate {
                    instrument_admin: &head.instrument_admin,
                    instrument_id: &head.instrument_id,
                    epoch: head.next_epoch,
                    account_id: &d.account_id,
                    batch_anchor_cid: &batch_anchor_cid,
                    account_state_before_cid: Some(&account_state_cid),
                    account_state_after_cid: Some(&account_state_cid),
                    delta_minor: account_delta_minor,
                    status: "Skipped",
                    applied_offset: None,
                    applied_update_id: None,
                    error: Some(format!(
                        "account has no cumulative deltas through epoch {}",
                        head.next_epoch
                    )),
                },
            )
            .await?;
            continue;
        }

        if state.cfg.apply_delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(state.cfg.apply_delay_ms)).await;
        }

        let apply_cmd = ApplyClearingDeltaCmd {
            account_state_cid: &account_state_cid,
            batch_anchor_cid: &batch_anchor_cid,
            delta_minor: account_delta_minor,
            instrument_admin: &head.instrument_admin,
            instrument_id: &head.instrument_id,
            epoch: head.next_epoch,
            account_id: &d.account_id,
        };
        let apply_result = apply_clearing_delta(state, &apply_cmd).await;

        account_writer_tx
            .commit()
            .await
            .context("commit account writer lock tx")?;

        match apply_result {
            Ok(tx) => {
                let next_account_state_cid =
                    extract_created_contract_id(&tx, "Pebble.Account", "AccountState");
                finalize_successful_apply_local_state(
                    state,
                    &head.instrument_admin,
                    &head.instrument_id,
                    &d.account_id,
                    account_fill_delta_minor,
                    &ClearingApplyUpdate {
                        instrument_admin: &head.instrument_admin,
                        instrument_id: &head.instrument_id,
                        epoch: head.next_epoch,
                        account_id: &d.account_id,
                        batch_anchor_cid: &batch_anchor_cid,
                        account_state_before_cid: Some(&account_state_cid),
                        account_state_after_cid: next_account_state_cid.as_deref(),
                        delta_minor: account_delta_minor,
                        status: "Applied",
                        applied_offset: Some(tx.offset),
                        applied_update_id: Some(&tx.update_id),
                        error: None,
                    },
                )
                .await?;
                applied_count = applied_count
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("applied count overflow"))?;
            }
            Err(err) => {
                any_failed = true;
                failed_count = failed_count
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("failed count overflow"))?;
                upsert_clearing_apply(
                    &state.db,
                    &ClearingApplyUpdate {
                        instrument_admin: &head.instrument_admin,
                        instrument_id: &head.instrument_id,
                        epoch: head.next_epoch,
                        account_id: &d.account_id,
                        batch_anchor_cid: &batch_anchor_cid,
                        account_state_before_cid: Some(&account_state_cid),
                        account_state_after_cid: None,
                        delta_minor: account_delta_minor,
                        status: "Failed",
                        applied_offset: None,
                        applied_update_id: None,
                        error: Some(err.to_string()),
                    },
                )
                .await?;
            }
        }
    }

    let (status, finalized_at): (&str, Option<chrono::DateTime<chrono::Utc>>) = if any_failed {
        ("Failed", Some(Utc::now()))
    } else if deferred_count > 0 {
        ("Applying", None)
    } else {
        ("Applied", Some(Utc::now()))
    };
    sqlx::query(
        r#"
        UPDATE clearing_epochs
        SET status = $4,
            finalized_at = $5,
            details = details
              || jsonb_build_object(
                'applied_count', $6::BIGINT,
                'failed_count', $7::BIGINT,
                'deferred_count', $8::BIGINT,
                'apply_budget', CASE WHEN $9::BIGINT = 0 THEN NULL ELSE $9::BIGINT END
              )
        WHERE instrument_admin = $1
          AND instrument_id = $2
          AND epoch = $3
        "#,
    )
    .bind(&head.instrument_admin)
    .bind(&head.instrument_id)
    .bind(head.next_epoch)
    .bind(status)
    .bind(finalized_at)
    .bind(applied_count)
    .bind(failed_count)
    .bind(deferred_count)
    .bind(i64::try_from(apply_budget).context("apply budget conversion overflow")?)
    .execute(&state.db)
    .await
    .context("mark clearing epoch terminal status")?;

    tracing::info!(
        instrument_admin = %head.instrument_admin,
        instrument_id = %head.instrument_id,
        epoch = head.next_epoch,
        status,
        applied_count,
        failed_count,
        deferred_count,
        "clearing epoch processed"
    );

    Ok(())
}

async fn materialize_epoch_deltas_from_fills(
    state: &AppState,
    head: &ClearingHeadRow,
) -> Result<()> {
    let mut tx = state
        .db
        .begin()
        .await
        .context("begin materialize deltas tx")?;

    lock_epoch(&mut tx, head).await?;

    let fills: Vec<UnsettledFillRow> = sqlx::query_as(
        r#"
        SELECT
          f.fill_id,
          f.maker_account_id,
          f.taker_account_id,
          f.price_ticks,
          f.quantity_minor,
          taker.side AS taker_side
        FROM fills f
        JOIN orders taker
          ON taker.order_id = f.taker_order_id
        WHERE f.instrument_admin = $1
          AND f.instrument_id = $2
          AND f.clearing_epoch IS NULL
        ORDER BY f.fill_sequence ASC, f.fill_id ASC
        FOR UPDATE
        "#,
    )
    .bind(&head.instrument_admin)
    .bind(&head.instrument_id)
    .fetch_all(&mut *tx)
    .await
    .context("query unsettled fills for epoch materialization")?;

    let mut deltas = BTreeMap::<String, (i64, i64)>::new();
    let mut fill_ids = Vec::<String>::with_capacity(fills.len());

    for fill in fills {
        fill_ids.push(fill.fill_id.clone());

        let taker_delta = cash_delta_minor(
            fill.taker_side.as_str(),
            fill.price_ticks,
            fill.quantity_minor,
        )?;
        let maker_delta = taker_delta
            .checked_neg()
            .ok_or_else(|| anyhow!("maker delta overflow"))?;

        let taker_entry = deltas.entry(fill.taker_account_id).or_insert((0, 0));
        taker_entry.0 = taker_entry
            .0
            .checked_add(taker_delta)
            .ok_or_else(|| anyhow!("taker delta accumulation overflow"))?;
        taker_entry.1 = taker_entry
            .1
            .checked_add(1)
            .ok_or_else(|| anyhow!("taker fill count overflow"))?;

        let maker_entry = deltas.entry(fill.maker_account_id).or_insert((0, 0));
        maker_entry.0 = maker_entry
            .0
            .checked_add(maker_delta)
            .ok_or_else(|| anyhow!("maker delta accumulation overflow"))?;
        maker_entry.1 = maker_entry
            .1
            .checked_add(1)
            .ok_or_else(|| anyhow!("maker fill count overflow"))?;
    }

    let mut queued_settlement_job_ids = BTreeSet::<String>::new();
    if state.cfg.market_settlement_enable && !state.cfg.market_settlement_dry_run {
        let queued_jobs: Vec<QueuedSettlementJobRow> = sqlx::query_as(
            r#"
            SELECT market_id
            FROM market_settlement_jobs
            WHERE instrument_admin = $1
              AND instrument_id = $2
              AND state = 'QueuedForClearing'
            ORDER BY created_at ASC, market_id ASC
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(&head.instrument_admin)
        .bind(&head.instrument_id)
        .fetch_all(&mut *tx)
        .await
        .context("query queued settlement jobs for epoch materialization")?;

        let queued_job_ids: Vec<String> =
            queued_jobs.into_iter().map(|row| row.market_id).collect();
        if !queued_job_ids.is_empty() {
            let settlement_rows: Vec<SettlementDeltaForEpochRow> = sqlx::query_as(
                r#"
                SELECT account_id, delta_minor
                FROM market_settlement_deltas
                WHERE instrument_admin = $1
                  AND instrument_id = $2
                  AND assigned_epoch IS NULL
                  AND market_id = ANY($3::TEXT[])
                ORDER BY market_id ASC, account_id ASC
                FOR UPDATE
                "#,
            )
            .bind(&head.instrument_admin)
            .bind(&head.instrument_id)
            .bind(&queued_job_ids)
            .fetch_all(&mut *tx)
            .await
            .context("query unassigned settlement deltas for epoch materialization")?;

            for row in settlement_rows {
                if row.delta_minor == 0 {
                    continue;
                }
                let entry = deltas.entry(row.account_id).or_insert((0, 0));
                entry.0 = entry
                    .0
                    .checked_add(row.delta_minor)
                    .ok_or_else(|| anyhow!("settlement delta accumulation overflow"))?;
            }

            for market_id in queued_job_ids {
                queued_settlement_job_ids.insert(market_id);
            }
        }
    }

    for (account_id, (delta_minor, source_fill_count)) in &deltas {
        if *delta_minor == 0 && *source_fill_count == 0 {
            continue;
        }
        let source_fill_count =
            i32::try_from(*source_fill_count).context("source_fill_count out of range for i32")?;

        sqlx::query(
            r#"
            INSERT INTO clearing_epoch_deltas (
              instrument_admin,
              instrument_id,
              epoch,
              account_id,
              delta_minor,
              source_fill_count
            )
            VALUES ($1,$2,$3,$4,$5,$6)
            ON CONFLICT (instrument_admin, instrument_id, epoch, account_id) DO UPDATE SET
              delta_minor = clearing_epoch_deltas.delta_minor + EXCLUDED.delta_minor,
              source_fill_count = clearing_epoch_deltas.source_fill_count + EXCLUDED.source_fill_count
            "#,
        )
        .bind(&head.instrument_admin)
        .bind(&head.instrument_id)
        .bind(head.next_epoch)
        .bind(account_id)
        .bind(*delta_minor)
        .bind(source_fill_count)
        .execute(&mut *tx)
        .await
        .context("upsert clearing_epoch_deltas from fills")?;
    }

    if !fill_ids.is_empty() {
        sqlx::query(
            r#"
            UPDATE fills
            SET clearing_epoch = $3
            WHERE instrument_admin = $1
              AND instrument_id = $2
              AND fill_id = ANY($4::TEXT[])
            "#,
        )
        .bind(&head.instrument_admin)
        .bind(&head.instrument_id)
        .bind(head.next_epoch)
        .bind(&fill_ids)
        .execute(&mut *tx)
        .await
        .context("mark fills with clearing_epoch")?;
    }

    if !queued_settlement_job_ids.is_empty() {
        let market_ids: Vec<String> = queued_settlement_job_ids.iter().cloned().collect();

        sqlx::query(
            r#"
            UPDATE market_settlement_deltas
            SET
              assigned_epoch = $2,
              updated_at = now()
            WHERE market_id = ANY($1::TEXT[])
              AND assigned_epoch IS NULL
            "#,
        )
        .bind(&market_ids)
        .bind(head.next_epoch)
        .execute(&mut *tx)
        .await
        .context("assign settlement deltas to clearing epoch")?;

        sqlx::query(
            r#"
            UPDATE market_settlement_jobs
            SET
              state = 'AwaitingApply',
              target_epoch = $2,
              error = NULL,
              updated_at = now()
            WHERE market_id = ANY($1::TEXT[])
              AND state = 'QueuedForClearing'
            "#,
        )
        .bind(&market_ids)
        .bind(head.next_epoch)
        .execute(&mut *tx)
        .await
        .context("mark settlement jobs awaiting apply")?;

        for market_id in queued_settlement_job_ids {
            insert_market_settlement_event_tx(
                &mut tx,
                &market_id,
                "AwaitingApply",
                serde_json::json!({
                  "target_epoch": head.next_epoch,
                }),
            )
            .await?;
        }
    }

    tx.commit().await.context("commit materialize deltas tx")?;

    Ok(())
}

async fn lock_epoch(tx: &mut Transaction<'_, Postgres>, head: &ClearingHeadRow) -> Result<()> {
    let epoch_key = format!(
        "{}:{}:{}",
        head.instrument_admin, head.instrument_id, head.next_epoch
    );
    sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
        .bind(31_i32)
        .bind(epoch_key)
        .execute(&mut **tx)
        .await
        .context("acquire epoch advisory lock")?;
    Ok(())
}

async fn lock_market(tx: &mut Transaction<'_, Postgres>, market_id: &str) -> Result<()> {
    sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
        .bind(MARKET_LOCK_NAMESPACE)
        .bind(market_id)
        .execute(&mut **tx)
        .await
        .context("acquire market advisory lock")?;
    Ok(())
}

async fn insert_market_settlement_event_tx(
    tx: &mut Transaction<'_, Postgres>,
    market_id: &str,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<()> {
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

fn compute_market_settlement_account_deltas(
    positions: &[SettlementPositionRow],
    resolved_outcome: &str,
    payout_per_share_minor: i64,
) -> Result<(Vec<ComputedSettlementDelta>, i64)> {
    if payout_per_share_minor <= 0 {
        return Err(anyhow!("payout_per_share_minor must be > 0"));
    }

    let mut by_account = BTreeMap::<String, (i64, i64)>::new();
    let mut total_delta_minor: i64 = 0;

    for position in positions {
        let contribution_minor = if position.outcome == resolved_outcome {
            position
                .net_quantity_minor
                .checked_mul(payout_per_share_minor)
                .ok_or_else(|| anyhow!("settlement contribution overflow"))?
        } else {
            0
        };

        let entry = by_account
            .entry(position.account_id.clone())
            .or_insert((0_i64, 0_i64));
        entry.0 = entry
            .0
            .checked_add(contribution_minor)
            .ok_or_else(|| anyhow!("account settlement delta overflow"))?;
        entry.1 = entry
            .1
            .checked_add(1)
            .ok_or_else(|| anyhow!("account source position count overflow"))?;

        total_delta_minor = total_delta_minor
            .checked_add(contribution_minor)
            .ok_or_else(|| anyhow!("market settlement total delta overflow"))?;
    }

    let mut out = Vec::with_capacity(by_account.len());
    for (account_id, (delta_minor, source_position_count)) in by_account {
        if delta_minor == 0 {
            continue;
        }
        let source_position_count = i32::try_from(source_position_count)
            .context("source_position_count out of range for i32")?;
        out.push(ComputedSettlementDelta {
            account_id,
            delta_minor,
            source_position_count,
        });
    }

    Ok((out, total_delta_minor))
}

fn cash_delta_minor(taker_side: &str, price_ticks: i64, quantity_minor: i64) -> Result<i64> {
    let gross = price_ticks
        .checked_mul(quantity_minor)
        .ok_or_else(|| anyhow!("fill cash delta overflow"))?;

    match taker_side {
        "Buy" => gross
            .checked_neg()
            .ok_or_else(|| anyhow!("fill cash delta overflow")),
        "Sell" => Ok(gross),
        _ => Err(anyhow!("unexpected taker side in fills: {taker_side}")),
    }
}

fn compute_batch_hash(
    instrument_admin: &str,
    instrument_id: &str,
    epoch: i64,
    engine_version: &str,
    deltas: &[DeltaRow],
) -> String {
    let total_abs: i64 = deltas.iter().map(|x| x.delta_minor.abs()).sum();
    format!(
        "m3:{instrument_admin}:{instrument_id}:{epoch}:{engine_version}:count={}:abs={}",
        deltas.len(),
        total_abs
    )
}

struct ClearingApplyUpdate<'a> {
    instrument_admin: &'a str,
    instrument_id: &'a str,
    epoch: i64,
    account_id: &'a str,
    batch_anchor_cid: &'a str,
    account_state_before_cid: Option<&'a str>,
    account_state_after_cid: Option<&'a str>,
    delta_minor: i64,
    status: &'a str,
    applied_offset: Option<i64>,
    applied_update_id: Option<&'a str>,
    error: Option<String>,
}

async fn upsert_clearing_apply_tx(
    tx: &mut Transaction<'_, Postgres>,
    row: &ClearingApplyUpdate<'_>,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO clearing_applies (
          instrument_admin,
          instrument_id,
          epoch,
          account_id,
          batch_anchor_cid,
          account_state_before_cid,
          account_state_after_cid,
          delta_minor,
          status,
          applied_offset,
          applied_update_id,
          error
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        ON CONFLICT (instrument_admin, instrument_id, epoch, account_id) DO UPDATE SET
          batch_anchor_cid = EXCLUDED.batch_anchor_cid,
          account_state_before_cid = EXCLUDED.account_state_before_cid,
          account_state_after_cid = EXCLUDED.account_state_after_cid,
          delta_minor = EXCLUDED.delta_minor,
          status = EXCLUDED.status,
          applied_offset = EXCLUDED.applied_offset,
          applied_update_id = EXCLUDED.applied_update_id,
          error = EXCLUDED.error,
          applied_at = now()
        "#,
    )
    .bind(row.instrument_admin)
    .bind(row.instrument_id)
    .bind(row.epoch)
    .bind(row.account_id)
    .bind(row.batch_anchor_cid)
    .bind(row.account_state_before_cid)
    .bind(row.account_state_after_cid)
    .bind(row.delta_minor)
    .bind(row.status)
    .bind(row.applied_offset)
    .bind(row.applied_update_id)
    .bind(row.error.clone())
    .execute(&mut **tx)
    .await
    .context("upsert clearing_applies")?;

    Ok(())
}

async fn upsert_clearing_apply(db: &PgPool, row: &ClearingApplyUpdate<'_>) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO clearing_applies (
          instrument_admin,
          instrument_id,
          epoch,
          account_id,
          batch_anchor_cid,
          account_state_before_cid,
          account_state_after_cid,
          delta_minor,
          status,
          applied_offset,
          applied_update_id,
          error
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        ON CONFLICT (instrument_admin, instrument_id, epoch, account_id) DO UPDATE SET
          batch_anchor_cid = EXCLUDED.batch_anchor_cid,
          account_state_before_cid = EXCLUDED.account_state_before_cid,
          account_state_after_cid = EXCLUDED.account_state_after_cid,
          delta_minor = EXCLUDED.delta_minor,
          status = EXCLUDED.status,
          applied_offset = EXCLUDED.applied_offset,
          applied_update_id = EXCLUDED.applied_update_id,
          error = EXCLUDED.error,
          applied_at = now()
        "#,
    )
    .bind(row.instrument_admin)
    .bind(row.instrument_id)
    .bind(row.epoch)
    .bind(row.account_id)
    .bind(row.batch_anchor_cid)
    .bind(row.account_state_before_cid)
    .bind(row.account_state_after_cid)
    .bind(row.delta_minor)
    .bind(row.status)
    .bind(row.applied_offset)
    .bind(row.applied_update_id)
    .bind(row.error.clone())
    .execute(db)
    .await
    .context("upsert clearing_applies")?;

    Ok(())
}

async fn load_account_cumulative_fill_delta(
    tx: &mut Transaction<'_, Postgres>,
    instrument_admin: &str,
    instrument_id: &str,
    account_id: &str,
    from_epoch_exclusive: i64,
    to_epoch_inclusive: i64,
) -> Result<i64> {
    sqlx::query_scalar(
        r#"
        SELECT COALESCE(SUM(delta_minor), 0)::BIGINT
        FROM (
          SELECT
            CASE
              WHEN taker.side = 'Buy' THEN -(f.price_ticks * f.quantity_minor)
              ELSE f.price_ticks * f.quantity_minor
            END AS delta_minor
          FROM fills f
          JOIN orders taker
            ON taker.order_id = f.taker_order_id
          WHERE f.instrument_admin = $1
            AND f.instrument_id = $2
            AND f.taker_account_id = $3
            AND f.clearing_epoch > $4
            AND f.clearing_epoch <= $5

          UNION ALL

          SELECT
            CASE
              WHEN taker.side = 'Buy' THEN f.price_ticks * f.quantity_minor
              ELSE -(f.price_ticks * f.quantity_minor)
            END AS delta_minor
          FROM fills f
          JOIN orders taker
            ON taker.order_id = f.taker_order_id
          WHERE f.instrument_admin = $1
            AND f.instrument_id = $2
            AND f.maker_account_id = $3
            AND f.clearing_epoch > $4
            AND f.clearing_epoch <= $5
        ) deltas
        "#,
    )
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(account_id)
    .bind(from_epoch_exclusive)
    .bind(to_epoch_inclusive)
    .fetch_one(&mut **tx)
    .await
    .context("query account cumulative fill delta for risk-state settlement")
}

async fn load_account_cumulative_epoch_delta(
    tx: &mut Transaction<'_, Postgres>,
    instrument_admin: &str,
    instrument_id: &str,
    account_id: &str,
    from_epoch_exclusive: i64,
    to_epoch_inclusive: i64,
) -> Result<i64> {
    sqlx::query_scalar(
        r#"
        SELECT COALESCE(SUM(delta_minor), 0)::BIGINT
        FROM clearing_epoch_deltas
        WHERE instrument_admin = $1
          AND instrument_id = $2
          AND account_id = $3
          AND epoch > $4
          AND epoch <= $5
        "#,
    )
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(account_id)
    .bind(from_epoch_exclusive)
    .bind(to_epoch_inclusive)
    .fetch_one(&mut **tx)
    .await
    .context("query account cumulative epoch delta")
}

async fn settle_applied_fill_delta_from_risk_state_tx(
    tx: &mut Transaction<'_, Postgres>,
    instrument_admin: &str,
    instrument_id: &str,
    account_id: &str,
    applied_fill_delta_minor: i64,
) -> Result<()> {
    if applied_fill_delta_minor == 0 {
        return Ok(());
    }

    let updated = sqlx::query(
        r#"
        UPDATE account_risk_state
        SET
          delta_pending_trades_minor = account_risk_state.delta_pending_trades_minor - $4,
          updated_at = now()
        WHERE account_id = $1
          AND instrument_admin = $2
          AND instrument_id = $3
        "#,
    )
    .bind(account_id)
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(applied_fill_delta_minor)
    .execute(&mut **tx)
    .await
    .context("settle applied fill delta from account_risk_state")?;

    if updated.rows_affected() == 0 {
        return Err(anyhow!(
            "missing account_risk_state while settling applied fill delta for account {}",
            account_id
        ));
    }

    Ok(())
}

async fn finalize_successful_apply_local_state(
    state: &AppState,
    instrument_admin: &str,
    instrument_id: &str,
    account_id: &str,
    account_fill_delta_minor: i64,
    apply_row: &ClearingApplyUpdate<'_>,
) -> Result<()> {
    let mut tx = state
        .db
        .begin()
        .await
        .context("begin finalize successful apply tx")?;
    sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
        .bind(state.cfg.account_lock_namespace)
        .bind(account_id)
        .execute(&mut *tx)
        .await
        .context("acquire account lock for apply finalization")?;

    settle_applied_fill_delta_from_risk_state_tx(
        &mut tx,
        instrument_admin,
        instrument_id,
        account_id,
        account_fill_delta_minor,
    )
    .await?;
    upsert_clearing_apply_tx(&mut tx, apply_row).await?;

    tx.commit()
        .await
        .context("commit finalize successful apply tx")
}

async fn load_account_state_recovery_row(
    tx: &mut Transaction<'_, Postgres>,
    instrument_admin: &str,
    instrument_id: &str,
    account_id: &str,
) -> Result<Option<AccountStateRecoveryRow>> {
    sqlx::query_as(
        r#"
        SELECT
          st.contract_id,
          st.last_applied_epoch,
          st.last_applied_batch_anchor
        FROM account_state_latest latest
        JOIN account_states st
          ON st.contract_id = latest.contract_id
        WHERE latest.account_id = $1
          AND st.active = TRUE
          AND st.instrument_admin = $2
          AND st.instrument_id = $3
        LIMIT 1
        FOR UPDATE OF st
        "#,
    )
    .bind(account_id)
    .bind(instrument_admin)
    .bind(instrument_id)
    .fetch_optional(&mut **tx)
    .await
    .context("query account state for post-apply recovery")
}

async fn load_account_max_applied_epoch(
    tx: &mut Transaction<'_, Postgres>,
    instrument_admin: &str,
    instrument_id: &str,
    account_id: &str,
) -> Result<i64> {
    let max_applied_epoch: Option<i64> = sqlx::query_scalar(
        r#"
        SELECT MAX(epoch)
        FROM clearing_applies
        WHERE instrument_admin = $1
          AND instrument_id = $2
          AND account_id = $3
          AND status = 'Applied'
        "#,
    )
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(account_id)
    .fetch_one(&mut **tx)
    .await
    .context("query max applied epoch for account")?;

    Ok(max_applied_epoch.unwrap_or(0))
}

async fn recover_missing_post_apply_local_state_for_account(
    state: &AppState,
    instrument_admin: &str,
    instrument_id: &str,
    account_id: &str,
) -> Result<Option<i64>> {
    let mut tx = state
        .db
        .begin()
        .await
        .context("begin account recovery tx")?;
    sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
        .bind(state.cfg.account_lock_namespace)
        .bind(account_id)
        .execute(&mut *tx)
        .await
        .context("acquire account lock for post-apply recovery")?;

    let Some(account_state) =
        load_account_state_recovery_row(&mut tx, instrument_admin, instrument_id, account_id)
            .await?
    else {
        tx.commit()
            .await
            .context("commit account recovery tx (no active state)")?;
        return Ok(None);
    };

    let max_applied_epoch =
        load_account_max_applied_epoch(&mut tx, instrument_admin, instrument_id, account_id)
            .await?;
    if account_state.last_applied_epoch <= max_applied_epoch {
        tx.commit()
            .await
            .context("commit account recovery tx (no gap)")?;
        return Ok(None);
    }

    // Intentional split:
    // - Fill delta drives account_risk_state.delta_pending_trades_minor reconciliation.
    // - Epoch delta records what was applied on-ledger, including settlement deltas.
    let account_fill_delta_minor = load_account_cumulative_fill_delta(
        &mut tx,
        instrument_admin,
        instrument_id,
        account_id,
        max_applied_epoch,
        account_state.last_applied_epoch,
    )
    .await?;
    let account_delta_minor = load_account_cumulative_epoch_delta(
        &mut tx,
        instrument_admin,
        instrument_id,
        account_id,
        max_applied_epoch,
        account_state.last_applied_epoch,
    )
    .await?;
    let batch_anchor_cid = account_state
        .last_applied_batch_anchor
        .as_deref()
        .ok_or_else(|| {
            anyhow!(
                "missing last_applied_batch_anchor for account {} at epoch {}",
                account_id,
                account_state.last_applied_epoch
            )
        })?;

    settle_applied_fill_delta_from_risk_state_tx(
        &mut tx,
        instrument_admin,
        instrument_id,
        account_id,
        account_fill_delta_minor,
    )
    .await?;
    upsert_clearing_apply_tx(
        &mut tx,
        &ClearingApplyUpdate {
            instrument_admin,
            instrument_id,
            epoch: account_state.last_applied_epoch,
            account_id,
            batch_anchor_cid,
            account_state_before_cid: None,
            account_state_after_cid: Some(&account_state.contract_id),
            delta_minor: account_delta_minor,
            status: "Applied",
            applied_offset: None,
            applied_update_id: None,
            error: None,
        },
    )
    .await?;

    tx.commit()
        .await
        .context("commit account post-apply recovery tx")?;

    Ok(Some(account_fill_delta_minor))
}

async fn recover_missing_post_apply_local_state_for_instrument(
    state: &AppState,
    instrument_admin: &str,
    instrument_id: &str,
) -> Result<()> {
    let mut total_recovered_accounts: i64 = 0;
    let mut total_recovered_fill_delta_minor: i64 = 0;
    loop {
        let candidates: Vec<RecoveryCandidateRow> = sqlx::query_as(
            r#"
            SELECT st.account_id
            FROM account_state_latest latest
            JOIN account_states st
              ON st.contract_id = latest.contract_id
            LEFT JOIN (
              SELECT account_id, MAX(epoch) AS max_applied_epoch
              FROM clearing_applies
              WHERE instrument_admin = $1
                AND instrument_id = $2
                AND status = 'Applied'
              GROUP BY account_id
            ) ca ON ca.account_id = st.account_id
            WHERE st.active = TRUE
              AND st.instrument_admin = $1
              AND st.instrument_id = $2
              AND st.last_applied_epoch > COALESCE(ca.max_applied_epoch, 0)
            ORDER BY st.account_id ASC
            LIMIT 256
            "#,
        )
        .bind(instrument_admin)
        .bind(instrument_id)
        .fetch_all(&state.db)
        .await
        .context("query accounts needing post-apply local recovery")?;
        if candidates.is_empty() {
            break;
        }

        for candidate in candidates {
            if let Some(fill_delta_minor) = recover_missing_post_apply_local_state_for_account(
                state,
                instrument_admin,
                instrument_id,
                &candidate.account_id,
            )
            .await?
            {
                total_recovered_accounts = total_recovered_accounts
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("recovered accounts counter overflow"))?;
                total_recovered_fill_delta_minor = total_recovered_fill_delta_minor
                    .checked_add(fill_delta_minor)
                    .ok_or_else(|| anyhow!("recovered fill delta counter overflow"))?;
            }
        }
    }

    if total_recovered_accounts > 0 {
        tracing::warn!(
            instrument_admin = %instrument_admin,
            instrument_id = %instrument_id,
            recovered_accounts = total_recovered_accounts,
            recovered_fill_delta_minor = total_recovered_fill_delta_minor,
            "recovered missing post-apply local state"
        );
    }

    Ok(())
}

async fn log_pending_trade_delta_drift_for_instrument(
    state: &AppState,
    instrument_admin: &str,
    instrument_id: &str,
) -> Result<()> {
    let summary: PendingTradeDriftSummaryRow = sqlx::query_as(
        r#"
        WITH latest_account_state AS (
          SELECT st.account_id, st.last_applied_epoch
          FROM account_state_latest latest
          JOIN account_states st
            ON st.contract_id = latest.contract_id
          WHERE st.active = TRUE
            AND st.instrument_admin = $1
            AND st.instrument_id = $2
        ),
        pending_fill_deltas AS (
          SELECT
            account_id,
            COALESCE(SUM(delta_minor), 0)::BIGINT AS pending_delta_minor
          FROM (
            SELECT
              f.taker_account_id AS account_id,
              CASE
                WHEN taker.side = 'Buy' THEN -(f.price_ticks * f.quantity_minor)
                ELSE f.price_ticks * f.quantity_minor
              END AS delta_minor
            FROM fills f
            JOIN orders taker
              ON taker.order_id = f.taker_order_id
            JOIN latest_account_state las
              ON las.account_id = f.taker_account_id
            WHERE f.clearing_epoch IS NULL OR f.clearing_epoch > las.last_applied_epoch

            UNION ALL

            SELECT
              f.maker_account_id AS account_id,
              CASE
                WHEN taker.side = 'Buy' THEN f.price_ticks * f.quantity_minor
                ELSE -(f.price_ticks * f.quantity_minor)
              END AS delta_minor
            FROM fills f
            JOIN orders taker
              ON taker.order_id = f.taker_order_id
            JOIN latest_account_state las
              ON las.account_id = f.maker_account_id
            WHERE f.clearing_epoch IS NULL OR f.clearing_epoch > las.last_applied_epoch
          ) contributions
          GROUP BY account_id
        ),
        drift AS (
          SELECT
            rs.delta_pending_trades_minor - COALESCE(pfd.pending_delta_minor, 0) AS drift_minor
          FROM account_risk_state rs
          JOIN latest_account_state las
            ON las.account_id = rs.account_id
          LEFT JOIN pending_fill_deltas pfd
            ON pfd.account_id = rs.account_id
          WHERE rs.instrument_admin = $1
            AND rs.instrument_id = $2
            AND rs.delta_pending_trades_minor <> COALESCE(pfd.pending_delta_minor, 0)
        )
        SELECT
          COUNT(*)::BIGINT AS drifted_accounts,
          COALESCE(SUM(drift_minor), 0)::BIGINT AS total_drift_minor,
          COALESCE(MAX(ABS(drift_minor)), 0)::BIGINT AS max_abs_drift_minor
        FROM drift
        "#,
    )
    .bind(instrument_admin)
    .bind(instrument_id)
    .fetch_one(&state.db)
    .await
    .context("query pending-trade drift summary")?;

    if summary.drifted_accounts == 0 {
        return Ok(());
    }

    let details: Vec<PendingTradeDriftDetailRow> = sqlx::query_as(
        r#"
        WITH latest_account_state AS (
          SELECT st.account_id, st.last_applied_epoch
          FROM account_state_latest latest
          JOIN account_states st
            ON st.contract_id = latest.contract_id
          WHERE st.active = TRUE
            AND st.instrument_admin = $1
            AND st.instrument_id = $2
        ),
        pending_fill_deltas AS (
          SELECT
            account_id,
            COALESCE(SUM(delta_minor), 0)::BIGINT AS pending_delta_minor
          FROM (
            SELECT
              f.taker_account_id AS account_id,
              CASE
                WHEN taker.side = 'Buy' THEN -(f.price_ticks * f.quantity_minor)
                ELSE f.price_ticks * f.quantity_minor
              END AS delta_minor
            FROM fills f
            JOIN orders taker
              ON taker.order_id = f.taker_order_id
            JOIN latest_account_state las
              ON las.account_id = f.taker_account_id
            WHERE f.clearing_epoch IS NULL OR f.clearing_epoch > las.last_applied_epoch

            UNION ALL

            SELECT
              f.maker_account_id AS account_id,
              CASE
                WHEN taker.side = 'Buy' THEN f.price_ticks * f.quantity_minor
                ELSE -(f.price_ticks * f.quantity_minor)
              END AS delta_minor
            FROM fills f
            JOIN orders taker
              ON taker.order_id = f.taker_order_id
            JOIN latest_account_state las
              ON las.account_id = f.maker_account_id
            WHERE f.clearing_epoch IS NULL OR f.clearing_epoch > las.last_applied_epoch
          ) contributions
          GROUP BY account_id
        )
        SELECT
          rs.account_id,
          rs.delta_pending_trades_minor AS observed_delta_pending_trades_minor,
          COALESCE(pfd.pending_delta_minor, 0) AS expected_pending_fill_delta_minor,
          rs.delta_pending_trades_minor - COALESCE(pfd.pending_delta_minor, 0) AS drift_minor
        FROM account_risk_state rs
        JOIN latest_account_state las
          ON las.account_id = rs.account_id
        LEFT JOIN pending_fill_deltas pfd
          ON pfd.account_id = rs.account_id
        WHERE rs.instrument_admin = $1
          AND rs.instrument_id = $2
          AND rs.delta_pending_trades_minor <> COALESCE(pfd.pending_delta_minor, 0)
        ORDER BY ABS(rs.delta_pending_trades_minor - COALESCE(pfd.pending_delta_minor, 0)) DESC,
                 rs.account_id ASC
        LIMIT 10
        "#,
    )
    .bind(instrument_admin)
    .bind(instrument_id)
    .fetch_all(&state.db)
    .await
    .context("query pending-trade drift details")?;

    let sample = details
        .iter()
        .map(|row| {
            format!(
                "{}:{}=>{} ({})",
                row.account_id,
                row.observed_delta_pending_trades_minor,
                row.expected_pending_fill_delta_minor,
                row.drift_minor
            )
        })
        .collect::<Vec<_>>()
        .join(", ");

    tracing::warn!(
        instrument_admin = %instrument_admin,
        instrument_id = %instrument_id,
        drifted_accounts = summary.drifted_accounts,
        total_drift_minor = summary.total_drift_minor,
        max_abs_drift_minor = summary.max_abs_drift_minor,
        sample = %sample,
        "pending-trade delta drift detected"
    );

    Ok(())
}

async fn create_next_batch(
    state: &AppState,
    clearing_head_cid: &str,
    batch_hash: &str,
    instrument_admin: &str,
    instrument_id: &str,
    epoch: i64,
) -> Result<lapi::Transaction> {
    let epoch = validate_positive_epoch(epoch).context("validate create_next_batch epoch")?;
    let batch_hash = parse_batch_hash(batch_hash).context("validate create_next_batch hash")?;

    let template_id = lapi::Identifier {
        package_id: "#pebble".to_string(),
        module_name: "Pebble.Clearing".to_string(),
        entity_name: "ClearingHead".to_string(),
    };

    let args = value_record(vec![record_field("batchHash", value_text(&batch_hash))]);
    let cmd = lapi::Command {
        command: Some(lapi::command::Command::Exercise(lapi::ExerciseCommand {
            template_id: Some(template_id),
            contract_id: clearing_head_cid.to_string(),
            choice: "CreateNextBatch".to_string(),
            choice_argument: Some(args),
        })),
    };
    let command_id = format!("clearing:batch:{instrument_admin}:{instrument_id}:{epoch}");
    submit_and_wait_for_transaction(state, command_id, vec![cmd]).await
}

struct ApplyClearingDeltaCmd<'a> {
    account_state_cid: &'a str,
    batch_anchor_cid: &'a str,
    delta_minor: i64,
    instrument_admin: &'a str,
    instrument_id: &'a str,
    epoch: i64,
    account_id: &'a str,
}

async fn apply_clearing_delta(
    state: &AppState,
    cmd: &ApplyClearingDeltaCmd<'_>,
) -> Result<lapi::Transaction> {
    validate_positive_epoch(cmd.epoch).context("validate apply_clearing_delta epoch")?;
    parse_account_id(cmd.account_id).context("validate apply_clearing_delta account_id")?;

    let template_id = lapi::Identifier {
        package_id: "#pebble".to_string(),
        module_name: "Pebble.Account".to_string(),
        entity_name: "AccountState".to_string(),
    };

    let args = value_record(vec![
        record_field("batchAnchorCid", value_contract_id(cmd.batch_anchor_cid)),
        record_field("deltaMinor", value_int64(cmd.delta_minor)),
    ]);
    let command = lapi::Command {
        command: Some(lapi::command::Command::Exercise(lapi::ExerciseCommand {
            template_id: Some(template_id),
            contract_id: cmd.account_state_cid.to_string(),
            choice: "ApplyClearingDelta".to_string(),
            choice_argument: Some(args),
        })),
    };
    let command_id = format!(
        "clearing:apply:{}:{}:{}:{}",
        cmd.instrument_admin, cmd.instrument_id, cmd.epoch, cmd.account_id
    );
    submit_and_wait_for_transaction(state, command_id, vec![command]).await
}

fn maybe_apply_auth<T>(
    auth_header: &Option<MetadataValue<tonic::metadata::Ascii>>,
    req: &mut Request<T>,
) {
    if let Some(auth_header) = auth_header.clone() {
        req.metadata_mut().insert("authorization", auth_header);
    }
}

async fn submit_and_wait_for_transaction(
    state: &AppState,
    command_id: String,
    commands: Vec<lapi::Command>,
) -> Result<lapi::Transaction> {
    let command_count = commands.len();
    tracing::debug!(command_id = %command_id, command_count, "submitting clearing ledger command");

    let commands = lapi::Commands {
        workflow_id: String::new(),
        user_id: state.cfg.ledger_user_id.clone(),
        command_id,
        commands,
        deduplication_period: None,
        min_ledger_time_abs: None,
        min_ledger_time_rel: None,
        act_as: vec![state.cfg.committee_party.clone()],
        read_as: vec![],
        submission_id: uuid::Uuid::new_v4().to_string(),
        disclosed_contracts: vec![],
        synchronizer_id: String::new(),
        package_id_selection_preference: vec![],
        prefetch_contract_keys: vec![],
    };
    let command_id_for_log = commands.command_id.clone();

    let mut client =
        lapi::command_service_client::CommandServiceClient::new(state.cfg.ledger_channel.clone());

    let mut req = Request::new(lapi::SubmitAndWaitForTransactionRequest {
        commands: Some(commands),
        transaction_format: None,
    });
    maybe_apply_auth(&state.cfg.auth_header, &mut req);

    let resp = client
        .submit_and_wait_for_transaction(req)
        .await
        .context("submit_and_wait_for_transaction")?
        .into_inner();

    let tx = resp
        .transaction
        .ok_or_else(|| anyhow!("missing transaction in response"))?;
    tracing::info!(
        command_id = %command_id_for_log,
        update_id = %tx.update_id,
        offset = tx.offset,
        "clearing ledger command committed"
    );

    Ok(tx)
}

fn extract_created_contract_id(
    tx: &lapi::Transaction,
    module: &str,
    entity: &str,
) -> Option<String> {
    tx.events.iter().find_map(|evt| {
        let Some(lapi::event::Event::Created(created)) = evt.event.as_ref() else {
            return None;
        };
        let template_id = created.template_id.as_ref()?;
        if template_id.module_name == module && template_id.entity_name == entity {
            Some(created.contract_id.clone())
        } else {
            None
        }
    })
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

fn value_text(value: &str) -> lapi::Value {
    lapi::Value {
        sum: Some(lapi::value::Sum::Text(value.to_string())),
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

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_env_bool_accepts_common_values() {
        assert!(parse_env_bool("K", "1").unwrap());
        assert!(parse_env_bool("K", "true").unwrap());
        assert!(parse_env_bool("K", "YES").unwrap());
        assert!(parse_env_bool("K", "on").unwrap());

        assert!(!parse_env_bool("K", "0").unwrap());
        assert!(!parse_env_bool("K", "false").unwrap());
        assert!(!parse_env_bool("K", "No").unwrap());
        assert!(!parse_env_bool("K", "OFF").unwrap());
    }

    #[test]
    fn compute_batch_hash_is_stable() {
        let deltas = vec![
            DeltaRow {
                delta_minor: 3,
                source_fill_count: 2,
            },
            DeltaRow {
                delta_minor: -1,
                source_fill_count: 1,
            },
        ];
        let got = compute_batch_hash("ia", "iid", 7, "ev1", &deltas);
        assert_eq!(got, "m3:ia:iid:7:ev1:count=2:abs=4");
    }

    #[test]
    fn cash_delta_minor_signs_by_taker_side() {
        assert_eq!(cash_delta_minor("Buy", 5, 2).unwrap(), -10);
        assert_eq!(cash_delta_minor("Sell", 5, 2).unwrap(), 10);
        assert!(cash_delta_minor("Unknown", 5, 2).is_err());
    }

    #[test]
    fn compute_market_settlement_account_deltas_uses_explicit_payout() {
        let positions = vec![
            SettlementPositionRow {
                account_id: "acct-a".to_string(),
                outcome: "YES".to_string(),
                net_quantity_minor: 10,
            },
            SettlementPositionRow {
                account_id: "acct-a".to_string(),
                outcome: "NO".to_string(),
                net_quantity_minor: -10,
            },
            SettlementPositionRow {
                account_id: "acct-b".to_string(),
                outcome: "YES".to_string(),
                net_quantity_minor: -6,
            },
            SettlementPositionRow {
                account_id: "acct-c".to_string(),
                outcome: "YES".to_string(),
                net_quantity_minor: -4,
            },
        ];

        let (deltas, total_delta_minor) =
            compute_market_settlement_account_deltas(&positions, "YES", 100).unwrap();
        assert_eq!(total_delta_minor, 0);

        let by_account: BTreeMap<String, (i64, i32)> = deltas
            .into_iter()
            .map(|row| (row.account_id, (row.delta_minor, row.source_position_count)))
            .collect();
        assert_eq!(by_account.get("acct-a"), Some(&(1000, 2)));
        assert_eq!(by_account.get("acct-b"), Some(&(-600, 1)));
        assert_eq!(by_account.get("acct-c"), Some(&(-400, 1)));
    }

    #[test]
    fn compute_market_settlement_account_deltas_rejects_non_positive_payout() {
        let positions = vec![SettlementPositionRow {
            account_id: "acct-a".to_string(),
            outcome: "YES".to_string(),
            net_quantity_minor: 1,
        }];

        assert!(compute_market_settlement_account_deltas(&positions, "YES", 0).is_err());
    }

    #[test]
    fn recovery_intentionally_splits_fill_and_epoch_delta_sources() {
        // Fill-driven pending trades can differ from epoch-applied deltas when market
        // settlement contributes additional account deltas in the same applied range.
        let fill_pending_delta_minor = 500_i64;
        let positions = vec![
            SettlementPositionRow {
                account_id: "acct-a".to_string(),
                outcome: "YES".to_string(),
                net_quantity_minor: -2,
            },
            SettlementPositionRow {
                account_id: "acct-b".to_string(),
                outcome: "YES".to_string(),
                net_quantity_minor: 2,
            },
        ];
        let (settlement_rows, total_delta_minor) =
            compute_market_settlement_account_deltas(&positions, "YES", 100).unwrap();
        assert_eq!(total_delta_minor, 0);
        let settlement_by_account: BTreeMap<String, i64> = settlement_rows
            .into_iter()
            .map(|row| (row.account_id, row.delta_minor))
            .collect();
        let settlement_delta_minor = *settlement_by_account.get("acct-a").unwrap();
        assert_eq!(settlement_delta_minor, -200);

        let epoch_delta_minor = fill_pending_delta_minor + settlement_delta_minor;
        assert_eq!(epoch_delta_minor, 300);
        assert_ne!(fill_pending_delta_minor, epoch_delta_minor);
    }
}
