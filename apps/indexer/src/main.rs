use std::{collections::HashMap, time::Duration};

use anyhow::{anyhow, Context as _, Result};
use chrono::{DateTime, Utc};
use pebble_daml_grpc::com::daml::ledger::api::v2 as lapi;
use sqlx::{postgres::PgPoolOptions, PgPool, Postgres, Row, Transaction};
use tonic::{metadata::MetadataValue, transport::Endpoint, Request};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const OFFSET_CONSUMER: &str = "pebble-indexer";
const DEFAULT_MARKET_PAYOUT_PER_SHARE_MINOR: i64 = 100;

#[derive(Debug)]
struct OffsetAfterLedgerEndError {
    stored_offset: i64,
    ledger_end: i64,
}

impl std::fmt::Display for OffsetAfterLedgerEndError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "stored begin offset ({}) is after current ledger end ({})",
            self.stored_offset, self.ledger_end
        )
    }
}

impl std::error::Error for OffsetAfterLedgerEndError {}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sqlx=warn".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg = Config::from_env()?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&cfg.db_url)
        .await
        .context("connect to postgres")?;

    // Keeps schema bootstrapping local to the repo without requiring external tooling.
    sqlx::migrate!("../../infra/sql/migrations")
        .run(&pool)
        .await
        .context("run migrations")?;

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    tracing::info!(
        ledger_host = %cfg.ledger_host,
        ledger_port = %cfg.ledger_port,
        read_parties = ?cfg.read_parties,
        submission_path = %cfg.submission_path,
        auto_reset = cfg.auto_reset,
        "pebble-indexer starting"
    );

    let mut backoff = Duration::from_secs(1);

    loop {
        let begin_exclusive = get_begin_exclusive(&pool).await?;

        let run_fut = run_updates(&cfg, &pool, begin_exclusive);
        tokio::pin!(run_fut);

        tokio::select! {
            _ = &mut shutdown => {
                tracing::info!("shutdown requested");
                break;
            }
            res = &mut run_fut => {
                match res {
                    Ok(()) => {
                        // If the stream ends cleanly (rare), restart with a small delay.
                        backoff = Duration::from_secs(1);
                        tracing::warn!("ledger update stream ended; reconnecting");
                    }
                    Err(err) => {
                        if let Some(e) = err.downcast_ref::<OffsetAfterLedgerEndError>() {
                            tracing::error!(
                                stored_offset = e.stored_offset,
                                ledger_end = e.ledger_end,
                                auto_reset = cfg.auto_reset,
                                "stored offset is after ledger end; derived state is out of sync with this ledger"
                            );
                            tracing::error!(
                                "fix: wipe local derived tables or enable auto-reset via PEBBLE_INDEXER_AUTO_RESET=1 \
                                 (auto-reset truncates derived tables and restarts indexing from offset 0)"
                            );
                            return Err(err);
                        }
                        // Use Debug formatting to preserve anyhow's error chain (tonic status, sources, etc.).
                        tracing::error!(error = ?err, "ledger update stream failed; will retry");
                    }
                }
            }
        }

        tokio::select! {
            _ = &mut shutdown => {
                tracing::info!("shutdown requested");
                break;
            }
            _ = tokio::time::sleep(backoff) => {}
        }

        backoff = (backoff * 2).min(Duration::from_secs(30));
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct Config {
    db_url: String,
    ledger_host: String,
    ledger_port: u16,
    read_parties: Vec<String>,
    auth_header: Option<MetadataValue<tonic::metadata::Ascii>>,
    submission_path: String,
    auto_reset: bool,
}

impl Config {
    fn from_env() -> Result<Self> {
        let db_url = std::env::var("PEBBLE_DB_URL")
            .unwrap_or_else(|_| "postgres://pebble:pebble@127.0.0.1:5432/pebble".to_string());

        let ledger_host = std::env::var("PEBBLE_CANTON_LEDGER_API_HOST")
            .unwrap_or_else(|_| "127.0.0.1".to_string());

        let ledger_port = std::env::var("PEBBLE_CANTON_LEDGER_API_PORT")
            .unwrap_or_else(|_| "6865".to_string())
            .parse::<u16>()
            .context("parse PEBBLE_CANTON_LEDGER_API_PORT")?;

        let primary_party = std::env::var("PEBBLE_INDEXER_PARTY")
            .context("PEBBLE_INDEXER_PARTY is required (primary party to read as)")?;
        let read_parties = match std::env::var("PEBBLE_INDEXER_READ_PARTIES") {
            Ok(raw) => {
                let mut parties = vec![primary_party.clone()];
                parties.extend(parse_party_csv("PEBBLE_INDEXER_READ_PARTIES", &raw)?);
                dedupe_nonempty_parties(parties)?
            }
            Err(_) => vec![primary_party],
        };

        let auth_header = match std::env::var("PEBBLE_CANTON_AUTH_TOKEN") {
            Ok(token) if !token.trim().is_empty() => Some(parse_auth_header(&token)?),
            _ => None,
        };

        // Defaults to enabled when talking to a local sandbox, disabled otherwise.
        let auto_reset = match std::env::var("PEBBLE_INDEXER_AUTO_RESET") {
            Ok(v) => parse_env_bool("PEBBLE_INDEXER_AUTO_RESET", &v)?,
            Err(_) => is_local_ledger_host(&ledger_host),
        };

        let submission_path = match std::env::var("PEBBLE_INDEXER_SUBMISSION_PATH") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => {
                // This is only used as a key for spend-safety gating; keep it explicit and stable.
                format!("ledger:{}:{}", ledger_host.trim(), ledger_port)
            }
        };

        Ok(Self {
            db_url,
            ledger_host,
            ledger_port,
            read_parties,
            auth_header,
            submission_path,
            auto_reset,
        })
    }
}

fn parse_party_csv(key: &str, raw: &str) -> Result<Vec<String>> {
    let parties = raw
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>();
    if parties.is_empty() {
        return Err(anyhow!("{key} must contain at least one non-empty party"));
    }
    Ok(parties)
}

fn dedupe_nonempty_parties(parties: Vec<String>) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for party in parties {
        let trimmed = party.trim();
        if trimmed.is_empty() {
            continue;
        }
        let candidate = trimmed.to_string();
        if !out.iter().any(|existing: &String| existing == &candidate) {
            out.push(candidate);
        }
    }
    if out.is_empty() {
        return Err(anyhow!("read party list must not be empty"));
    }
    Ok(out)
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

fn is_local_ledger_host(host: &str) -> bool {
    // Keep this intentionally conservative: only auto-reset by default when we are clearly talking
    // to a locally-running dev ledger.
    matches!(
        host.trim().to_ascii_lowercase().as_str(),
        "127.0.0.1" | "localhost" | "::1" | "0.0.0.0"
    )
}

fn maybe_apply_auth<T>(cfg: &Config, req: &mut Request<T>) {
    if let Some(auth_header) = cfg.auth_header.clone() {
        req.metadata_mut().insert("authorization", auth_header);
    }
}

async fn get_ledger_end(cfg: &Config, channel: tonic::transport::Channel) -> Result<i64> {
    let mut client = lapi::state_service_client::StateServiceClient::new(channel);

    let mut req = Request::new(lapi::GetLedgerEndRequest {});
    maybe_apply_auth(cfg, &mut req);

    let resp = client
        .get_ledger_end(req)
        .await
        .context("get_ledger_end gRPC call")?
        .into_inner();

    Ok(resp.offset)
}

async fn reset_derived_state(pool: &PgPool) -> Result<()> {
    // Projections are derived from the ledger and safe to wipe when connecting to a fresh/restarted
    // local sandbox.
    let mut dbtx = pool.begin().await.context("begin db transaction")?;

    sqlx::query(
        r#"
        TRUNCATE TABLE
          ledger_updates,
          submission_path_watermarks,
          markets,
          token_configs,
          token_holdings,
          token_holding_claims,
          token_transfer_instructions,
          token_transfer_instruction_latest,
          token_wallets,
          fee_schedules,
          oracle_configs,
          account_refs,
          account_ref_latest,
          account_states,
          account_state_latest,
          clearing_heads,
          clearing_head_latest,
          batch_anchors,
          deposit_requests,
          deposit_pendings,
          deposit_pending_latest,
          deposit_receipts,
          withdrawal_requests,
          withdrawal_pendings,
          withdrawal_pending_latest,
          withdrawal_receipts,
          quarantined_holdings
        "#,
    )
    .execute(&mut *dbtx)
    .await
    .context("truncate derived tables")?;

    sqlx::query(
        r#"
        DELETE FROM ledger_offsets
        WHERE consumer = $1
        "#,
    )
    .bind(OFFSET_CONSUMER)
    .execute(&mut *dbtx)
    .await
    .context("reset stored ledger offset")?;

    dbtx.commit().await.context("commit db transaction")?;

    Ok(())
}

async fn validate_or_reset_begin_offset(
    cfg: &Config,
    pool: &PgPool,
    stored_offset: i64,
    ledger_end: i64,
) -> Result<i64> {
    if stored_offset <= ledger_end {
        return Ok(stored_offset);
    }

    if !cfg.auto_reset {
        return Err(anyhow::Error::new(OffsetAfterLedgerEndError {
            stored_offset,
            ledger_end,
        }));
    }

    tracing::warn!(
        stored_offset,
        ledger_end,
        "stored offset is after ledger end; assuming ledger reset; wiping derived state and restarting from offset 0"
    );
    reset_derived_state(pool).await?;
    Ok(0)
}

async fn run_updates(cfg: &Config, pool: &PgPool, stored_offset: i64) -> Result<()> {
    let endpoint = Endpoint::from_shared(format!("http://{}:{}", cfg.ledger_host, cfg.ledger_port))
        .context("build ledger endpoint")?;
    let channel = endpoint.connect().await.context("connect to ledger")?;

    let ledger_end = get_ledger_end(cfg, channel.clone()).await?;
    let begin_exclusive =
        validate_or_reset_begin_offset(cfg, pool, stored_offset, ledger_end).await?;

    let mut client = lapi::update_service_client::UpdateServiceClient::new(channel);

    let update_format = pebble_update_format(&cfg.read_parties)?;
    let req = lapi::GetUpdatesRequest {
        begin_exclusive,
        end_inclusive: None,
        update_format: Some(update_format),
    };

    let mut req = Request::new(req);
    maybe_apply_auth(cfg, &mut req);

    let mut stream = client
        .get_updates(req)
        .await
        .context("get_updates gRPC call")?
        .into_inner();

    while let Some(resp) = stream.message().await.context("read stream message")? {
        let update = resp.update.context("missing update")?;
        match update {
            lapi::get_updates_response::Update::Transaction(tx) => {
                handle_transaction(cfg, pool, tx).await?;
            }
            lapi::get_updates_response::Update::OffsetCheckpoint(_) => {}
            lapi::get_updates_response::Update::Reassignment(_) => {}
            lapi::get_updates_response::Update::TopologyTransaction(_) => {}
        }
    }

    Ok(())
}

fn pebble_update_format(parties: &[String]) -> Result<lapi::UpdateFormat> {
    if parties.is_empty() {
        return Err(anyhow!("indexer read parties must not be empty"));
    }

    let templates = [
        ("#pebble", "Pebble.MarketLifecycle", "MarketLifecycle"),
        ("#pebble", "Pebble.Reference", "TokenConfig"),
        ("#pebble", "Pebble.Reference", "FeeSchedule"),
        ("#pebble", "Pebble.Reference", "OracleConfig"),
        ("#pebble", "Pebble.Account", "AccountRef"),
        ("#pebble", "Pebble.Account", "AccountState"),
        ("#pebble", "Pebble.Clearing", "ClearingHead"),
        ("#pebble", "Pebble.Clearing", "BatchAnchor"),
        ("#pebble", "Pebble.Treasury", "DepositRequest"),
        ("#pebble", "Pebble.Treasury", "DepositPending"),
        ("#pebble", "Pebble.Treasury", "DepositReceipt"),
        ("#pebble", "Pebble.Treasury", "WithdrawalRequest"),
        ("#pebble", "Pebble.Treasury", "WithdrawalPending"),
        ("#pebble", "Pebble.Treasury", "WithdrawalReceipt"),
        ("#pebble", "Pebble.Treasury", "QuarantinedHolding"),
        (
            "#wizardcat-token-standard",
            "Wizardcat.Token.Standard",
            "Holding",
        ),
        (
            "#wizardcat-token-standard",
            "Wizardcat.Token.Standard",
            "HoldingClaim",
        ),
        (
            "#wizardcat-token-standard",
            "Wizardcat.Token.Standard",
            "OperatorAcceptPolicy",
        ),
        (
            "#wizardcat-token-standard",
            "Wizardcat.Token.Standard",
            "TransferInstruction",
        ),
        (
            "#wizardcat-token-standard",
            "Wizardcat.Token.Standard",
            "Wallet",
        ),
    ];

    let cumulative = templates
        .into_iter()
        .map(|(package_id, module_name, entity_name)| {
            let template_id = lapi::Identifier {
                package_id: package_id.to_string(),
                module_name: module_name.to_string(),
                entity_name: entity_name.to_string(),
            };

            lapi::CumulativeFilter {
                identifier_filter: Some(lapi::cumulative_filter::IdentifierFilter::TemplateFilter(
                    lapi::TemplateFilter {
                        template_id: Some(template_id),
                        include_created_event_blob: false,
                    },
                )),
            }
        })
        .collect();

    let filters = lapi::Filters { cumulative };

    let mut filters_by_party: HashMap<String, lapi::Filters> = HashMap::new();
    for party in parties {
        if party.trim().is_empty() {
            return Err(anyhow!("indexer read party must not be empty"));
        }
        filters_by_party.insert(party.clone(), filters.clone());
    }

    let event_format = lapi::EventFormat {
        filters_by_party,
        filters_for_any_party: None,
        verbose: true,
    };

    let tx_format = lapi::TransactionFormat {
        event_format: Some(event_format),
        transaction_shape: lapi::TransactionShape::AcsDelta as i32,
    };

    Ok(lapi::UpdateFormat {
        include_transactions: Some(tx_format),
        include_reassignments: None,
        include_topology_events: None,
    })
}

async fn handle_transaction(cfg: &Config, pool: &PgPool, tx: lapi::Transaction) -> Result<()> {
    let offset = tx.offset;
    if offset <= 0 {
        return Err(anyhow!("invalid transaction offset: {offset}"));
    }

    let update_id = tx.update_id.trim().to_string();
    if update_id.is_empty() {
        return Err(anyhow!("missing update_id"));
    }

    let record_time = tx
        .record_time
        .as_ref()
        .ok_or_else(|| anyhow!("missing record_time"))?;
    let record_time: DateTime<Utc> = timestamp_to_utc(record_time)?;
    let update_span = tracing::info_span!("ledger_update", offset, update_id = %update_id);
    let _update_span = update_span.enter();
    tracing::debug!(record_time = %record_time, "processing ledger update");

    let mut dbtx = pool.begin().await.context("begin db transaction")?;

    upsert_ledger_update(&mut dbtx, offset, &update_id, record_time).await?;

    for evt in tx.events {
        handle_event(&mut dbtx, offset, evt).await?;
    }

    upsert_offset(&mut dbtx, offset).await?;
    upsert_submission_path_watermark(&mut dbtx, &cfg.submission_path, offset).await?;
    dbtx.commit().await.context("commit db transaction")?;
    tracing::debug!("ledger update committed");

    Ok(())
}

async fn upsert_ledger_update(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    update_id: &str,
    record_time: DateTime<Utc>,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO ledger_updates (ledger_offset, update_id, record_time)
        VALUES ($1,$2,$3)
        ON CONFLICT (ledger_offset) DO UPDATE SET
          update_id = EXCLUDED.update_id,
          record_time = EXCLUDED.record_time
        "#,
    )
    .bind(offset)
    .bind(update_id)
    .bind(record_time)
    .execute(&mut **dbtx)
    .await
    .context("upsert ledger_update")?;

    Ok(())
}

async fn handle_event(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    evt: lapi::Event,
) -> Result<()> {
    let evt = evt.event.context("missing event body")?;
    match evt {
        lapi::event::Event::Created(created) => handle_created(dbtx, offset, created).await,
        lapi::event::Event::Archived(archived) => handle_archived(dbtx, offset, archived).await,
        lapi::event::Event::Exercised(_) => Ok(()),
    }
}

async fn handle_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let template_id = created
        .template_id
        .as_ref()
        .ok_or_else(|| anyhow!("missing template_id"))?;

    match (
        created.package_name.as_str(),
        template_id.module_name.as_str(),
        template_id.entity_name.as_str(),
    ) {
        ("pebble", "Pebble.MarketLifecycle", "MarketLifecycle") => {
            handle_market_created(dbtx, offset, created).await
        }
        ("pebble", "Pebble.Reference", "TokenConfig") => {
            handle_token_config_created(dbtx, offset, created).await
        }
        ("pebble", "Pebble.Reference", "FeeSchedule") => {
            handle_fee_schedule_created(dbtx, offset, created).await
        }
        ("pebble", "Pebble.Reference", "OracleConfig") => {
            handle_oracle_config_created(dbtx, offset, created).await
        }
        ("pebble", "Pebble.Account", "AccountRef") => {
            handle_account_ref_created(dbtx, offset, created).await
        }
        ("pebble", "Pebble.Account", "AccountState") => {
            handle_account_state_created(dbtx, offset, created).await
        }
        ("pebble", "Pebble.Clearing", "ClearingHead") => {
            handle_clearing_head_created(dbtx, offset, created).await
        }
        ("pebble", "Pebble.Clearing", "BatchAnchor") => {
            handle_batch_anchor_created(dbtx, offset, created).await
        }
        ("pebble", "Pebble.Treasury", "DepositRequest") => {
            handle_deposit_request_created(dbtx, offset, created).await
        }
        ("pebble", "Pebble.Treasury", "DepositPending") => {
            handle_deposit_pending_created(dbtx, offset, created).await
        }
        ("pebble", "Pebble.Treasury", "DepositReceipt") => {
            handle_deposit_receipt_created(dbtx, offset, created).await
        }
        ("pebble", "Pebble.Treasury", "WithdrawalRequest") => {
            handle_withdrawal_request_created(dbtx, offset, created).await
        }
        ("pebble", "Pebble.Treasury", "WithdrawalPending") => {
            handle_withdrawal_pending_created(dbtx, offset, created).await
        }
        ("pebble", "Pebble.Treasury", "WithdrawalReceipt") => {
            handle_withdrawal_receipt_created(dbtx, offset, created).await
        }
        ("pebble", "Pebble.Treasury", "QuarantinedHolding") => {
            handle_quarantined_holding_created(dbtx, offset, created).await
        }
        ("wizardcat-token-standard", "Wizardcat.Token.Standard", "Holding") => {
            handle_token_holding_created(dbtx, offset, created).await
        }
        ("wizardcat-token-standard", "Wizardcat.Token.Standard", "HoldingClaim") => {
            handle_token_holding_claim_created(dbtx, offset, created).await
        }
        ("wizardcat-token-standard", "Wizardcat.Token.Standard", "OperatorAcceptPolicy") => {
            handle_token_operator_accept_policy_created(dbtx, offset, created).await
        }
        ("wizardcat-token-standard", "Wizardcat.Token.Standard", "TransferInstruction") => {
            handle_token_transfer_instruction_created(dbtx, offset, created).await
        }
        ("wizardcat-token-standard", "Wizardcat.Token.Standard", "Wallet") => {
            handle_token_wallet_created(dbtx, offset, created).await
        }
        _ => Ok(()),
    }
}

async fn handle_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    let template_id = archived
        .template_id
        .as_ref()
        .ok_or_else(|| anyhow!("missing template_id"))?;

    match (
        archived.package_name.as_str(),
        template_id.module_name.as_str(),
        template_id.entity_name.as_str(),
    ) {
        ("pebble", "Pebble.MarketLifecycle", "MarketLifecycle") => {
            handle_market_archived(dbtx, offset, archived).await
        }
        ("pebble", "Pebble.Reference", "TokenConfig") => {
            handle_token_config_archived(dbtx, offset, archived).await
        }
        ("pebble", "Pebble.Reference", "FeeSchedule") => {
            handle_fee_schedule_archived(dbtx, offset, archived).await
        }
        ("pebble", "Pebble.Reference", "OracleConfig") => {
            handle_oracle_config_archived(dbtx, offset, archived).await
        }
        ("pebble", "Pebble.Account", "AccountRef") => {
            handle_account_ref_archived(dbtx, offset, archived).await
        }
        ("pebble", "Pebble.Account", "AccountState") => {
            handle_account_state_archived(dbtx, offset, archived).await
        }
        ("pebble", "Pebble.Clearing", "ClearingHead") => {
            handle_clearing_head_archived(dbtx, offset, archived).await
        }
        ("pebble", "Pebble.Clearing", "BatchAnchor") => {
            handle_batch_anchor_archived(dbtx, offset, archived).await
        }
        ("pebble", "Pebble.Treasury", "DepositRequest") => {
            handle_deposit_request_archived(dbtx, offset, archived).await
        }
        ("pebble", "Pebble.Treasury", "DepositPending") => {
            handle_deposit_pending_archived(dbtx, offset, archived).await
        }
        ("pebble", "Pebble.Treasury", "DepositReceipt") => {
            handle_deposit_receipt_archived(dbtx, offset, archived).await
        }
        ("pebble", "Pebble.Treasury", "WithdrawalRequest") => {
            handle_withdrawal_request_archived(dbtx, offset, archived).await
        }
        ("pebble", "Pebble.Treasury", "WithdrawalPending") => {
            handle_withdrawal_pending_archived(dbtx, offset, archived).await
        }
        ("pebble", "Pebble.Treasury", "WithdrawalReceipt") => {
            handle_withdrawal_receipt_archived(dbtx, offset, archived).await
        }
        ("pebble", "Pebble.Treasury", "QuarantinedHolding") => {
            handle_quarantined_holding_archived(dbtx, offset, archived).await
        }
        ("wizardcat-token-standard", "Wizardcat.Token.Standard", "Holding") => {
            handle_token_holding_archived(dbtx, offset, archived).await
        }
        ("wizardcat-token-standard", "Wizardcat.Token.Standard", "HoldingClaim") => {
            handle_token_holding_claim_archived(dbtx, offset, archived).await
        }
        ("wizardcat-token-standard", "Wizardcat.Token.Standard", "OperatorAcceptPolicy") => {
            handle_token_operator_accept_policy_archived(dbtx, offset, archived).await
        }
        ("wizardcat-token-standard", "Wizardcat.Token.Standard", "TransferInstruction") => {
            handle_token_transfer_instruction_archived(dbtx, offset, archived).await
        }
        ("wizardcat-token-standard", "Wizardcat.Token.Standard", "Wallet") => {
            handle_token_wallet_archived(dbtx, offset, archived).await
        }
        _ => Ok(()),
    }
}

fn created_at_utc(created: &lapi::CreatedEvent) -> Result<DateTime<Utc>> {
    let created_at = created
        .created_at
        .as_ref()
        .ok_or_else(|| anyhow!("missing created_at"))?;
    timestamp_to_utc(created_at)
}

#[derive(sqlx::FromRow)]
struct MarketBindingRow {
    instrument_admin: String,
    instrument_id: String,
    payout_per_share_minor: i64,
}

async fn resolve_market_binding(
    dbtx: &mut Transaction<'_, Postgres>,
    market_id: &str,
) -> Result<MarketBindingRow> {
    let existing: Option<MarketBindingRow> = sqlx::query_as(
        r#"
        SELECT
          instrument_admin,
          instrument_id,
          payout_per_share_minor
        FROM markets
        WHERE market_id = $1
          AND instrument_admin IS NOT NULL
          AND instrument_id IS NOT NULL
          AND payout_per_share_minor IS NOT NULL
        ORDER BY created_at DESC, contract_id DESC
        LIMIT 1
        "#,
    )
    .bind(market_id)
    .fetch_optional(&mut **dbtx)
    .await
    .context("query existing market binding")?;
    if let Some(existing) = existing {
        return Ok(existing);
    }

    let token_binding: Option<(String, String)> = sqlx::query_as(
        r#"
        SELECT instrument_admin, instrument_id
        FROM token_configs
        WHERE active = TRUE
        ORDER BY created_at DESC, contract_id DESC
        LIMIT 1
        "#,
    )
    .fetch_optional(&mut **dbtx)
    .await
    .context("query default token binding for market")?;
    if let Some((instrument_admin, instrument_id)) = token_binding {
        return Ok(MarketBindingRow {
            instrument_admin,
            instrument_id,
            payout_per_share_minor: DEFAULT_MARKET_PAYOUT_PER_SHARE_MINOR,
        });
    }

    let account_binding: Option<(String, String)> = sqlx::query_as(
        r#"
        SELECT
          ar.instrument_admin,
          ar.instrument_id
        FROM account_ref_latest latest
        JOIN account_refs ar
          ON ar.contract_id = latest.contract_id
        WHERE ar.active = TRUE
        GROUP BY ar.instrument_admin, ar.instrument_id
        ORDER BY COUNT(*) DESC, MAX(ar.created_at) DESC, ar.instrument_admin ASC, ar.instrument_id ASC
        LIMIT 1
        "#,
    )
    .fetch_optional(&mut **dbtx)
    .await
    .context("query fallback account-ref binding for market")?;
    let Some((instrument_admin, instrument_id)) = account_binding else {
        return Err(anyhow!(
            "cannot bind market {}: no active token_configs row and no active account_refs row",
            market_id
        ));
    };

    tracing::warn!(
        market_id = %market_id,
        instrument_admin = %instrument_admin,
        instrument_id = %instrument_id,
        "binding market from account_refs fallback (token_configs missing)"
    );

    Ok(MarketBindingRow {
        instrument_admin,
        instrument_id,
        payout_per_share_minor: DEFAULT_MARKET_PAYOUT_PER_SHARE_MINOR,
    })
}

async fn handle_market_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let args = created
        .create_arguments
        .as_ref()
        .ok_or_else(|| anyhow!("missing create_arguments"))?;
    let fields = record_fields_by_label(args)?;

    let committee = get_party(&fields, "committee")?;
    let creator = get_party(&fields, "creator")?;
    let market_id = get_text(&fields, "marketId")?;
    let question = get_text(&fields, "question")?;
    let outcomes = get_list_text(&fields, "outcomes")?;
    let status = get_enum(&fields, "status")?;
    let resolved_outcome = get_optional_text(&fields, "resolvedOutcome")?;

    let created_at = created_at_utc(&created)?;
    let market_binding = resolve_market_binding(dbtx, &market_id).await?;

    let outcomes_json = serde_json::to_value(&outcomes).context("serialize outcomes")?;

    sqlx::query(
        r#"
        INSERT INTO markets (
          contract_id,
          market_id,
          committee_party,
          creator_party,
          question,
          outcomes,
          status,
          resolved_outcome,
          instrument_admin,
          instrument_id,
          payout_per_share_minor,
          created_at,
          active,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,TRUE,$13)
        ON CONFLICT (contract_id) DO UPDATE SET
          market_id = EXCLUDED.market_id,
          committee_party = EXCLUDED.committee_party,
          creator_party = EXCLUDED.creator_party,
          question = EXCLUDED.question,
          outcomes = EXCLUDED.outcomes,
          status = EXCLUDED.status,
          resolved_outcome = EXCLUDED.resolved_outcome,
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          payout_per_share_minor = EXCLUDED.payout_per_share_minor,
          created_at = EXCLUDED.created_at,
          active = TRUE,
          last_offset = EXCLUDED.last_offset
        "#,
    )
    .bind(created.contract_id)
    .bind(market_id)
    .bind(committee)
    .bind(creator)
    .bind(question)
    .bind(outcomes_json)
    .bind(status)
    .bind(resolved_outcome)
    .bind(market_binding.instrument_admin)
    .bind(market_binding.instrument_id)
    .bind(market_binding.payout_per_share_minor)
    .bind(created_at)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert market")?;

    Ok(())
}

async fn handle_market_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    let res = sqlx::query(
        r#"
        UPDATE markets
        SET active = FALSE, last_offset = $2
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("archive market")?;

    if res.rows_affected() == 0 {
        tracing::warn!("archive for unknown market contract (contract_id not found)");
    }

    Ok(())
}

async fn handle_token_config_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let args = created
        .create_arguments
        .as_ref()
        .ok_or_else(|| anyhow!("missing create_arguments"))?;
    let fields = record_fields_by_label(args)?;

    let committee_party = get_party(&fields, "committee")?;
    let instrument_admin = get_party(&fields, "instrumentAdmin")?;
    let instrument_id = get_text(&fields, "instrumentId")?;
    let registry_url = get_text(&fields, "registryUrl")?;
    let decimals = get_int64(&fields, "decimals")?;
    let decimals_i32 = i32::try_from(decimals).context("decimals out of range")?;
    let symbol = get_text(&fields, "symbol")?;
    let deposit_mode = get_enum(&fields, "depositMode")?;
    let inbound_requires_acceptance = get_bool(&fields, "inboundRequiresAcceptance")?;
    let hard_max_inputs_per_transfer = get_int64(&fields, "hardMaxInputsPerTransfer")?;
    let operational_max_inputs_per_transfer =
        get_int64(&fields, "operationalMaxInputsPerTransfer")?;
    let target_utxo_count_min = get_int64(&fields, "targetUtxoCountMin")?;
    let target_utxo_count_max = get_int64(&fields, "targetUtxoCountMax")?;
    let dust_threshold_minor = get_int64(&fields, "dustThresholdMinor")?;
    let withdrawal_fee_headroom_minor = get_int64(&fields, "withdrawalFeeHeadroomMinor")?;
    let unexpected_fee_buffer_minor = get_int64(&fields, "unexpectedFeeBufferMinor")?;
    let requires_deposit_id_metadata = get_bool(&fields, "requiresDepositIdMetadata")?;
    let allowed_deposit_pending_status_classes =
        get_list_text(&fields, "allowedDepositPendingStatusClasses")?;
    let allowed_withdrawal_pending_status_classes =
        get_list_text(&fields, "allowedWithdrawalPendingStatusClasses")?;
    let allowed_cancel_pending_status_classes =
        get_list_text(&fields, "allowedCancelPendingStatusClasses")?;
    let requires_original_instruction_cid = get_bool(&fields, "requiresOriginalInstructionCid")?;
    let auditor_party = get_optional_party(&fields, "auditor")?;

    let created_at = created_at_utc(&created)?;

    let allowed_deposit_pending_status_classes_json =
        serde_json::to_value(&allowed_deposit_pending_status_classes)
            .context("serialize allowedDepositPendingStatusClasses")?;
    let allowed_withdrawal_pending_status_classes_json =
        serde_json::to_value(&allowed_withdrawal_pending_status_classes)
            .context("serialize allowedWithdrawalPendingStatusClasses")?;
    let allowed_cancel_pending_status_classes_json =
        serde_json::to_value(&allowed_cancel_pending_status_classes)
            .context("serialize allowedCancelPendingStatusClasses")?;

    sqlx::query(
        r#"
        INSERT INTO token_configs (
          contract_id,
          committee_party,
          instrument_admin,
          instrument_id,
          registry_url,
          decimals,
          symbol,
          deposit_mode,
          inbound_requires_acceptance,
          hard_max_inputs_per_transfer,
          operational_max_inputs_per_transfer,
          target_utxo_count_min,
          target_utxo_count_max,
          dust_threshold_minor,
          withdrawal_fee_headroom_minor,
          unexpected_fee_buffer_minor,
          requires_deposit_id_metadata,
          allowed_deposit_pending_status_classes,
          allowed_withdrawal_pending_status_classes,
          allowed_cancel_pending_status_classes,
          requires_original_instruction_cid,
          auditor_party,
          created_at,
          active,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,TRUE,$24)
        ON CONFLICT (contract_id) DO UPDATE SET
          committee_party = EXCLUDED.committee_party,
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          registry_url = EXCLUDED.registry_url,
          decimals = EXCLUDED.decimals,
          symbol = EXCLUDED.symbol,
          deposit_mode = EXCLUDED.deposit_mode,
          inbound_requires_acceptance = EXCLUDED.inbound_requires_acceptance,
          hard_max_inputs_per_transfer = EXCLUDED.hard_max_inputs_per_transfer,
          operational_max_inputs_per_transfer = EXCLUDED.operational_max_inputs_per_transfer,
          target_utxo_count_min = EXCLUDED.target_utxo_count_min,
          target_utxo_count_max = EXCLUDED.target_utxo_count_max,
          dust_threshold_minor = EXCLUDED.dust_threshold_minor,
          withdrawal_fee_headroom_minor = EXCLUDED.withdrawal_fee_headroom_minor,
          unexpected_fee_buffer_minor = EXCLUDED.unexpected_fee_buffer_minor,
          requires_deposit_id_metadata = EXCLUDED.requires_deposit_id_metadata,
          allowed_deposit_pending_status_classes = EXCLUDED.allowed_deposit_pending_status_classes,
          allowed_withdrawal_pending_status_classes = EXCLUDED.allowed_withdrawal_pending_status_classes,
          allowed_cancel_pending_status_classes = EXCLUDED.allowed_cancel_pending_status_classes,
          requires_original_instruction_cid = EXCLUDED.requires_original_instruction_cid,
          auditor_party = EXCLUDED.auditor_party,
          created_at = EXCLUDED.created_at,
          active = TRUE,
          last_offset = EXCLUDED.last_offset
        "#,
    )
    .bind(created.contract_id)
    .bind(committee_party)
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(registry_url)
    .bind(decimals_i32)
    .bind(symbol)
    .bind(deposit_mode)
    .bind(inbound_requires_acceptance)
    .bind(hard_max_inputs_per_transfer)
    .bind(operational_max_inputs_per_transfer)
    .bind(target_utxo_count_min)
    .bind(target_utxo_count_max)
    .bind(dust_threshold_minor)
    .bind(withdrawal_fee_headroom_minor)
    .bind(unexpected_fee_buffer_minor)
    .bind(requires_deposit_id_metadata)
    .bind(allowed_deposit_pending_status_classes_json)
    .bind(allowed_withdrawal_pending_status_classes_json)
    .bind(allowed_cancel_pending_status_classes_json)
    .bind(requires_original_instruction_cid)
    .bind(auditor_party)
    .bind(created_at)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert token_config")?;

    Ok(())
}

async fn handle_token_config_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE token_configs
        SET active = FALSE, last_offset = $2
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("archive token_config")?;

    Ok(())
}

async fn handle_fee_schedule_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let args = created
        .create_arguments
        .as_ref()
        .ok_or_else(|| anyhow!("missing create_arguments"))?;
    let fields = record_fields_by_label(args)?;

    let instrument_admin = get_party(&fields, "instrumentAdmin")?;
    let instrument_id = get_text(&fields, "instrumentId")?;
    let deposit_policy = get_enum(&fields, "depositPolicy")?;
    let withdrawal_policy = get_enum(&fields, "withdrawalPolicy")?;

    let created_at = created_at_utc(&created)?;

    sqlx::query(
        r#"
        INSERT INTO fee_schedules (
          contract_id,
          instrument_admin,
          instrument_id,
          deposit_policy,
          withdrawal_policy,
          created_at,
          active,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,$6,TRUE,$7)
        ON CONFLICT (contract_id) DO UPDATE SET
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          deposit_policy = EXCLUDED.deposit_policy,
          withdrawal_policy = EXCLUDED.withdrawal_policy,
          created_at = EXCLUDED.created_at,
          active = TRUE,
          last_offset = EXCLUDED.last_offset
        "#,
    )
    .bind(created.contract_id)
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(deposit_policy)
    .bind(withdrawal_policy)
    .bind(created_at)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert fee_schedule")?;

    Ok(())
}

async fn handle_fee_schedule_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE fee_schedules
        SET active = FALSE, last_offset = $2
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("archive fee_schedule")?;

    Ok(())
}

async fn handle_oracle_config_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let args = created
        .create_arguments
        .as_ref()
        .ok_or_else(|| anyhow!("missing create_arguments"))?;
    let fields = record_fields_by_label(args)?;

    let instrument_admin = get_party(&fields, "instrumentAdmin")?;
    let instrument_id = get_text(&fields, "instrumentId")?;
    let mode = get_enum(&fields, "mode")?;

    let created_at = created_at_utc(&created)?;

    sqlx::query(
        r#"
        INSERT INTO oracle_configs (
          contract_id,
          instrument_admin,
          instrument_id,
          mode,
          created_at,
          active,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,TRUE,$6)
        ON CONFLICT (contract_id) DO UPDATE SET
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          mode = EXCLUDED.mode,
          created_at = EXCLUDED.created_at,
          active = TRUE,
          last_offset = EXCLUDED.last_offset
        "#,
    )
    .bind(created.contract_id)
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(mode)
    .bind(created_at)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert oracle_config")?;

    Ok(())
}

async fn handle_oracle_config_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE oracle_configs
        SET active = FALSE, last_offset = $2
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("archive oracle_config")?;

    Ok(())
}

async fn handle_account_ref_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let args = created
        .create_arguments
        .as_ref()
        .ok_or_else(|| anyhow!("missing create_arguments"))?;
    let fields = record_fields_by_label(args)?;

    let committee_party = get_party(&fields, "committee")?;
    let owner_party = get_party(&fields, "owner")?;
    let account_id = get_text(&fields, "accountId")?;
    let instrument_admin = get_party(&fields, "instrumentAdmin")?;
    let instrument_id = get_text(&fields, "instrumentId")?;
    let status = get_enum(&fields, "status")?;
    let finalized_epoch = get_optional_int64(&fields, "finalizedEpoch")?;

    let created_at = created_at_utc(&created)?;

    sqlx::query(
        r#"
        INSERT INTO account_refs (
          contract_id,
          account_id,
          owner_party,
          committee_party,
          instrument_admin,
          instrument_id,
          status,
          finalized_epoch,
          created_at,
          active,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,TRUE,$10)
        ON CONFLICT (contract_id) DO UPDATE SET
          account_id = EXCLUDED.account_id,
          owner_party = EXCLUDED.owner_party,
          committee_party = EXCLUDED.committee_party,
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          status = EXCLUDED.status,
          finalized_epoch = EXCLUDED.finalized_epoch,
          created_at = EXCLUDED.created_at,
          active = TRUE,
          last_offset = EXCLUDED.last_offset
        "#,
    )
    .bind(created.contract_id.clone())
    .bind(account_id.clone())
    .bind(owner_party)
    .bind(committee_party)
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(status)
    .bind(finalized_epoch)
    .bind(created_at)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert account_ref")?;

    sqlx::query(
        r#"
        INSERT INTO account_ref_latest (account_id, contract_id, last_offset)
        VALUES ($1,$2,$3)
        ON CONFLICT (account_id) DO UPDATE SET
          contract_id = EXCLUDED.contract_id,
          last_offset = EXCLUDED.last_offset,
          updated_at = now()
        "#,
    )
    .bind(account_id)
    .bind(created.contract_id)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert account_ref_latest")?;

    Ok(())
}

async fn handle_account_ref_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE account_refs
        SET active = FALSE, last_offset = $2
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id.clone())
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("archive account_ref")?;

    sqlx::query(
        r#"
        DELETE FROM account_ref_latest
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id)
    .execute(&mut **dbtx)
    .await
    .context("delete account_ref_latest")?;

    Ok(())
}

async fn handle_account_state_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let args = created
        .create_arguments
        .as_ref()
        .ok_or_else(|| anyhow!("missing create_arguments"))?;
    let fields = record_fields_by_label(args)?;

    let committee_party = get_party(&fields, "committee")?;
    let owner_party = get_party(&fields, "owner")?;
    let account_id = get_text(&fields, "accountId")?;
    let instrument_admin = get_party(&fields, "instrumentAdmin")?;
    let instrument_id = get_text(&fields, "instrumentId")?;
    let cleared_cash_minor = get_int64(&fields, "clearedCashMinor")?;
    let last_applied_epoch = get_int64(&fields, "lastAppliedEpoch")?;
    let last_applied_batch_anchor = get_optional_contract_id(&fields, "lastAppliedBatchAnchor")?;

    let created_at = created_at_utc(&created)?;

    sqlx::query(
        r#"
        INSERT INTO account_states (
          contract_id,
          account_id,
          owner_party,
          committee_party,
          instrument_admin,
          instrument_id,
          cleared_cash_minor,
          last_applied_epoch,
          last_applied_batch_anchor,
          created_at,
          active,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,TRUE,$11)
        ON CONFLICT (contract_id) DO UPDATE SET
          account_id = EXCLUDED.account_id,
          owner_party = EXCLUDED.owner_party,
          committee_party = EXCLUDED.committee_party,
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          cleared_cash_minor = EXCLUDED.cleared_cash_minor,
          last_applied_epoch = EXCLUDED.last_applied_epoch,
          last_applied_batch_anchor = EXCLUDED.last_applied_batch_anchor,
          created_at = EXCLUDED.created_at,
          active = TRUE,
          last_offset = EXCLUDED.last_offset
        "#,
    )
    .bind(created.contract_id.clone())
    .bind(account_id.clone())
    .bind(owner_party)
    .bind(committee_party)
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(cleared_cash_minor)
    .bind(last_applied_epoch)
    .bind(last_applied_batch_anchor)
    .bind(created_at)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert account_state")?;

    sqlx::query(
        r#"
        INSERT INTO account_state_latest (account_id, contract_id, last_offset)
        VALUES ($1,$2,$3)
        ON CONFLICT (account_id) DO UPDATE SET
          contract_id = EXCLUDED.contract_id,
          last_offset = EXCLUDED.last_offset,
          updated_at = now()
        "#,
    )
    .bind(account_id)
    .bind(created.contract_id)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert account_state_latest")?;

    Ok(())
}

async fn handle_account_state_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE account_states
        SET active = FALSE, last_offset = $2
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id.clone())
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("archive account_state")?;

    sqlx::query(
        r#"
        DELETE FROM account_state_latest
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id)
    .execute(&mut **dbtx)
    .await
    .context("delete account_state_latest")?;

    Ok(())
}

async fn handle_clearing_head_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let args = created
        .create_arguments
        .as_ref()
        .ok_or_else(|| anyhow!("missing create_arguments"))?;
    let fields = record_fields_by_label(args)?;

    let committee_party = get_party(&fields, "committee")?;
    let instrument_admin = get_party(&fields, "instrumentAdmin")?;
    let instrument_id = get_text(&fields, "instrumentId")?;
    let next_epoch = get_int64(&fields, "nextEpoch")?;
    let prev_batch_hash = get_text(&fields, "prevBatchHash")?;
    let engine_version = get_text_from_record(&fields, "engineVersion", "value")?;
    let last_batch_anchor = get_optional_contract_id(&fields, "lastBatchAnchor")?;

    let created_at = created_at_utc(&created)?;

    sqlx::query(
        r#"
        INSERT INTO clearing_heads (
          contract_id,
          committee_party,
          instrument_admin,
          instrument_id,
          next_epoch,
          prev_batch_hash,
          engine_version,
          last_batch_anchor,
          created_at,
          active,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,TRUE,$10)
        ON CONFLICT (contract_id) DO UPDATE SET
          committee_party = EXCLUDED.committee_party,
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          next_epoch = EXCLUDED.next_epoch,
          prev_batch_hash = EXCLUDED.prev_batch_hash,
          engine_version = EXCLUDED.engine_version,
          last_batch_anchor = EXCLUDED.last_batch_anchor,
          created_at = EXCLUDED.created_at,
          active = TRUE,
          last_offset = EXCLUDED.last_offset
        "#,
    )
    .bind(created.contract_id.clone())
    .bind(committee_party)
    .bind(instrument_admin.clone())
    .bind(instrument_id.clone())
    .bind(next_epoch)
    .bind(prev_batch_hash)
    .bind(engine_version)
    .bind(last_batch_anchor)
    .bind(created_at)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert clearing_head")?;

    sqlx::query(
        r#"
        INSERT INTO clearing_head_latest (instrument_admin, instrument_id, contract_id, last_offset)
        VALUES ($1,$2,$3,$4)
        ON CONFLICT (instrument_admin, instrument_id) DO UPDATE SET
          contract_id = EXCLUDED.contract_id,
          last_offset = EXCLUDED.last_offset,
          updated_at = now()
        "#,
    )
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(created.contract_id)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert clearing_head_latest")?;

    Ok(())
}

async fn handle_clearing_head_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE clearing_heads
        SET active = FALSE, last_offset = $2
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id.clone())
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("archive clearing_head")?;

    sqlx::query(
        r#"
        DELETE FROM clearing_head_latest
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id)
    .execute(&mut **dbtx)
    .await
    .context("delete clearing_head_latest")?;

    Ok(())
}

async fn handle_batch_anchor_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let args = created
        .create_arguments
        .as_ref()
        .ok_or_else(|| anyhow!("missing create_arguments"))?;
    let fields = record_fields_by_label(args)?;

    let committee_party = get_party(&fields, "committee")?;
    let instrument_admin = get_party(&fields, "instrumentAdmin")?;
    let instrument_id = get_text(&fields, "instrumentId")?;
    let epoch = get_int64(&fields, "epoch")?;
    let prev_batch_hash = get_text(&fields, "prevBatchHash")?;
    let batch_hash = get_text(&fields, "batchHash")?;
    let engine_version = get_text_from_record(&fields, "engineVersion", "value")?;

    let created_at = created_at_utc(&created)?;

    sqlx::query(
        r#"
        INSERT INTO batch_anchors (
          contract_id,
          committee_party,
          instrument_admin,
          instrument_id,
          epoch,
          prev_batch_hash,
          batch_hash,
          engine_version,
          created_at,
          active,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,TRUE,$10)
        ON CONFLICT (contract_id) DO UPDATE SET
          committee_party = EXCLUDED.committee_party,
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          epoch = EXCLUDED.epoch,
          prev_batch_hash = EXCLUDED.prev_batch_hash,
          batch_hash = EXCLUDED.batch_hash,
          engine_version = EXCLUDED.engine_version,
          created_at = EXCLUDED.created_at,
          active = TRUE,
          last_offset = EXCLUDED.last_offset
        "#,
    )
    .bind(created.contract_id)
    .bind(committee_party)
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(epoch)
    .bind(prev_batch_hash)
    .bind(batch_hash)
    .bind(engine_version)
    .bind(created_at)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert batch_anchor")?;

    Ok(())
}

async fn handle_batch_anchor_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE batch_anchors
        SET active = FALSE, last_offset = $2
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("archive batch_anchor")?;

    Ok(())
}

// --------------------
// M2: Mock token standard projections (wizardcat-token-standard)
// --------------------

async fn handle_token_holding_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let args = created
        .create_arguments
        .as_ref()
        .ok_or_else(|| anyhow!("missing create_arguments"))?;
    let fields = record_fields_by_label(args)?;

    let owner_party = get_party(&fields, "owner")?;
    let instrument_admin = get_party(&fields, "instrumentAdmin")?;
    let instrument_id = get_text(&fields, "instrumentId")?;
    let amount_minor = get_int64(&fields, "amountMinor")?;
    let lock_owner_party = get_optional_party(&fields, "lockOwner")?;
    let origin_instruction_cid = get_optional_contract_id(&fields, "originInstructionCid")?;

    let (lock_status_class, lock_expires_at) = match get_optional_record(&fields, "lock")? {
        None => (None, None),
        Some(lock) => {
            let lock_fields = record_fields_by_label(&lock)?;
            let status_class = get_text(&lock_fields, "statusClass")?;
            let expires_at = get_time(&lock_fields, "expiresAt")?;
            (Some(status_class), Some(expires_at))
        }
    };

    let created_at = created_at_utc(&created)?;

    sqlx::query(
        r#"
        INSERT INTO token_holdings (
          contract_id,
          owner_party,
          instrument_admin,
          instrument_id,
          amount_minor,
          lock_status_class,
          lock_expires_at,
          lock_owner_party,
          origin_instruction_cid,
          created_at,
          active,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,TRUE,$11)
        ON CONFLICT (contract_id) DO UPDATE SET
          owner_party = EXCLUDED.owner_party,
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          amount_minor = EXCLUDED.amount_minor,
          lock_status_class = EXCLUDED.lock_status_class,
          lock_expires_at = EXCLUDED.lock_expires_at,
          lock_owner_party = EXCLUDED.lock_owner_party,
          origin_instruction_cid = EXCLUDED.origin_instruction_cid,
          created_at = EXCLUDED.created_at,
          active = TRUE,
          last_offset = EXCLUDED.last_offset
        "#,
    )
    .bind(created.contract_id)
    .bind(owner_party)
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(amount_minor)
    .bind(lock_status_class)
    .bind(lock_expires_at)
    .bind(lock_owner_party)
    .bind(origin_instruction_cid)
    .bind(created_at)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert token_holding")?;

    Ok(())
}

async fn handle_token_holding_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE token_holdings
        SET active = FALSE, last_offset = $2
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("archive token_holding")?;

    Ok(())
}

async fn handle_token_holding_claim_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let args = created
        .create_arguments
        .as_ref()
        .ok_or_else(|| anyhow!("missing create_arguments"))?;
    let fields = record_fields_by_label(args)?;

    let issuer_party = get_party(&fields, "issuer")?;
    let owner_party = get_party(&fields, "owner")?;
    let instrument_admin = get_party(&fields, "instrumentAdmin")?;
    let instrument_id = get_text(&fields, "instrumentId")?;
    let amount_minor = get_int64(&fields, "amountMinor")?;
    let origin_instruction_cid = get_optional_contract_id(&fields, "originInstructionCid")?;

    let created_at = created_at_utc(&created)?;

    sqlx::query(
        r#"
        INSERT INTO token_holding_claims (
          contract_id,
          issuer_party,
          owner_party,
          instrument_admin,
          instrument_id,
          amount_minor,
          origin_instruction_cid,
          created_at,
          active,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,TRUE,$9)
        ON CONFLICT (contract_id) DO UPDATE SET
          issuer_party = EXCLUDED.issuer_party,
          owner_party = EXCLUDED.owner_party,
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          amount_minor = EXCLUDED.amount_minor,
          origin_instruction_cid = EXCLUDED.origin_instruction_cid,
          created_at = EXCLUDED.created_at,
          active = TRUE,
          last_offset = EXCLUDED.last_offset
        "#,
    )
    .bind(created.contract_id)
    .bind(issuer_party)
    .bind(owner_party)
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(amount_minor)
    .bind(origin_instruction_cid)
    .bind(created_at)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert token_holding_claim")?;

    Ok(())
}

async fn handle_token_holding_claim_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE token_holding_claims
        SET active = FALSE, last_offset = $2
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("archive token_holding_claim")?;

    Ok(())
}

async fn handle_token_operator_accept_policy_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let args = created
        .create_arguments
        .as_ref()
        .ok_or_else(|| anyhow!("missing create_arguments"))?;
    let fields = record_fields_by_label(args)?;

    let instrument_admin = get_party(&fields, "instrumentAdmin")?;
    let instrument_id = get_text(&fields, "instrumentId")?;
    let authorizer_party = get_party(&fields, "authorizer")?;
    let operator_party = get_party(&fields, "operator")?;
    let mode = get_text(&fields, "mode")?;

    let created_at = created_at_utc(&created)?;

    sqlx::query(
        r#"
        INSERT INTO operator_accept_policies (
          contract_id,
          instrument_admin,
          instrument_id,
          authorizer_party,
          operator_party,
          mode,
          created_at,
          active,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,TRUE,$8)
        ON CONFLICT (contract_id) DO UPDATE SET
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          authorizer_party = EXCLUDED.authorizer_party,
          operator_party = EXCLUDED.operator_party,
          mode = EXCLUDED.mode,
          created_at = EXCLUDED.created_at,
          active = TRUE,
          last_offset = EXCLUDED.last_offset
        "#,
    )
    .bind(created.contract_id)
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(authorizer_party)
    .bind(operator_party)
    .bind(mode)
    .bind(created_at)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert operator_accept_policy")?;

    Ok(())
}

async fn handle_token_operator_accept_policy_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE operator_accept_policies
        SET active = FALSE, last_offset = $2
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("archive operator_accept_policy")?;

    Ok(())
}

async fn handle_token_transfer_instruction_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let args = created
        .create_arguments
        .as_ref()
        .ok_or_else(|| anyhow!("missing create_arguments"))?;
    let fields = record_fields_by_label(args)?;

    let instrument_admin = get_party(&fields, "instrumentAdmin")?;
    let instrument_id = get_text(&fields, "instrumentId")?;
    let sender_party = get_party(&fields, "sender")?;
    let receiver_party = get_party(&fields, "receiver")?;
    let amount_minor = get_int64(&fields, "amountMinor")?;
    let metadata = get_text_map_text_json(&fields, "metadata")?;
    let reason = get_text(&fields, "reason")?;
    let output = get_enum(&fields, "output")?;
    let status_class = get_optional_text(&fields, "statusClass")?;
    let original_instruction_cid = get_optional_contract_id(&fields, "originalInstructionCid")?;
    let step_seq = get_int64(&fields, "stepSeq")?;
    let pending_actions = get_list_text(&fields, "pendingActions")?;
    let locked_holding_cid = get_optional_contract_id(&fields, "lockedHoldingCid")?;

    let lineage_root_instruction_cid = original_instruction_cid
        .clone()
        .unwrap_or_else(|| created.contract_id.clone());

    let created_at = created_at_utc(&created)?;
    let pending_actions_json =
        serde_json::to_value(&pending_actions).context("serialize pendingActions")?;

    sqlx::query(
        r#"
        INSERT INTO token_transfer_instructions (
          contract_id,
          lineage_root_instruction_cid,
          instrument_admin,
          instrument_id,
          sender_party,
          receiver_party,
          amount_minor,
          metadata,
          reason,
          output,
          status_class,
          original_instruction_cid,
          step_seq,
          pending_actions,
          locked_holding_cid,
          created_at,
          active,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,TRUE,$17)
        ON CONFLICT (contract_id) DO UPDATE SET
          lineage_root_instruction_cid = EXCLUDED.lineage_root_instruction_cid,
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          sender_party = EXCLUDED.sender_party,
          receiver_party = EXCLUDED.receiver_party,
          amount_minor = EXCLUDED.amount_minor,
          metadata = EXCLUDED.metadata,
          reason = EXCLUDED.reason,
          output = EXCLUDED.output,
          status_class = EXCLUDED.status_class,
          original_instruction_cid = EXCLUDED.original_instruction_cid,
          step_seq = EXCLUDED.step_seq,
          pending_actions = EXCLUDED.pending_actions,
          locked_holding_cid = EXCLUDED.locked_holding_cid,
          created_at = EXCLUDED.created_at,
          active = TRUE,
          last_offset = EXCLUDED.last_offset
        "#,
    )
    .bind(created.contract_id.clone())
    .bind(lineage_root_instruction_cid.clone())
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(sender_party)
    .bind(receiver_party)
    .bind(amount_minor)
    .bind(metadata)
    .bind(reason)
    .bind(output)
    .bind(status_class)
    .bind(original_instruction_cid)
    .bind(step_seq)
    .bind(pending_actions_json)
    .bind(locked_holding_cid)
    .bind(created_at)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert token_transfer_instruction")?;

    sqlx::query(
        r#"
        INSERT INTO token_transfer_instruction_latest (lineage_root_instruction_cid, contract_id, last_offset)
        VALUES ($1,$2,$3)
        ON CONFLICT (lineage_root_instruction_cid) DO UPDATE SET
          contract_id = EXCLUDED.contract_id,
          last_offset = EXCLUDED.last_offset,
          updated_at = now()
        "#,
    )
    .bind(lineage_root_instruction_cid)
    .bind(created.contract_id)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert token_transfer_instruction_latest")?;

    Ok(())
}

async fn handle_token_transfer_instruction_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE token_transfer_instructions
        SET active = FALSE, last_offset = $2
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id.clone())
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("archive token_transfer_instruction")?;

    sqlx::query(
        r#"
        DELETE FROM token_transfer_instruction_latest
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id)
    .execute(&mut **dbtx)
    .await
    .context("delete token_transfer_instruction_latest")?;

    Ok(())
}

async fn handle_token_wallet_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let args = created
        .create_arguments
        .as_ref()
        .ok_or_else(|| anyhow!("missing create_arguments"))?;
    let fields = record_fields_by_label(args)?;

    let owner_party = get_party(&fields, "owner")?;
    let instrument_admin = get_party(&fields, "instrumentAdmin")?;
    let instrument_id = get_text(&fields, "instrumentId")?;

    let created_at = created_at_utc(&created)?;

    sqlx::query(
        r#"
        INSERT INTO token_wallets (
          contract_id,
          owner_party,
          instrument_admin,
          instrument_id,
          created_at,
          active,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,TRUE,$6)
        ON CONFLICT (contract_id) DO UPDATE SET
          owner_party = EXCLUDED.owner_party,
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          created_at = EXCLUDED.created_at,
          active = TRUE,
          last_offset = EXCLUDED.last_offset
        "#,
    )
    .bind(created.contract_id)
    .bind(owner_party)
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(created_at)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert token_wallet")?;

    Ok(())
}

async fn handle_token_wallet_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE token_wallets
        SET active = FALSE, last_offset = $2
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("archive token_wallet")?;

    Ok(())
}

// --------------------
// M2: Treasury projections (pebble: Pebble.Treasury)
// --------------------

async fn handle_deposit_request_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let args = created
        .create_arguments
        .as_ref()
        .ok_or_else(|| anyhow!("missing create_arguments"))?;
    let fields = record_fields_by_label(args)?;

    let committee_party = get_party(&fields, "committee")?;
    let owner_party = get_party(&fields, "owner")?;
    let account_id = get_text(&fields, "accountId")?;
    let instrument_admin = get_party(&fields, "instrumentAdmin")?;
    let instrument_id = get_text(&fields, "instrumentId")?;
    let deposit_id = get_text(&fields, "depositId")?;
    let token_config_cid = get_contract_id(&fields, "tokenConfigCid")?;
    let account_state_cid = get_contract_id(&fields, "accountStateCid")?;
    let offer_instruction_cid = get_contract_id(&fields, "offerInstructionCid")?;

    let created_at = created_at_utc(&created)?;

    sqlx::query(
        r#"
        INSERT INTO deposit_requests (
          contract_id,
          committee_party,
          owner_party,
          account_id,
          instrument_admin,
          instrument_id,
          deposit_id,
          token_config_cid,
          account_state_cid,
          offer_instruction_cid,
          created_at,
          active,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,TRUE,$12)
        ON CONFLICT (contract_id) DO UPDATE SET
          committee_party = EXCLUDED.committee_party,
          owner_party = EXCLUDED.owner_party,
          account_id = EXCLUDED.account_id,
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          deposit_id = EXCLUDED.deposit_id,
          token_config_cid = EXCLUDED.token_config_cid,
          account_state_cid = EXCLUDED.account_state_cid,
          offer_instruction_cid = EXCLUDED.offer_instruction_cid,
          created_at = EXCLUDED.created_at,
          active = TRUE,
          last_offset = EXCLUDED.last_offset
        "#,
    )
    .bind(created.contract_id)
    .bind(committee_party)
    .bind(owner_party)
    .bind(account_id)
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(deposit_id)
    .bind(token_config_cid)
    .bind(account_state_cid)
    .bind(offer_instruction_cid)
    .bind(created_at)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert deposit_request")?;

    Ok(())
}

async fn handle_deposit_request_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE deposit_requests
        SET active = FALSE, last_offset = $2
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("archive deposit_request")?;

    Ok(())
}

async fn handle_deposit_pending_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let args = created
        .create_arguments
        .as_ref()
        .ok_or_else(|| anyhow!("missing create_arguments"))?;
    let fields = record_fields_by_label(args)?;

    let committee_party = get_party(&fields, "committee")?;
    let owner_party = get_party(&fields, "owner")?;
    let account_id = get_text(&fields, "accountId")?;
    let instrument_admin = get_party(&fields, "instrumentAdmin")?;
    let instrument_id = get_text(&fields, "instrumentId")?;
    let deposit_id = get_text(&fields, "depositId")?;
    let token_config_cid = get_contract_id(&fields, "tokenConfigCid")?;
    let lineage_root_instruction_cid = get_contract_id(&fields, "lineageRootInstructionCid")?;
    let current_instruction_cid = get_contract_id(&fields, "currentInstructionCid")?;
    let step_seq = get_int64(&fields, "stepSeq")?;
    let expected_amount_minor = get_int64(&fields, "expectedAmountMinor")?;
    let metadata = get_text_map_text_json(&fields, "metadata")?;
    let reason = get_text(&fields, "reason")?;

    let created_at = created_at_utc(&created)?;

    sqlx::query(
        r#"
        INSERT INTO deposit_pendings (
          contract_id,
          committee_party,
          owner_party,
          account_id,
          instrument_admin,
          instrument_id,
          deposit_id,
          token_config_cid,
          lineage_root_instruction_cid,
          current_instruction_cid,
          step_seq,
          expected_amount_minor,
          metadata,
          reason,
          created_at,
          active,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,TRUE,$16)
        ON CONFLICT (contract_id) DO UPDATE SET
          committee_party = EXCLUDED.committee_party,
          owner_party = EXCLUDED.owner_party,
          account_id = EXCLUDED.account_id,
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          deposit_id = EXCLUDED.deposit_id,
          token_config_cid = EXCLUDED.token_config_cid,
          lineage_root_instruction_cid = EXCLUDED.lineage_root_instruction_cid,
          current_instruction_cid = EXCLUDED.current_instruction_cid,
          step_seq = EXCLUDED.step_seq,
          expected_amount_minor = EXCLUDED.expected_amount_minor,
          metadata = EXCLUDED.metadata,
          reason = EXCLUDED.reason,
          created_at = EXCLUDED.created_at,
          active = TRUE,
          last_offset = EXCLUDED.last_offset
        "#,
    )
    .bind(created.contract_id.clone())
    .bind(committee_party)
    .bind(owner_party.clone())
    .bind(account_id)
    .bind(instrument_admin.clone())
    .bind(instrument_id.clone())
    .bind(deposit_id.clone())
    .bind(token_config_cid)
    .bind(lineage_root_instruction_cid)
    .bind(current_instruction_cid)
    .bind(step_seq)
    .bind(expected_amount_minor)
    .bind(metadata)
    .bind(reason)
    .bind(created_at)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert deposit_pending")?;

    sqlx::query(
        r#"
        INSERT INTO deposit_pending_latest (
          owner_party,
          instrument_admin,
          instrument_id,
          deposit_id,
          contract_id,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (owner_party, instrument_admin, instrument_id, deposit_id) DO UPDATE SET
          contract_id = EXCLUDED.contract_id,
          last_offset = EXCLUDED.last_offset,
          updated_at = now()
        "#,
    )
    .bind(owner_party)
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(deposit_id)
    .bind(created.contract_id)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert deposit_pending_latest")?;

    Ok(())
}

async fn handle_deposit_pending_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE deposit_pendings
        SET active = FALSE, last_offset = $2
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id.clone())
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("archive deposit_pending")?;

    sqlx::query(
        r#"
        DELETE FROM deposit_pending_latest
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id)
    .execute(&mut **dbtx)
    .await
    .context("delete deposit_pending_latest")?;

    Ok(())
}

async fn handle_deposit_receipt_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let args = created
        .create_arguments
        .as_ref()
        .ok_or_else(|| anyhow!("missing create_arguments"))?;
    let fields = record_fields_by_label(args)?;

    let committee_party = get_party(&fields, "committee")?;
    let owner_party = get_party(&fields, "owner")?;
    let account_id = get_text(&fields, "accountId")?;
    let instrument_admin = get_party(&fields, "instrumentAdmin")?;
    let instrument_id = get_text(&fields, "instrumentId")?;
    let deposit_id = get_text(&fields, "depositId")?;
    let lineage_root_instruction_cid = get_contract_id(&fields, "lineageRootInstructionCid")?;
    let terminal_instruction_cid = get_contract_id(&fields, "terminalInstructionCid")?;
    let credited_minor = get_int64(&fields, "creditedMinor")?;
    let receiver_holding_cids = get_list_contract_id(&fields, "receiverHoldingCids")?;
    let receiver_holding_cids_json =
        serde_json::to_value(&receiver_holding_cids).context("serialize receiverHoldingCids")?;
    let metadata = get_text_map_text_json(&fields, "metadata")?;
    let reason = get_text(&fields, "reason")?;

    let created_at = created_at_utc(&created)?;

    sqlx::query(
        r#"
        INSERT INTO deposit_receipts (
          contract_id,
          committee_party,
          owner_party,
          account_id,
          instrument_admin,
          instrument_id,
          deposit_id,
          lineage_root_instruction_cid,
          terminal_instruction_cid,
          credited_minor,
          receiver_holding_cids,
          metadata,
          reason,
          created_at,
          active,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,TRUE,$15)
        ON CONFLICT (contract_id) DO UPDATE SET
          committee_party = EXCLUDED.committee_party,
          owner_party = EXCLUDED.owner_party,
          account_id = EXCLUDED.account_id,
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          deposit_id = EXCLUDED.deposit_id,
          lineage_root_instruction_cid = EXCLUDED.lineage_root_instruction_cid,
          terminal_instruction_cid = EXCLUDED.terminal_instruction_cid,
          credited_minor = EXCLUDED.credited_minor,
          receiver_holding_cids = EXCLUDED.receiver_holding_cids,
          metadata = EXCLUDED.metadata,
          reason = EXCLUDED.reason,
          created_at = EXCLUDED.created_at,
          active = TRUE,
          last_offset = EXCLUDED.last_offset
        "#,
    )
    .bind(created.contract_id)
    .bind(committee_party)
    .bind(owner_party)
    .bind(account_id)
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(deposit_id)
    .bind(lineage_root_instruction_cid)
    .bind(terminal_instruction_cid)
    .bind(credited_minor)
    .bind(receiver_holding_cids_json)
    .bind(metadata)
    .bind(reason)
    .bind(created_at)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert deposit_receipt")?;

    Ok(())
}

async fn handle_deposit_receipt_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE deposit_receipts
        SET active = FALSE, last_offset = $2
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("archive deposit_receipt")?;

    Ok(())
}

async fn handle_withdrawal_request_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let args = created
        .create_arguments
        .as_ref()
        .ok_or_else(|| anyhow!("missing create_arguments"))?;
    let fields = record_fields_by_label(args)?;

    let committee_party = get_party(&fields, "committee")?;
    let owner_party = get_party(&fields, "owner")?;
    let account_id = get_text(&fields, "accountId")?;
    let instrument_admin = get_party(&fields, "instrumentAdmin")?;
    let instrument_id = get_text(&fields, "instrumentId")?;
    let withdrawal_id = get_text(&fields, "withdrawalId")?;
    let amount_minor = get_int64(&fields, "amountMinor")?;

    let created_at = created_at_utc(&created)?;

    sqlx::query(
        r#"
        INSERT INTO withdrawal_requests (
          contract_id,
          committee_party,
          owner_party,
          account_id,
          instrument_admin,
          instrument_id,
          withdrawal_id,
          amount_minor,
          created_at,
          active,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,TRUE,$10)
        ON CONFLICT (contract_id) DO UPDATE SET
          committee_party = EXCLUDED.committee_party,
          owner_party = EXCLUDED.owner_party,
          account_id = EXCLUDED.account_id,
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          withdrawal_id = EXCLUDED.withdrawal_id,
          amount_minor = EXCLUDED.amount_minor,
          created_at = EXCLUDED.created_at,
          active = TRUE,
          last_offset = EXCLUDED.last_offset
        "#,
    )
    .bind(created.contract_id)
    .bind(committee_party)
    .bind(owner_party)
    .bind(account_id)
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(withdrawal_id)
    .bind(amount_minor)
    .bind(created_at)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert withdrawal_request")?;

    Ok(())
}

async fn handle_withdrawal_request_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE withdrawal_requests
        SET
          active = FALSE,
          last_offset = $2,
          request_state = CASE
            WHEN request_state IN ('Processed', 'RejectedIneligible', 'Failed', 'Superseded')
              THEN request_state
            ELSE 'Superseded'
          END,
          request_state_reason = CASE
            WHEN request_state IN ('Processed', 'RejectedIneligible', 'Failed', 'Superseded')
              THEN request_state_reason
            ELSE COALESCE(request_state_reason, 'WITHDRAWAL_REQUEST_ARCHIVED')
          END,
          last_evaluated_at = COALESCE(last_evaluated_at, now()),
          processed_at = CASE
            WHEN request_state IN ('Processed', 'RejectedIneligible', 'Failed', 'Superseded')
              THEN processed_at
            ELSE COALESCE(processed_at, now())
          END
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("archive withdrawal_request")?;

    Ok(())
}

async fn handle_withdrawal_pending_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let args = created
        .create_arguments
        .as_ref()
        .ok_or_else(|| anyhow!("missing create_arguments"))?;
    let fields = record_fields_by_label(args)?;

    let committee_party = get_party(&fields, "committee")?;
    let owner_party = get_party(&fields, "owner")?;
    let account_id = get_text(&fields, "accountId")?;
    let instrument_admin = get_party(&fields, "instrumentAdmin")?;
    let instrument_id = get_text(&fields, "instrumentId")?;
    let withdrawal_id = get_text(&fields, "withdrawalId")?;
    let amount_minor = get_int64(&fields, "amountMinor")?;
    let token_config_cid = get_contract_id(&fields, "tokenConfigCid")?;
    let lineage_root_instruction_cid = get_contract_id(&fields, "lineageRootInstructionCid")?;
    let current_instruction_cid = get_contract_id(&fields, "currentInstructionCid")?;
    let step_seq = get_int64(&fields, "stepSeq")?;
    let pending_state = get_enum(&fields, "pendingState")?;
    let pending_actions = get_list_text(&fields, "pendingActions")?;
    let pending_actions_json =
        serde_json::to_value(&pending_actions).context("serialize pendingActions")?;
    let metadata = get_text_map_text_json(&fields, "metadata")?;
    let reason = get_text(&fields, "reason")?;

    let created_at = created_at_utc(&created)?;

    sqlx::query(
        r#"
        INSERT INTO withdrawal_pendings (
          contract_id,
          committee_party,
          owner_party,
          account_id,
          instrument_admin,
          instrument_id,
          withdrawal_id,
          amount_minor,
          token_config_cid,
          lineage_root_instruction_cid,
          current_instruction_cid,
          step_seq,
          pending_state,
          pending_actions,
          metadata,
          reason,
          created_at,
          active,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,TRUE,$18)
        ON CONFLICT (contract_id) DO UPDATE SET
          committee_party = EXCLUDED.committee_party,
          owner_party = EXCLUDED.owner_party,
          account_id = EXCLUDED.account_id,
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          withdrawal_id = EXCLUDED.withdrawal_id,
          amount_minor = EXCLUDED.amount_minor,
          token_config_cid = EXCLUDED.token_config_cid,
          lineage_root_instruction_cid = EXCLUDED.lineage_root_instruction_cid,
          current_instruction_cid = EXCLUDED.current_instruction_cid,
          step_seq = EXCLUDED.step_seq,
          pending_state = EXCLUDED.pending_state,
          pending_actions = EXCLUDED.pending_actions,
          metadata = EXCLUDED.metadata,
          reason = EXCLUDED.reason,
          created_at = EXCLUDED.created_at,
          active = TRUE,
          last_offset = EXCLUDED.last_offset
        "#,
    )
    .bind(created.contract_id.clone())
    .bind(committee_party)
    .bind(owner_party.clone())
    .bind(account_id)
    .bind(instrument_admin.clone())
    .bind(instrument_id.clone())
    .bind(withdrawal_id.clone())
    .bind(amount_minor)
    .bind(token_config_cid)
    .bind(lineage_root_instruction_cid)
    .bind(current_instruction_cid)
    .bind(step_seq)
    .bind(pending_state)
    .bind(pending_actions_json)
    .bind(metadata)
    .bind(reason)
    .bind(created_at)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert withdrawal_pending")?;

    sqlx::query(
        r#"
        INSERT INTO withdrawal_pending_latest (
          owner_party,
          instrument_admin,
          instrument_id,
          withdrawal_id,
          contract_id,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (owner_party, instrument_admin, instrument_id, withdrawal_id) DO UPDATE SET
          contract_id = EXCLUDED.contract_id,
          last_offset = EXCLUDED.last_offset,
          updated_at = now()
        "#,
    )
    .bind(owner_party)
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(withdrawal_id)
    .bind(created.contract_id)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert withdrawal_pending_latest")?;

    Ok(())
}

async fn handle_withdrawal_pending_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE withdrawal_pendings
        SET active = FALSE, last_offset = $2
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id.clone())
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("archive withdrawal_pending")?;

    sqlx::query(
        r#"
        DELETE FROM withdrawal_pending_latest
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id)
    .execute(&mut **dbtx)
    .await
    .context("delete withdrawal_pending_latest")?;

    Ok(())
}

async fn handle_withdrawal_receipt_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let args = created
        .create_arguments
        .as_ref()
        .ok_or_else(|| anyhow!("missing create_arguments"))?;
    let fields = record_fields_by_label(args)?;

    let committee_party = get_party(&fields, "committee")?;
    let owner_party = get_party(&fields, "owner")?;
    let account_id = get_text(&fields, "accountId")?;
    let instrument_admin = get_party(&fields, "instrumentAdmin")?;
    let instrument_id = get_text(&fields, "instrumentId")?;
    let withdrawal_id = get_text(&fields, "withdrawalId")?;
    let amount_minor = get_int64(&fields, "amountMinor")?;
    let status = get_enum(&fields, "status")?;
    let lineage_root_instruction_cid = get_contract_id(&fields, "lineageRootInstructionCid")?;
    let terminal_instruction_cid = get_contract_id(&fields, "terminalInstructionCid")?;

    let created_at = created_at_utc(&created)?;

    sqlx::query(
        r#"
        INSERT INTO withdrawal_receipts (
          contract_id,
          committee_party,
          owner_party,
          account_id,
          instrument_admin,
          instrument_id,
          withdrawal_id,
          amount_minor,
          status,
          lineage_root_instruction_cid,
          terminal_instruction_cid,
          created_at,
          active,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,TRUE,$13)
        ON CONFLICT (contract_id) DO UPDATE SET
          committee_party = EXCLUDED.committee_party,
          owner_party = EXCLUDED.owner_party,
          account_id = EXCLUDED.account_id,
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          withdrawal_id = EXCLUDED.withdrawal_id,
          amount_minor = EXCLUDED.amount_minor,
          status = EXCLUDED.status,
          lineage_root_instruction_cid = EXCLUDED.lineage_root_instruction_cid,
          terminal_instruction_cid = EXCLUDED.terminal_instruction_cid,
          created_at = EXCLUDED.created_at,
          active = TRUE,
          last_offset = EXCLUDED.last_offset
        "#,
    )
    .bind(created.contract_id)
    .bind(committee_party)
    .bind(owner_party)
    .bind(account_id)
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(withdrawal_id)
    .bind(amount_minor)
    .bind(status)
    .bind(lineage_root_instruction_cid)
    .bind(terminal_instruction_cid)
    .bind(created_at)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert withdrawal_receipt")?;

    Ok(())
}

async fn handle_withdrawal_receipt_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE withdrawal_receipts
        SET active = FALSE, last_offset = $2
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("archive withdrawal_receipt")?;

    Ok(())
}

async fn handle_quarantined_holding_created(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    created: lapi::CreatedEvent,
) -> Result<()> {
    let args = created
        .create_arguments
        .as_ref()
        .ok_or_else(|| anyhow!("missing create_arguments"))?;
    let fields = record_fields_by_label(args)?;

    let committee_party = get_party(&fields, "committee")?;
    let instrument_admin = get_party(&fields, "instrumentAdmin")?;
    let instrument_id = get_text(&fields, "instrumentId")?;
    let holding_cid = get_contract_id(&fields, "holdingCid")?;
    let discovered_at = get_time(&fields, "discoveredAt")?;
    let reason = get_text(&fields, "reason")?;
    let related_account_id = get_optional_text(&fields, "relatedAccountId")?;
    let related_deposit_id = get_optional_text(&fields, "relatedDepositId")?;

    let created_at = created_at_utc(&created)?;

    sqlx::query(
        r#"
        INSERT INTO quarantined_holdings (
          contract_id,
          committee_party,
          instrument_admin,
          instrument_id,
          holding_cid,
          discovered_at,
          reason,
          related_account_id,
          related_deposit_id,
          created_at,
          active,
          last_offset
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,TRUE,$11)
        ON CONFLICT (contract_id) DO UPDATE SET
          committee_party = EXCLUDED.committee_party,
          instrument_admin = EXCLUDED.instrument_admin,
          instrument_id = EXCLUDED.instrument_id,
          holding_cid = EXCLUDED.holding_cid,
          discovered_at = EXCLUDED.discovered_at,
          reason = EXCLUDED.reason,
          related_account_id = EXCLUDED.related_account_id,
          related_deposit_id = EXCLUDED.related_deposit_id,
          created_at = EXCLUDED.created_at,
          active = TRUE,
          last_offset = EXCLUDED.last_offset
        "#,
    )
    .bind(created.contract_id)
    .bind(committee_party)
    .bind(instrument_admin)
    .bind(instrument_id)
    .bind(holding_cid)
    .bind(discovered_at)
    .bind(reason)
    .bind(related_account_id)
    .bind(related_deposit_id)
    .bind(created_at)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert quarantined_holding")?;

    Ok(())
}

async fn handle_quarantined_holding_archived(
    dbtx: &mut Transaction<'_, Postgres>,
    offset: i64,
    archived: lapi::ArchivedEvent,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE quarantined_holdings
        SET active = FALSE, last_offset = $2
        WHERE contract_id = $1
        "#,
    )
    .bind(archived.contract_id)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("archive quarantined_holding")?;

    Ok(())
}

async fn get_begin_exclusive(pool: &PgPool) -> Result<i64> {
    let row = sqlx::query(
        r#"
        SELECT ledger_offset
        FROM ledger_offsets
        WHERE consumer = $1
        "#,
    )
    .bind(OFFSET_CONSUMER)
    .fetch_optional(pool)
    .await
    .context("query ledger offset")?;

    let Some(row) = row else {
        return Ok(0);
    };

    let offset: i64 = row.try_get("ledger_offset").context("read ledger_offset")?;

    Ok(offset)
}

async fn upsert_offset(dbtx: &mut Transaction<'_, Postgres>, offset: i64) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO ledger_offsets (consumer, ledger_offset)
        VALUES ($1, $2)
        ON CONFLICT (consumer) DO UPDATE SET
          ledger_offset = EXCLUDED.ledger_offset,
          updated_at = now()
        "#,
    )
    .bind(OFFSET_CONSUMER)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert ledger offset")?;

    Ok(())
}

async fn upsert_submission_path_watermark(
    dbtx: &mut Transaction<'_, Postgres>,
    submission_path: &str,
    offset: i64,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO submission_path_watermarks (submission_path, watermark_offset)
        VALUES ($1, $2)
        ON CONFLICT (submission_path) DO UPDATE SET
          watermark_offset = EXCLUDED.watermark_offset,
          updated_at = now()
        "#,
    )
    .bind(submission_path)
    .bind(offset)
    .execute(&mut **dbtx)
    .await
    .context("upsert submission_path_watermark")?;

    Ok(())
}

fn record_fields_by_label(record: &lapi::Record) -> Result<HashMap<String, lapi::Value>> {
    let mut out = HashMap::new();
    for field in &record.fields {
        if field.label.is_empty() {
            return Err(anyhow!(
                "missing record field label (enable verbose streaming)"
            ));
        }
        let value = field
            .value
            .as_ref()
            .ok_or_else(|| anyhow!("missing record field value"))?;
        out.insert(field.label.clone(), value.clone());
    }
    Ok(out)
}

fn get_field<'a>(fields: &'a HashMap<String, lapi::Value>, label: &str) -> Result<&'a lapi::Value> {
    fields.get(label).ok_or_else(|| {
        let mut keys: Vec<&str> = fields.keys().map(String::as_str).collect();
        keys.sort_unstable();
        anyhow!("missing field: {label} (available: {keys:?})")
    })
}

fn get_party(fields: &HashMap<String, lapi::Value>, label: &str) -> Result<String> {
    let v = get_field(fields, label)?;
    match v.sum.as_ref() {
        Some(lapi::value::Sum::Party(p)) => Ok(p.clone()),
        other => Err(anyhow!("field {label} expected Party, got {other:?}")),
    }
}

fn get_text(fields: &HashMap<String, lapi::Value>, label: &str) -> Result<String> {
    let v = get_field(fields, label)?;
    match v.sum.as_ref() {
        Some(lapi::value::Sum::Text(t)) => Ok(t.clone()),
        other => Err(anyhow!("field {label} expected Text, got {other:?}")),
    }
}

fn get_enum(fields: &HashMap<String, lapi::Value>, label: &str) -> Result<String> {
    let v = get_field(fields, label)?;
    match v.sum.as_ref() {
        Some(lapi::value::Sum::Enum(e)) => Ok(e.constructor.clone()),
        other => Err(anyhow!("field {label} expected Enum, got {other:?}")),
    }
}

fn get_bool(fields: &HashMap<String, lapi::Value>, label: &str) -> Result<bool> {
    let v = get_field(fields, label)?;
    match v.sum.as_ref() {
        Some(lapi::value::Sum::Bool(b)) => Ok(*b),
        other => Err(anyhow!("field {label} expected Bool, got {other:?}")),
    }
}

fn get_int64(fields: &HashMap<String, lapi::Value>, label: &str) -> Result<i64> {
    let v = get_field(fields, label)?;
    match v.sum.as_ref() {
        Some(lapi::value::Sum::Int64(i)) => Ok(*i),
        other => Err(anyhow!("field {label} expected Int64, got {other:?}")),
    }
}

fn get_optional_int64(fields: &HashMap<String, lapi::Value>, label: &str) -> Result<Option<i64>> {
    // Mirror optional text behavior: missing field is treated as Optional(None).
    let Some(v) = fields.get(label) else {
        return Ok(None);
    };
    match v.sum.as_ref() {
        Some(lapi::value::Sum::Optional(opt)) => match opt.value.as_ref() {
            None => Ok(None),
            Some(inner) => match inner.sum.as_ref() {
                Some(lapi::value::Sum::Int64(i)) => Ok(Some(*i)),
                other => Err(anyhow!(
                    "field {label} optional value expected Int64, got {other:?}"
                )),
            },
        },
        other => Err(anyhow!("field {label} expected Optional, got {other:?}")),
    }
}

fn get_optional_contract_id(
    fields: &HashMap<String, lapi::Value>,
    label: &str,
) -> Result<Option<String>> {
    // Mirror optional text behavior: missing field is treated as Optional(None).
    let Some(v) = fields.get(label) else {
        return Ok(None);
    };
    match v.sum.as_ref() {
        Some(lapi::value::Sum::Optional(opt)) => match opt.value.as_ref() {
            None => Ok(None),
            Some(inner) => match inner.sum.as_ref() {
                Some(lapi::value::Sum::ContractId(cid)) => Ok(Some(cid.clone())),
                other => Err(anyhow!(
                    "field {label} optional value expected ContractId, got {other:?}"
                )),
            },
        },
        other => Err(anyhow!("field {label} expected Optional, got {other:?}")),
    }
}

fn get_contract_id(fields: &HashMap<String, lapi::Value>, label: &str) -> Result<String> {
    let v = get_field(fields, label)?;
    match v.sum.as_ref() {
        Some(lapi::value::Sum::ContractId(cid)) => Ok(cid.clone()),
        other => Err(anyhow!("field {label} expected ContractId, got {other:?}")),
    }
}

fn get_list_contract_id(fields: &HashMap<String, lapi::Value>, label: &str) -> Result<Vec<String>> {
    let v = get_field(fields, label)?;
    match v.sum.as_ref() {
        Some(lapi::value::Sum::List(list)) => list
            .elements
            .iter()
            .map(|el| match el.sum.as_ref() {
                Some(lapi::value::Sum::ContractId(cid)) => Ok(cid.clone()),
                other => Err(anyhow!(
                    "field {label} list element expected ContractId, got {other:?}"
                )),
            })
            .collect(),
        other => Err(anyhow!("field {label} expected List, got {other:?}")),
    }
}

fn get_optional_party(
    fields: &HashMap<String, lapi::Value>,
    label: &str,
) -> Result<Option<String>> {
    // Mirror optional text behavior: missing field is treated as Optional(None).
    let Some(v) = fields.get(label) else {
        return Ok(None);
    };
    match v.sum.as_ref() {
        Some(lapi::value::Sum::Optional(opt)) => match opt.value.as_ref() {
            None => Ok(None),
            Some(inner) => match inner.sum.as_ref() {
                Some(lapi::value::Sum::Party(p)) => Ok(Some(p.clone())),
                other => Err(anyhow!(
                    "field {label} optional value expected Party, got {other:?}"
                )),
            },
        },
        other => Err(anyhow!("field {label} expected Optional, got {other:?}")),
    }
}

fn get_optional_record(
    fields: &HashMap<String, lapi::Value>,
    label: &str,
) -> Result<Option<lapi::Record>> {
    // Mirror optional text behavior: missing field is treated as Optional(None).
    let Some(v) = fields.get(label) else {
        return Ok(None);
    };
    match v.sum.as_ref() {
        Some(lapi::value::Sum::Optional(opt)) => match opt.value.as_ref() {
            None => Ok(None),
            Some(inner) => match inner.sum.as_ref() {
                Some(lapi::value::Sum::Record(r)) => Ok(Some(r.clone())),
                other => Err(anyhow!(
                    "field {label} optional value expected Record, got {other:?}"
                )),
            },
        },
        other => Err(anyhow!("field {label} expected Optional, got {other:?}")),
    }
}

fn timestamp_micros_to_utc(micros: i64) -> Result<DateTime<Utc>> {
    let secs = micros.div_euclid(1_000_000);
    let rem_micros = micros.rem_euclid(1_000_000);
    let nanos =
        u32::try_from(rem_micros * 1_000).context("timestamp micros remainder out of range")?;

    DateTime::<Utc>::from_timestamp(secs, nanos).ok_or_else(|| anyhow!("timestamp out of range"))
}

fn get_time(fields: &HashMap<String, lapi::Value>, label: &str) -> Result<DateTime<Utc>> {
    let v = get_field(fields, label)?;
    match v.sum.as_ref() {
        Some(lapi::value::Sum::Timestamp(micros)) => timestamp_micros_to_utc(*micros),
        other => Err(anyhow!("field {label} expected Time, got {other:?}")),
    }
}

fn get_text_map_text_json(
    fields: &HashMap<String, lapi::Value>,
    label: &str,
) -> Result<serde_json::Value> {
    let v = get_field(fields, label)?;
    match v.sum.as_ref() {
        Some(lapi::value::Sum::TextMap(tm)) => {
            let mut out = serde_json::Map::new();
            for entry in &tm.entries {
                let value = entry
                    .value
                    .as_ref()
                    .ok_or_else(|| anyhow!("text_map entry missing value"))?;
                let text = match value.sum.as_ref() {
                    Some(lapi::value::Sum::Text(t)) => t.clone(),
                    other => {
                        return Err(anyhow!(
                            "field {label} expected TextMap Text entry value, got {other:?}"
                        ))
                    }
                };
                out.insert(entry.key.clone(), serde_json::Value::String(text));
            }
            Ok(serde_json::Value::Object(out))
        }
        other => Err(anyhow!("field {label} expected TextMap, got {other:?}")),
    }
}

fn get_record(fields: &HashMap<String, lapi::Value>, label: &str) -> Result<lapi::Record> {
    let v = get_field(fields, label)?;
    match v.sum.as_ref() {
        Some(lapi::value::Sum::Record(r)) => Ok(r.clone()),
        other => Err(anyhow!("field {label} expected Record, got {other:?}")),
    }
}

fn get_text_from_record(
    fields: &HashMap<String, lapi::Value>,
    record_label: &str,
    inner_label: &str,
) -> Result<String> {
    let record = get_record(fields, record_label)?;
    let inner_fields = record_fields_by_label(&record)?;
    get_text(&inner_fields, inner_label)
}

fn get_list_text(fields: &HashMap<String, lapi::Value>, label: &str) -> Result<Vec<String>> {
    let v = get_field(fields, label)?;
    match v.sum.as_ref() {
        Some(lapi::value::Sum::List(list)) => list
            .elements
            .iter()
            .map(|el| match el.sum.as_ref() {
                Some(lapi::value::Sum::Text(t)) => Ok(t.clone()),
                other => Err(anyhow!(
                    "field {label} list element expected Text, got {other:?}"
                )),
            })
            .collect(),
        other => Err(anyhow!("field {label} expected List, got {other:?}")),
    }
}

fn get_optional_text(fields: &HashMap<String, lapi::Value>, label: &str) -> Result<Option<String>> {
    // Daml JSON API treats omitted optional fields as None. Mirror that leniency here because
    // some API servers omit Optional(None) fields in practice.
    let Some(v) = fields.get(label) else {
        return Ok(None);
    };
    match v.sum.as_ref() {
        Some(lapi::value::Sum::Optional(opt)) => match opt.value.as_ref() {
            None => Ok(None),
            Some(inner) => match inner.sum.as_ref() {
                Some(lapi::value::Sum::Text(t)) => Ok(Some(t.clone())),
                other => Err(anyhow!(
                    "field {label} optional value expected Text, got {other:?}"
                )),
            },
        },
        other => Err(anyhow!("field {label} expected Optional, got {other:?}")),
    }
}

fn timestamp_to_utc(ts: &prost_types::Timestamp) -> Result<DateTime<Utc>> {
    let secs = ts.seconds;
    let nanos = ts.nanos;
    if nanos < 0 {
        return Err(anyhow!("timestamp nanos must be non-negative"));
    }

    let nanos_u32 = u32::try_from(nanos).context("timestamp nanos out of range")?;
    let dt = DateTime::<Utc>::from_timestamp(secs, nanos_u32)
        .ok_or_else(|| anyhow!("timestamp out of range"))?;
    Ok(dt)
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
    fn local_ledger_host_detection() {
        assert!(is_local_ledger_host("127.0.0.1"));
        assert!(is_local_ledger_host("localhost"));
        assert!(is_local_ledger_host("::1"));
        assert!(is_local_ledger_host("0.0.0.0"));

        assert!(!is_local_ledger_host("example.com"));
        assert!(!is_local_ledger_host("10.0.0.1"));
    }
}
