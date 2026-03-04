use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context as _};
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use chrono::Utc;
use pebble_fluid::replication::ReplicationBroadcaster;
use pebble_fluid::wal::{WalCommand, WalOffset};
use pebble_ids::parse_account_id;
use pebble_trading::{
    match_order_presorted, Fill, IncomingOrder, MarketId, OrderBookView, OrderId, OrderNonce,
    OrderSide, OrderType, RestingOrder, TimeInForce,
};
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::{
    fluid_bridge, perf_metrics::ApiPerfMetrics, projection_sql, wal_projector, ApiError, AppState,
};

const MARKET_LOCK_NAMESPACE: i32 = 12;
const DEFAULT_QUERY_LIMIT: i64 = 100;
const MAX_QUERY_LIMIT: i64 = 500;
const ENDPOINT_PLACE_ORDER: &str = "orders.place";
const ENDPOINT_CANCEL_ORDER: &str = "orders.cancel";
const MARKET_LOCK_TIMEOUT: Duration = Duration::from_secs(30);

struct RiskReservationGuard {
    risk_handle: pebble_fluid::actor::RiskActorHandle,
    account_id: String,
    release_amount: i64,
    armed: bool,
    health_handle: pebble_fluid::engine::HealthHandle,
}

impl RiskReservationGuard {
    fn new(
        risk_handle: &pebble_fluid::actor::RiskActorHandle,
        account_id: &str,
        release_amount: i64,
        health_handle: pebble_fluid::engine::HealthHandle,
    ) -> Self {
        Self {
            risk_handle: risk_handle.clone(),
            account_id: account_id.to_string(),
            release_amount,
            armed: true,
            health_handle,
        }
    }

    async fn abort_if_active(&mut self) -> Result<(), pebble_fluid::error::FluidError> {
        if !self.armed {
            return Ok(());
        }
        let result = self
            .risk_handle
            .abort(&self.account_id, self.release_amount)
            .await;
        if result.is_ok() {
            self.armed = false;
        }
        result
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RiskReservationGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        let risk_handle = self.risk_handle.clone();
        let account_id = self.account_id.clone();
        let release_amount = self.release_amount;
        let health_handle = self.health_handle.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    if let Err(err) = risk_handle.abort(&account_id, release_amount).await {
                        health_handle.mark_unhealthy(&format!(
                            "fluid reserve abort in drop guard failed: {err:?}"
                        ));
                        tracing::error!(reason = %err, "fluid reserve abort failed in drop guard");
                    }
                });
            }
            Err(err) => {
                health_handle.mark_unhealthy(&format!(
                    "fluid reserve abort in drop guard failed: no runtime handle ({err})"
                ));
                tracing::error!(
                    "failed to run reserve abort in drop guard: no tokio runtime handle"
                );
            }
        }
    }
}

async fn lock_market<'a, T>(
    market: &'a tokio::sync::Mutex<T>,
    market_id: &str,
) -> Result<tokio::sync::MutexGuard<'a, T>, ApiError> {
    match tokio::time::timeout(MARKET_LOCK_TIMEOUT, market.lock()).await {
        Ok(guard) => Ok(guard),
        Err(_) => Err(ApiError::service_unavailable_code(
            format!("market lock timed out for {market_id}"),
            "market_lock_timeout",
        )),
    }
}

#[derive(Clone, Debug)]
pub(crate) struct M3ApiConfig {
    pub(crate) api_keys: HashMap<String, String>,
    pub(crate) admin_keys: HashSet<String>,
    pub(crate) engine_version: String,
}

impl M3ApiConfig {
    pub(crate) fn from_env() -> Result<Self, anyhow::Error> {
        let api_keys_raw = std::env::var("PEBBLE_API_KEYS").unwrap_or_default();
        let api_keys = parse_api_keys(&api_keys_raw)?;

        let admin_keys_raw = std::env::var("PEBBLE_ADMIN_KEYS").unwrap_or_default();
        let admin_keys = parse_admin_keys(&admin_keys_raw)?;

        let engine_version = std::env::var("PEBBLE_TRADING_ENGINE_VERSION")
            .unwrap_or_else(|_| "m3-dev-v1".to_string());
        if engine_version.trim().is_empty() {
            return Err(anyhow!("PEBBLE_TRADING_ENGINE_VERSION must be non-empty"));
        }

        Ok(Self {
            api_keys,
            admin_keys,
            engine_version,
        })
    }
}

#[derive(Debug, Clone)]
struct AuthPrincipal {
    account_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PlaceOrderRequest {
    pub(crate) order_id: Option<String>,
    pub(crate) account_id: Option<String>,
    pub(crate) market_id: String,
    pub(crate) outcome: String,
    pub(crate) side: OrderSide,
    pub(crate) order_type: Option<OrderType>,
    pub(crate) nonce: i64,
    pub(crate) price_ticks: Option<i64>,
    pub(crate) quantity_minor: i64,
}

#[derive(Debug, Serialize)]
pub(crate) struct OrderFillView {
    pub(crate) fill_id: String,
    pub(crate) fill_sequence: i64,
    pub(crate) maker_order_id: String,
    pub(crate) taker_order_id: String,
    pub(crate) outcome: String,
    pub(crate) price_ticks: i64,
    pub(crate) quantity_minor: i64,
    pub(crate) engine_version: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct PlaceOrderResponse {
    pub(crate) order_id: String,
    pub(crate) account_id: String,
    pub(crate) market_id: String,
    pub(crate) outcome: String,
    pub(crate) side: String,
    pub(crate) order_type: String,
    pub(crate) tif: String,
    pub(crate) status: String,
    pub(crate) quantity_minor: i64,
    pub(crate) remaining_minor: i64,
    pub(crate) locked_minor: i64,
    pub(crate) nonce: i64,
    pub(crate) available_minor_after: i64,
    pub(crate) fills: Vec<OrderFillView>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ReconcileOrderQuery {
    pub(crate) nonce: i64,
    pub(crate) wal_offset: u64,
    pub(crate) token: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct ReconcileOrderView {
    pub(crate) order_id: String,
    pub(crate) market_id: String,
    pub(crate) status: String,
    pub(crate) wal_offset: Option<u64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ReconcileOrderResponse {
    pub(crate) status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) original_lost: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) projection_cursor: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) order: Option<ReconcileOrderView>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListOrdersQuery {
    pub(crate) market_id: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) limit: Option<i64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct OrderRowView {
    pub(crate) order_id: String,
    pub(crate) market_id: String,
    pub(crate) account_id: String,
    pub(crate) owner_party: String,
    pub(crate) outcome: String,
    pub(crate) side: String,
    pub(crate) order_type: String,
    pub(crate) tif: String,
    pub(crate) nonce: i64,
    pub(crate) price_ticks: i64,
    pub(crate) quantity_minor: i64,
    pub(crate) remaining_minor: i64,
    pub(crate) locked_minor: i64,
    pub(crate) status: String,
    pub(crate) engine_version: String,
    pub(crate) submitted_at: chrono::DateTime<chrono::Utc>,
    pub(crate) updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListFillsQuery {
    pub(crate) market_id: Option<String>,
    pub(crate) limit: Option<i64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct FillRowView {
    pub(crate) fill_id: String,
    pub(crate) fill_sequence: i64,
    pub(crate) market_id: String,
    pub(crate) outcome: String,
    pub(crate) maker_order_id: String,
    pub(crate) taker_order_id: String,
    pub(crate) perspective_role: String,
    pub(crate) price_ticks: i64,
    pub(crate) quantity_minor: i64,
    pub(crate) engine_version: String,
    pub(crate) matched_at: chrono::DateTime<chrono::Utc>,
    pub(crate) clearing_epoch: Option<i64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct CancelOrderResponse {
    pub(crate) order_id: String,
    pub(crate) status: String,
    pub(crate) released_locked_minor: i64,
}

#[derive(sqlx::FromRow)]
struct AccountContextRow {
    owner_party: String,
    instrument_admin: String,
    instrument_id: String,
    status: String,
    cleared_cash_minor: i64,
    delta_pending_trades_minor: i64,
    locked_open_orders_minor: i64,
    pending_withdrawals_reserved_minor: i64,
}

#[derive(sqlx::FromRow)]
struct MarketContextRow {
    status: String,
    outcomes: sqlx::types::Json<Vec<String>>,
    instrument_admin: String,
    instrument_id: String,
}

#[derive(sqlx::FromRow, Clone)]
struct RestingOrderRow {
    order_id: String,
    account_id: String,
    side: String,
    outcome: String,
    price_ticks: i64,
    remaining_minor: i64,
    locked_minor: i64,
    submitted_at_micros: i64,
}

#[derive(sqlx::FromRow)]
struct OrderRow {
    order_id: String,
    market_id: String,
    account_id: String,
    owner_party: String,
    outcome: String,
    side: String,
    order_type: String,
    tif: String,
    nonce: i64,
    price_ticks: i64,
    quantity_minor: i64,
    remaining_minor: i64,
    locked_minor: i64,
    status: String,
    engine_version: String,
    submitted_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(sqlx::FromRow)]
struct ReconcileOrderRow {
    order_id: String,
    market_id: String,
    status: String,
    wal_offset: Option<i64>,
}

#[derive(sqlx::FromRow)]
struct FillPerspectiveRow {
    fill_id: String,
    fill_sequence: i64,
    market_id: String,
    maker_order_id: String,
    taker_order_id: String,
    quantity_minor: i64,
    engine_version: String,
    matched_at: chrono::DateTime<chrono::Utc>,
    clearing_epoch: Option<i64>,
    maker_account_id: String,
    taker_account_id: String,
    maker_outcome: String,
    taker_outcome: String,
    maker_price_ticks: i64,
    taker_price_ticks: i64,
}

#[derive(sqlx::FromRow)]
struct CancelOrderRow {
    order_id: String,
    account_id: String,
    market_id: String,
    outcome: String,
    side: String,
    price_ticks: i64,
    remaining_minor: i64,
    locked_minor: i64,
    status: String,
}

#[derive(Default, Debug, Clone)]
struct RiskDelta {
    delta_pending_minor: i64,
    locked_delta_minor: i64,
}

#[derive(Debug, Clone)]
struct MakerOrderUpdate {
    order_id: String,
    new_remaining_minor: i64,
    new_locked_minor: i64,
    new_status: &'static str,
}

#[derive(Debug, Clone)]
struct PositionDelta {
    net_quantity_delta_minor: i64,
    last_price_ticks: i64,
}

#[derive(Debug, Clone)]
struct IncomingOrderInput<'a> {
    order_id: &'a str,
    account_id: &'a str,
    market_id: &'a str,
    outcome: &'a str,
    side: OrderSide,
    order_type: Option<OrderType>,
    nonce: i64,
    price_ticks: Option<i64>,
    quantity_minor: i64,
}

struct InsertTakerOrderArgs<'a> {
    incoming: &'a IncomingOrder,
    account_ctx: &'a AccountContextRow,
    engine_version: &'a str,
    remaining_minor: i64,
    locked_minor: i64,
    status: &'a str,
    wal_offset: Option<WalOffset>,
}

pub(crate) async fn list_orders(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListOrdersQuery>,
) -> Result<Json<Vec<OrderRowView>>, ApiError> {
    let principal = authenticate(&state, &headers).await?;
    let limit = clamp_limit(query.limit);

    let rows: Vec<OrderRow> = sqlx::query_as(
        r#"
        SELECT
          order_id,
          market_id,
          account_id,
          owner_party,
          outcome,
          side,
          order_type,
          tif,
          nonce,
          price_ticks,
          quantity_minor,
          remaining_minor,
          locked_minor,
          status,
          engine_version,
          submitted_at,
          updated_at
        FROM orders
        WHERE account_id = $1
          AND ($2::TEXT IS NULL OR market_id = $2)
          AND ($3::TEXT IS NULL OR status = $3)
        ORDER BY submitted_at DESC, order_id DESC
        LIMIT $4
        "#,
    )
    .bind(&principal.account_id)
    .bind(query.market_id)
    .bind(query.status)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .context("query orders")?;

    let out = rows
        .into_iter()
        .map(|row| OrderRowView {
            order_id: row.order_id,
            market_id: row.market_id,
            account_id: row.account_id,
            owner_party: row.owner_party,
            outcome: row.outcome,
            side: row.side,
            order_type: row.order_type,
            tif: row.tif,
            nonce: row.nonce,
            price_ticks: row.price_ticks,
            quantity_minor: row.quantity_minor,
            remaining_minor: row.remaining_minor,
            locked_minor: row.locked_minor,
            status: row.status,
            engine_version: row.engine_version,
            submitted_at: row.submitted_at,
            updated_at: row.updated_at,
        })
        .collect();

    Ok(Json(out))
}

pub(crate) async fn list_fills(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListFillsQuery>,
) -> Result<Json<Vec<FillRowView>>, ApiError> {
    let principal = authenticate(&state, &headers).await?;
    let limit = clamp_limit(query.limit);

    let rows: Vec<FillPerspectiveRow> = sqlx::query_as(
        r#"
        SELECT
          fill_id,
          fill_sequence,
          market_id,
          maker_order_id,
          taker_order_id,
          quantity_minor,
          engine_version,
          matched_at,
          clearing_epoch,
          maker_account_id,
          taker_account_id,
          maker_outcome,
          taker_outcome,
          maker_price_ticks,
          taker_price_ticks
        FROM fills
        WHERE (maker_account_id = $1 OR taker_account_id = $1)
          AND ($2::TEXT IS NULL OR market_id = $2)
        ORDER BY matched_at DESC, fill_sequence DESC
        LIMIT $3
        "#,
    )
    .bind(&principal.account_id)
    .bind(query.market_id)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .context("query fills")?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(project_fill_row_for_account(&row, &principal.account_id)?);
    }

    Ok(Json(out))
}

pub(crate) async fn reconcile_order_outcome(
    State(state): State<AppState>,
    Path(account_id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<ReconcileOrderQuery>,
) -> Result<(StatusCode, Json<ReconcileOrderResponse>), ApiError> {
    let principal = authenticate(&state, &headers).await?;
    let account_id = normalize_account_id(&principal, Some(account_id))?;
    let _ = parse_account_id(&account_id).map_err(|err| ApiError::bad_request(err.to_string()))?;
    let verified_token = state.verify_reconcile_token(
        &account_id,
        query.nonce,
        query.wal_offset,
        query.token.as_str(),
    )?;

    let maybe_order: Option<ReconcileOrderRow> = sqlx::query_as(
        r#"
        SELECT
          order_id,
          market_id,
          status,
          wal_offset
        FROM orders
        WHERE account_id = $1
          AND nonce = $2
        ORDER BY submitted_at DESC, order_id DESC
        LIMIT 1
        "#,
    )
    .bind(&account_id)
    .bind(query.nonce)
    .fetch_optional(&state.db)
    .await
    .context("reconcile: query order by account nonce")
    .map_err(ApiError::from)?;

    if let Some(order) = maybe_order {
        let order_wal_offset_u64 = match order.wal_offset {
            Some(raw) if raw >= 0 => {
                u64::try_from(raw).context("reconcile: order wal_offset conversion")
            }
            Some(raw) => Err(anyhow!(
                "reconcile: order wal_offset must be non-negative: {raw}"
            )),
            None => Ok(0),
        }
        .map_err(ApiError::from)?;
        let matches_requested =
            order.wal_offset.is_some() && order_wal_offset_u64 == query.wal_offset;
        let response = ReconcileOrderResponse {
            status: if matches_requested {
                "found".to_string()
            } else {
                "found_retry".to_string()
            },
            original_lost: if matches_requested { None } else { Some(true) },
            projection_cursor: None,
            order: Some(ReconcileOrderView {
                order_id: order.order_id,
                market_id: order.market_id,
                status: order.status,
                wal_offset: order.wal_offset.and_then(|raw| u64::try_from(raw).ok()),
            }),
        };
        return Ok((StatusCode::OK, Json(response)));
    }

    let projection_cursor_raw: i64 =
        sqlx::query_scalar("SELECT last_projected_offset FROM wal_projection_cursor WHERE id = 1")
            .fetch_one(&state.db)
            .await
            .context("reconcile: query wal projection cursor")
            .map_err(ApiError::from)?;
    if projection_cursor_raw < 0 {
        return Err(ApiError::internal(
            "reconcile: wal projection cursor must be non-negative",
        ));
    }
    let projection_cursor = u64::try_from(projection_cursor_raw)
        .context("reconcile: wal projection cursor conversion")
        .map_err(ApiError::from)?;

    if projection_cursor >= query.wal_offset {
        return Ok((
            StatusCode::OK,
            Json(ReconcileOrderResponse {
                status: "not_found_final".to_string(),
                original_lost: None,
                projection_cursor: Some(projection_cursor),
                order: None,
            }),
        ));
    }

    if state.pending_reconciliation_timed_out(verified_token.unknown_at) {
        return Ok((
            StatusCode::OK,
            Json(ReconcileOrderResponse {
                status: "not_found_timeout".to_string(),
                original_lost: None,
                projection_cursor: Some(projection_cursor),
                order: None,
            }),
        ));
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(ReconcileOrderResponse {
            status: "pending".to_string(),
            original_lost: None,
            projection_cursor: Some(projection_cursor),
            order: None,
        }),
    ))
}

fn project_fill_row_for_account(
    row: &FillPerspectiveRow,
    account_id: &str,
) -> Result<FillRowView, ApiError> {
    let (outcome, price_ticks, perspective_role) = if row.maker_account_id == account_id {
        (row.maker_outcome.clone(), row.maker_price_ticks, "Maker")
    } else if row.taker_account_id == account_id {
        (row.taker_outcome.clone(), row.taker_price_ticks, "Taker")
    } else {
        return Err(ApiError::internal(format!(
            "fill {} missing account perspective for {}",
            row.fill_id, account_id
        )));
    };

    Ok(FillRowView {
        fill_id: row.fill_id.clone(),
        fill_sequence: row.fill_sequence,
        market_id: row.market_id.clone(),
        outcome,
        maker_order_id: row.maker_order_id.clone(),
        taker_order_id: row.taker_order_id.clone(),
        perspective_role: perspective_role.to_string(),
        price_ticks,
        quantity_minor: row.quantity_minor,
        engine_version: row.engine_version.clone(),
        matched_at: row.matched_at,
        clearing_epoch: row.clearing_epoch,
    })
}

pub(crate) async fn place_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<PlaceOrderRequest>,
) -> Result<Json<PlaceOrderResponse>, ApiError> {
    let metrics = state.perf_metrics.clone();
    let market_id_tag = req.market_id.trim().to_string();
    let endpoint_started = Instant::now();

    let result: Result<Json<PlaceOrderResponse>, ApiError> = async {
        let principal = authenticate(&state, &headers).await?;

        let account_id = normalize_account_id(&principal, req.account_id)?;
        let market_id = req.market_id.trim().to_string();
        let outcome = req.outcome.trim().to_string();
        let order_id = normalize_or_generate_order_id(req.order_id);

        let incoming = build_incoming_order(&IncomingOrderInput {
            order_id: &order_id,
            account_id: &account_id,
            market_id: &market_id,
            outcome: &outcome,
            side: req.side,
            order_type: req.order_type,
            nonce: req.nonce,
            price_ticks: req.price_ticks,
            quantity_minor: req.quantity_minor,
        })?;

        // ── Write-mode dispatch ──
        match state.current_write_mode() {
            fluid_bridge::WriteMode::FluidFollower => {
                return Err(ApiError::service_unavailable_code(
                    "node is follower read-only",
                    "follower_read_only",
                ));
            }
            fluid_bridge::WriteMode::FluidLeader => {
                return place_order_fluid(&state, incoming, &metrics)
                    .await
                    .map(Json);
            }
            fluid_bridge::WriteMode::Fatal => {
                return Err(ApiError::service_unavailable_code(
                    "node is in fatal mode",
                    "node_fatal",
                ));
            }
            fluid_bridge::WriteMode::Legacy => {
                if state.fluid_leader_detected.load(Ordering::Relaxed) {
                    return Err(ApiError::service_unavailable_code(
                        "fluid leader detected",
                        "fluid_leader_detected",
                    ));
                }
            }
        }

        let tx_begin_started = Instant::now();
        let tx_result = state
            .db
            .begin()
            .await
            .context("begin place_order tx")
            .map_err(ApiError::from);
        record_stage_result(
            &metrics,
            ENDPOINT_PLACE_ORDER,
            "tx_begin",
            &market_id,
            tx_begin_started,
            &tx_result,
        );
        let mut tx = tx_result?;

        let market_lock_started = Instant::now();
        let market_lock_result = advisory_lock(&mut tx, MARKET_LOCK_NAMESPACE, &market_id).await;
        record_stage_result(
            &metrics,
            ENDPOINT_PLACE_ORDER,
            "lock_market_wait",
            &market_id,
            market_lock_started,
            &market_lock_result,
        );
        market_lock_result?;

        let account_lock_started = Instant::now();
        let account_lock_result =
            advisory_lock(&mut tx, state.account_lock_namespace, &account_id).await;
        record_stage_result(
            &metrics,
            ENDPOINT_PLACE_ORDER,
            "lock_account_wait",
            &market_id,
            account_lock_started,
            &account_lock_result,
        );
        account_lock_result?;

        let account_context_started = Instant::now();
        let account_ctx_result = load_account_context(&mut tx, &account_id).await;
        record_stage_result(
            &metrics,
            ENDPOINT_PLACE_ORDER,
            "load_account_context",
            &market_id,
            account_context_started,
            &account_ctx_result,
        );
        let account_ctx = account_ctx_result?;
        if account_ctx.status != "Active" {
            return Err(ApiError::bad_request("account must be Active"));
        }

        let market_validation_started = Instant::now();
        let market_validation_result =
            validate_market_state(&mut tx, &market_id, &outcome, &account_ctx).await;
        record_stage_result(
            &metrics,
            ENDPOINT_PLACE_ORDER,
            "validate_market_state",
            &market_id,
            market_validation_started,
            &market_validation_result,
        );
        market_validation_result?;

        let nonce_started = Instant::now();
        let nonce_result = enforce_and_advance_nonce(&mut tx, &account_id, incoming.nonce.0).await;
        record_stage_result(
            &metrics,
            ENDPOINT_PLACE_ORDER,
            "nonce_progression",
            &market_id,
            nonce_started,
            &nonce_result,
        );
        nonce_result?;

        let available_before = {
            let legacy_available = checked_add3(
                account_ctx.cleared_cash_minor,
                account_ctx.delta_pending_trades_minor,
                -account_ctx.locked_open_orders_minor,
            )?;
            if state.order_available_includes_withdrawal_reserves {
                legacy_available
                    .checked_sub(account_ctx.pending_withdrawals_reserved_minor)
                    .ok_or_else(|| ApiError::internal("available balance underflow"))?
            } else {
                legacy_available
            }
        };
        let required_lock = match incoming.order_type {
            OrderType::Limit => {
                lock_required_minor(incoming.side, incoming.price_ticks, incoming.quantity_minor)?
            }
            OrderType::Market => 0,
        };

        if available_before < required_lock {
            return Err(ApiError::bad_request(format!(
                "insufficient available balance: available={available_before}, required_lock={required_lock}"
            )));
        }

        let book_load_started = Instant::now();
        let resting_rows_result = load_resting_orders(
            &mut tx,
            &incoming,
            &account_ctx.instrument_admin,
            &account_ctx.instrument_id,
        )
        .await;
        record_stage_result(
            &metrics,
            ENDPOINT_PLACE_ORDER,
            "load_resting_orders",
            &market_id,
            book_load_started,
            &resting_rows_result,
        );
        let resting_rows = resting_rows_result?;

        let mut maker_by_order_id: HashMap<String, RestingOrderRow> = HashMap::new();
        let mut maker_orders = Vec::with_capacity(resting_rows.len());

        for row in &resting_rows {
            let side = parse_db_side(&row.side)?;
            maker_by_order_id.insert(row.order_id.clone(), row.clone());
            maker_orders.push(RestingOrder {
                order_id: OrderId::new(row.order_id.clone())
                    .map_err(|e| ApiError::bad_request(e.to_string()))?,
                account_id: row.account_id.clone(),
                side,
                outcome: row.outcome.clone(),
                price_ticks: row.price_ticks,
                remaining_minor: row.remaining_minor,
                submitted_at_micros: row.submitted_at_micros,
            });
        }

        let mut book = OrderBookView::default();
        match incoming.side {
            OrderSide::Buy => {
                book.asks = maker_orders;
            }
            OrderSide::Sell => {
                book.bids = maker_orders;
            }
        }

        let match_compute_started = Instant::now();
        let match_result = match_order_presorted(book, &incoming, &state.m3_cfg.engine_version, 0)
            .map_err(|e| ApiError::bad_request(format!("match failed: {e}")));
        record_stage_result(
            &metrics,
            ENDPOINT_PLACE_ORDER,
            "match_compute",
            &market_id,
            match_compute_started,
            &match_result,
        );
        let mut match_result = match_result?;

        if incoming.order_type == OrderType::Market && incoming.side == OrderSide::Buy {
            let market_buy_required_minor = market_buy_required_minor(&match_result.fills)?;
            if available_before < market_buy_required_minor {
                return Err(ApiError::bad_request(format!(
                    "insufficient available balance for market buy: available={available_before}, required_fill_cost={market_buy_required_minor}"
                )));
            }
        }

        let matcher_remaining_minor = match_result.incoming_remaining_minor;
        let (taker_remaining_minor, taker_locked_minor, taker_status) = match incoming.order_type {
            OrderType::Limit => (
                matcher_remaining_minor,
                lock_required_minor(incoming.side, incoming.price_ticks, matcher_remaining_minor)?,
                order_status(incoming.quantity_minor, matcher_remaining_minor),
            ),
            OrderType::Market => (
                0,
                0,
                if matcher_remaining_minor == 0 {
                    "Filled"
                } else {
                    "Cancelled"
                },
            ),
        };

        let write_phase_started = Instant::now();
        let write_phase_result: Result<(BTreeMap<String, RiskDelta>, Vec<OrderFillView>), ApiError> =
            async {
                insert_taker_order(
                    &mut tx,
                    InsertTakerOrderArgs {
                        incoming: &incoming,
                        account_ctx: &account_ctx,
                        engine_version: &state.m3_cfg.engine_version,
                        remaining_minor: taker_remaining_minor,
                        locked_minor: taker_locked_minor,
                        status: taker_status,
                        wal_offset: None,
                    },
                )
                .await?;

                let fill_count = match_result.fills.len();
                let event_count = fill_count
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("market event count overflow"))?;
                let (first_fill_sequence, first_market_event_sequence) =
                    reserve_market_sequences(&mut tx, &incoming.market_id.0, fill_count, event_count)
                        .await?;
                assign_fill_sequences(
                    &mut match_result.fills,
                    &incoming.market_id.0,
                    &state.m3_cfg.engine_version,
                    first_fill_sequence,
                )?;

                let mut risk_deltas = BTreeMap::<String, RiskDelta>::new();
                if taker_locked_minor != 0 {
                    risk_deltas.insert(
                        account_id.clone(),
                        RiskDelta {
                            delta_pending_minor: 0,
                            locked_delta_minor: taker_locked_minor,
                        },
                    );
                }

                let mut maker_order_updates = Vec::with_capacity(match_result.maker_updates.len());
                for maker_update in &match_result.maker_updates {
                    let Some(maker_row) = maker_by_order_id.get(&maker_update.order_id.0) else {
                        return Err(ApiError::internal(
                            "matcher returned unknown maker order id",
                        ));
                    };
                    let maker_side = parse_db_side(&maker_row.side)?;
                    let maker_new_locked = lock_required_minor(
                        maker_side,
                        maker_row.price_ticks,
                        maker_update.new_remaining_minor,
                    )?;
                    let maker_status = if maker_update.new_remaining_minor == 0 {
                        "Filled"
                    } else {
                        "PartiallyFilled"
                    };

                    maker_order_updates.push(MakerOrderUpdate {
                        order_id: maker_row.order_id.clone(),
                        new_remaining_minor: maker_update.new_remaining_minor,
                        new_locked_minor: maker_new_locked,
                        new_status: maker_status,
                    });

                    let locked_delta_minor = maker_new_locked
                        .checked_sub(maker_row.locked_minor)
                        .ok_or_else(|| anyhow!("maker locked delta overflow"))?;

                    let entry = risk_deltas.entry(maker_row.account_id.clone()).or_default();
                    entry.locked_delta_minor = entry
                        .locked_delta_minor
                        .checked_add(locked_delta_minor)
                        .ok_or_else(|| anyhow!("maker locked delta accumulation overflow"))?;
                }

                if !maker_order_updates.is_empty() {
                    batch_update_maker_orders(&mut tx, &maker_order_updates).await?;
                }

                if !match_result.fills.is_empty() {
                    batch_insert_fills(
                        &mut tx,
                        &account_ctx.instrument_admin,
                        &account_ctx.instrument_id,
                        &incoming.market_id.0,
                        &match_result.fills,
                    )
                    .await?;
                }

                let mut fill_views = Vec::with_capacity(match_result.fills.len());
                let mut position_deltas = BTreeMap::<(String, String), PositionDelta>::new();

                for fill in &match_result.fills {
                    if !maker_by_order_id.contains_key(&fill.maker_order_id.0) {
                        return Err(ApiError::internal(
                            "missing maker row while persisting fill",
                        ));
                    }

                    accumulate_position_delta(
                        &mut position_deltas,
                        &fill.maker_account_id,
                        &fill.maker_outcome,
                        fill.maker_side,
                        fill.quantity_minor,
                        fill.maker_price_ticks,
                    )?;
                    accumulate_position_delta(
                        &mut position_deltas,
                        &fill.taker_account_id,
                        &fill.taker_outcome,
                        fill.taker_side,
                        fill.quantity_minor,
                        fill.taker_price_ticks,
                    )?;

                    let maker_cash_delta =
                        cash_delta_minor(fill.maker_side, fill.maker_price_ticks, fill.quantity_minor)?;
                    let taker_cash_delta =
                        cash_delta_minor(fill.taker_side, fill.taker_price_ticks, fill.quantity_minor)?;

                    let maker_entry = risk_deltas.entry(fill.maker_account_id.clone()).or_default();
                    maker_entry.delta_pending_minor = maker_entry
                        .delta_pending_minor
                        .checked_add(maker_cash_delta)
                        .ok_or_else(|| anyhow!("maker pending delta overflow"))?;

                    let taker_entry = risk_deltas.entry(fill.taker_account_id.clone()).or_default();
                    taker_entry.delta_pending_minor = taker_entry
                        .delta_pending_minor
                        .checked_add(taker_cash_delta)
                        .ok_or_else(|| anyhow!("taker pending delta overflow"))?;

                    fill_views.push(OrderFillView {
                        fill_id: fill.fill_id.clone(),
                        fill_sequence: fill.sequence,
                        maker_order_id: fill.maker_order_id.0.clone(),
                        taker_order_id: fill.taker_order_id.0.clone(),
                        outcome: fill.taker_outcome.clone(),
                        price_ticks: fill.taker_price_ticks,
                        quantity_minor: fill.quantity_minor,
                        engine_version: fill.engine_version.clone(),
                    });
                }

                if !position_deltas.is_empty() {
                    batch_upsert_position_deltas(&mut tx, &incoming.market_id.0, &position_deltas)
                        .await?;
                }

                if !risk_deltas.is_empty() {
                    batch_apply_risk_deltas(
                        &mut tx,
                        &account_ctx.instrument_admin,
                        &account_ctx.instrument_id,
                        &risk_deltas,
                    )
                    .await?;
                }

                insert_trading_event(
                    &mut tx,
                    "market",
                    &incoming.market_id.0,
                    first_market_event_sequence,
                    "OrderAccepted",
                    serde_json::json!({
                      "order_id": incoming.order_id.0,
                      "account_id": incoming.account_id,
                      "outcome": incoming.outcome,
                      "side": side_to_db(incoming.side),
                      "order_type": order_type_to_db(incoming.order_type),
                      "tif": tif_to_db(incoming.tif),
                      "nonce": incoming.nonce.0,
                      "price_ticks": incoming.price_ticks,
                      "quantity_minor": incoming.quantity_minor,
                      "remaining_minor": taker_remaining_minor,
                      "unfilled_minor": matcher_remaining_minor,
                      "locked_minor": taker_locked_minor,
                      "status": taker_status,
                      "fills": fill_views.len(),
                    }),
                )
                .await?;

                if !match_result.fills.is_empty() {
                    let first_fill_event_sequence = first_market_event_sequence
                        .checked_add(1)
                        .ok_or_else(|| anyhow!("market event sequence overflow"))?;
                    batch_insert_fill_recorded_events(
                        &mut tx,
                        &incoming.market_id.0,
                        first_fill_event_sequence,
                        &match_result.fills,
                    )
                    .await?;
                }

                Ok((risk_deltas, fill_views))
            }
            .await;
        record_stage_result(
            &metrics,
            ENDPOINT_PLACE_ORDER,
            "write_phase",
            &market_id,
            write_phase_started,
            &write_phase_result,
        );
        let (risk_deltas, fill_views) = write_phase_result?;

        let commit_started = Instant::now();
        let commit_result = tx
            .commit()
            .await
            .context("commit place_order tx")
            .map_err(ApiError::from);
        record_stage_result(
            &metrics,
            ENDPOINT_PLACE_ORDER,
            "commit",
            &market_id,
            commit_started,
            &commit_result,
        );
        commit_result?;

        let taker_delta = risk_deltas.get(&account_id).cloned().unwrap_or_default();
        let available_minor_after = checked_add3(
            available_before,
            taker_delta.delta_pending_minor,
            -taker_delta.locked_delta_minor,
        )?;

        Ok(Json(PlaceOrderResponse {
            order_id,
            account_id,
            market_id,
            outcome,
            side: side_to_db(incoming.side).to_string(),
            order_type: order_type_to_db(incoming.order_type).to_string(),
            tif: tif_to_db(incoming.tif).to_string(),
            status: taker_status.to_string(),
            quantity_minor: incoming.quantity_minor,
            remaining_minor: taker_remaining_minor,
            locked_minor: taker_locked_minor,
            nonce: incoming.nonce.0,
            available_minor_after,
            fills: fill_views,
        }))
    }
    .await;

    metrics.record_duration(
        ENDPOINT_PLACE_ORDER,
        "total",
        &market_id_tag,
        result_class_for_result(&result),
        endpoint_started.elapsed(),
    );

    result
}

pub(crate) async fn cancel_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(order_id): Path<String>,
) -> Result<Json<CancelOrderResponse>, ApiError> {
    let metrics = state.perf_metrics.clone();
    let endpoint_started = Instant::now();

    let result_with_market: Result<(String, Json<CancelOrderResponse>), ApiError> = async {
        let principal = authenticate(&state, &headers).await?;

        // ── Write-mode dispatch ──
        match state.current_write_mode() {
            fluid_bridge::WriteMode::FluidFollower => {
                return Err(ApiError::service_unavailable_code(
                    "node is follower read-only",
                    "follower_read_only",
                ));
            }
            fluid_bridge::WriteMode::FluidLeader => {
                let (cancel_market_id, resp) =
                    cancel_order_fluid(&state, &order_id, &principal.account_id, &metrics).await?;
                return Ok((cancel_market_id, Json(resp)));
            }
            fluid_bridge::WriteMode::Fatal => {
                return Err(ApiError::service_unavailable_code(
                    "node is in fatal mode",
                    "node_fatal",
                ));
            }
            fluid_bridge::WriteMode::Legacy => {
                if state.fluid_leader_detected.load(Ordering::Relaxed) {
                    return Err(ApiError::service_unavailable_code(
                        "fluid leader detected",
                        "fluid_leader_detected",
                    ));
                }
            }
        }

        let tx_begin_started = Instant::now();
        let tx_result = state
            .db
            .begin()
            .await
            .context("begin cancel_order tx")
            .map_err(ApiError::from);
        record_stage_result(
            &metrics,
            ENDPOINT_CANCEL_ORDER,
            "tx_begin",
            "",
            tx_begin_started,
            &tx_result,
        );
        let mut tx = tx_result?;

        let snapshot_started = Instant::now();
        let order_snapshot_result: Result<Option<CancelOrderRow>, ApiError> = sqlx::query_as(
            r#"
            SELECT
              order_id,
              account_id,
              market_id,
              outcome,
              side,
              price_ticks,
              remaining_minor,
              locked_minor,
              status
            FROM orders
            WHERE order_id = $1
              AND account_id = $2
            "#,
        )
        .bind(&order_id)
        .bind(&principal.account_id)
        .fetch_optional(&mut *tx)
        .await
        .context("query order snapshot for cancel")
        .map_err(ApiError::from);
        record_stage_result(
            &metrics,
            ENDPOINT_CANCEL_ORDER,
            "load_order_snapshot",
            "",
            snapshot_started,
            &order_snapshot_result,
        );
        let order_snapshot = order_snapshot_result?;
        let Some(order_snapshot) = order_snapshot else {
            return Err(ApiError::not_found("order not found"));
        };
        let market_id = order_snapshot.market_id.clone();

        // Fast-fail non-cancellable terminal statuses before lock acquisition.
        ensure_cancel_snapshot_status(&order_snapshot.status)?;

        let market_lock_started = Instant::now();
        let market_lock_result =
            advisory_lock(&mut tx, MARKET_LOCK_NAMESPACE, &order_snapshot.market_id).await;
        record_stage_result(
            &metrics,
            ENDPOINT_CANCEL_ORDER,
            "lock_market_wait",
            &market_id,
            market_lock_started,
            &market_lock_result,
        );
        market_lock_result?;

        let account_lock_started = Instant::now();
        let account_lock_result = advisory_lock(
            &mut tx,
            state.account_lock_namespace,
            &order_snapshot.account_id,
        )
        .await;
        record_stage_result(
            &metrics,
            ENDPOINT_CANCEL_ORDER,
            "lock_account_wait",
            &market_id,
            account_lock_started,
            &account_lock_result,
        );
        account_lock_result?;

        let order_for_update_started = Instant::now();
        let order_for_update_result: Result<Option<CancelOrderRow>, ApiError> = sqlx::query_as(
            r#"
            SELECT
              order_id,
              account_id,
              market_id,
              outcome,
              side,
              price_ticks,
              remaining_minor,
              locked_minor,
              status
            FROM orders
            WHERE order_id = $1
              AND account_id = $2
            FOR UPDATE
            "#,
        )
        .bind(&order_id)
        .bind(&principal.account_id)
        .fetch_optional(&mut *tx)
        .await
        .context("query order for cancel")
        .map_err(ApiError::from);
        record_stage_result(
            &metrics,
            ENDPOINT_CANCEL_ORDER,
            "load_order_for_update",
            &market_id,
            order_for_update_started,
            &order_for_update_result,
        );
        let order = order_for_update_result?;
        let Some(order) = order else {
            return Err(ApiError::not_found("order not found"));
        };

        ensure_cancel_for_update_status(&order.status)?;

        let write_phase_started = Instant::now();
        let write_phase_result: Result<i64, ApiError> = async {
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
            .bind(&order.order_id)
            .execute(&mut *tx)
            .await
            .context("cancel order")
            .map_err(ApiError::from)?;

            let instrument: Option<(String, String)> = sqlx::query_as(
                r#"
                SELECT instrument_admin, instrument_id
                FROM account_risk_state
                WHERE account_id = $1
                "#,
            )
            .bind(&order.account_id)
            .fetch_optional(&mut *tx)
            .await
            .context("query instrument for cancel risk update")
            .map_err(ApiError::from)?;

            let Some((instrument_admin, instrument_id)) = instrument else {
                return Err(ApiError::internal(
                    "missing account_risk_state for cancellable order",
                ));
            };

            let released_locked_minor = order.locked_minor;
            if released_locked_minor != 0 {
                let mut risk_deltas = BTreeMap::new();
                risk_deltas.insert(
                    order.account_id.clone(),
                    RiskDelta {
                        delta_pending_minor: 0,
                        locked_delta_minor: -released_locked_minor,
                    },
                );
                batch_apply_risk_deltas(&mut tx, &instrument_admin, &instrument_id, &risk_deltas)
                    .await?;
            }

            let (_, event_sequence) =
                reserve_market_sequences(&mut tx, &order.market_id, 0, 1).await?;
            insert_trading_event(
                &mut tx,
                "market",
                &order.market_id,
                event_sequence,
                "OrderCancelled",
                serde_json::json!({
                  "order_id": order.order_id,
                  "account_id": order.account_id,
                  "outcome": order.outcome,
                  "side": order.side,
                  "price_ticks": order.price_ticks,
                  "remaining_minor_before_cancel": order.remaining_minor,
                  "released_locked_minor": released_locked_minor,
                }),
            )
            .await?;

            Ok(released_locked_minor)
        }
        .await;
        record_stage_result(
            &metrics,
            ENDPOINT_CANCEL_ORDER,
            "write_phase",
            &market_id,
            write_phase_started,
            &write_phase_result,
        );
        let released_locked_minor = write_phase_result?;

        let commit_started = Instant::now();
        let commit_result = tx
            .commit()
            .await
            .context("commit cancel_order tx")
            .map_err(ApiError::from);
        record_stage_result(
            &metrics,
            ENDPOINT_CANCEL_ORDER,
            "commit",
            &market_id,
            commit_started,
            &commit_result,
        );
        commit_result?;

        Ok((
            market_id,
            Json(CancelOrderResponse {
                order_id,
                status: "Cancelled".to_string(),
                released_locked_minor,
            }),
        ))
    }
    .await;

    let total_market_id = match result_with_market.as_ref() {
        Ok((market_id, _)) => market_id.as_str(),
        Err(_) => "",
    };
    metrics.record_duration(
        ENDPOINT_CANCEL_ORDER,
        "total",
        total_market_id,
        result_class_for_result(&result_with_market),
        endpoint_started.elapsed(),
    );

    match result_with_market {
        Ok((_, response)) => Ok(response),
        Err(err) => Err(err),
    }
}

fn record_stage_result<T>(
    metrics: &ApiPerfMetrics,
    endpoint: &'static str,
    stage: &'static str,
    market_id: &str,
    started: Instant,
    result: &Result<T, ApiError>,
) {
    metrics.record_duration(
        endpoint,
        stage,
        market_id,
        result_class_for_result(result),
        started.elapsed(),
    );
}

fn result_class_for_result<T>(result: &Result<T, ApiError>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(err) => result_class_from_status(err.status_code()),
    }
}

fn result_class_from_status(status: StatusCode) -> &'static str {
    if status.is_success() {
        return "ok";
    }

    match status {
        StatusCode::BAD_REQUEST => "bad_request",
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => "auth_error",
        StatusCode::NOT_FOUND => "not_found",
        StatusCode::CONFLICT => "conflict",
        StatusCode::TOO_MANY_REQUESTS => "rate_limit",
        _ if status.is_client_error() => "client_error",
        _ => "server_error",
    }
}

async fn authenticate(state: &AppState, headers: &HeaderMap) -> Result<AuthPrincipal, ApiError> {
    let auth = crate::authenticate_user_request(state, headers).await?;
    Ok(AuthPrincipal {
        account_id: auth.account_id,
    })
}

fn normalize_or_generate_order_id(order_id: Option<String>) -> String {
    match order_id {
        Some(id) if !id.trim().is_empty() => id.trim().to_string(),
        _ => Uuid::new_v4().to_string(),
    }
}

fn normalize_account_id(
    principal: &AuthPrincipal,
    body_account_id: Option<String>,
) -> Result<String, ApiError> {
    match body_account_id {
        Some(v) if !v.trim().is_empty() => {
            let account_id = v.trim().to_string();
            if account_id != principal.account_id {
                return Err(ApiError::forbidden(
                    "API key cannot submit on behalf of another account",
                ));
            }
            Ok(account_id)
        }
        _ => Ok(principal.account_id.clone()),
    }
}

fn build_incoming_order(input: &IncomingOrderInput<'_>) -> Result<IncomingOrder, ApiError> {
    let order_id = OrderId::new(input.order_id.to_string())
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    let market_id = MarketId::new(input.market_id.to_string())
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    let nonce = OrderNonce::new(input.nonce).map_err(|e| ApiError::bad_request(e.to_string()))?;
    let order_type = input.order_type.unwrap_or(OrderType::Limit);
    let (tif, price_ticks) = match order_type {
        OrderType::Limit => {
            let price_ticks = input
                .price_ticks
                .ok_or_else(|| ApiError::bad_request("price_ticks is required for limit orders"))?;
            (TimeInForce::Gtc, price_ticks)
        }
        OrderType::Market => {
            if let Some(price_ticks) = input.price_ticks {
                if price_ticks != 0 {
                    return Err(ApiError::bad_request(
                        "price_ticks must be 0 (or omitted) for market orders",
                    ));
                }
            }
            (TimeInForce::Ioc, 0)
        }
    };

    let incoming = IncomingOrder {
        order_id,
        account_id: input.account_id.to_string(),
        market_id,
        outcome: input.outcome.to_string(),
        side: input.side,
        order_type,
        tif,
        nonce,
        price_ticks,
        quantity_minor: input.quantity_minor,
        submitted_at_micros: Utc::now().timestamp_micros(),
    };
    incoming
        .validate()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;

    Ok(incoming)
}

#[cfg(test)]
fn is_order_id_unique_violation_sqlstate(
    sqlstate_code: Option<&str>,
    constraint_name: Option<&str>,
) -> bool {
    sqlstate_code == Some("23505")
        && matches!(
            constraint_name,
            Some("orders_pkey") | Some("orders_order_id_key")
        )
}

fn validate_nonce_progression(
    existing_last_nonce: Option<i64>,
    nonce: i64,
) -> Result<(), ApiError> {
    match existing_last_nonce {
        Some(last_nonce) => {
            let expected = last_nonce
                .checked_add(1)
                .ok_or_else(|| anyhow!("nonce overflow"))
                .map_err(ApiError::from)?;
            if nonce != expected {
                return Err(ApiError::bad_request(format!(
                    "nonce mismatch: expected {expected}, got {nonce}"
                )));
            }
        }
        None => {
            if nonce != 0 {
                return Err(ApiError::bad_request(
                    "first order nonce must be 0 for this account",
                ));
            }
        }
    }

    Ok(())
}

async fn load_account_context(
    tx: &mut Transaction<'_, Postgres>,
    account_id: &str,
) -> Result<AccountContextRow, ApiError> {
    let row: Option<AccountContextRow> = sqlx::query_as(
        r#"
        SELECT
          ar.owner_party,
          ar.instrument_admin,
          ar.instrument_id,
          ar.status,
          ast.cleared_cash_minor,
          COALESCE(rs.delta_pending_trades_minor, 0) AS delta_pending_trades_minor,
          COALESCE(rs.locked_open_orders_minor, 0) AS locked_open_orders_minor,
          COALESCE(ls.pending_withdrawals_reserved_minor, 0) AS pending_withdrawals_reserved_minor
        FROM account_refs ar
        JOIN account_ref_latest arl
          ON arl.account_id = ar.account_id
         AND arl.contract_id = ar.contract_id
        JOIN account_states ast
          ON ast.account_id = ar.account_id
        JOIN account_state_latest asl
          ON asl.account_id = ast.account_id
         AND asl.contract_id = ast.contract_id
        LEFT JOIN account_risk_state rs
          ON rs.account_id = ar.account_id
        LEFT JOIN account_liquidity_state ls
          ON ls.account_id = ar.account_id
        WHERE ar.account_id = $1
          AND ar.active = TRUE
          AND ast.active = TRUE
        LIMIT 1
        "#,
    )
    .bind(account_id)
    .fetch_optional(&mut **tx)
    .await
    .context("query account context")?;

    match row {
        Some(row) => Ok(row),
        None => Err(ApiError::not_found("account not found")),
    }
}

async fn validate_market_state(
    tx: &mut Transaction<'_, Postgres>,
    market_id: &str,
    outcome: &str,
    account_ctx: &AccountContextRow,
) -> Result<(), ApiError> {
    if outcome.trim().is_empty() {
        return Err(ApiError::bad_request("outcome must be non-empty"));
    }

    let row: Option<MarketContextRow> = sqlx::query_as(
        r#"
        SELECT status, outcomes, instrument_admin, instrument_id
        FROM markets
        WHERE market_id = $1
          AND active = TRUE
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(market_id)
    .fetch_optional(&mut **tx)
    .await
    .context("query market state")?;

    let Some(row) = row else {
        return Err(ApiError::not_found("market not found"));
    };

    if row.status != "Open" {
        return Err(ApiError::bad_request("market is not open"));
    }

    if !row.outcomes.0.iter().any(|x| x == outcome) {
        return Err(ApiError::bad_request("outcome not in market"));
    }

    if row.instrument_admin != account_ctx.instrument_admin
        || row.instrument_id != account_ctx.instrument_id
    {
        return Err(ApiError::bad_request(
            "account instrument does not match market instrument",
        ));
    }

    Ok(())
}

async fn enforce_and_advance_nonce(
    tx: &mut Transaction<'_, Postgres>,
    account_id: &str,
    nonce: i64,
) -> Result<(), ApiError> {
    projection_sql::enforce_and_advance_nonce(tx, account_id, nonce, &validate_nonce_progression)
        .await
}

async fn load_resting_orders(
    tx: &mut Transaction<'_, Postgres>,
    incoming: &IncomingOrder,
    instrument_admin: &str,
    instrument_id: &str,
) -> Result<Vec<RestingOrderRow>, ApiError> {
    match incoming.side {
        OrderSide::Buy => {
            if incoming.order_type == OrderType::Limit {
                let rows = sqlx::query_as(
                    r#"
                    SELECT
                      order_id,
                      account_id,
                      side,
                      outcome,
                      price_ticks,
                      remaining_minor,
                      locked_minor,
                      (EXTRACT(EPOCH FROM submitted_at) * 1000000)::BIGINT AS submitted_at_micros
                    FROM orders
                    WHERE market_id = $1
                      AND outcome = $2
                      AND side = 'Sell'
                      AND instrument_admin = $3
                      AND instrument_id = $4
                      AND status IN ('Open', 'PartiallyFilled')
                      AND remaining_minor > 0
                      AND price_ticks <= $5
                    ORDER BY price_ticks ASC, submitted_at ASC, order_id ASC
                    "#,
                )
                .bind(&incoming.market_id.0)
                .bind(&incoming.outcome)
                .bind(instrument_admin)
                .bind(instrument_id)
                .bind(incoming.price_ticks)
                .fetch_all(&mut **tx)
                .await
                .context("query resting asks for buy limit order")?;
                Ok(rows)
            } else {
                let rows = sqlx::query_as(
                    r#"
                    SELECT
                      order_id,
                      account_id,
                      side,
                      outcome,
                      price_ticks,
                      remaining_minor,
                      locked_minor,
                      (EXTRACT(EPOCH FROM submitted_at) * 1000000)::BIGINT AS submitted_at_micros
                    FROM orders
                    WHERE market_id = $1
                      AND outcome = $2
                      AND side = 'Sell'
                      AND instrument_admin = $3
                      AND instrument_id = $4
                      AND status IN ('Open', 'PartiallyFilled')
                      AND remaining_minor > 0
                    ORDER BY price_ticks ASC, submitted_at ASC, order_id ASC
                    "#,
                )
                .bind(&incoming.market_id.0)
                .bind(&incoming.outcome)
                .bind(instrument_admin)
                .bind(instrument_id)
                .fetch_all(&mut **tx)
                .await
                .context("query resting asks for buy market order")?;
                Ok(rows)
            }
        }
        OrderSide::Sell => {
            if incoming.order_type == OrderType::Limit {
                let rows = sqlx::query_as(
                    r#"
                    SELECT
                      order_id,
                      account_id,
                      side,
                      outcome,
                      price_ticks,
                      remaining_minor,
                      locked_minor,
                      (EXTRACT(EPOCH FROM submitted_at) * 1000000)::BIGINT AS submitted_at_micros
                    FROM orders
                    WHERE market_id = $1
                      AND outcome = $2
                      AND side = 'Buy'
                      AND instrument_admin = $3
                      AND instrument_id = $4
                      AND status IN ('Open', 'PartiallyFilled')
                      AND remaining_minor > 0
                      AND price_ticks >= $5
                    ORDER BY price_ticks DESC, submitted_at ASC, order_id ASC
                    "#,
                )
                .bind(&incoming.market_id.0)
                .bind(&incoming.outcome)
                .bind(instrument_admin)
                .bind(instrument_id)
                .bind(incoming.price_ticks)
                .fetch_all(&mut **tx)
                .await
                .context("query resting bids for sell limit order")?;
                Ok(rows)
            } else {
                let rows = sqlx::query_as(
                    r#"
                    SELECT
                      order_id,
                      account_id,
                      side,
                      outcome,
                      price_ticks,
                      remaining_minor,
                      locked_minor,
                      (EXTRACT(EPOCH FROM submitted_at) * 1000000)::BIGINT AS submitted_at_micros
                    FROM orders
                    WHERE market_id = $1
                      AND outcome = $2
                      AND side = 'Buy'
                      AND instrument_admin = $3
                      AND instrument_id = $4
                      AND status IN ('Open', 'PartiallyFilled')
                      AND remaining_minor > 0
                    ORDER BY price_ticks DESC, submitted_at ASC, order_id ASC
                    "#,
                )
                .bind(&incoming.market_id.0)
                .bind(&incoming.outcome)
                .bind(instrument_admin)
                .bind(instrument_id)
                .fetch_all(&mut **tx)
                .await
                .context("query resting bids for sell market order")?;
                Ok(rows)
            }
        }
    }
}

async fn reserve_market_sequences(
    tx: &mut Transaction<'_, Postgres>,
    market_id: &str,
    fill_count: usize,
    event_count: usize,
) -> Result<(i64, i64), ApiError> {
    projection_sql::reserve_market_sequences(tx, market_id, fill_count, event_count).await
}

fn assign_fill_sequences(
    fills: &mut [Fill],
    market_id: &str,
    engine_version: &str,
    first_fill_sequence: i64,
) -> Result<(), ApiError> {
    for (index, fill) in fills.iter_mut().enumerate() {
        let offset = i64::try_from(index).context("fill index conversion overflow")?;
        let sequence = first_fill_sequence
            .checked_add(offset)
            .ok_or_else(|| anyhow!("fill sequence overflow"))?;
        fill.sequence = sequence;
        fill.fill_id = format!("fill:{engine_version}:{market_id}:{sequence:020}");
    }

    Ok(())
}

async fn batch_update_maker_orders(
    tx: &mut Transaction<'_, Postgres>,
    updates: &[MakerOrderUpdate],
) -> Result<(), ApiError> {
    let projection_updates = updates
        .iter()
        .map(|update| projection_sql::MakerOrderUpdate {
            order_id: update.order_id.clone(),
            new_remaining_minor: update.new_remaining_minor,
            new_locked_minor: update.new_locked_minor,
            new_status: update.new_status,
        })
        .collect::<Vec<_>>();
    projection_sql::batch_update_maker_orders(tx, &projection_updates).await
}

async fn batch_insert_fills(
    tx: &mut Transaction<'_, Postgres>,
    instrument_admin: &str,
    instrument_id: &str,
    market_id: &str,
    fills: &[Fill],
) -> Result<(), ApiError> {
    projection_sql::batch_insert_fills(tx, instrument_admin, instrument_id, market_id, fills).await
}

fn accumulate_position_delta(
    position_deltas: &mut BTreeMap<(String, String), PositionDelta>,
    account_id: &str,
    outcome: &str,
    side: OrderSide,
    quantity_minor: i64,
    fill_price_ticks: i64,
) -> Result<(), ApiError> {
    let signed_qty = signed_quantity_delta(side, quantity_minor)?;
    let key = (account_id.to_string(), outcome.to_string());

    let entry = position_deltas.entry(key).or_insert(PositionDelta {
        net_quantity_delta_minor: 0,
        last_price_ticks: fill_price_ticks,
    });
    entry.net_quantity_delta_minor = entry
        .net_quantity_delta_minor
        .checked_add(signed_qty)
        .ok_or_else(|| ApiError::bad_request("position delta overflow"))?;
    entry.last_price_ticks = fill_price_ticks;

    Ok(())
}

async fn batch_upsert_position_deltas(
    tx: &mut Transaction<'_, Postgres>,
    market_id: &str,
    position_deltas: &BTreeMap<(String, String), PositionDelta>,
) -> Result<(), ApiError> {
    let projection_deltas = position_deltas
        .iter()
        .map(|((account_id, outcome), delta)| {
            (
                (account_id.clone(), outcome.clone()),
                projection_sql::PositionDelta {
                    net_quantity_delta_minor: delta.net_quantity_delta_minor,
                    last_price_ticks: delta.last_price_ticks,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    projection_sql::batch_upsert_position_deltas(tx, market_id, &projection_deltas).await
}

async fn batch_insert_fill_recorded_events(
    tx: &mut Transaction<'_, Postgres>,
    market_id: &str,
    first_sequence: i64,
    fills: &[Fill],
) -> Result<(), ApiError> {
    projection_sql::batch_insert_fill_recorded_events(tx, market_id, first_sequence, fills).await
}

async fn insert_taker_order(
    tx: &mut Transaction<'_, Postgres>,
    args: InsertTakerOrderArgs<'_>,
) -> Result<(), ApiError> {
    projection_sql::insert_taker_order(
        tx,
        args.incoming,
        &projection_sql::InsertTakerOrderParams {
            owner_party: &args.account_ctx.owner_party,
            instrument_admin: &args.account_ctx.instrument_admin,
            instrument_id: &args.account_ctx.instrument_id,
            side: side_to_db(args.incoming.side),
            order_type: order_type_to_db(args.incoming.order_type),
            tif: tif_to_db(args.incoming.tif),
            engine_version: args.engine_version,
            remaining_minor: args.remaining_minor,
            locked_minor: args.locked_minor,
            status: args.status,
            wal_offset: args.wal_offset,
        },
    )
    .await
}

async fn batch_apply_risk_deltas(
    tx: &mut Transaction<'_, Postgres>,
    instrument_admin: &str,
    instrument_id: &str,
    risk_deltas: &BTreeMap<String, RiskDelta>,
) -> Result<(), ApiError> {
    let projection_deltas = risk_deltas
        .iter()
        .map(|(account_id, delta)| {
            (
                account_id.clone(),
                projection_sql::RiskDelta {
                    delta_pending_minor: delta.delta_pending_minor,
                    locked_delta_minor: delta.locked_delta_minor,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    projection_sql::batch_apply_risk_deltas(tx, instrument_admin, instrument_id, &projection_deltas)
        .await
}

async fn sync_projection_nonce(
    tx: &mut Transaction<'_, Postgres>,
    account_id: &str,
    nonce: i64,
) -> Result<(), ApiError> {
    projection_sql::sync_projection_nonce(tx, account_id, nonce).await
}

async fn apply_projection_risk_deltas(
    tx: &mut Transaction<'_, Postgres>,
    instrument_admin: &str,
    instrument_id: &str,
    risk_deltas: &BTreeMap<String, RiskDelta>,
) -> Result<(), ApiError> {
    let projection_deltas = risk_deltas
        .iter()
        .map(|(account_id, delta)| {
            (
                account_id.clone(),
                projection_sql::RiskDelta {
                    delta_pending_minor: delta.delta_pending_minor,
                    locked_delta_minor: delta.locked_delta_minor,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    projection_sql::apply_projection_risk_deltas(
        tx,
        instrument_admin,
        instrument_id,
        &projection_deltas,
    )
    .await
}

async fn insert_trading_event(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_type: &str,
    aggregate_id: &str,
    sequence: i64,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<(), ApiError> {
    projection_sql::insert_trading_event(
        tx,
        aggregate_type,
        aggregate_id,
        sequence,
        event_type,
        payload,
    )
    .await
}

async fn advisory_lock(
    tx: &mut Transaction<'_, Postgres>,
    namespace: i32,
    key: &str,
) -> Result<(), ApiError> {
    projection_sql::advisory_lock(tx, namespace, key).await
}

fn clamp_limit(limit: Option<i64>) -> i64 {
    let raw = limit.unwrap_or(DEFAULT_QUERY_LIMIT);
    raw.clamp(1, MAX_QUERY_LIMIT)
}

fn parse_api_keys(raw: &str) -> Result<HashMap<String, String>, anyhow::Error> {
    let mut out = HashMap::new();

    for entry in raw.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }

        let (api_key, account_id) = entry.split_once(':').ok_or_else(|| {
            anyhow!("invalid PEBBLE_API_KEYS entry (expected key:account_id): {entry}")
        })?;

        let api_key = api_key.trim();
        let account_id = account_id.trim();

        if api_key.is_empty() || account_id.is_empty() {
            return Err(anyhow!(
                "invalid PEBBLE_API_KEYS entry (empty key/account_id): {entry}"
            ));
        }

        if out
            .insert(api_key.to_string(), account_id.to_string())
            .is_some()
        {
            return Err(anyhow!("duplicate API key in PEBBLE_API_KEYS"));
        }
    }

    Ok(out)
}

fn parse_admin_keys(raw: &str) -> Result<HashSet<String>, anyhow::Error> {
    let mut out = HashSet::new();

    for entry in raw.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }

        if !out.insert(entry.to_string()) {
            return Err(anyhow!("duplicate API key in PEBBLE_ADMIN_KEYS"));
        }
    }

    Ok(out)
}

fn side_to_db(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "Buy",
        OrderSide::Sell => "Sell",
    }
}

fn order_type_to_db(order_type: OrderType) -> &'static str {
    match order_type {
        OrderType::Limit => "Limit",
        OrderType::Market => "Market",
    }
}

fn tif_to_db(tif: TimeInForce) -> &'static str {
    match tif {
        TimeInForce::Gtc => "GTC",
        TimeInForce::Ioc => "IOC",
    }
}

fn parse_db_side(raw: &str) -> Result<OrderSide, ApiError> {
    match raw {
        "Buy" => Ok(OrderSide::Buy),
        "Sell" => Ok(OrderSide::Sell),
        _ => Err(ApiError::internal("invalid side value in db")),
    }
}

fn order_status(quantity_minor: i64, remaining_minor: i64) -> &'static str {
    if remaining_minor == quantity_minor {
        "Open"
    } else if remaining_minor == 0 {
        "Filled"
    } else {
        "PartiallyFilled"
    }
}

fn is_order_cancellable_status(status: &str) -> bool {
    matches!(status, "Open" | "PartiallyFilled")
}

fn cancel_status_rejected_error(status: &str) -> ApiError {
    ApiError::bad_request(format!("order cannot be cancelled from status {status}"))
}

fn ensure_cancel_snapshot_status(status: &str) -> Result<(), ApiError> {
    if is_order_cancellable_status(status) {
        Ok(())
    } else {
        Err(cancel_status_rejected_error(status))
    }
}

fn ensure_cancel_for_update_status(status: &str) -> Result<(), ApiError> {
    if is_order_cancellable_status(status) {
        Ok(())
    } else {
        Err(cancel_status_rejected_error(status))
    }
}

fn lock_required_minor(
    side: OrderSide,
    price_ticks: i64,
    quantity_minor: i64,
) -> Result<i64, ApiError> {
    if quantity_minor < 0 {
        return Err(ApiError::bad_request("quantity_minor must be >= 0"));
    }

    let lock = match side {
        OrderSide::Buy => price_ticks
            .checked_mul(quantity_minor)
            .ok_or_else(|| ApiError::bad_request("buy lock overflow"))?,
        OrderSide::Sell => quantity_minor,
    };

    Ok(lock)
}

fn signed_quantity_delta(side: OrderSide, quantity_minor: i64) -> Result<i64, ApiError> {
    match side {
        OrderSide::Buy => Ok(quantity_minor),
        OrderSide::Sell => quantity_minor
            .checked_neg()
            .ok_or_else(|| ApiError::bad_request("signed quantity overflow")),
    }
}

fn cash_delta_minor(
    side: OrderSide,
    price_ticks: i64,
    quantity_minor: i64,
) -> Result<i64, ApiError> {
    let gross = price_ticks
        .checked_mul(quantity_minor)
        .ok_or_else(|| ApiError::bad_request("cash delta overflow"))?;

    match side {
        OrderSide::Buy => gross
            .checked_neg()
            .ok_or_else(|| ApiError::bad_request("cash delta overflow")),
        OrderSide::Sell => Ok(gross),
    }
}

fn market_buy_required_minor(fills: &[Fill]) -> Result<i64, ApiError> {
    fills.iter().try_fold(0_i64, |sum, fill| {
        let line_cost = fill
            .taker_price_ticks
            .checked_mul(fill.quantity_minor)
            .ok_or_else(|| ApiError::bad_request("market fill cost overflow"))?;
        sum.checked_add(line_cost)
            .ok_or_else(|| ApiError::bad_request("market fill cost overflow"))
    })
}

fn checked_add3(a: i64, b: i64, c: i64) -> Result<i64, ApiError> {
    let ab = a
        .checked_add(b)
        .ok_or_else(|| ApiError::bad_request("integer overflow"))?;
    ab.checked_add(c)
        .ok_or_else(|| ApiError::bad_request("integer overflow"))
}

// ── Fluid path: place_order ──

async fn place_order_fluid(
    state: &AppState,
    incoming: IncomingOrder,
    metrics: &ApiPerfMetrics,
) -> Result<PlaceOrderResponse, ApiError> {
    let market_id_str = incoming.market_id.0.clone();

    // Pre-mutex gate: check leader connection
    if !state.leader_connection_healthy.load(Ordering::Relaxed) {
        return Err(ApiError::service_unavailable_code(
            "fluid leader connection unhealthy",
            "leader_unhealthy",
        ));
    }

    let fluid = state
        .fluid
        .as_ref()
        .ok_or_else(|| ApiError::internal("fluid state not initialized"))?;
    let _resync_barrier = fluid.resync_barrier().read().await;

    // Post-mutex gate: re-check after potentially waiting on the mutex
    if !state.leader_connection_healthy.load(Ordering::Relaxed) {
        return Err(ApiError::service_unavailable_code(
            "fluid leader connection unhealthy",
            "leader_unhealthy",
        ));
    }
    if !fluid.is_healthy() {
        return Err(ApiError::service_unavailable_code(
            "fluid engine unhealthy",
            "engine_unhealthy",
        ));
    }

    let required_lock =
        lock_required_minor(incoming.side, incoming.price_ticks, incoming.quantity_minor)?;

    let market = fluid
        .get_market(&incoming.market_id)
        .await
        .ok_or_else(|| ApiError::not_found("market not found"))?;
    let mut market_guard = lock_market(&market, &market_id_str).await?;

    // Re-check after lock acquisition.
    if !state.leader_connection_healthy.load(Ordering::Relaxed) {
        return Err(ApiError::service_unavailable_code(
            "fluid leader connection unhealthy",
            "leader_unhealthy",
        ));
    }
    if !fluid.is_healthy() {
        return Err(ApiError::service_unavailable_code(
            "fluid engine unhealthy",
            "engine_unhealthy",
        ));
    }

    let account = fluid
        .risk_handle()
        .get_account(&incoming.account_id)
        .await
        .map_err(|err| fluid_bridge::fluid_error_to_api_error(&err))?;
    let reserve_started = Instant::now();
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
        .map_err(|err| fluid_bridge::fluid_error_to_api_error(&err));
    record_stage_result(
        metrics,
        ENDPOINT_PLACE_ORDER,
        "fluid_reserve",
        &market_id_str,
        reserve_started,
        &reserve,
    );
    let reserve = reserve?;
    let mut reserve_guard = RiskReservationGuard::new(
        fluid.risk_handle(),
        &incoming.account_id,
        required_lock,
        fluid.health_handle(),
    );

    let compute_started = Instant::now();
    let order_interner_read = fluid.order_interner().read().await;
    let account_interner_read = fluid.account_interner().read().await;
    let compute_result = market_guard
        .compute_place_order(
            &incoming,
            reserve.available_before,
            fluid.engine_version(),
            &order_interner_read,
            &account_interner_read,
        )
        .map_err(|e| fluid_bridge::fluid_error_to_api_error(&e));
    drop(account_interner_read);
    drop(order_interner_read);
    record_stage_result(
        metrics,
        ENDPOINT_PLACE_ORDER,
        "fluid_compute",
        &market_id_str,
        compute_started,
        &compute_result,
    );
    let transition = match compute_result {
        Ok(transition) => transition,
        Err(err) => {
            if let Err(abort_err) = reserve_guard.abort_if_active().await {
                fluid.mark_unhealthy(&format!(
                    "fluid abort after compute failure failed: {abort_err:?}"
                ));
                return Err(fluid_bridge::fluid_error_to_api_error(&abort_err));
            }
            return Err(err);
        }
    };

    let wal_handle = state
        .wal_handle
        .as_ref()
        .ok_or_else(|| ApiError::internal("wal handle not initialized"))?;
    let wal_command = WalCommand::PlaceOrder(incoming.clone());
    let wal_append_started = Instant::now();
    let wal_append_result: Result<WalOffset, ApiError> = wal_handle
        .append(wal_command.clone())
        .await
        .map_err(|err| fluid_bridge::fluid_error_to_api_error(&err));
    record_stage_result(
        metrics,
        ENDPOINT_PLACE_ORDER,
        "fluid_wal_append",
        &market_id_str,
        wal_append_started,
        &wal_append_result,
    );
    let wal_offset = match wal_append_result {
        Ok(offset) => offset,
        Err(err) => {
            if let Err(abort_err) = reserve_guard.abort_if_active().await {
                enter_fatal_mode(
                    state,
                    fluid,
                    &format!("abort after WAL append failure failed: {abort_err:?}"),
                );
                return Err(fluid_bridge::fluid_error_to_api_error(&abort_err));
            }
            enter_fatal_mode(state, fluid, &format!("fluid WAL append failed: {err:?}"));
            return Err(err);
        }
    };

    // Stage C post-durability semantics:
    // once WAL append succeeds, return committed response regardless of apply failures.
    reserve_guard.disarm();

    if let Err(err) = fluid
        .risk_handle()
        .commit(&incoming.account_id, transition.risk_transition.clone())
        .await
    {
        fluid.mark_unhealthy(&format!(
            "place order commit failed after WAL fsync: {err:?}"
        ));
    } else {
        let mut order_interner = fluid.order_interner().write().await;
        let mut account_interner = fluid.account_interner().write().await;
        if let Err(err) =
            market_guard.apply_place_order(&transition, &mut order_interner, &mut account_interner)
        {
            drop(account_interner);
            drop(order_interner);
            fluid.mark_unhealthy(&format!(
                "apply_place_order failed after WAL fsync: {err:?}"
            ));
        } else {
            drop(account_interner);
            drop(order_interner);
            fluid
                .apply_place_index_updates(&incoming.market_id, &transition)
                .await;
        }
    }

    if let Some(projection_tx) = state.projection_tx.as_ref() {
        let send_result = projection_tx.try_send(wal_projector::ProjectionPayload {
            wal_offset,
            command_bytes: serialize_projection_command_bytes(&wal_command),
            data: wal_projector::ProjectionData::PlaceOrder {
                incoming: incoming.clone(),
                transition: Box::new(transition.clone()),
                instrument_admin: reserve.instrument_admin.clone(),
                instrument_id: reserve.instrument_id.clone(),
            },
        });
        if let Err(err) = send_result {
            tracing::warn!(error = ?err, "projector transition cache send failed");
        }
    } else {
        tracing::warn!("projector transition cache channel not initialized");
    }

    let unknown_context = UnknownOutcomeContext {
        account_id: incoming.account_id.clone(),
        nonce: incoming.nonce.0,
        wal_offset,
    };
    enforce_post_fsync_durability(state, fluid, wal_offset, Some(&unknown_context)).await?;

    let fill_views = build_fill_views_from_transition(&transition);

    Ok(PlaceOrderResponse {
        order_id: incoming.order_id.0,
        account_id: incoming.account_id,
        market_id: incoming.market_id.0,
        outcome: incoming.outcome,
        side: side_to_db(incoming.side).to_string(),
        order_type: order_type_to_db(incoming.order_type).to_string(),
        tif: tif_to_db(incoming.tif).to_string(),
        status: transition.taker_status.as_db_str().to_string(),
        quantity_minor: incoming.quantity_minor,
        remaining_minor: transition.taker_remaining,
        locked_minor: transition.taker_locked,
        nonce: incoming.nonce.0,
        available_minor_after: transition.available_minor_after,
        fills: fill_views,
    })
}

fn build_fill_views_from_transition(
    transition: &pebble_fluid::transition::PlaceOrderTransition,
) -> Vec<OrderFillView> {
    transition
        .fills
        .iter()
        .map(|fill| OrderFillView {
            fill_id: fill.fill_id.clone(),
            fill_sequence: fill.sequence,
            maker_order_id: fill.maker_order_id.0.clone(),
            taker_order_id: fill.taker_order_id.0.clone(),
            outcome: fill.taker_outcome.clone(),
            price_ticks: fill.taker_price_ticks,
            quantity_minor: fill.quantity_minor,
            engine_version: fill.engine_version.clone(),
        })
        .collect()
}

fn serialize_projection_command_bytes(command: &WalCommand) -> Option<Vec<u8>> {
    match rmp_serde::to_vec(command) {
        Ok(bytes) => Some(bytes),
        Err(err) => {
            tracing::error!(
                error = ?err,
                "failed to serialize WAL command for projector cache validation"
            );
            None
        }
    }
}

async fn enforce_post_fsync_durability(
    state: &AppState,
    fluid: &Arc<pebble_fluid::engine::ExchangeEngine>,
    wal_offset: WalOffset,
    unknown_context: Option<&UnknownOutcomeContext>,
) -> Result<(), ApiError> {
    if state.durability_config.replication_mode == crate::FluidReplicationMode::Async {
        state.consecutive_ack_timeouts.store(0, Ordering::Relaxed);
        clear_pg_publish_unreachable_since(state);
        return Ok(());
    }

    if state.durability_config.min_ack_count > 0 {
        let Some(broadcaster) = state.replication_broadcaster.as_ref() else {
            return handle_post_fsync_failure(
                state,
                fluid,
                "replication broadcaster unavailable",
                PostFsyncFailureReason::AckTimeout,
                unknown_context,
            );
        };
        let acked = wait_for_durable_acks(
            broadcaster,
            wal_offset,
            state.durability_config.min_ack_count,
            state.durability_config.ack_timeout,
        )
        .await?;
        if !acked {
            return handle_post_fsync_failure(
                state,
                fluid,
                "durable ack timeout",
                PostFsyncFailureReason::AckTimeout,
                unknown_context,
            );
        }
    }

    state.consecutive_ack_timeouts.store(0, Ordering::Relaxed);
    match publish_committed_offset_fenced(state, wal_offset).await {
        Ok(()) => {
            clear_pg_publish_unreachable_since(state);
            Ok(())
        }
        Err(err) => {
            let (reason, log_reason) = match err {
                PublishCommittedOffsetError::PgUnavailable(sql_err) => (
                    PostFsyncFailureReason::PgPublishUnavailable,
                    format!("committed offset publish failed: {sql_err}"),
                ),
                PublishCommittedOffsetError::EpochSuperseded => (
                    PostFsyncFailureReason::EpochSuperseded,
                    "committed offset publish fenced out by newer epoch".to_string(),
                ),
                PublishCommittedOffsetError::MissingNodeId => (
                    PostFsyncFailureReason::PgPublishUnavailable,
                    "committed offset publish missing fluid node id".to_string(),
                ),
                PublishCommittedOffsetError::EpochOverflow => (
                    PostFsyncFailureReason::PgPublishUnavailable,
                    "committed offset publish leader epoch overflow".to_string(),
                ),
                PublishCommittedOffsetError::OffsetOverflow => (
                    PostFsyncFailureReason::PgPublishUnavailable,
                    "committed offset publish wal offset overflow".to_string(),
                ),
            };
            handle_post_fsync_failure(state, fluid, &log_reason, reason, unknown_context)
        }
    }
}

async fn wait_for_durable_acks(
    broadcaster: &ReplicationBroadcaster,
    target_offset: WalOffset,
    min_ack_count: usize,
    timeout: Duration,
) -> Result<bool, ApiError> {
    if min_ack_count == 0 {
        return Ok(true);
    }
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| ApiError::internal("ack wait timeout overflow"))?;
    loop {
        let ack_count = broadcaster
            .durable_ack_count_at_or_above(target_offset)
            .map_err(|err| ApiError::internal(format!("durable ack count failed: {err}")))?;
        if ack_count >= min_ack_count {
            return Ok(true);
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Ok(false);
        }
        let remaining = deadline.saturating_duration_since(now);
        let sleep_for = remaining.min(Duration::from_millis(5));
        tokio::time::sleep(sleep_for).await;
    }
}

async fn publish_committed_offset_fenced(
    state: &AppState,
    wal_offset: WalOffset,
) -> Result<(), PublishCommittedOffsetError> {
    let Some(node_id) = state.fluid_node_id.as_ref() else {
        return Err(PublishCommittedOffsetError::MissingNodeId);
    };
    let epoch = state.leader_epoch.load(Ordering::Relaxed);
    let epoch_i64 = i64::try_from(epoch).map_err(|_| PublishCommittedOffsetError::EpochOverflow)?;
    let offset_i64 =
        i64::try_from(wal_offset.0).map_err(|_| PublishCommittedOffsetError::OffsetOverflow)?;

    let result = sqlx::query(
        r#"
        UPDATE fluid_leader_state
        SET committed_offset = $1, last_heartbeat_at = NOW()
        WHERE singleton_key = TRUE
          AND leader_epoch = $2
          AND leader_node_id = $3
        "#,
    )
    .bind(offset_i64)
    .bind(epoch_i64)
    .bind(node_id)
    .execute(&state.db)
    .await
    .context("publish committed offset")
    .map_err(PublishCommittedOffsetError::PgUnavailable)?;
    if result.rows_affected() != 1 {
        return Err(PublishCommittedOffsetError::EpochSuperseded);
    }
    state
        .committed_offset
        .store(wal_offset.0, Ordering::Relaxed);
    Ok(())
}

#[derive(Clone, Debug)]
struct UnknownOutcomeContext {
    account_id: String,
    nonce: i64,
    wal_offset: WalOffset,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PostFsyncFailureReason {
    AckTimeout,
    PgPublishUnavailable,
    EpochSuperseded,
}

#[derive(Debug)]
enum PublishCommittedOffsetError {
    MissingNodeId,
    EpochOverflow,
    OffsetOverflow,
    PgUnavailable(anyhow::Error),
    EpochSuperseded,
}

fn handle_post_fsync_failure(
    state: &AppState,
    fluid: &Arc<pebble_fluid::engine::ExchangeEngine>,
    reason: &str,
    failure_reason: PostFsyncFailureReason,
    unknown_context: Option<&UnknownOutcomeContext>,
) -> Result<(), ApiError> {
    match state.durability_config.failure_policy {
        crate::PostFsyncFailurePolicy::DegradeToAsync => match failure_reason {
            PostFsyncFailureReason::AckTimeout => {
                tracing::warn!(reason = %reason, "fluid semisync degraded to async on ack timeout");
                Ok(())
            }
            PostFsyncFailureReason::PgPublishUnavailable => {
                let elapsed = note_pg_publish_unreachable_since(state).unwrap_or(Duration::ZERO);
                tracing::warn!(
                    reason = %reason,
                    elapsed_secs = elapsed.as_secs_f64(),
                    demote_after_secs = state.durability_config.pg_unreachable_demote_after.as_secs_f64(),
                    "fluid semisync degraded to async on PG publish failure"
                );
                if elapsed >= state.durability_config.pg_unreachable_demote_after {
                    demote_leader(state, fluid, "degrade mode PG unreachable demotion");
                }
                Ok(())
            }
            PostFsyncFailureReason::EpochSuperseded => {
                demote_leader(state, fluid, "epoch superseded during committed publish");
                Err(order_outcome_unknown_error(Some(state), unknown_context))
            }
        },
        crate::PostFsyncFailurePolicy::Strict => {
            if failure_reason == PostFsyncFailureReason::AckTimeout {
                let consecutive = state
                    .consecutive_ack_timeouts
                    .fetch_add(1, Ordering::Relaxed)
                    .saturating_add(1);
                if consecutive >= state.durability_config.ack_timeout_demote_count {
                    demote_leader(state, fluid, "strict semisync ack timeout demotion");
                }
            } else {
                demote_leader(state, fluid, "strict semisync committed publish failure");
            }
            Err(order_outcome_unknown_error(Some(state), unknown_context))
        }
    }
}

fn clear_pg_publish_unreachable_since(state: &AppState) {
    match state.pg_publish_unreachable_since.lock() {
        Ok(mut guard) => {
            *guard = None;
        }
        Err(err) => {
            tracing::error!(error = %err, "pg publish unreachable state lock poisoned");
        }
    }
}

fn note_pg_publish_unreachable_since(state: &AppState) -> Option<Duration> {
    match state.pg_publish_unreachable_since.lock() {
        Ok(mut guard) => {
            let now = Instant::now();
            let started = guard.get_or_insert(now);
            Some(now.saturating_duration_since(*started))
        }
        Err(err) => {
            tracing::error!(error = %err, "pg publish unreachable state lock poisoned");
            None
        }
    }
}

fn demote_leader(
    state: &AppState,
    fluid: &Arc<pebble_fluid::engine::ExchangeEngine>,
    reason: &str,
) {
    clear_pg_publish_unreachable_since(state);
    state
        .leader_connection_healthy
        .store(false, Ordering::Relaxed);
    fluid.mark_unhealthy(reason);
    fluid_bridge::store_write_mode(&state.write_mode, fluid_bridge::WriteMode::FluidFollower);
}

fn enter_fatal_mode(
    state: &AppState,
    fluid: &Arc<pebble_fluid::engine::ExchangeEngine>,
    reason: &str,
) {
    state
        .leader_connection_healthy
        .store(false, Ordering::Relaxed);
    fluid.mark_unhealthy(reason);
    fluid_bridge::store_write_mode(&state.write_mode, fluid_bridge::WriteMode::Fatal);
}

fn order_outcome_unknown_error(
    state: Option<&AppState>,
    unknown_context: Option<&UnknownOutcomeContext>,
) -> ApiError {
    let mut details = serde_json::Map::new();
    details.insert(
        "message".to_string(),
        serde_json::Value::String(
            "Order may have been executed. Reconcile before retrying.".to_string(),
        ),
    );

    if let Some(ctx) = unknown_context {
        let unknown_at = Utc::now();
        let unknown_at_unix_ms = unknown_at.timestamp_millis();
        let unknown_at_iso = unknown_at.to_rfc3339();
        details.insert(
            "account_id".to_string(),
            serde_json::Value::String(ctx.account_id.clone()),
        );
        details.insert(
            "nonce".to_string(),
            serde_json::Value::Number(ctx.nonce.into()),
        );
        details.insert(
            "wal_offset".to_string(),
            serde_json::Value::Number(ctx.wal_offset.0.into()),
        );
        details.insert(
            "unknown_at".to_string(),
            serde_json::Value::String(unknown_at_iso),
        );

        let token_payload = crate::ReconcileTokenPayload {
            account_id: ctx.account_id.clone(),
            nonce: ctx.nonce,
            wal_offset: ctx.wal_offset.0,
            unknown_at_unix_ms,
        };
        if let Some(state) = state {
            match state.sign_reconcile_token(&token_payload) {
                Ok(token) => {
                    let reconcile_url = format!(
                        "GET /v1/accounts/{}/orders/reconcile?nonce={}&wal_offset={}&token={}",
                        ctx.account_id, ctx.nonce, ctx.wal_offset.0, token
                    );
                    details.insert("token".to_string(), serde_json::Value::String(token));
                    details.insert(
                        "reconciliation".to_string(),
                        serde_json::Value::String(reconcile_url),
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err.message,
                        "failed to sign reconciliation token for unknown outcome"
                    );
                }
            }
        }
    }

    ApiError::conflict_code("OrderOutcomeUnknown", "order_outcome_unknown")
        .with_details(serde_json::Value::Object(details))
}

pub(crate) async fn persist_place_order_fluid_in_tx(
    state: &AppState,
    tx: &mut Transaction<'_, Postgres>,
    incoming: &IncomingOrder,
    transition: &pebble_fluid::transition::PlaceOrderTransition,
    wal_offset: WalOffset,
    instrument_admin: &str,
    instrument_id: &str,
) -> Result<Vec<OrderFillView>, ApiError> {
    // Advisory locks (belt-and-suspenders)
    advisory_lock(tx, MARKET_LOCK_NAMESPACE, &incoming.market_id.0).await?;
    advisory_lock(tx, state.account_lock_namespace, &incoming.account_id).await?;

    // Projection path is WAL-authoritative. Do not reject already-accepted WAL commands
    // due to external status drift; only require enough context to materialize rows.
    let owner_party: Option<String> = sqlx::query_scalar(
        r#"
        SELECT ar.owner_party
        FROM account_refs ar
        JOIN account_ref_latest arl ON arl.contract_id = ar.contract_id
        WHERE arl.account_id = $1
        "#,
    )
    .bind(&incoming.account_id)
    .fetch_optional(&mut **tx)
    .await
    .context("fluid persist: query account context")
    .map_err(ApiError::from)?;
    let owner_party = owner_party.ok_or_else(|| ApiError::not_found("account not found"))?;

    let market_row: Option<(String, String)> = sqlx::query_as(
        r#"
        SELECT instrument_admin, instrument_id
        FROM markets
        WHERE market_id = $1
        ORDER BY created_at DESC
        LIMIT 1
        "#,
    )
    .bind(&incoming.market_id.0)
    .fetch_optional(&mut **tx)
    .await
    .context("fluid persist: query market context")
    .map_err(ApiError::from)?;
    if let Some((market_instrument_admin, market_instrument_id)) = market_row {
        if market_instrument_admin != instrument_admin || market_instrument_id != instrument_id {
            tracing::warn!(
                market_id = %incoming.market_id.0,
                wal_offset = wal_offset.0,
                market_instrument_admin,
                market_instrument_id,
                projection_instrument_admin = instrument_admin,
                projection_instrument_id = instrument_id,
                "fluid projector: market/account instrument drift while applying WAL place order"
            );
        }
    }

    crate::projection_sql::record_wal_projection_offset(tx, wal_offset).await?;

    // Keep nonce monotonic without enforcing strict contiguous progression.
    sync_projection_nonce(tx, &incoming.account_id, incoming.nonce.0).await?;

    // Insert taker order
    let account_ctx = AccountContextRow {
        owner_party,
        instrument_admin: instrument_admin.to_string(),
        instrument_id: instrument_id.to_string(),
        status: String::new(),
        cleared_cash_minor: 0,
        delta_pending_trades_minor: 0,
        locked_open_orders_minor: 0,
        pending_withdrawals_reserved_minor: 0,
    };
    insert_taker_order(
        tx,
        InsertTakerOrderArgs {
            incoming,
            account_ctx: &account_ctx,
            engine_version: &state.m3_cfg.engine_version,
            remaining_minor: transition.taker_remaining,
            locked_minor: transition.taker_locked,
            status: transition.taker_status.as_db_str(),
            wal_offset: Some(wal_offset),
        },
    )
    .await?;

    // Reserve market sequences in DB (keeps DB counters in sync)
    let fill_count = transition.fills.len();
    let event_count = fill_count
        .checked_add(1)
        .ok_or_else(|| anyhow!("market event count overflow"))?;
    let (first_fill_sequence, first_market_event_sequence) =
        reserve_market_sequences(tx, &incoming.market_id.0, fill_count, event_count).await?;

    // Verify sequence consistency: fills already have sequences from compute
    if let Some(first_fill) = transition.fills.first() {
        if first_fill_sequence != first_fill.sequence {
            return Err(ApiError::internal(format!(
                "fill sequence mismatch: DB={first_fill_sequence}, in-memory={}",
                first_fill.sequence
            )));
        }
    }

    // Build maker order updates for DB
    let mut maker_order_updates = Vec::new();
    for (order_id, new_remaining, new_locked) in &transition.book_transition.maker_remaining_updates
    {
        maker_order_updates.push(MakerOrderUpdate {
            order_id: order_id.0.clone(),
            new_remaining_minor: *new_remaining,
            new_locked_minor: *new_locked,
            new_status: "PartiallyFilled",
        });
    }
    for order_id in &transition.book_transition.maker_removals {
        maker_order_updates.push(MakerOrderUpdate {
            order_id: order_id.0.clone(),
            new_remaining_minor: 0,
            new_locked_minor: 0,
            new_status: "Filled",
        });
    }
    if !maker_order_updates.is_empty() {
        batch_update_maker_orders(tx, &maker_order_updates).await?;
    }

    // Insert fills
    if !transition.fills.is_empty() {
        batch_insert_fills(
            tx,
            instrument_admin,
            instrument_id,
            &incoming.market_id.0,
            &transition.fills,
        )
        .await?;
    }

    // Build fill views and position deltas
    let mut fill_views = Vec::with_capacity(transition.fills.len());
    let mut position_deltas = BTreeMap::<(String, String), PositionDelta>::new();
    for fill in &transition.fills {
        accumulate_position_delta(
            &mut position_deltas,
            &fill.maker_account_id,
            &fill.maker_outcome,
            fill.maker_side,
            fill.quantity_minor,
            fill.maker_price_ticks,
        )?;
        accumulate_position_delta(
            &mut position_deltas,
            &fill.taker_account_id,
            &fill.taker_outcome,
            fill.taker_side,
            fill.quantity_minor,
            fill.taker_price_ticks,
        )?;

        fill_views.push(OrderFillView {
            fill_id: fill.fill_id.clone(),
            fill_sequence: fill.sequence,
            maker_order_id: fill.maker_order_id.0.clone(),
            taker_order_id: fill.taker_order_id.0.clone(),
            outcome: fill.taker_outcome.clone(),
            price_ticks: fill.taker_price_ticks,
            quantity_minor: fill.quantity_minor,
            engine_version: fill.engine_version.clone(),
        });
    }

    if !position_deltas.is_empty() {
        batch_upsert_position_deltas(tx, &incoming.market_id.0, &position_deltas).await?;
    }

    // Build risk deltas for DB from transition
    let mut risk_deltas = BTreeMap::<String, RiskDelta>::new();
    for delta in &transition.risk_transition.deltas {
        risk_deltas.insert(
            delta.account_id.clone(),
            RiskDelta {
                delta_pending_minor: delta.delta_pending_minor,
                locked_delta_minor: delta.locked_delta_minor,
            },
        );
    }
    if !risk_deltas.is_empty() {
        apply_projection_risk_deltas(tx, instrument_admin, instrument_id, &risk_deltas).await?;
    }

    // Trading events
    let total_filled_qty: i64 = transition
        .fills
        .iter()
        .try_fold(0i64, |acc, f| acc.checked_add(f.quantity_minor))
        .ok_or_else(|| ApiError::internal("fill quantity sum overflow"))?;
    let unfilled_minor = incoming
        .quantity_minor
        .checked_sub(total_filled_qty)
        .ok_or_else(|| ApiError::internal("unfilled minor underflow"))?;

    insert_trading_event(
        tx,
        "market",
        &incoming.market_id.0,
        first_market_event_sequence,
        "OrderAccepted",
        serde_json::json!({
          "order_id": incoming.order_id.0,
          "account_id": incoming.account_id,
          "outcome": incoming.outcome,
          "side": side_to_db(incoming.side),
          "order_type": order_type_to_db(incoming.order_type),
          "tif": tif_to_db(incoming.tif),
          "nonce": incoming.nonce.0,
          "price_ticks": incoming.price_ticks,
          "quantity_minor": incoming.quantity_minor,
          "remaining_minor": transition.taker_remaining,
          "unfilled_minor": unfilled_minor,
          "locked_minor": transition.taker_locked,
          "status": transition.taker_status.as_db_str(),
          "fills": fill_views.len(),
        }),
    )
    .await?;

    if !transition.fills.is_empty() {
        let first_fill_event_sequence = first_market_event_sequence
            .checked_add(1)
            .ok_or_else(|| anyhow!("market event sequence overflow"))?;
        batch_insert_fill_recorded_events(
            tx,
            &incoming.market_id.0,
            first_fill_event_sequence,
            &transition.fills,
        )
        .await?;
    }

    // Post-persist balance validation (safety net for stale in-memory available)
    let include_withdrawal_reserves = state.order_available_includes_withdrawal_reserves;
    let db_available_after: Option<i64> = sqlx::query_scalar(
        r#"
        SELECT
          ast.cleared_cash_minor
          + COALESCE(rs.delta_pending_trades_minor, 0)
          - COALESCE(rs.locked_open_orders_minor, 0)
          - CASE WHEN $2 THEN COALESCE(ls.pending_withdrawals_reserved_minor, 0) ELSE 0 END
        FROM account_state_latest asl
        JOIN account_states ast ON ast.contract_id = asl.contract_id
        LEFT JOIN account_risk_state rs ON rs.account_id = $1
        LEFT JOIN account_liquidity_state ls ON ls.account_id = $1
        WHERE asl.account_id = $1
        "#,
    )
    .bind(&incoming.account_id)
    .bind(include_withdrawal_reserves)
    .fetch_optional(&mut **tx)
    .await
    .context("post-persist balance validation")
    .map_err(ApiError::from)?;

    if let Some(available) = db_available_after {
        if available < 0 {
            tracing::warn!(
                account_id = %incoming.account_id,
                market_id = %incoming.market_id.0,
                wal_offset = wal_offset.0,
                db_available_after = available,
                "fluid projector: post-persist available balance is negative"
            );
        }
    }

    Ok(fill_views)
}

// ── Fluid path: cancel_order ──

async fn cancel_order_fluid(
    state: &AppState,
    order_id: &str,
    account_id: &str,
    metrics: &ApiPerfMetrics,
) -> Result<(String, CancelOrderResponse), ApiError> {
    // Pre-mutex gate: check leader connection
    if !state.leader_connection_healthy.load(Ordering::Relaxed) {
        return Err(ApiError::service_unavailable_code(
            "fluid leader connection unhealthy",
            "leader_unhealthy",
        ));
    }

    let fluid = state
        .fluid
        .as_ref()
        .ok_or_else(|| ApiError::internal("fluid state not initialized"))?;
    let _resync_barrier = fluid.resync_barrier().read().await;

    // Post-barrier gate: re-check after potentially waiting on the barrier.
    if !state.leader_connection_healthy.load(Ordering::Relaxed) {
        return Err(ApiError::service_unavailable_code(
            "fluid leader connection unhealthy",
            "leader_unhealthy",
        ));
    }
    if !fluid.is_healthy() {
        return Err(ApiError::service_unavailable_code(
            "fluid engine unhealthy",
            "engine_unhealthy",
        ));
    }

    let oid = OrderId(order_id.to_string());
    let market_id = {
        fluid
            .lookup_market_for_order(&oid)
            .await
            .ok_or_else(|| ApiError::not_found("order not found"))?
    };
    let market_id_str = market_id.0.clone();
    let market = fluid
        .get_market(&market_id)
        .await
        .ok_or_else(|| ApiError::not_found("order not found"))?;
    let mut market_guard = lock_market(&market, &market_id_str).await?;

    if !state.leader_connection_healthy.load(Ordering::Relaxed) {
        return Err(ApiError::service_unavailable_code(
            "fluid leader connection unhealthy",
            "leader_unhealthy",
        ));
    }
    if !fluid.is_healthy() {
        return Err(ApiError::service_unavailable_code(
            "fluid engine unhealthy",
            "engine_unhealthy",
        ));
    }

    // Compute cancel transition (read-only)
    let compute_started = Instant::now();
    let order_interner_read = fluid.order_interner().read().await;
    let account_interner_read = fluid.account_interner().read().await;
    let transition = market_guard
        .compute_cancel_order(
            &market_id,
            &oid,
            account_id,
            &order_interner_read,
            &account_interner_read,
        )
        .map_err(|e| fluid_bridge::fluid_error_to_api_error(&e));
    drop(account_interner_read);
    drop(order_interner_read);
    record_stage_result(
        metrics,
        ENDPOINT_CANCEL_ORDER,
        "fluid_compute",
        &market_id_str,
        compute_started,
        &transition,
    );
    let transition = transition?;

    let account = fluid
        .risk_handle()
        .get_account(account_id)
        .await
        .map_err(|e| fluid_bridge::fluid_error_to_api_error(&e))?;
    let instrument_admin = account.instrument_admin;
    let instrument_id = account.instrument_id;

    let wal_handle = state
        .wal_handle
        .as_ref()
        .ok_or_else(|| ApiError::internal("wal handle not initialized"))?;
    let wal_command = WalCommand::CancelOrder {
        order_id: transition.order_id.clone(),
        market_id: transition.market_id.clone(),
        account_id: account_id.to_string(),
    };
    let wal_append_started = Instant::now();
    let append_result: Result<WalOffset, ApiError> = wal_handle
        .append(wal_command.clone())
        .await
        .map_err(|err| fluid_bridge::fluid_error_to_api_error(&err));
    record_stage_result(
        metrics,
        ENDPOINT_CANCEL_ORDER,
        "fluid_wal_append",
        &market_id_str,
        wal_append_started,
        &append_result,
    );
    let wal_offset = match append_result {
        Ok(offset) => offset,
        Err(err) => {
            enter_fatal_mode(
                state,
                fluid,
                &format!("fluid WAL append failed on cancel: {err:?}"),
            );
            return Err(err);
        }
    };

    let order_interner_read = fluid.order_interner().read().await;
    if let Err(err) = market_guard.apply_cancel_order(&transition, &order_interner_read) {
        drop(order_interner_read);
        fluid.mark_unhealthy(&format!(
            "apply_cancel_order failed after WAL fsync: {err:?}"
        ));
    } else {
        drop(order_interner_read);
        fluid.apply_cancel_index_update(&oid).await;

        if transition.released_locked_minor != 0 {
            let risk_transition = pebble_fluid::transition::RiskTransition {
                deltas: vec![pebble_fluid::transition::AccountRiskDelta {
                    account_id: account_id.to_string(),
                    delta_pending_minor: 0,
                    locked_delta_minor: -transition.released_locked_minor,
                }],
                nonce_advance: None,
            };
            if let Err(err) = fluid.risk_handle().apply_delta(risk_transition).await {
                fluid.mark_unhealthy(&format!("apply_delta failed after WAL fsync: {err:?}"));
            }
        }
    }

    if let Some(projection_tx) = state.projection_tx.as_ref() {
        let send_result = projection_tx.try_send(wal_projector::ProjectionPayload {
            wal_offset,
            command_bytes: serialize_projection_command_bytes(&wal_command),
            data: wal_projector::ProjectionData::CancelOrder {
                transition: transition.clone(),
                instrument_admin,
                instrument_id,
            },
        });
        if let Err(err) = send_result {
            tracing::warn!(error = ?err, "projector transition cache send failed");
        }
    } else {
        tracing::warn!("projector transition cache channel not initialized");
    }

    enforce_post_fsync_durability(state, fluid, wal_offset, None).await?;

    Ok((
        market_id_str,
        CancelOrderResponse {
            order_id: order_id.to_string(),
            status: "Cancelled".to_string(),
            released_locked_minor: transition.released_locked_minor,
        },
    ))
}

pub(crate) async fn persist_cancel_order_fluid_in_tx(
    state: &AppState,
    tx: &mut Transaction<'_, Postgres>,
    transition: &pebble_fluid::transition::CancelOrderTransition,
    wal_offset: WalOffset,
    instrument_admin: &str,
    instrument_id: &str,
) -> Result<(), ApiError> {
    // Advisory locks
    advisory_lock(tx, MARKET_LOCK_NAMESPACE, &transition.market_id.0).await?;
    advisory_lock(tx, state.account_lock_namespace, &transition.account_id).await?;

    // Cancel the order in DB
    let result = sqlx::query(
        r#"
        UPDATE orders
        SET
          status = 'Cancelled',
          remaining_minor = 0,
          locked_minor = 0,
          updated_at = now()
        WHERE order_id = $1
          AND account_id = $2
          AND status IN ('Open', 'PartiallyFilled')
        "#,
    )
    .bind(&transition.order_id.0)
    .bind(&transition.account_id)
    .execute(&mut **tx)
    .await
    .context("cancel order in fluid path")
    .map_err(ApiError::from)?;

    let order_updated = result.rows_affected() > 0;
    if !order_updated {
        tracing::warn!(
            order_id = %transition.order_id.0,
            account_id = %transition.account_id,
            market_id = %transition.market_id.0,
            wal_offset = wal_offset.0,
            "fluid projector: cancel update matched 0 open rows; treating as idempotent drift"
        );
    }

    crate::projection_sql::record_wal_projection_offset(tx, wal_offset).await?;

    // Apply risk delta (release locked)
    if order_updated && transition.released_locked_minor != 0 {
        let mut risk_deltas = BTreeMap::new();
        risk_deltas.insert(
            transition.account_id.clone(),
            RiskDelta {
                delta_pending_minor: 0,
                locked_delta_minor: -transition.released_locked_minor,
            },
        );
        apply_projection_risk_deltas(tx, instrument_admin, instrument_id, &risk_deltas).await?;
    } else if !order_updated && transition.released_locked_minor != 0 {
        tracing::warn!(
            order_id = %transition.order_id.0,
            released_locked_minor = transition.released_locked_minor,
            wal_offset = wal_offset.0,
            "fluid projector: skipped cancel risk release due to drifted terminal order state"
        );
    }

    // Reserve event sequence and insert cancel event
    if order_updated {
        let (_, event_sequence) =
            reserve_market_sequences(tx, &transition.market_id.0, 0, 1).await?;
        insert_trading_event(
            tx,
            "market",
            &transition.market_id.0,
            event_sequence,
            "OrderCancelled",
            serde_json::json!({
              "order_id": transition.order_id.0,
              "account_id": transition.account_id,
              "released_locked_minor": transition.released_locked_minor,
            }),
        )
        .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn simulate_cancel_status_flow(
        snapshot_status: &str,
        for_update_status: &str,
    ) -> (usize, Result<(), ApiError>) {
        let mut lock_phases = 0usize;
        let result = (|| {
            ensure_cancel_snapshot_status(snapshot_status)?;
            lock_phases += 2;
            ensure_cancel_for_update_status(for_update_status)?;
            Ok(())
        })();
        (lock_phases, result)
    }

    #[test]
    fn parse_api_keys_accepts_valid_entries() {
        let got = parse_api_keys("k1:acc-1, k2:acc-2").unwrap();
        assert_eq!(got.get("k1"), Some(&"acc-1".to_string()));
        assert_eq!(got.get("k2"), Some(&"acc-2".to_string()));
    }

    #[test]
    fn parse_api_keys_rejects_invalid_entries() {
        assert!(parse_api_keys("missing-delimiter").is_err());
        assert!(parse_api_keys("k1:").is_err());
        assert!(parse_api_keys("k1:acc-1,k1:acc-2").is_err());
    }

    #[test]
    fn parse_admin_keys_accepts_valid_entries() {
        let got = parse_admin_keys("adm-1, adm-2").unwrap();
        assert!(got.contains("adm-1"));
        assert!(got.contains("adm-2"));
    }

    #[test]
    fn parse_admin_keys_rejects_duplicates() {
        assert!(parse_admin_keys("adm-1,adm-1").is_err());
    }

    #[test]
    fn clamp_limit_bounds_values() {
        assert_eq!(clamp_limit(None), DEFAULT_QUERY_LIMIT);
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(999_999)), MAX_QUERY_LIMIT);
    }

    #[test]
    fn lock_required_minor_for_buy_and_sell() {
        assert_eq!(lock_required_minor(OrderSide::Buy, 7, 3).unwrap(), 21);
        assert_eq!(lock_required_minor(OrderSide::Sell, 7, 3).unwrap(), 3);
    }

    #[test]
    fn cash_delta_minor_signs_with_side() {
        assert_eq!(cash_delta_minor(OrderSide::Buy, 5, 2).unwrap(), -10);
        assert_eq!(cash_delta_minor(OrderSide::Sell, 5, 2).unwrap(), 10);
    }

    #[test]
    fn build_incoming_order_defaults_to_limit_gtc() {
        let incoming = build_incoming_order(&IncomingOrderInput {
            order_id: "ord-1",
            account_id: "acc-1",
            market_id: "mkt-1",
            outcome: "YES",
            side: OrderSide::Buy,
            order_type: None,
            nonce: 0,
            price_ticks: Some(42),
            quantity_minor: 3,
        })
        .unwrap();

        assert_eq!(incoming.order_type, OrderType::Limit);
        assert_eq!(incoming.tif, TimeInForce::Gtc);
        assert_eq!(incoming.price_ticks, 42);
    }

    #[test]
    fn build_incoming_order_market_is_ioc_with_zero_price() {
        let incoming = build_incoming_order(&IncomingOrderInput {
            order_id: "ord-2",
            account_id: "acc-1",
            market_id: "mkt-1",
            outcome: "YES",
            side: OrderSide::Buy,
            order_type: Some(OrderType::Market),
            nonce: 1,
            price_ticks: None,
            quantity_minor: 3,
        })
        .unwrap();

        assert_eq!(incoming.order_type, OrderType::Market);
        assert_eq!(incoming.tif, TimeInForce::Ioc);
        assert_eq!(incoming.price_ticks, 0);
    }

    #[test]
    fn build_incoming_order_market_rejects_non_zero_price() {
        let err = build_incoming_order(&IncomingOrderInput {
            order_id: "ord-3",
            account_id: "acc-1",
            market_id: "mkt-1",
            outcome: "YES",
            side: OrderSide::Buy,
            order_type: Some(OrderType::Market),
            nonce: 2,
            price_ticks: Some(1),
            quantity_minor: 3,
        })
        .unwrap_err();

        assert!(err.message.contains("price_ticks must be 0"));
    }

    #[test]
    fn market_buy_required_minor_sums_fill_cost() {
        let fills = vec![
            Fill {
                fill_id: "fill-1".to_string(),
                maker_order_id: OrderId("maker-1".to_string()),
                taker_order_id: OrderId("taker-1".to_string()),
                maker_account_id: "acc-maker-1".to_string(),
                taker_account_id: "acc-taker-1".to_string(),
                outcome: "YES".to_string(),
                price_ticks: 12,
                quantity_minor: 2,
                sequence: 0,
                engine_version: "v".to_string(),
                canonical_price_ticks: 12,
                maker_outcome: "YES".to_string(),
                maker_side: OrderSide::Sell,
                maker_price_ticks: 12,
                taker_outcome: "YES".to_string(),
                taker_side: OrderSide::Buy,
                taker_price_ticks: 12,
            },
            Fill {
                fill_id: "fill-2".to_string(),
                maker_order_id: OrderId("maker-2".to_string()),
                taker_order_id: OrderId("taker-1".to_string()),
                maker_account_id: "acc-maker-2".to_string(),
                taker_account_id: "acc-taker-1".to_string(),
                outcome: "YES".to_string(),
                price_ticks: 20,
                quantity_minor: 1,
                sequence: 1,
                engine_version: "v".to_string(),
                canonical_price_ticks: 20,
                maker_outcome: "YES".to_string(),
                maker_side: OrderSide::Sell,
                maker_price_ticks: 20,
                taker_outcome: "YES".to_string(),
                taker_side: OrderSide::Buy,
                taker_price_ticks: 20,
            },
        ];

        assert_eq!(market_buy_required_minor(&fills).unwrap(), 44);
    }

    #[test]
    fn market_buy_required_minor_uses_taker_price_ticks() {
        let fills = vec![Fill {
            fill_id: "fill-1".to_string(),
            maker_order_id: OrderId("maker-1".to_string()),
            taker_order_id: OrderId("taker-1".to_string()),
            maker_account_id: "acc-maker-1".to_string(),
            taker_account_id: "acc-taker-1".to_string(),
            outcome: "NO".to_string(),
            price_ticks: 2,
            quantity_minor: 3,
            sequence: 0,
            engine_version: "v".to_string(),
            canonical_price_ticks: 2,
            maker_outcome: "YES".to_string(),
            maker_side: OrderSide::Buy,
            maker_price_ticks: 2,
            taker_outcome: "NO".to_string(),
            taker_side: OrderSide::Buy,
            taker_price_ticks: 98,
        }];

        assert_eq!(market_buy_required_minor(&fills).unwrap(), 294);
    }

    #[test]
    fn is_order_cancellable_status_accepts_open_statuses() {
        assert!(is_order_cancellable_status("Open"));
        assert!(is_order_cancellable_status("PartiallyFilled"));
    }

    #[test]
    fn is_order_cancellable_status_rejects_terminal_statuses() {
        assert!(!is_order_cancellable_status("Filled"));
        assert!(!is_order_cancellable_status("Cancelled"));
        assert!(!is_order_cancellable_status("Rejected"));
        assert!(!is_order_cancellable_status("Unknown"));
    }

    #[test]
    fn order_id_unique_violation_detects_orders_primary_key_constraint() {
        assert!(is_order_id_unique_violation_sqlstate(
            Some("23505"),
            Some("orders_pkey")
        ));
    }

    #[test]
    fn order_id_unique_violation_detects_orders_order_id_unique_constraint() {
        assert!(is_order_id_unique_violation_sqlstate(
            Some("23505"),
            Some("orders_order_id_key")
        ));
    }

    #[test]
    fn order_id_unique_violation_rejects_unknown_constraint() {
        assert!(!is_order_id_unique_violation_sqlstate(
            Some("23505"),
            Some("fills_pkey")
        ));
    }

    #[test]
    fn order_id_unique_violation_rejects_non_unique_sqlstate() {
        assert!(!is_order_id_unique_violation_sqlstate(
            Some("23503"),
            Some("orders_pkey")
        ));
    }

    #[test]
    fn validate_nonce_progression_accepts_first_nonce_zero() {
        assert!(validate_nonce_progression(None, 0).is_ok());
    }

    #[test]
    fn validate_nonce_progression_rejects_first_nonce_non_zero() {
        let err = validate_nonce_progression(None, 1).unwrap_err();
        assert_eq!(err.status_code(), StatusCode::BAD_REQUEST);
        assert!(err.message.contains("first order nonce must be 0"));
    }

    #[test]
    fn validate_nonce_progression_accepts_incrementing_nonce() {
        assert!(validate_nonce_progression(Some(7), 8).is_ok());
    }

    #[test]
    fn validate_nonce_progression_rejects_nonce_mismatch() {
        let err = validate_nonce_progression(Some(7), 9).unwrap_err();
        assert_eq!(err.status_code(), StatusCode::BAD_REQUEST);
        assert!(err.message.contains("expected 8, got 9"));
    }

    #[test]
    fn validate_nonce_progression_surfaces_overflow_as_internal_error() {
        let err = validate_nonce_progression(Some(i64::MAX), i64::MAX).unwrap_err();
        assert_eq!(err.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn cancel_snapshot_status_rejects_terminal_statuses_before_lock_phase() {
        for status in ["Filled", "Cancelled", "Rejected", "Unknown"] {
            let (lock_phases, result) = simulate_cancel_status_flow(status, "Open");
            assert_eq!(lock_phases, 0);
            let err = result.unwrap_err();
            assert_eq!(err.status_code(), StatusCode::BAD_REQUEST);
            assert!(err.message.contains(status));
        }
    }

    #[test]
    fn cancel_status_flow_allows_cancellable_statuses_through_write_phase() {
        for snapshot_status in ["Open", "PartiallyFilled"] {
            for for_update_status in ["Open", "PartiallyFilled"] {
                let (lock_phases, result) =
                    simulate_cancel_status_flow(snapshot_status, for_update_status);
                assert_eq!(lock_phases, 2);
                assert!(result.is_ok());
            }
        }
    }

    #[test]
    fn cancel_status_flow_rejects_terminal_status_after_lock_phase() {
        let (lock_phases, result) = simulate_cancel_status_flow("Open", "Filled");
        assert_eq!(lock_phases, 2);
        let err = result.unwrap_err();
        assert_eq!(err.status_code(), StatusCode::BAD_REQUEST);
        assert!(err.message.contains("Filled"));
    }

    #[test]
    fn build_fill_views_maps_taker_perspective_fields() {
        let transition = pebble_fluid::transition::PlaceOrderTransition {
            market_id: MarketId("m1".to_string()),
            book_transition: pebble_fluid::transition::BookTransition::default(),
            risk_transition: pebble_fluid::transition::RiskTransition::default(),
            fills: vec![Fill {
                fill_id: "fill-1".to_string(),
                maker_order_id: OrderId("maker-1".to_string()),
                taker_order_id: OrderId("taker-1".to_string()),
                maker_account_id: "maker-account".to_string(),
                taker_account_id: "taker-account".to_string(),
                outcome: "NO".to_string(),
                price_ticks: 42,
                quantity_minor: 7,
                sequence: 3,
                engine_version: "engine-v".to_string(),
                canonical_price_ticks: 58,
                maker_outcome: "YES".to_string(),
                maker_side: OrderSide::Sell,
                maker_price_ticks: 58,
                taker_outcome: "NO".to_string(),
                taker_side: OrderSide::Buy,
                taker_price_ticks: 42,
            }],
            taker_status: pebble_fluid::events::OrderStatus::Open,
            taker_remaining: 0,
            taker_locked: 0,
            available_minor_after: 0,
            maker_updates: Vec::new(),
            next_fill_sequence: 4,
            next_event_sequence: 5,
        };

        let views = build_fill_views_from_transition(&transition);
        assert_eq!(views.len(), 1);
        let view = &views[0];
        assert_eq!(view.fill_id, "fill-1");
        assert_eq!(view.fill_sequence, 3);
        assert_eq!(view.maker_order_id, "maker-1");
        assert_eq!(view.taker_order_id, "taker-1");
        assert_eq!(view.outcome, "NO");
        assert_eq!(view.price_ticks, 42);
        assert_eq!(view.quantity_minor, 7);
        assert_eq!(view.engine_version, "engine-v");
    }

    #[test]
    fn test_account_fill_history_perspective() {
        let row = FillPerspectiveRow {
            fill_id: "fill-1".to_string(),
            fill_sequence: 11,
            market_id: "m1".to_string(),
            maker_order_id: "maker-1".to_string(),
            taker_order_id: "taker-1".to_string(),
            quantity_minor: 5,
            engine_version: "v4".to_string(),
            matched_at: chrono::Utc::now(),
            clearing_epoch: None,
            maker_account_id: "alice".to_string(),
            taker_account_id: "bob".to_string(),
            maker_outcome: "YES".to_string(),
            taker_outcome: "YES".to_string(),
            maker_price_ticks: 4_500,
            taker_price_ticks: 4_500,
        };

        let maker_view = project_fill_row_for_account(&row, "alice").unwrap();
        assert_eq!(maker_view.perspective_role, "Maker");
        assert_eq!(maker_view.outcome, "YES");
        assert_eq!(maker_view.price_ticks, 4_500);

        let taker_view = project_fill_row_for_account(&row, "bob").unwrap();
        assert_eq!(taker_view.perspective_role, "Taker");
        assert_eq!(taker_view.outcome, "YES");
        assert_eq!(taker_view.price_ticks, 4_500);
    }

    #[test]
    fn test_account_fill_history_cross_outcome_perspective() {
        let row = FillPerspectiveRow {
            fill_id: "fill-2".to_string(),
            fill_sequence: 12,
            market_id: "m1".to_string(),
            maker_order_id: "maker-buy-no".to_string(),
            taker_order_id: "taker-buy-yes".to_string(),
            quantity_minor: 3,
            engine_version: "v4".to_string(),
            matched_at: chrono::Utc::now(),
            clearing_epoch: Some(7),
            maker_account_id: "maker".to_string(),
            taker_account_id: "taker".to_string(),
            maker_outcome: "NO".to_string(),
            taker_outcome: "YES".to_string(),
            maker_price_ticks: 4_000,
            taker_price_ticks: 6_000,
        };

        let maker_view = project_fill_row_for_account(&row, "maker").unwrap();
        assert_eq!(maker_view.perspective_role, "Maker");
        assert_eq!(maker_view.outcome, "NO");
        assert_eq!(maker_view.price_ticks, 4_000);

        let taker_view = project_fill_row_for_account(&row, "taker").unwrap();
        assert_eq!(taker_view.perspective_role, "Taker");
        assert_eq!(taker_view.outcome, "YES");
        assert_eq!(taker_view.price_ticks, 6_000);
    }

    #[test]
    fn test_cross_outcome_position_tracking_e2e() {
        let mut position_deltas = BTreeMap::<(String, String), PositionDelta>::new();

        accumulate_position_delta(
            &mut position_deltas,
            "maker",
            "NO",
            OrderSide::Buy,
            3,
            4_000,
        )
        .unwrap();
        accumulate_position_delta(
            &mut position_deltas,
            "taker",
            "YES",
            OrderSide::Buy,
            3,
            6_000,
        )
        .unwrap();

        let maker = position_deltas
            .get(&(String::from("maker"), String::from("NO")))
            .unwrap();
        assert_eq!(maker.net_quantity_delta_minor, 3);
        assert_eq!(maker.last_price_ticks, 4_000);

        let taker = position_deltas
            .get(&(String::from("taker"), String::from("YES")))
            .unwrap();
        assert_eq!(taker.net_quantity_delta_minor, 3);
        assert_eq!(taker.last_price_ticks, 6_000);
    }

    #[tokio::test]
    async fn wait_for_durable_acks_succeeds_when_minimum_met() {
        let broadcaster =
            pebble_fluid::replication::ReplicationBroadcaster::with_default_capacity(WalOffset(0))
                .expect("broadcaster");
        let registration = broadcaster
            .register_follower("follower-a".to_string())
            .expect("register");
        drop(registration);
        broadcaster
            .update_durable_applied("follower-a", WalOffset(5))
            .expect("ack");

        let acked = wait_for_durable_acks(&broadcaster, WalOffset(5), 1, Duration::from_millis(20))
            .await
            .expect("wait");
        assert!(acked);
    }

    #[tokio::test]
    async fn wait_for_durable_acks_times_out_without_enough_followers() {
        let broadcaster =
            pebble_fluid::replication::ReplicationBroadcaster::with_default_capacity(WalOffset(0))
                .expect("broadcaster");

        let acked = wait_for_durable_acks(&broadcaster, WalOffset(5), 1, Duration::from_millis(20))
            .await
            .expect("wait");
        assert!(!acked);
    }

    #[test]
    fn order_outcome_unknown_error_shape() {
        let err = order_outcome_unknown_error(None, None);
        assert_eq!(err.status_code(), StatusCode::CONFLICT);
    }
}
