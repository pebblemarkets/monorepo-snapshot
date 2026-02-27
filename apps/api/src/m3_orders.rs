use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Instant,
};

use anyhow::{anyhow, Context as _};
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use chrono::Utc;
use pebble_trading::{
    match_order_presorted, Fill, IncomingOrder, MarketId, OrderBookView, OrderId, OrderNonce,
    OrderSide, OrderType, RestingOrder, TimeInForce,
};
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, QueryBuilder, Transaction};
use uuid::Uuid;

use crate::{perf_metrics::ApiPerfMetrics, ApiError, AppState};

const MARKET_LOCK_NAMESPACE: i32 = 12;
const DEFAULT_QUERY_LIMIT: i64 = 100;
const MAX_QUERY_LIMIT: i64 = 500;
const ENDPOINT_PLACE_ORDER: &str = "orders.place";
const ENDPOINT_CANCEL_ORDER: &str = "orders.cancel";

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
struct FillRow {
    fill_id: String,
    fill_sequence: i64,
    market_id: String,
    outcome: String,
    maker_order_id: String,
    taker_order_id: String,
    price_ticks: i64,
    quantity_minor: i64,
    engine_version: String,
    matched_at: chrono::DateTime<chrono::Utc>,
    clearing_epoch: Option<i64>,
    perspective_role: String,
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

pub(crate) async fn list_orders(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListOrdersQuery>,
) -> Result<Json<Vec<OrderRowView>>, ApiError> {
    let principal = authenticate(&state, &headers)?;
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
    let principal = authenticate(&state, &headers)?;
    let limit = clamp_limit(query.limit);

    let rows: Vec<FillRow> = sqlx::query_as(
        r#"
        SELECT
          fill_id,
          fill_sequence,
          market_id,
          outcome,
          maker_order_id,
          taker_order_id,
          price_ticks,
          quantity_minor,
          engine_version,
          matched_at,
          clearing_epoch,
          CASE
            WHEN maker_account_id = $1 THEN 'Maker'
            ELSE 'Taker'
          END AS perspective_role
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

    let out = rows
        .into_iter()
        .map(|row| FillRowView {
            fill_id: row.fill_id,
            fill_sequence: row.fill_sequence,
            market_id: row.market_id,
            outcome: row.outcome,
            maker_order_id: row.maker_order_id,
            taker_order_id: row.taker_order_id,
            perspective_role: row.perspective_role,
            price_ticks: row.price_ticks,
            quantity_minor: row.quantity_minor,
            engine_version: row.engine_version,
            matched_at: row.matched_at,
            clearing_epoch: row.clearing_epoch,
        })
        .collect();

    Ok(Json(out))
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
        let principal = authenticate(&state, &headers)?;

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
                    &incoming,
                    &account_ctx,
                    &state.m3_cfg.engine_version,
                    taker_remaining_minor,
                    taker_locked_minor,
                    taker_status,
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
                    let Some(maker_row) = maker_by_order_id.get(&fill.maker_order_id.0) else {
                        return Err(ApiError::internal(
                            "missing maker row while persisting fill",
                        ));
                    };
                    let maker_side = parse_db_side(&maker_row.side)?;

                    accumulate_position_delta(
                        &mut position_deltas,
                        &fill.maker_account_id,
                        &fill.outcome,
                        maker_side,
                        fill.quantity_minor,
                        fill.price_ticks,
                    )?;
                    accumulate_position_delta(
                        &mut position_deltas,
                        &fill.taker_account_id,
                        &fill.outcome,
                        incoming.side,
                        fill.quantity_minor,
                        fill.price_ticks,
                    )?;

                    let maker_cash_delta =
                        cash_delta_minor(maker_side, fill.price_ticks, fill.quantity_minor)?;
                    let taker_cash_delta =
                        cash_delta_minor(incoming.side, fill.price_ticks, fill.quantity_minor)?;

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
                        outcome: fill.outcome.clone(),
                        price_ticks: fill.price_ticks,
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
        let principal = authenticate(&state, &headers)?;

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

fn authenticate(state: &AppState, headers: &HeaderMap) -> Result<AuthPrincipal, ApiError> {
    if state.auth_keys.api_key_count() == 0 {
        return Err(ApiError::unauthorized("no API keys configured"));
    }

    let key = extract_user_api_key_for_orders(headers)?;

    let Some(account_id) = state.auth_keys.account_id_for_api_key(&key) else {
        return Err(ApiError::unauthorized("invalid API key"));
    };

    Ok(AuthPrincipal { account_id })
}

fn extract_user_api_key_for_orders(headers: &HeaderMap) -> Result<String, ApiError> {
    if let Some(raw_admin_key) = headers.get("x-admin-key") {
        let admin_key = raw_admin_key
            .to_str()
            .map_err(|_| ApiError::unauthorized("x-admin-key must be valid ASCII"))?
            .trim();
        if admin_key.is_empty() {
            return Err(ApiError::unauthorized("x-admin-key must be non-empty"));
        }
        return Err(ApiError::forbidden(
            "x-admin-key is not allowed on order endpoints",
        ));
    }

    let key = headers
        .get("x-api-key")
        .ok_or_else(|| ApiError::unauthorized("missing x-api-key header"))?
        .to_str()
        .map_err(|_| ApiError::unauthorized("x-api-key must be valid ASCII"))?
        .trim()
        .to_string();

    if key.is_empty() {
        return Err(ApiError::unauthorized("x-api-key must be non-empty"));
    }

    Ok(key)
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

fn is_order_id_unique_violation(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => {
            is_order_id_unique_violation_sqlstate(db_err.code().as_deref(), db_err.constraint())
        }
        _ => false,
    }
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
    let advanced_nonce: Option<i64> = sqlx::query_scalar(
        r#"
        WITH updated AS (
          UPDATE account_nonces
          SET
            last_nonce = $2,
            updated_at = now()
          WHERE account_id = $1
            AND last_nonce = $2 - 1
          RETURNING last_nonce
        ),
        inserted AS (
          INSERT INTO account_nonces (account_id, last_nonce, updated_at)
          SELECT
            $1,
            $2,
            now()
          WHERE $2 = 0
            AND NOT EXISTS (
              SELECT 1
              FROM account_nonces
              WHERE account_id = $1
            )
          ON CONFLICT (account_id) DO NOTHING
          RETURNING last_nonce
        )
        SELECT last_nonce
        FROM updated
        UNION ALL
        SELECT last_nonce
        FROM inserted
        LIMIT 1
        "#,
    )
    .bind(account_id)
    .bind(nonce)
    .fetch_optional(&mut **tx)
    .await
    .context("advance nonce")?;

    if advanced_nonce.is_some() {
        return Ok(());
    }

    let existing_last_nonce: Option<i64> = sqlx::query_scalar(
        r#"
        SELECT last_nonce
        FROM account_nonces
        WHERE account_id = $1
        "#,
    )
    .bind(account_id)
    .fetch_optional(&mut **tx)
    .await
    .context("query nonce for validation")?;

    validate_nonce_progression(existing_last_nonce, nonce)?;
    Err(ApiError::internal(
        "nonce progression did not update account nonce state",
    ))
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
    let fill_increment =
        i64::try_from(fill_count).context("fill sequence reservation conversion overflow")?;
    let event_increment =
        i64::try_from(event_count).context("event sequence reservation conversion overflow")?;

    let reserved: (i64, i64) = sqlx::query_as(
        r#"
        INSERT INTO market_sequences (
          market_id,
          next_fill_sequence,
          next_event_sequence,
          updated_at
        )
        VALUES ($1, $2, $3, now())
        ON CONFLICT (market_id) DO UPDATE
        SET
          next_fill_sequence = market_sequences.next_fill_sequence + EXCLUDED.next_fill_sequence,
          next_event_sequence = market_sequences.next_event_sequence + EXCLUDED.next_event_sequence,
          updated_at = now()
        RETURNING
          next_fill_sequence - $2 AS first_fill_sequence,
          next_event_sequence - $3 AS first_event_sequence
        "#,
    )
    .bind(market_id)
    .bind(fill_increment)
    .bind(event_increment)
    .fetch_one(&mut **tx)
    .await
    .context("reserve market sequences")?;

    Ok(reserved)
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
    if updates.is_empty() {
        return Ok(());
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        "UPDATE orders AS o \
         SET remaining_minor = v.remaining_minor, \
             locked_minor = v.locked_minor, \
             status = v.status, \
             updated_at = now() \
         FROM (",
    );
    builder.push_values(updates, |mut row, update| {
        row.push_bind(&update.order_id)
            .push_bind(update.new_remaining_minor)
            .push_bind(update.new_locked_minor)
            .push_bind(update.new_status);
    });
    builder.push(
        ") AS v(order_id, remaining_minor, locked_minor, status) \
         WHERE o.order_id = v.order_id",
    );

    let result = builder
        .build()
        .execute(&mut **tx)
        .await
        .context("batch update maker orders")?;

    let expected =
        u64::try_from(updates.len()).context("maker update count conversion overflow")?;
    if result.rows_affected() != expected {
        return Err(ApiError::internal(
            "maker order batch update row count mismatch",
        ));
    }

    Ok(())
}

async fn batch_insert_fills(
    tx: &mut Transaction<'_, Postgres>,
    instrument_admin: &str,
    instrument_id: &str,
    market_id: &str,
    fills: &[Fill],
) -> Result<(), ApiError> {
    if fills.is_empty() {
        return Ok(());
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        "INSERT INTO fills (\
          fill_id,\
          fill_sequence,\
          instrument_admin,\
          instrument_id,\
          market_id,\
          outcome,\
          maker_order_id,\
          taker_order_id,\
          maker_account_id,\
          taker_account_id,\
          price_ticks,\
          quantity_minor,\
          engine_version\
        ) ",
    );
    builder.push_values(fills, |mut row, fill| {
        row.push_bind(&fill.fill_id)
            .push_bind(fill.sequence)
            .push_bind(instrument_admin)
            .push_bind(instrument_id)
            .push_bind(market_id)
            .push_bind(&fill.outcome)
            .push_bind(&fill.maker_order_id.0)
            .push_bind(&fill.taker_order_id.0)
            .push_bind(&fill.maker_account_id)
            .push_bind(&fill.taker_account_id)
            .push_bind(fill.price_ticks)
            .push_bind(fill.quantity_minor)
            .push_bind(&fill.engine_version);
    });

    let result = builder
        .build()
        .execute(&mut **tx)
        .await
        .context("batch insert fills")?;

    let expected = u64::try_from(fills.len()).context("fill count conversion overflow")?;
    if result.rows_affected() != expected {
        return Err(ApiError::internal("fill batch insert row count mismatch"));
    }

    Ok(())
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
    if position_deltas.is_empty() {
        return Ok(());
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        "INSERT INTO positions (\
          account_id,\
          market_id,\
          outcome,\
          net_quantity_minor,\
          avg_entry_price_ticks,\
          realized_pnl_minor,\
          updated_at\
        ) ",
    );
    builder.push_values(
        position_deltas,
        |mut row, ((account_id, outcome), delta)| {
            row.push_bind(account_id)
                .push_bind(market_id)
                .push_bind(outcome)
                .push_bind(delta.net_quantity_delta_minor)
                .push_bind(delta.last_price_ticks)
                .push_bind(0_i64)
                .push("now()");
        },
    );
    builder.push(
        " ON CONFLICT (account_id, market_id, outcome) DO UPDATE SET \
          net_quantity_minor = positions.net_quantity_minor + EXCLUDED.net_quantity_minor, \
          avg_entry_price_ticks = CASE \
            WHEN (positions.net_quantity_minor + EXCLUDED.net_quantity_minor) = 0 THEN NULL \
            ELSE EXCLUDED.avg_entry_price_ticks \
          END, \
          updated_at = now()",
    );

    builder
        .build()
        .execute(&mut **tx)
        .await
        .context("batch upsert positions")?;

    Ok(())
}

async fn batch_insert_fill_recorded_events(
    tx: &mut Transaction<'_, Postgres>,
    market_id: &str,
    first_sequence: i64,
    fills: &[Fill],
) -> Result<(), ApiError> {
    if fills.is_empty() {
        return Ok(());
    }

    let mut event_rows = Vec::<(i64, serde_json::Value)>::with_capacity(fills.len());
    for (index, fill) in fills.iter().enumerate() {
        let offset = i64::try_from(index).context("fill event index conversion overflow")?;
        let sequence = first_sequence
            .checked_add(offset)
            .ok_or_else(|| anyhow!("market event sequence overflow"))?;
        event_rows.push((
            sequence,
            serde_json::json!({
              "fill_id": fill.fill_id,
              "fill_sequence": fill.sequence,
              "maker_order_id": fill.maker_order_id.0,
              "taker_order_id": fill.taker_order_id.0,
              "maker_account_id": fill.maker_account_id,
              "taker_account_id": fill.taker_account_id,
              "outcome": fill.outcome,
              "price_ticks": fill.price_ticks,
              "quantity_minor": fill.quantity_minor,
              "engine_version": fill.engine_version,
            }),
        ));
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        "INSERT INTO trading_events (\
          aggregate_type,\
          aggregate_id,\
          sequence,\
          event_type,\
          payload\
        ) ",
    );
    builder.push_values(event_rows, |mut row, (sequence, payload)| {
        row.push_bind("market")
            .push_bind(market_id)
            .push_bind(sequence)
            .push_bind("FillRecorded")
            .push_bind(payload);
    });

    let result = builder
        .build()
        .execute(&mut **tx)
        .await
        .context("batch insert fill recorded events")?;
    let expected = u64::try_from(fills.len()).context("fill event count conversion overflow")?;
    if result.rows_affected() != expected {
        return Err(ApiError::internal(
            "fill event batch insert row count mismatch",
        ));
    }

    Ok(())
}

async fn insert_taker_order(
    tx: &mut Transaction<'_, Postgres>,
    incoming: &IncomingOrder,
    account_ctx: &AccountContextRow,
    engine_version: &str,
    remaining_minor: i64,
    locked_minor: i64,
    status: &str,
) -> Result<(), ApiError> {
    let insert_result = sqlx::query(
        r#"
        INSERT INTO orders (
          order_id,
          market_id,
          account_id,
          owner_party,
          instrument_admin,
          instrument_id,
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
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,now(),now())
        "#,
    )
    .bind(&incoming.order_id.0)
    .bind(&incoming.market_id.0)
    .bind(&incoming.account_id)
    .bind(&account_ctx.owner_party)
    .bind(&account_ctx.instrument_admin)
    .bind(&account_ctx.instrument_id)
    .bind(&incoming.outcome)
    .bind(side_to_db(incoming.side))
    .bind(order_type_to_db(incoming.order_type))
    .bind(tif_to_db(incoming.tif))
    .bind(incoming.nonce.0)
    .bind(incoming.price_ticks)
    .bind(incoming.quantity_minor)
    .bind(remaining_minor)
    .bind(locked_minor)
    .bind(status)
    .bind(engine_version)
    .execute(&mut **tx)
    .await;

    match insert_result {
        Ok(_) => Ok(()),
        Err(err) if is_order_id_unique_violation(&err) => {
            Err(ApiError::conflict("order_id already exists"))
        }
        Err(err) => Err(ApiError::from(
            anyhow::Error::from(err).context("insert taker order"),
        )),
    }
}

async fn batch_apply_risk_deltas(
    tx: &mut Transaction<'_, Postgres>,
    instrument_admin: &str,
    instrument_id: &str,
    risk_deltas: &BTreeMap<String, RiskDelta>,
) -> Result<(), ApiError> {
    if risk_deltas.is_empty() {
        return Ok(());
    }

    let non_zero_deltas: Vec<(&str, i64, i64)> = risk_deltas
        .iter()
        .filter_map(|(account_id, delta)| {
            if delta.delta_pending_minor == 0 && delta.locked_delta_minor == 0 {
                None
            } else {
                Some((
                    account_id.as_str(),
                    delta.delta_pending_minor,
                    delta.locked_delta_minor,
                ))
            }
        })
        .collect();
    if non_zero_deltas.is_empty() {
        return Ok(());
    }

    let mut update_builder = QueryBuilder::<Postgres>::new(
        "UPDATE account_risk_state AS rs \
         SET \
           instrument_admin = ",
    );
    update_builder
        .push_bind(instrument_admin)
        .push(", instrument_id = ")
        .push_bind(instrument_id)
        .push(
            ", delta_pending_trades_minor = rs.delta_pending_trades_minor + v.delta_pending_minor, \
             locked_open_orders_minor = rs.locked_open_orders_minor + v.locked_delta_minor, \
             updated_at = now() \
             FROM (",
        );
    update_builder.push_values(
        non_zero_deltas.iter(),
        |mut row, (account_id, pending, locked)| {
            row.push_bind(*account_id)
                .push_bind(*pending)
                .push_bind(*locked);
        },
    );
    update_builder.push(
        ") AS v(account_id, delta_pending_minor, locked_delta_minor) \
         WHERE rs.account_id = v.account_id \
           AND rs.locked_open_orders_minor + v.locked_delta_minor >= 0 \
         RETURNING rs.account_id",
    );

    let updated_accounts: Vec<String> = update_builder
        .build_query_scalar()
        .fetch_all(&mut **tx)
        .await
        .context("batch update account_risk_state")?;
    let mut resolved_accounts: HashSet<String> = updated_accounts.into_iter().collect();

    let insert_candidates: Vec<(&str, i64, i64)> = non_zero_deltas
        .iter()
        .filter_map(|(account_id, pending, locked)| {
            if *locked < 0 || resolved_accounts.contains(*account_id) {
                None
            } else {
                Some((*account_id, *pending, *locked))
            }
        })
        .collect();

    if !insert_candidates.is_empty() {
        let mut insert_builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO account_risk_state (\
              account_id,\
              instrument_admin,\
              instrument_id,\
              delta_pending_trades_minor,\
              locked_open_orders_minor,\
              updated_at\
            ) ",
        );
        insert_builder.push_values(
            insert_candidates.iter(),
            |mut row, (account_id, pending, locked)| {
                row.push_bind(*account_id)
                    .push_bind(instrument_admin)
                    .push_bind(instrument_id)
                    .push_bind(*pending)
                    .push_bind(*locked)
                    .push("now()");
            },
        );
        insert_builder.push(" ON CONFLICT (account_id) DO NOTHING RETURNING account_id");

        let inserted_accounts: Vec<String> = insert_builder
            .build_query_scalar()
            .fetch_all(&mut **tx)
            .await
            .context("batch insert missing account_risk_state")?;
        for account_id in inserted_accounts {
            resolved_accounts.insert(account_id);
        }
    }

    let unresolved: Vec<(&str, i64)> = non_zero_deltas
        .iter()
        .filter_map(|(account_id, _, locked)| {
            if resolved_accounts.contains(*account_id) {
                None
            } else {
                Some((*account_id, *locked))
            }
        })
        .collect();
    if !unresolved.is_empty() {
        let unresolved_account_ids: Vec<String> = unresolved
            .iter()
            .map(|(account_id, _)| (*account_id).to_string())
            .collect();
        let existing_locked_rows: Vec<(String, i64)> = sqlx::query_as(
            r#"
            SELECT account_id, locked_open_orders_minor
            FROM account_risk_state
            WHERE account_id = ANY($1)
            "#,
        )
        .bind(&unresolved_account_ids)
        .fetch_all(&mut **tx)
        .await
        .context("query unresolved account_risk_state rows")?;
        let existing_locked_by_account: HashMap<String, i64> =
            existing_locked_rows.into_iter().collect();

        let debug_reasons: Vec<String> = unresolved
            .into_iter()
            .map(|(account_id, locked_delta_minor)| {
                let existing_locked = existing_locked_by_account.get(account_id).copied();
                let reason = match existing_locked {
                    None if locked_delta_minor < 0 => "missing_row_for_negative_locked_delta",
                    None => "missing_row_for_non_negative_locked_delta",
                    Some(current_locked)
                        if current_locked
                            .checked_add(locked_delta_minor)
                            .is_some_and(|next_locked| next_locked < 0) =>
                    {
                        "locked_underflow"
                    }
                    Some(_) => "unexpected_unresolved",
                };
                format!(
                    "account_id={account_id} locked_delta_minor={locked_delta_minor} existing_locked={existing_locked:?} reason={reason}"
                )
            })
            .collect();

        tracing::warn!(
            instrument_admin,
            instrument_id,
            unresolved_count = debug_reasons.len(),
            unresolved = ?debug_reasons,
            "batch_apply_risk_deltas left unresolved accounts"
        );

        return Err(ApiError::internal(
            "account_risk_state batch update failed (missing risk row or locked underflow)",
        ));
    }

    Ok(())
}

async fn insert_trading_event(
    tx: &mut Transaction<'_, Postgres>,
    aggregate_type: &str,
    aggregate_id: &str,
    sequence: i64,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<(), ApiError> {
    sqlx::query(
        r#"
        INSERT INTO trading_events (
          aggregate_type,
          aggregate_id,
          sequence,
          event_type,
          payload,
          created_at
        )
        VALUES ($1,$2,$3,$4,$5,now())
        "#,
    )
    .bind(aggregate_type)
    .bind(aggregate_id)
    .bind(sequence)
    .bind(event_type)
    .bind(payload)
    .execute(&mut **tx)
    .await
    .context("insert trading event")?;

    Ok(())
}

async fn advisory_lock(
    tx: &mut Transaction<'_, Postgres>,
    namespace: i32,
    key: &str,
) -> Result<(), ApiError> {
    sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
        .bind(namespace)
        .bind(key)
        .execute(&mut **tx)
        .await
        .context("advisory lock")?;

    Ok(())
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
            .price_ticks
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
            },
        ];

        assert_eq!(market_buy_required_minor(&fills).unwrap(), 44);
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
    fn extract_user_api_key_for_orders_rejects_admin_key_header() {
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", "user-key".parse().unwrap());
        headers.insert("x-admin-key", "admin-key".parse().unwrap());

        assert!(extract_user_api_key_for_orders(&headers).is_err());
    }

    #[test]
    fn extract_user_api_key_for_orders_accepts_api_key_header() {
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", "user-key".parse().unwrap());

        let key = extract_user_api_key_for_orders(&headers).unwrap();
        assert_eq!(key, "user-key");
    }
}
