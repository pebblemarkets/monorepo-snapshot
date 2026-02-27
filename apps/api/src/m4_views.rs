use std::{collections::HashSet, convert::Infallible, sync::Arc, time::Duration};

use anyhow::Context as _;
use axum::{
    extract::{Multipart, Path, Query, State},
    http::HeaderMap,
    response::sse::{Event, KeepAlive, Sse},
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{types::Json as SqlJson, PgPool};
use tokio::sync::Mutex;
use tokio_stream::{wrappers::IntervalStream, StreamExt as _};
use uuid::Uuid;

use crate::{ApiError, AppState};

const DEFAULT_QUERY_LIMIT: i64 = 100;
const MAX_QUERY_LIMIT: i64 = 500;
const CHART_STREAM_BATCH_LIMIT: i64 = 200;
const CHART_STREAM_POLL_MS_DEFAULT: u64 = 1_000;
const CHART_STREAM_POLL_MS_MIN: u64 = 250;
const CHART_STREAM_POLL_MS_MAX: u64 = 5_000;
#[cfg(test)]
const CHART_SNAPSHOT_SAMPLE_POINTS_DEFAULT: i64 = 360;
#[cfg(test)]
const CHART_SNAPSHOT_SAMPLE_POINTS_MIN: i64 = 16;
#[cfg(test)]
const CHART_SNAPSHOT_SAMPLE_POINTS_MAX: i64 = 2_000;
const CHART_DEFAULT_PRICE_TICKS: i64 = 50;
const CHART_BIN_SIZE_1M_MS: i64 = 60_000;
const CHART_BIN_SIZE_5M_MS: i64 = 5 * 60_000;
const CHART_BIN_SIZE_30M_MS: i64 = 30 * 60_000;
const CHART_BIN_SIZE_3H_MS: i64 = 3 * 60 * 60_000;
const CHART_BIN_SIZE_1D_MS: i64 = 24 * 60 * 60_000;
const CHART_LIVE_6H_MS: i64 = 6 * 60 * 60_000;
const CHART_LIVE_1D_MS: i64 = 24 * 60 * 60_000;
const CHART_LIVE_1W_MS: i64 = 7 * 24 * 60 * 60_000;
const CHART_LIVE_1M_MS: i64 = 30 * 24 * 60 * 60_000;

const META_ACCOUNT_ID_KEY: &str = "wizardcat.xyz/pebble.accountId";
const META_DEPOSIT_ID_KEY: &str = "wizardcat.xyz/pebble.depositId";

#[derive(Debug, Deserialize)]
pub(crate) struct ListLimitQuery {
    pub(crate) limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PublicMarketStatsQuery {
    pub(crate) limit: Option<i64>,
    pub(crate) market_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MarketChartSnapshotQuery {
    pub(crate) range: Option<String>,
    pub(crate) outcome: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MarketChartUpdatesQuery {
    pub(crate) after_fill_sequence: Option<i64>,
    pub(crate) poll_interval_ms: Option<u64>,
    pub(crate) outcome: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarketChartRange {
    OneHour,
    SixHours,
    OneDay,
    OneWeek,
    OneMonth,
    All,
}

#[derive(Debug, Clone, Copy)]
struct ChartFixedBinSpec {
    sample_points: i64,
    bin_size_ms: i64,
}

impl MarketChartRange {
    fn parse(raw: Option<&str>) -> Result<Self, ApiError> {
        let normalized = raw.unwrap_or("ALL").trim().to_ascii_uppercase();
        match normalized.as_str() {
            "1H" => Ok(Self::OneHour),
            "6H" => Ok(Self::SixHours),
            "1D" => Ok(Self::OneDay),
            "1W" => Ok(Self::OneWeek),
            "1M" => Ok(Self::OneMonth),
            "ALL" => Ok(Self::All),
            _ => Err(ApiError::bad_request(
                "range must be one of 1H, 6H, 1D, 1W, 1M, ALL",
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::OneHour => "1H",
            Self::SixHours => "6H",
            Self::OneDay => "1D",
            Self::OneWeek => "1W",
            Self::OneMonth => "1M",
            Self::All => "ALL",
        }
    }

    fn fixed_bin_spec(self) -> Option<ChartFixedBinSpec> {
        match self {
            Self::OneHour => Some(ChartFixedBinSpec {
                sample_points: 60,
                bin_size_ms: CHART_BIN_SIZE_1M_MS,
            }),
            Self::SixHours => Some(ChartFixedBinSpec {
                sample_points: 360,
                bin_size_ms: CHART_BIN_SIZE_1M_MS,
            }),
            Self::OneDay => Some(ChartFixedBinSpec {
                sample_points: 288,
                bin_size_ms: CHART_BIN_SIZE_5M_MS,
            }),
            Self::OneWeek => Some(ChartFixedBinSpec {
                sample_points: 336,
                bin_size_ms: CHART_BIN_SIZE_30M_MS,
            }),
            Self::OneMonth => Some(ChartFixedBinSpec {
                sample_points: 240,
                bin_size_ms: CHART_BIN_SIZE_3H_MS,
            }),
            Self::All => None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct InstrumentFilterQuery {
    pub(crate) limit: Option<i64>,
    pub(crate) instrument_admin: Option<String>,
    pub(crate) instrument_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TreasuryOpsQuery {
    pub(crate) limit: Option<i64>,
    pub(crate) state: Option<String>,
    pub(crate) op_type: Option<String>,
    pub(crate) account_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MarketMetadataQuery {
    pub(crate) limit: Option<i64>,
    pub(crate) market_id: Option<String>,
    pub(crate) category: Option<String>,
    pub(crate) tag: Option<String>,
    pub(crate) featured: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct UpsertMarketMetadataRequest {
    pub(crate) category: Option<String>,
    pub(crate) tags: Option<Vec<String>>,
    pub(crate) featured: Option<bool>,
    pub(crate) resolution_time: Option<String>,
    pub(crate) card_background_image_url: Option<String>,
    pub(crate) hero_background_image_url: Option<String>,
    pub(crate) thumbnail_image_url: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SessionView {
    pub(crate) account_id: Option<String>,
    pub(crate) is_admin: bool,
}

pub(crate) async fn get_session(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<SessionView>, ApiError> {
    let user_key = optional_header(&headers, "x-api-key")?;
    let admin_key = optional_header(&headers, "x-admin-key")?;
    reject_mixed_auth_headers(&user_key, &admin_key)?;

    let account_id = match user_key {
        Some(key) => {
            let Some(account_id) = state.auth_keys.account_id_for_api_key(&key) else {
                return Err(ApiError::unauthorized("invalid API key"));
            };
            Some(account_id)
        }
        None => None,
    };

    let is_admin = match admin_key {
        Some(key) => {
            if !state.auth_keys.is_admin_key(&key) {
                return Err(ApiError::unauthorized("invalid admin API key"));
            }
            true
        }
        None => false,
    };

    if account_id.is_none() && !is_admin {
        return Err(ApiError::unauthorized(
            "missing x-api-key or x-admin-key header",
        ));
    }

    Ok(Json(SessionView {
        account_id,
        is_admin,
    }))
}

#[derive(Debug, sqlx::FromRow)]
struct MarketRowDb {
    contract_id: String,
    market_id: String,
    question: String,
    outcomes: SqlJson<Vec<String>>,
    status: String,
    resolved_outcome: Option<String>,
    created_at: DateTime<Utc>,
    active: bool,
    last_offset: i64,
}

#[derive(Debug, Serialize)]
pub(crate) struct MarketView {
    pub(crate) contract_id: String,
    pub(crate) market_id: String,
    pub(crate) question: String,
    pub(crate) outcomes: Vec<String>,
    pub(crate) status: String,
    pub(crate) resolved_outcome: Option<String>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) active: bool,
    pub(crate) last_offset: i64,
}

pub(crate) async fn get_market(
    State(state): State<AppState>,
    Path(market_id): Path<String>,
) -> Result<Json<MarketView>, ApiError> {
    let row: Option<MarketRowDb> = sqlx::query_as(
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
        WHERE market_id = $1
          AND active = TRUE
        ORDER BY created_at DESC, contract_id DESC
        LIMIT 1
        "#,
    )
    .bind(&market_id)
    .fetch_optional(&state.db)
    .await
    .context("query market detail")?;

    let Some(row) = row else {
        return Err(ApiError::not_found("market not found"));
    };

    Ok(Json(MarketView {
        contract_id: row.contract_id,
        market_id: row.market_id,
        question: row.question,
        outcomes: row.outcomes.0,
        status: row.status,
        resolved_outcome: row.resolved_outcome,
        created_at: row.created_at,
        active: row.active,
        last_offset: row.last_offset,
    }))
}

#[derive(Debug, sqlx::FromRow)]
struct OrderBookLevelRow {
    outcome: String,
    side: String,
    price_ticks: i64,
    quantity_minor: i64,
    order_count: i64,
}

#[derive(Debug, Serialize)]
pub(crate) struct OrderBookLevelView {
    pub(crate) outcome: String,
    pub(crate) price_ticks: i64,
    pub(crate) quantity_minor: i64,
    pub(crate) order_count: i64,
}

#[derive(Debug, Serialize)]
pub(crate) struct MarketOrderBookView {
    pub(crate) market_id: String,
    pub(crate) bids: Vec<OrderBookLevelView>,
    pub(crate) asks: Vec<OrderBookLevelView>,
}

pub(crate) async fn get_market_order_book(
    State(state): State<AppState>,
    Path(market_id): Path<String>,
) -> Result<Json<MarketOrderBookView>, ApiError> {
    let rows: Vec<OrderBookLevelRow> = sqlx::query_as(
        r#"
        SELECT
          outcome,
          side,
          price_ticks,
          SUM(remaining_minor)::BIGINT AS quantity_minor,
          COUNT(*)::BIGINT AS order_count
        FROM orders
        WHERE market_id = $1
          AND status IN ('Open', 'PartiallyFilled')
        GROUP BY outcome, side, price_ticks
        ORDER BY outcome ASC, side ASC, price_ticks ASC
        "#,
    )
    .bind(&market_id)
    .fetch_all(&state.db)
    .await
    .context("query market order book")?;

    let mut bids = Vec::new();
    let mut asks = Vec::new();

    for row in rows {
        let level = OrderBookLevelView {
            outcome: row.outcome,
            price_ticks: row.price_ticks,
            quantity_minor: row.quantity_minor,
            order_count: row.order_count,
        };

        match row.side.as_str() {
            "Buy" => bids.push(level),
            "Sell" => asks.push(level),
            _ => return Err(ApiError::internal("invalid order side in order book")),
        }
    }

    bids.sort_by(|left, right| {
        left.outcome
            .cmp(&right.outcome)
            .then_with(|| right.price_ticks.cmp(&left.price_ticks))
    });
    asks.sort_by(|left, right| {
        left.outcome
            .cmp(&right.outcome)
            .then_with(|| left.price_ticks.cmp(&right.price_ticks))
    });

    Ok(Json(MarketOrderBookView {
        market_id,
        bids,
        asks,
    }))
}

#[derive(Debug, sqlx::FromRow)]
struct MarketFillRow {
    fill_id: String,
    fill_sequence: i64,
    market_id: String,
    outcome: String,
    maker_order_id: String,
    taker_order_id: String,
    price_ticks: i64,
    quantity_minor: i64,
    engine_version: String,
    matched_at: DateTime<Utc>,
    clearing_epoch: Option<i64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MarketFillView {
    pub(crate) fill_id: String,
    pub(crate) fill_sequence: i64,
    pub(crate) market_id: String,
    pub(crate) outcome: String,
    pub(crate) maker_order_id: String,
    pub(crate) taker_order_id: String,
    pub(crate) price_ticks: i64,
    pub(crate) quantity_minor: i64,
    pub(crate) engine_version: String,
    pub(crate) matched_at: DateTime<Utc>,
    pub(crate) clearing_epoch: Option<i64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MarketChartSnapshotView {
    pub(crate) market_id: String,
    pub(crate) range: String,
    pub(crate) start_at: DateTime<Utc>,
    pub(crate) end_at: DateTime<Utc>,
    pub(crate) sample_points: i64,
    pub(crate) samples: Vec<MarketChartSamplePointView>,
    pub(crate) last_fill_sequence: Option<i64>,
    pub(crate) previous_fill: Option<MarketFillView>,
    pub(crate) fills: Vec<MarketFillView>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MarketChartSamplePointView {
    pub(crate) ts: DateTime<Utc>,
    pub(crate) price_ticks: Option<i64>,
}

#[derive(Debug, Serialize)]
struct MarketChartUpdatesEvent {
    market_id: String,
    fills: Vec<MarketChartUpdateFillView>,
    last_fill_sequence: i64,
}

#[derive(Debug, Serialize)]
pub(crate) struct MarketChartUpdateFillView {
    pub(crate) fill_sequence: i64,
    pub(crate) price_ticks: i64,
    pub(crate) matched_at: DateTime<Utc>,
}

#[derive(Debug, sqlx::FromRow)]
struct MarketChartUpdateFillRow {
    fill_sequence: i64,
    price_ticks: i64,
    matched_at: DateTime<Utc>,
}

fn map_market_fill_row(row: MarketFillRow) -> MarketFillView {
    MarketFillView {
        fill_id: row.fill_id,
        fill_sequence: row.fill_sequence,
        market_id: row.market_id,
        outcome: row.outcome,
        maker_order_id: row.maker_order_id,
        taker_order_id: row.taker_order_id,
        price_ticks: row.price_ticks,
        quantity_minor: row.quantity_minor,
        engine_version: row.engine_version,
        matched_at: row.matched_at,
        clearing_epoch: row.clearing_epoch,
    }
}

fn map_market_chart_update_fill_row(row: MarketChartUpdateFillRow) -> MarketChartUpdateFillView {
    MarketChartUpdateFillView {
        fill_sequence: row.fill_sequence,
        price_ticks: row.price_ticks,
        matched_at: row.matched_at,
    }
}

pub(crate) async fn list_market_fills(
    State(state): State<AppState>,
    Path(market_id): Path<String>,
    Query(query): Query<ListLimitQuery>,
) -> Result<Json<Vec<MarketFillView>>, ApiError> {
    let limit = clamp_limit(query.limit);

    let rows: Vec<MarketFillRow> = sqlx::query_as(
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
          clearing_epoch
        FROM fills
        WHERE market_id = $1
        ORDER BY matched_at DESC, fill_sequence DESC
        LIMIT $2
        "#,
    )
    .bind(&market_id)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .context("query market fills")?;

    let fills = rows.into_iter().map(map_market_fill_row).collect();

    Ok(Json(fills))
}

async fn load_market_created_at(
    db: &PgPool,
    market_id: &str,
) -> Result<Option<DateTime<Utc>>, ApiError> {
    let row: Option<(DateTime<Utc>,)> = sqlx::query_as(
        r#"
        SELECT created_at
        FROM markets
        WHERE market_id = $1
        LIMIT 1
        "#,
    )
    .bind(market_id)
    .fetch_optional(db)
    .await
    .context("query market created_at")?;

    Ok(row.map(|(created_at,)| created_at))
}

async fn load_latest_market_fill_row(
    db: &PgPool,
    market_id: &str,
    outcome: Option<&str>,
) -> Result<Option<MarketFillRow>, ApiError> {
    let row = sqlx::query_as(
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
          clearing_epoch
        FROM fills
        WHERE market_id = $1
          AND ($2::TEXT IS NULL OR outcome = $2)
        ORDER BY fill_sequence DESC
        LIMIT 1
        "#,
    )
    .bind(market_id)
    .bind(outcome)
    .fetch_optional(db)
    .await
    .context("query latest market fill")?;

    Ok(row)
}

async fn load_market_fill_row_before(
    db: &PgPool,
    market_id: &str,
    before: DateTime<Utc>,
    outcome: Option<&str>,
) -> Result<Option<MarketFillRow>, ApiError> {
    let row = sqlx::query_as(
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
          clearing_epoch
        FROM fills
        WHERE market_id = $1
          AND matched_at < $2
          AND ($3::TEXT IS NULL OR outcome = $3)
        ORDER BY matched_at DESC, fill_sequence DESC
        LIMIT 1
        "#,
    )
    .bind(market_id)
    .bind(before)
    .bind(outcome)
    .fetch_optional(db)
    .await
    .context("query prior market fill")?;

    Ok(row)
}

async fn load_market_fill_rows_bucketed(
    db: &PgPool,
    market_id: &str,
    start_at: DateTime<Utc>,
    end_at: DateTime<Utc>,
    bin_size_ms: i64,
    outcome: Option<&str>,
) -> Result<Vec<MarketFillRow>, ApiError> {
    if bin_size_ms <= 0 {
        return Err(ApiError::internal("chart bin size must be > 0"));
    }

    let rows = sqlx::query_as(
        r#"
        WITH ranked AS (
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
            ROW_NUMBER() OVER (
              PARTITION BY FLOOR(EXTRACT(EPOCH FROM matched_at) * 1000)::BIGINT / $4
              ORDER BY matched_at DESC, fill_sequence DESC
            ) AS bucket_rank
          FROM fills
          WHERE market_id = $1
            AND matched_at >= $2
            AND matched_at <= $3
            AND ($5::TEXT IS NULL OR outcome = $5)
        )
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
          clearing_epoch
        FROM ranked
        WHERE bucket_rank = 1
        ORDER BY matched_at ASC, fill_sequence ASC
        "#,
    )
    .bind(market_id)
    .bind(start_at)
    .bind(end_at)
    .bind(bin_size_ms)
    .bind(outcome)
    .fetch_all(db)
    .await
    .context("query bucketed market chart fills in range")?;

    Ok(rows)
}

async fn load_market_chart_update_rows_after_sequence(
    db: &PgPool,
    market_id: &str,
    after_fill_sequence: i64,
    outcome: Option<&str>,
    limit: i64,
) -> Result<Vec<MarketChartUpdateFillRow>, ApiError> {
    let rows = sqlx::query_as(
        r#"
        SELECT
          fill_sequence,
          price_ticks,
          matched_at
        FROM fills
        WHERE market_id = $1
          AND fill_sequence > $2
          AND ($3::TEXT IS NULL OR outcome = $3)
        ORDER BY fill_sequence ASC
        LIMIT $4
        "#,
    )
    .bind(market_id)
    .bind(after_fill_sequence)
    .bind(outcome)
    .bind(limit)
    .fetch_all(db)
    .await
    .context("query market chart incremental update fills")?;

    Ok(rows)
}

fn clamp_chart_stream_poll_interval_ms(raw: Option<u64>) -> u64 {
    raw.unwrap_or(CHART_STREAM_POLL_MS_DEFAULT)
        .clamp(CHART_STREAM_POLL_MS_MIN, CHART_STREAM_POLL_MS_MAX)
}

fn normalize_chart_outcome(raw: Option<&str>) -> Option<String> {
    let trimmed = raw.map(str::trim).unwrap_or_default();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[cfg(test)]
fn clamp_chart_snapshot_sample_points(raw: Option<i64>) -> i64 {
    raw.unwrap_or(CHART_SNAPSHOT_SAMPLE_POINTS_DEFAULT).clamp(
        CHART_SNAPSHOT_SAMPLE_POINTS_MIN,
        CHART_SNAPSHOT_SAMPLE_POINTS_MAX,
    )
}

fn chart_all_bin_size_ms(live_duration_ms: i64) -> i64 {
    if live_duration_ms < CHART_LIVE_6H_MS {
        CHART_BIN_SIZE_1M_MS
    } else if live_duration_ms < CHART_LIVE_1D_MS {
        CHART_BIN_SIZE_5M_MS
    } else if live_duration_ms < CHART_LIVE_1W_MS {
        CHART_BIN_SIZE_30M_MS
    } else if live_duration_ms < CHART_LIVE_1M_MS {
        CHART_BIN_SIZE_3H_MS
    } else {
        CHART_BIN_SIZE_1D_MS
    }
}

fn sample_points_for_fixed_bins(
    start_at: DateTime<Utc>,
    end_at: DateTime<Utc>,
    bin_size_ms: i64,
) -> Result<i64, ApiError> {
    if bin_size_ms <= 0 {
        return Err(ApiError::internal("chart bin size must be > 0"));
    }

    let span_ms = end_at
        .timestamp_millis()
        .checked_sub(start_at.timestamp_millis())
        .ok_or_else(|| ApiError::internal("chart span overflow"))?;
    let bins = span_ms
        .checked_div(bin_size_ms)
        .ok_or_else(|| ApiError::internal("chart bin division failed"))?;
    bins.checked_add(1)
        .ok_or_else(|| ApiError::internal("chart sample_points overflow"))
}

fn floor_timestamp_millis(ts: DateTime<Utc>, step_ms: i64) -> Result<DateTime<Utc>, ApiError> {
    if step_ms <= 0 {
        return Err(ApiError::internal("chart bin size must be > 0"));
    }

    let ts_ms = ts.timestamp_millis();
    let aligned_ms = ts_ms - ts_ms.rem_euclid(step_ms);
    DateTime::<Utc>::from_timestamp_millis(aligned_ms)
        .ok_or_else(|| ApiError::internal("invalid aligned chart timestamp"))
}

#[cfg(test)]
fn build_market_chart_samples(
    start_at: DateTime<Utc>,
    end_at: DateTime<Utc>,
    sample_points: i64,
    previous_fill: Option<&MarketFillRow>,
    fills_in_range: &[MarketFillRow],
) -> Result<Vec<MarketChartSamplePointView>, ApiError> {
    let sample_count =
        usize::try_from(sample_points).map_err(|_| ApiError::internal("sample_points overflow"))?;
    if sample_count == 0 {
        return Ok(Vec::new());
    }

    let start_ms = start_at.timestamp_millis();
    let end_ms = end_at.timestamp_millis();
    let span_ms = (end_ms - start_ms).max(0);
    let denom = i64::try_from(sample_count.saturating_sub(1))
        .map_err(|_| ApiError::internal("sample_points overflow"))?;

    let mut samples = Vec::with_capacity(sample_count);
    let mut fill_cursor = 0usize;
    let mut current_price = previous_fill.map(|fill| fill.price_ticks);

    for sample_index in 0..sample_count {
        let index_i64 =
            i64::try_from(sample_index).map_err(|_| ApiError::internal("sample index overflow"))?;
        let ts_ms = if denom == 0 {
            start_ms
        } else {
            start_ms + ((span_ms * index_i64) / denom)
        };
        let ts = DateTime::<Utc>::from_timestamp_millis(ts_ms)
            .ok_or_else(|| ApiError::internal("invalid chart sample timestamp"))?;

        while fill_cursor < fills_in_range.len() {
            let fill = &fills_in_range[fill_cursor];
            if fill.matched_at > ts {
                break;
            }
            current_price = Some(fill.price_ticks);
            fill_cursor += 1;
        }

        samples.push(MarketChartSamplePointView {
            ts,
            price_ticks: current_price,
        });
    }

    Ok(samples)
}

fn build_market_chart_samples_fixed_bins(
    start_at: DateTime<Utc>,
    sample_points: i64,
    bin_size_ms: i64,
    default_price_ticks: Option<i64>,
    previous_fill: Option<&MarketFillRow>,
    fills_in_range: &[MarketFillRow],
) -> Result<Vec<MarketChartSamplePointView>, ApiError> {
    if sample_points <= 0 {
        return Ok(Vec::new());
    }
    if bin_size_ms <= 0 {
        return Err(ApiError::internal("chart bin size must be > 0"));
    }

    let sample_count =
        usize::try_from(sample_points).map_err(|_| ApiError::internal("sample_points overflow"))?;
    let start_ms = start_at.timestamp_millis();
    let mut samples = Vec::with_capacity(sample_count);
    let mut fill_cursor = 0usize;
    let mut current_price = previous_fill
        .map(|fill| fill.price_ticks)
        .or(default_price_ticks);

    for sample_index in 0..sample_count {
        let sample_index_i64 =
            i64::try_from(sample_index).map_err(|_| ApiError::internal("sample index overflow"))?;
        let offset_ms = bin_size_ms
            .checked_mul(sample_index_i64)
            .ok_or_else(|| ApiError::internal("chart sample offset overflow"))?;
        let sample_start_ms = start_ms
            .checked_add(offset_ms)
            .ok_or_else(|| ApiError::internal("chart sample timestamp overflow"))?;
        let sample_ts = DateTime::<Utc>::from_timestamp_millis(sample_start_ms)
            .ok_or_else(|| ApiError::internal("invalid chart sample timestamp"))?;

        let next_boundary_ms = if sample_index + 1 < sample_count {
            let next_index_i64 = i64::try_from(sample_index + 1)
                .map_err(|_| ApiError::internal("sample index overflow"))?;
            let next_offset_ms = bin_size_ms
                .checked_mul(next_index_i64)
                .ok_or_else(|| ApiError::internal("chart sample offset overflow"))?;
            Some(
                start_ms
                    .checked_add(next_offset_ms)
                    .ok_or_else(|| ApiError::internal("chart sample timestamp overflow"))?,
            )
        } else {
            None
        };

        while fill_cursor < fills_in_range.len() {
            let fill = &fills_in_range[fill_cursor];
            let fill_ms = fill.matched_at.timestamp_millis();
            if fill_ms < sample_start_ms {
                fill_cursor += 1;
                continue;
            }

            if let Some(boundary_ms) = next_boundary_ms {
                if fill_ms >= boundary_ms {
                    break;
                }
            }

            current_price = Some(fill.price_ticks);
            fill_cursor += 1;
        }

        samples.push(MarketChartSamplePointView {
            ts: sample_ts,
            price_ticks: current_price,
        });
    }

    Ok(samples)
}

pub(crate) async fn get_market_chart_snapshot(
    State(state): State<AppState>,
    Path(market_id): Path<String>,
    Query(query): Query<MarketChartSnapshotQuery>,
) -> Result<Json<MarketChartSnapshotView>, ApiError> {
    let range = MarketChartRange::parse(query.range.as_deref())?;
    let chart_outcome = normalize_chart_outcome(query.outcome.as_deref());
    let now = Utc::now();
    let (start_at, end_at, fills_in_range, previous_fill_row, sample_points, samples) =
        match range.fixed_bin_spec() {
            Some(spec) => {
                let end_at = floor_timestamp_millis(now, spec.bin_size_ms)?;
                let end_ms = end_at.timestamp_millis();
                let bins_minus_one = spec
                    .sample_points
                    .checked_sub(1)
                    .ok_or_else(|| ApiError::internal("sample_points underflow"))?;
                let span_ms = spec
                    .bin_size_ms
                    .checked_mul(bins_minus_one)
                    .ok_or_else(|| ApiError::internal("chart span overflow"))?;
                let start_ms = end_ms
                    .checked_sub(span_ms)
                    .ok_or_else(|| ApiError::internal("chart start overflow"))?;
                let start_at = DateTime::<Utc>::from_timestamp_millis(start_ms)
                    .ok_or_else(|| ApiError::internal("invalid chart start timestamp"))?;

                let fills_in_range = load_market_fill_rows_bucketed(
                    &state.db,
                    &market_id,
                    start_at,
                    now,
                    spec.bin_size_ms,
                    chart_outcome.as_deref(),
                )
                .await?;
                let previous_fill_row = load_market_fill_row_before(
                    &state.db,
                    &market_id,
                    start_at,
                    chart_outcome.as_deref(),
                )
                .await?;
                let samples = build_market_chart_samples_fixed_bins(
                    start_at,
                    spec.sample_points,
                    spec.bin_size_ms,
                    Some(CHART_DEFAULT_PRICE_TICKS),
                    previous_fill_row.as_ref(),
                    &fills_in_range,
                )?;

                (
                    start_at,
                    end_at,
                    fills_in_range,
                    previous_fill_row,
                    spec.sample_points,
                    samples,
                )
            }
            None => {
                let market_created_at = load_market_created_at(&state.db, &market_id)
                    .await?
                    .unwrap_or(now);
                let live_start_at = if market_created_at <= now {
                    market_created_at
                } else {
                    now
                };
                let live_duration_ms = now
                    .timestamp_millis()
                    .saturating_sub(live_start_at.timestamp_millis());
                let bin_size_ms = chart_all_bin_size_ms(live_duration_ms);
                let start_at = floor_timestamp_millis(live_start_at, bin_size_ms)?;
                let end_at = floor_timestamp_millis(now, bin_size_ms)?;
                let sample_points = sample_points_for_fixed_bins(start_at, end_at, bin_size_ms)?;
                let fills_in_range = load_market_fill_rows_bucketed(
                    &state.db,
                    &market_id,
                    start_at,
                    now,
                    bin_size_ms,
                    chart_outcome.as_deref(),
                )
                .await?;
                let previous_fill_row = load_market_fill_row_before(
                    &state.db,
                    &market_id,
                    start_at,
                    chart_outcome.as_deref(),
                )
                .await?;
                let samples = build_market_chart_samples_fixed_bins(
                    start_at,
                    sample_points,
                    bin_size_ms,
                    None,
                    previous_fill_row.as_ref(),
                    &fills_in_range,
                )?;

                (
                    start_at,
                    end_at,
                    fills_in_range,
                    previous_fill_row,
                    sample_points,
                    samples,
                )
            }
        };
    let last_fill_sequence = if let Some(last_row) = fills_in_range.last() {
        Some(last_row.fill_sequence)
    } else if let Some(previous_row) = &previous_fill_row {
        Some(previous_row.fill_sequence)
    } else {
        load_latest_market_fill_row(&state.db, &market_id, chart_outcome.as_deref())
            .await?
            .map(|row| row.fill_sequence)
    };

    let previous_fill = previous_fill_row.map(map_market_fill_row);
    let fills = Vec::new();

    Ok(Json(MarketChartSnapshotView {
        market_id,
        range: range.as_str().to_string(),
        start_at,
        end_at,
        sample_points,
        samples,
        last_fill_sequence,
        previous_fill,
        fills,
    }))
}

pub(crate) async fn stream_market_chart_updates(
    State(state): State<AppState>,
    Path(market_id): Path<String>,
    Query(query): Query<MarketChartUpdatesQuery>,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let poll_interval_ms = clamp_chart_stream_poll_interval_ms(query.poll_interval_ms);
    let chart_outcome = normalize_chart_outcome(query.outcome.as_deref());
    let initial_cursor = match query.after_fill_sequence {
        Some(value) if value >= -1 => value,
        Some(_) => return Err(ApiError::bad_request("after_fill_sequence must be >= -1")),
        None => load_latest_market_fill_row(&state.db, &market_id, chart_outcome.as_deref())
            .await?
            .map_or(-1, |row| row.fill_sequence),
    };

    let cursor = Arc::new(Mutex::new(initial_cursor));
    let db = state.db.clone();
    let market_id_for_stream = market_id.clone();
    let chart_outcome_for_stream = chart_outcome.clone();
    let stream = IntervalStream::new(tokio::time::interval(Duration::from_millis(poll_interval_ms)))
        .then(move |_| {
            let db = db.clone();
            let market_id = market_id_for_stream.clone();
            let chart_outcome = chart_outcome_for_stream.clone();
            let cursor = Arc::clone(&cursor);
            async move {
                let after_fill_sequence = {
                    let guard = cursor.lock().await;
                    *guard
                };

                let rows = match load_market_chart_update_rows_after_sequence(
                    &db,
                    &market_id,
                    after_fill_sequence,
                    chart_outcome.as_deref(),
                    CHART_STREAM_BATCH_LIMIT,
                )
                .await
                {
                    Ok(rows) => rows,
                    Err(err) => {
                        tracing::error!(error = ?err, market_id = %market_id, "chart updates poll failed");
                        return Some(Ok(Event::default().event("error").data("internal server error")));
                    }
                };

                if rows.is_empty() {
                    return None;
                }

                let fills: Vec<MarketChartUpdateFillView> =
                    rows.into_iter().map(map_market_chart_update_fill_row).collect();
                let last_fill = fills.last()?;
                let last_fill_sequence = last_fill.fill_sequence;
                {
                    let mut guard = cursor.lock().await;
                    *guard = last_fill_sequence;
                }

                let payload = MarketChartUpdatesEvent {
                    market_id: market_id.clone(),
                    fills,
                    last_fill_sequence,
                };
                match serde_json::to_string(&payload) {
                    Ok(serialized) => Some(Ok(
                        Event::default()
                            .event("fills")
                            .id(last_fill_sequence.to_string())
                            .data(serialized),
                    )),
                    Err(err) => {
                        tracing::error!(
                            error = ?err,
                            market_id = %market_id,
                            "serialize chart updates payload failed"
                        );
                        Some(Ok(Event::default().event("error").data("internal server error")))
                    }
                }
            }
        })
        .filter_map(|event| event);

    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    ))
}

#[derive(Debug, Serialize)]
pub(crate) struct PublicStatsOverviewView {
    pub(crate) tvl_minor: i64,
    pub(crate) volume_24h_minor: i64,
    pub(crate) fills_24h: i64,
    pub(crate) markets_total: i64,
    pub(crate) markets_open: i64,
    pub(crate) markets_resolved: i64,
    pub(crate) accounts_total: i64,
    pub(crate) generated_at: DateTime<Utc>,
}

#[derive(Debug, sqlx::FromRow)]
struct PublicOverviewLiabilityRow {
    tvl_minor: i64,
    accounts_total: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct PublicOverviewFillRow {
    volume_24h_minor: i64,
    fills_24h: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct PublicOverviewMarketRow {
    markets_total: i64,
    markets_open: i64,
    markets_resolved: i64,
}

pub(crate) async fn get_public_stats_overview(
    State(state): State<AppState>,
) -> Result<Json<PublicStatsOverviewView>, ApiError> {
    let liabilities: PublicOverviewLiabilityRow = sqlx::query_as(
        r#"
        SELECT
          COALESCE(SUM(ast.cleared_cash_minor), 0)::BIGINT AS tvl_minor,
          COUNT(*)::BIGINT AS accounts_total
        FROM account_states ast
        JOIN account_state_latest latest
          ON latest.account_id = ast.account_id
         AND latest.contract_id = ast.contract_id
        WHERE ast.active = TRUE
        "#,
    )
    .fetch_one(&state.db)
    .await
    .context("query public liabilities overview")?;

    let fills: PublicOverviewFillRow = sqlx::query_as(
        r#"
        SELECT
          COALESCE(SUM((price_ticks::NUMERIC * quantity_minor::NUMERIC)), 0)::BIGINT AS volume_24h_minor,
          COUNT(*)::BIGINT AS fills_24h
        FROM fills
        WHERE matched_at >= now() - INTERVAL '24 hours'
        "#,
    )
    .fetch_one(&state.db)
    .await
    .context("query public fill overview")?;

    let markets: PublicOverviewMarketRow = sqlx::query_as(
        r#"
        SELECT
          COUNT(*)::BIGINT AS markets_total,
          COUNT(*) FILTER (WHERE status = 'Open')::BIGINT AS markets_open,
          COUNT(*) FILTER (WHERE status = 'Resolved')::BIGINT AS markets_resolved
        FROM markets
        WHERE active = TRUE
        "#,
    )
    .fetch_one(&state.db)
    .await
    .context("query public markets overview")?;

    Ok(Json(PublicStatsOverviewView {
        tvl_minor: liabilities.tvl_minor,
        volume_24h_minor: fills.volume_24h_minor,
        fills_24h: fills.fills_24h,
        markets_total: markets.markets_total,
        markets_open: markets.markets_open,
        markets_resolved: markets.markets_resolved,
        accounts_total: liabilities.accounts_total,
        generated_at: Utc::now(),
    }))
}

#[derive(Debug, sqlx::FromRow)]
struct PublicMarketStatsRow {
    market_id: String,
    question: String,
    status: String,
    resolved_outcome: Option<String>,
    created_at: DateTime<Utc>,
    fills_24h: i64,
    volume_24h_minor: i64,
    open_interest_minor: i64,
    last_traded_price_ticks: Option<i64>,
    last_traded_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub(crate) struct PublicMarketStatsView {
    pub(crate) market_id: String,
    pub(crate) question: String,
    pub(crate) status: String,
    pub(crate) resolved_outcome: Option<String>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) fills_24h: i64,
    pub(crate) volume_24h_minor: i64,
    pub(crate) open_interest_minor: i64,
    pub(crate) last_traded_price_ticks: Option<i64>,
    pub(crate) last_traded_at: Option<DateTime<Utc>>,
}

#[derive(Debug, sqlx::FromRow)]
struct PositionRow {
    account_id: String,
    market_id: String,
    question: Option<String>,
    outcome: String,
    net_quantity_minor: i64,
    avg_entry_price_ticks: Option<i64>,
    realized_pnl_minor: i64,
    mark_price_ticks: Option<i64>,
    mark_value_minor: Option<i64>,
    updated_at: DateTime<Utc>,
    market_status: Option<String>,
    market_resolved_outcome: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct PositionView {
    pub(crate) account_id: String,
    pub(crate) market_id: String,
    pub(crate) question: Option<String>,
    pub(crate) outcome: String,
    pub(crate) net_quantity_minor: i64,
    pub(crate) avg_entry_price_ticks: Option<i64>,
    pub(crate) realized_pnl_minor: i64,
    pub(crate) mark_price_ticks: Option<i64>,
    pub(crate) mark_value_minor: Option<i64>,
    pub(crate) updated_at: DateTime<Utc>,
    pub(crate) market_status: Option<String>,
    pub(crate) market_resolved_outcome: Option<String>,
}

pub(crate) async fn list_my_positions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListLimitQuery>,
) -> Result<Json<Vec<PositionView>>, ApiError> {
    let account_id = authenticate_user_account_id(&state, &headers)?;
    let limit = clamp_limit(query.limit);

    let rows: Vec<PositionRow> = sqlx::query_as(
        r#"
        WITH latest_market AS (
          SELECT DISTINCT ON (m.market_id)
            m.market_id,
            m.question,
            m.status,
            m.resolved_outcome
          FROM markets m
          WHERE m.active = TRUE
          ORDER BY m.market_id, m.created_at DESC, m.last_offset DESC, m.contract_id DESC
        )
        SELECT
          p.account_id,
          p.market_id,
          lm.question,
          p.outcome,
          p.net_quantity_minor,
          p.avg_entry_price_ticks,
          p.realized_pnl_minor,
          last_fill.price_ticks AS mark_price_ticks,
          CASE
            WHEN last_fill.price_ticks IS NULL THEN NULL
            ELSE (last_fill.price_ticks::NUMERIC * p.net_quantity_minor::NUMERIC)::BIGINT
          END AS mark_value_minor,
          p.updated_at,
          lm.status AS market_status,
          lm.resolved_outcome AS market_resolved_outcome
        FROM positions p
        LEFT JOIN latest_market lm
          ON lm.market_id = p.market_id
        LEFT JOIN LATERAL (
          SELECT f.price_ticks
          FROM fills f
          WHERE f.market_id = p.market_id
            AND f.outcome = p.outcome
          ORDER BY f.matched_at DESC, f.fill_sequence DESC
          LIMIT 1
        ) last_fill ON TRUE
        WHERE p.account_id = $1
          AND p.net_quantity_minor <> 0
        ORDER BY p.updated_at DESC, p.market_id ASC, p.outcome ASC
        LIMIT $2
        "#,
    )
    .bind(&account_id)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .context("query my positions")?;

    let out = rows
        .into_iter()
        .map(|row| PositionView {
            account_id: row.account_id,
            market_id: row.market_id,
            question: row.question,
            outcome: row.outcome,
            net_quantity_minor: row.net_quantity_minor,
            avg_entry_price_ticks: row.avg_entry_price_ticks,
            realized_pnl_minor: row.realized_pnl_minor,
            mark_price_ticks: row.mark_price_ticks,
            mark_value_minor: row.mark_value_minor,
            updated_at: row.updated_at,
            market_status: row.market_status,
            market_resolved_outcome: row.market_resolved_outcome,
        })
        .collect();

    Ok(Json(out))
}

#[derive(Debug, sqlx::FromRow)]
struct PortfolioHistoryRow {
    contract_id: String,
    account_id: String,
    cleared_cash_minor: i64,
    position_mark_value_minor: i64,
    total_equity_minor: i64,
    last_applied_epoch: i64,
    created_at: DateTime<Utc>,
    active: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct PortfolioHistoryView {
    pub(crate) contract_id: String,
    pub(crate) account_id: String,
    pub(crate) cleared_cash_minor: i64,
    pub(crate) position_mark_value_minor: i64,
    pub(crate) total_equity_minor: i64,
    pub(crate) last_applied_epoch: i64,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) active: bool,
}

pub(crate) async fn list_my_portfolio_history(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListLimitQuery>,
) -> Result<Json<Vec<PortfolioHistoryView>>, ApiError> {
    let account_id = authenticate_user_account_id(&state, &headers)?;
    let limit = clamp_limit(query.limit);

    let rows: Vec<PortfolioHistoryRow> = sqlx::query_as(
        r#"
        WITH snapshots AS (
          SELECT
            contract_id,
            account_id,
            cleared_cash_minor,
            last_applied_epoch,
            created_at,
            active
          FROM account_states
          WHERE account_id = $1
          ORDER BY created_at DESC, contract_id DESC
          LIMIT $2
        ),
        account_fills AS (
          SELECT
            f.market_id,
            f.outcome,
            f.matched_at,
            f.fill_sequence,
            CASE
              WHEN o.side = 'Buy' THEN f.quantity_minor
              ELSE -f.quantity_minor
            END AS signed_qty_minor
          FROM fills f
          JOIN orders o
            ON o.order_id = f.maker_order_id
          WHERE f.maker_account_id = $1
          UNION ALL
          SELECT
            f.market_id,
            f.outcome,
            f.matched_at,
            f.fill_sequence,
            CASE
              WHEN o.side = 'Buy' THEN f.quantity_minor
              ELSE -f.quantity_minor
            END AS signed_qty_minor
          FROM fills f
          JOIN orders o
            ON o.order_id = f.taker_order_id
          WHERE f.taker_account_id = $1
        ),
        snapshot_positions AS (
          SELECT
            s.contract_id,
            s.created_at AS snapshot_created_at,
            af.market_id,
            af.outcome,
            SUM(af.signed_qty_minor)::BIGINT AS net_quantity_minor
          FROM snapshots s
          JOIN account_fills af
            ON af.matched_at <= s.created_at
          GROUP BY s.contract_id, s.created_at, af.market_id, af.outcome
          HAVING SUM(af.signed_qty_minor) <> 0
        ),
        snapshot_positions_marked AS (
          SELECT
            sp.contract_id,
            sp.net_quantity_minor,
            COALESCE(mark.price_ticks, 0)::BIGINT AS mark_price_ticks
          FROM snapshot_positions sp
          LEFT JOIN LATERAL (
            SELECT f.price_ticks
            FROM fills f
            WHERE f.market_id = sp.market_id
              AND f.outcome = sp.outcome
              AND f.matched_at <= sp.snapshot_created_at
            ORDER BY f.matched_at DESC, f.fill_sequence DESC
            LIMIT 1
          ) mark ON TRUE
        ),
        snapshot_equity AS (
          SELECT
            spm.contract_id,
            COALESCE(
              SUM(
                (spm.mark_price_ticks::NUMERIC * spm.net_quantity_minor::NUMERIC)::BIGINT
              ),
              0
            )::BIGINT AS position_mark_value_minor
          FROM snapshot_positions_marked spm
          GROUP BY spm.contract_id
        )
        SELECT
          s.contract_id,
          s.account_id,
          s.cleared_cash_minor,
          COALESCE(se.position_mark_value_minor, 0)::BIGINT AS position_mark_value_minor,
          (
            s.cleared_cash_minor::NUMERIC + COALESCE(se.position_mark_value_minor, 0)::NUMERIC
          )::BIGINT AS total_equity_minor,
          s.last_applied_epoch,
          s.created_at,
          s.active
        FROM snapshots s
        LEFT JOIN snapshot_equity se
          ON se.contract_id = s.contract_id
        ORDER BY s.created_at DESC, s.contract_id DESC
        "#,
    )
    .bind(&account_id)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .context("query my portfolio history")?;

    let out = rows
        .into_iter()
        .map(|row| PortfolioHistoryView {
            contract_id: row.contract_id,
            account_id: row.account_id,
            cleared_cash_minor: row.cleared_cash_minor,
            position_mark_value_minor: row.position_mark_value_minor,
            total_equity_minor: row.total_equity_minor,
            last_applied_epoch: row.last_applied_epoch,
            created_at: row.created_at,
            active: row.active,
        })
        .collect();

    Ok(Json(out))
}

#[derive(Debug, sqlx::FromRow)]
struct MarketMetadataRow {
    market_id: String,
    question: String,
    outcomes: SqlJson<Vec<String>>,
    status: String,
    created_at: DateTime<Utc>,
    category: String,
    tags: SqlJson<Vec<String>>,
    featured: bool,
    resolution_time: Option<DateTime<Utc>>,
    card_background_image_url: Option<String>,
    hero_background_image_url: Option<String>,
    thumbnail_image_url: Option<String>,
    updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, sqlx::FromRow)]
struct MarketMetadataCurrentRow {
    category: String,
    tags: SqlJson<Vec<String>>,
    featured: bool,
    resolution_time: Option<DateTime<Utc>>,
    card_background_image_url: Option<String>,
    hero_background_image_url: Option<String>,
    thumbnail_image_url: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct MarketMetadataView {
    pub(crate) market_id: String,
    pub(crate) question: String,
    pub(crate) outcomes: Vec<String>,
    pub(crate) status: String,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) category: String,
    pub(crate) tags: Vec<String>,
    pub(crate) featured: bool,
    pub(crate) resolution_time: Option<DateTime<Utc>>,
    pub(crate) card_background_image_url: Option<String>,
    pub(crate) hero_background_image_url: Option<String>,
    pub(crate) thumbnail_image_url: Option<String>,
    pub(crate) updated_at: Option<DateTime<Utc>>,
}

pub(crate) async fn list_market_metadata(
    State(state): State<AppState>,
    Query(query): Query<MarketMetadataQuery>,
) -> Result<Json<Vec<MarketMetadataView>>, ApiError> {
    let limit = clamp_limit(query.limit);
    let category = query.category.map(|value| value.trim().to_string());
    let category = category.filter(|value| !value.is_empty());
    let market_id = query.market_id.map(|value| value.trim().to_string());
    let market_id = market_id.filter(|value| !value.is_empty());
    let tag = query.tag.map(|value| value.trim().to_string());
    let tag = tag.filter(|value| !value.is_empty());

    let rows: Vec<MarketMetadataRow> = sqlx::query_as(
        r#"
        WITH latest_market AS (
          SELECT DISTINCT ON (m.market_id)
            m.market_id,
            m.question,
            m.outcomes,
            m.status,
            m.created_at
          FROM markets m
          WHERE m.active = TRUE
          ORDER BY m.market_id, m.created_at DESC, m.last_offset DESC, m.contract_id DESC
        )
        SELECT
          lm.market_id,
          lm.question,
          lm.outcomes,
          lm.status,
          lm.created_at,
          COALESCE(mm.category, 'General') AS category,
          COALESCE(mm.tags, '[]'::jsonb) AS tags,
          COALESCE(mm.featured, FALSE) AS featured,
          mm.resolution_time,
          mm.card_background_image_url,
          mm.hero_background_image_url,
          mm.thumbnail_image_url,
          mm.updated_at
        FROM latest_market lm
        LEFT JOIN market_metadata mm
          ON mm.market_id = lm.market_id
        WHERE ($1::TEXT IS NULL OR lm.market_id = $1)
          AND ($2::TEXT IS NULL OR COALESCE(mm.category, 'General') = $2)
          AND ($3::TEXT IS NULL OR COALESCE(mm.tags, '[]'::jsonb) ? $3)
          AND ($4::BOOL IS NULL OR COALESCE(mm.featured, FALSE) = $4)
        ORDER BY COALESCE(mm.featured, FALSE) DESC, lm.created_at DESC, lm.market_id ASC
        LIMIT $5
        "#,
    )
    .bind(market_id)
    .bind(category)
    .bind(tag)
    .bind(query.featured)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .context("query market metadata")?;

    let out = rows
        .into_iter()
        .map(|row| MarketMetadataView {
            market_id: row.market_id,
            question: row.question,
            outcomes: row.outcomes.0,
            status: row.status,
            created_at: row.created_at,
            category: row.category,
            tags: row.tags.0,
            featured: row.featured,
            resolution_time: row.resolution_time,
            card_background_image_url: row.card_background_image_url,
            hero_background_image_url: row.hero_background_image_url,
            thumbnail_image_url: row.thumbnail_image_url,
            updated_at: row.updated_at,
        })
        .collect();

    Ok(Json(out))
}

pub(crate) async fn admin_upsert_market_metadata(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(market_id): Path<String>,
    Json(req): Json<UpsertMarketMetadataRequest>,
) -> Result<Json<MarketMetadataView>, ApiError> {
    authenticate_admin(&state, &headers)?;
    let market_id = market_id.trim().to_string();
    if market_id.is_empty() {
        return Err(ApiError::bad_request("market_id must be non-empty"));
    }

    let exists: Option<bool> = sqlx::query_scalar(
        r#"
        SELECT TRUE
        FROM markets
        WHERE market_id = $1
          AND active = TRUE
        LIMIT 1
        "#,
    )
    .bind(&market_id)
    .fetch_optional(&state.db)
    .await
    .context("check market exists for metadata")?;
    if exists != Some(true) {
        return Err(ApiError::not_found("market not found"));
    }

    let current: Option<MarketMetadataCurrentRow> = sqlx::query_as(
        r#"
        SELECT
          category,
          tags,
          featured,
          resolution_time,
          card_background_image_url,
          hero_background_image_url,
          thumbnail_image_url
        FROM market_metadata
        WHERE market_id = $1
        "#,
    )
    .bind(&market_id)
    .fetch_optional(&state.db)
    .await
    .context("query current market metadata")?;

    let current_category = current
        .as_ref()
        .map(|row| row.category.clone())
        .unwrap_or_else(|| "General".to_string());
    let current_tags = current
        .as_ref()
        .map(|row| row.tags.0.clone())
        .unwrap_or_default();
    let current_featured = current.as_ref().map(|row| row.featured).unwrap_or(false);
    let current_resolution_time = current.as_ref().and_then(|row| row.resolution_time);
    let current_card_background_image_url = current
        .as_ref()
        .and_then(|row| row.card_background_image_url.clone());
    let current_hero_background_image_url = current
        .as_ref()
        .and_then(|row| row.hero_background_image_url.clone());
    let current_thumbnail_image_url = current
        .as_ref()
        .and_then(|row| row.thumbnail_image_url.clone());

    let category = normalize_market_category(req.category, &current_category)?;
    let tags = normalize_market_tags(req.tags, current_tags)?;
    let featured = req.featured.unwrap_or(current_featured);
    let resolution_time =
        normalize_market_resolution_time(req.resolution_time, current_resolution_time)?;
    let card_background_image_url = req
        .card_background_image_url
        .or(current_card_background_image_url);
    let hero_background_image_url = req
        .hero_background_image_url
        .or(current_hero_background_image_url);
    let thumbnail_image_url = req.thumbnail_image_url.or(current_thumbnail_image_url);

    sqlx::query(
        r#"
        INSERT INTO market_metadata (
          market_id,
          category,
          tags,
          featured,
          resolution_time,
          card_background_image_url,
          hero_background_image_url,
          thumbnail_image_url,
          created_at,
          updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now(), now())
        ON CONFLICT (market_id) DO UPDATE SET
          category = EXCLUDED.category,
          tags = EXCLUDED.tags,
          featured = EXCLUDED.featured,
          resolution_time = EXCLUDED.resolution_time,
          card_background_image_url = EXCLUDED.card_background_image_url,
          hero_background_image_url = EXCLUDED.hero_background_image_url,
          thumbnail_image_url = EXCLUDED.thumbnail_image_url,
          updated_at = now()
        "#,
    )
    .bind(&market_id)
    .bind(&category)
    .bind(SqlJson(tags))
    .bind(featured)
    .bind(resolution_time)
    .bind(card_background_image_url)
    .bind(hero_background_image_url)
    .bind(thumbnail_image_url)
    .execute(&state.db)
    .await
    .context("upsert market metadata")?;

    let row: Option<MarketMetadataRow> = sqlx::query_as(
        r#"
        WITH latest_market AS (
          SELECT DISTINCT ON (m.market_id)
            m.market_id,
            m.question,
            m.outcomes,
            m.status,
            m.created_at
          FROM markets m
          WHERE m.active = TRUE
            AND m.market_id = $1
          ORDER BY m.market_id, m.created_at DESC, m.last_offset DESC, m.contract_id DESC
        )
        SELECT
          lm.market_id,
          lm.question,
          lm.outcomes,
          lm.status,
          lm.created_at,
          COALESCE(mm.category, 'General') AS category,
          COALESCE(mm.tags, '[]'::jsonb) AS tags,
          COALESCE(mm.featured, FALSE) AS featured,
          mm.resolution_time,
          mm.card_background_image_url,
          mm.hero_background_image_url,
          mm.thumbnail_image_url,
          mm.updated_at
        FROM latest_market lm
        LEFT JOIN market_metadata mm
          ON mm.market_id = lm.market_id
        LIMIT 1
        "#,
    )
    .bind(&market_id)
    .fetch_optional(&state.db)
    .await
    .context("load market metadata response")?;

    let row = row.ok_or_else(|| ApiError::not_found("market not found"))?;
    Ok(Json(MarketMetadataView {
        market_id: row.market_id,
        question: row.question,
        outcomes: row.outcomes.0,
        status: row.status,
        created_at: row.created_at,
        category: row.category,
        tags: row.tags.0,
        featured: row.featured,
        resolution_time: row.resolution_time,
        card_background_image_url: row.card_background_image_url,
        hero_background_image_url: row.hero_background_image_url,
        thumbnail_image_url: row.thumbnail_image_url,
        updated_at: row.updated_at,
    }))
}

#[derive(Debug, Serialize)]
pub(crate) struct AdminAssetUploadResponse {
    pub(crate) asset_url: String,
    pub(crate) filename: String,
    pub(crate) content_type: String,
    pub(crate) bytes: usize,
}

struct UploadedAssetFile {
    bytes: Vec<u8>,
    file_name: Option<String>,
    content_type: Option<String>,
}

pub(crate) async fn admin_upload_market_asset(
    State(state): State<AppState>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<Json<AdminAssetUploadResponse>, ApiError> {
    authenticate_admin(&state, &headers)?;

    let mut market_id: Option<String> = None;
    let mut slot: Option<String> = None;
    let mut file: Option<UploadedAssetFile> = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|_| ApiError::bad_request("invalid multipart form data"))?
    {
        let field_name = field
            .name()
            .map(ToString::to_string)
            .ok_or_else(|| ApiError::bad_request("multipart field is missing a name"))?;
        match field_name.as_str() {
            "market_id" => {
                let raw = field
                    .text()
                    .await
                    .map_err(|_| ApiError::bad_request("invalid market_id field"))?;
                market_id = Some(raw);
            }
            "slot" => {
                let raw = field
                    .text()
                    .await
                    .map_err(|_| ApiError::bad_request("invalid slot field"))?;
                slot = Some(raw);
            }
            "file" => {
                if file.is_some() {
                    return Err(ApiError::bad_request(
                        "multipart form must contain exactly one file field",
                    ));
                }
                let file_name = field.file_name().map(ToString::to_string);
                let content_type = field.content_type().map(ToString::to_string);
                let bytes = field
                    .bytes()
                    .await
                    .map_err(|_| ApiError::bad_request("failed to read uploaded file"))?;
                let bytes = bytes.to_vec();
                if bytes.is_empty() {
                    return Err(ApiError::bad_request("uploaded file must not be empty"));
                }
                if bytes.len() > state.market_assets_cfg.max_upload_bytes {
                    return Err(ApiError::bad_request(format!(
                        "uploaded file exceeds max size ({} bytes)",
                        state.market_assets_cfg.max_upload_bytes
                    )));
                }

                file = Some(UploadedAssetFile {
                    bytes,
                    file_name,
                    content_type,
                });
            }
            _ => {}
        }
    }

    let file = file.ok_or_else(|| ApiError::bad_request("multipart form is missing file field"))?;
    let normalized_slot = normalize_asset_slot(slot)?;
    let market_slug = normalize_asset_market_slug(market_id);
    let extension = detect_image_extension(file.content_type.as_deref(), file.file_name.as_deref())
        .ok_or_else(|| {
            ApiError::bad_request(
                "unsupported image type; allowed: png, jpg, jpeg, webp, gif, avif",
            )
        })?;
    let content_type = normalize_content_type(file.content_type.as_deref(), extension);
    let filename = format!(
        "{}_{}_{}.{}",
        market_slug,
        normalized_slot,
        Uuid::new_v4(),
        extension
    );
    let path = state.market_assets_cfg.data_dir.join(&filename);

    tokio::fs::create_dir_all(&state.market_assets_cfg.data_dir)
        .await
        .context("create assets data directory")
        .map_err(|err| {
            tracing::error!(error = ?err, "failed to create assets data directory");
            ApiError::internal("failed to prepare asset storage")
        })?;

    tokio::fs::write(&path, &file.bytes)
        .await
        .with_context(|| format!("write uploaded asset file {}", path.display()))
        .map_err(|err| {
            tracing::error!(error = ?err, path = %path.display(), "failed to write uploaded asset file");
            ApiError::internal("failed to persist uploaded asset")
        })?;

    let asset_url = format!(
        "{}/{}",
        state
            .market_assets_cfg
            .public_base_url
            .trim_end_matches('/'),
        filename
    );

    Ok(Json(AdminAssetUploadResponse {
        asset_url,
        filename,
        content_type: content_type.to_string(),
        bytes: file.bytes.len(),
    }))
}

pub(crate) async fn list_public_market_stats(
    State(state): State<AppState>,
    Query(query): Query<PublicMarketStatsQuery>,
) -> Result<Json<Vec<PublicMarketStatsView>>, ApiError> {
    let limit = clamp_limit(query.limit);
    let market_id = query.market_id.map(|value| value.trim().to_string());
    let market_id = market_id.filter(|value| !value.is_empty());

    let rows: Vec<PublicMarketStatsRow> = sqlx::query_as(
        r#"
        SELECT
          m.market_id,
          m.question,
          m.status,
          m.resolved_outcome,
          m.created_at,
          COALESCE(f24.fills_24h, 0)::BIGINT AS fills_24h,
          COALESCE(f24.volume_24h_minor, 0)::BIGINT AS volume_24h_minor,
          COALESCE(oi.open_interest_minor, 0)::BIGINT AS open_interest_minor,
          flast.last_traded_price_ticks,
          flast.last_traded_at
        FROM markets m
        LEFT JOIN LATERAL (
          SELECT
            COUNT(*)::BIGINT AS fills_24h,
            COALESCE(SUM((price_ticks::NUMERIC * quantity_minor::NUMERIC)), 0)::BIGINT AS volume_24h_minor
          FROM fills f
          WHERE f.market_id = m.market_id
            AND f.matched_at >= now() - INTERVAL '24 hours'
        ) f24 ON TRUE
        LEFT JOIN LATERAL (
          SELECT
            COALESCE(SUM(p.net_quantity_minor), 0)::BIGINT AS open_interest_minor
          FROM positions p
          WHERE p.market_id = m.market_id
            AND p.net_quantity_minor > 0
        ) oi ON TRUE
        LEFT JOIN LATERAL (
          SELECT
            COALESCE(
              (
                SELECT normalized.outcome_text
                FROM (
                  SELECT
                    BTRIM(raw_outcome.value) AS outcome_text,
                    LOWER(BTRIM(raw_outcome.value)) AS outcome_norm,
                    raw_outcome.ordinality AS outcome_index
                  FROM jsonb_array_elements_text(m.outcomes) WITH ORDINALITY AS raw_outcome(value, ordinality)
                ) normalized
                WHERE normalized.outcome_norm IN ('yes', 'true')
                ORDER BY normalized.outcome_index
                LIMIT 1
              ),
              (
                SELECT BTRIM(raw_outcome.value)
                FROM jsonb_array_elements_text(m.outcomes) WITH ORDINALITY AS raw_outcome(value, ordinality)
                ORDER BY raw_outcome.ordinality
                LIMIT 1
              )
            ) AS chart_outcome
        ) chart ON TRUE
        LEFT JOIN LATERAL (
          SELECT
            f.price_ticks AS last_traded_price_ticks,
            f.matched_at AS last_traded_at
          FROM fills f
          WHERE f.market_id = m.market_id
            AND chart.chart_outcome IS NOT NULL
            AND f.outcome = chart.chart_outcome
          ORDER BY f.matched_at DESC, f.fill_sequence DESC
          LIMIT 1
        ) flast ON TRUE
        WHERE m.active = TRUE
          AND ($1::TEXT IS NULL OR m.market_id = $1)
        ORDER BY m.created_at DESC, m.market_id ASC
        LIMIT $2
        "#,
    )
    .bind(market_id)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .context("query public market stats")?;

    let out = rows
        .into_iter()
        .map(|row| PublicMarketStatsView {
            market_id: row.market_id,
            question: row.question,
            status: row.status,
            resolved_outcome: row.resolved_outcome,
            created_at: row.created_at,
            fills_24h: row.fills_24h,
            volume_24h_minor: row.volume_24h_minor,
            open_interest_minor: row.open_interest_minor,
            last_traded_price_ticks: row.last_traded_price_ticks,
            last_traded_at: row.last_traded_at,
        })
        .collect();

    Ok(Json(out))
}

#[derive(Debug, sqlx::FromRow)]
struct AccountSummaryRow {
    account_id: String,
    owner_party: String,
    committee_party: String,
    instrument_admin: String,
    instrument_id: String,
    account_status: String,
    cleared_cash_minor: i64,
    delta_pending_trades_minor: i64,
    locked_open_orders_minor: i64,
    pending_withdrawals_reserved_minor: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct TokenConfigRow {
    instrument_admin: String,
    instrument_id: String,
    symbol: String,
    decimals: i32,
    deposit_mode: String,
    inbound_requires_acceptance: bool,
    dust_threshold_minor: i64,
    hard_max_inputs_per_transfer: i64,
    operational_max_inputs_per_transfer: i64,
    target_utxo_count_min: Option<i64>,
    target_utxo_count_max: Option<i64>,
    withdrawal_fee_headroom_minor: Option<i64>,
    unexpected_fee_buffer_minor: Option<i64>,
    requires_deposit_id_metadata: Option<bool>,
    allowed_deposit_pending_status_classes: Option<SqlJson<Vec<String>>>,
    allowed_withdrawal_pending_status_classes: Option<SqlJson<Vec<String>>>,
    allowed_cancel_pending_status_classes: Option<SqlJson<Vec<String>>>,
}

#[derive(Debug, Serialize, Clone)]
pub(crate) struct TokenConfigView {
    pub(crate) instrument_admin: String,
    pub(crate) instrument_id: String,
    pub(crate) symbol: String,
    pub(crate) decimals: i32,
    pub(crate) deposit_mode: String,
    pub(crate) inbound_requires_acceptance: bool,
    pub(crate) dust_threshold_minor: i64,
    pub(crate) hard_max_inputs_per_transfer: i64,
    pub(crate) operational_max_inputs_per_transfer: i64,
    pub(crate) target_utxo_count_min: Option<i64>,
    pub(crate) target_utxo_count_max: Option<i64>,
    pub(crate) withdrawal_fee_headroom_minor: Option<i64>,
    pub(crate) unexpected_fee_buffer_minor: Option<i64>,
    pub(crate) requires_deposit_id_metadata: Option<bool>,
    pub(crate) allowed_deposit_pending_status_classes: Option<Vec<String>>,
    pub(crate) allowed_withdrawal_pending_status_classes: Option<Vec<String>>,
    pub(crate) allowed_cancel_pending_status_classes: Option<Vec<String>>,
}

impl From<TokenConfigRow> for TokenConfigView {
    fn from(row: TokenConfigRow) -> Self {
        Self {
            instrument_admin: row.instrument_admin,
            instrument_id: row.instrument_id,
            symbol: row.symbol,
            decimals: row.decimals,
            deposit_mode: row.deposit_mode,
            inbound_requires_acceptance: row.inbound_requires_acceptance,
            dust_threshold_minor: row.dust_threshold_minor,
            hard_max_inputs_per_transfer: row.hard_max_inputs_per_transfer,
            operational_max_inputs_per_transfer: row.operational_max_inputs_per_transfer,
            target_utxo_count_min: row.target_utxo_count_min,
            target_utxo_count_max: row.target_utxo_count_max,
            withdrawal_fee_headroom_minor: row.withdrawal_fee_headroom_minor,
            unexpected_fee_buffer_minor: row.unexpected_fee_buffer_minor,
            requires_deposit_id_metadata: row.requires_deposit_id_metadata,
            allowed_deposit_pending_status_classes: row
                .allowed_deposit_pending_status_classes
                .map(|x| x.0),
            allowed_withdrawal_pending_status_classes: row
                .allowed_withdrawal_pending_status_classes
                .map(|x| x.0),
            allowed_cancel_pending_status_classes: row
                .allowed_cancel_pending_status_classes
                .map(|x| x.0),
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct FeeScheduleRow {
    instrument_admin: String,
    instrument_id: String,
    deposit_policy: String,
    withdrawal_policy: String,
}

#[derive(Debug, Serialize, Clone)]
pub(crate) struct FeeScheduleView {
    pub(crate) instrument_admin: String,
    pub(crate) instrument_id: String,
    pub(crate) deposit_policy: String,
    pub(crate) withdrawal_policy: String,
}

impl From<FeeScheduleRow> for FeeScheduleView {
    fn from(row: FeeScheduleRow) -> Self {
        Self {
            instrument_admin: row.instrument_admin,
            instrument_id: row.instrument_id,
            deposit_policy: row.deposit_policy,
            withdrawal_policy: row.withdrawal_policy,
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct MySummaryView {
    pub(crate) account_id: String,
    pub(crate) owner_party: String,
    pub(crate) committee_party: String,
    pub(crate) instrument_admin: String,
    pub(crate) instrument_id: String,
    pub(crate) account_status: String,
    pub(crate) cleared_cash_minor: i64,
    pub(crate) delta_pending_trades_minor: i64,
    pub(crate) locked_open_orders_minor: i64,
    pub(crate) available_minor: i64,
    pub(crate) pending_withdrawals_reserved_minor: i64,
    pub(crate) withdrawable_minor: i64,
    pub(crate) effective_available_minor: i64,
    pub(crate) token_config: Option<TokenConfigView>,
    pub(crate) fee_schedule: Option<FeeScheduleView>,
}

pub(crate) async fn get_my_summary(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<MySummaryView>, ApiError> {
    let account_id = authenticate_user_account_id(&state, &headers)?;

    let summary = load_account_summary_row(&state.db, &account_id).await?;
    let token_config =
        load_active_token_config(&state.db, &summary.instrument_admin, &summary.instrument_id)
            .await?;
    let fee_schedule =
        load_active_fee_schedule(&state.db, &summary.instrument_admin, &summary.instrument_id)
            .await?;

    Ok(Json(build_my_summary_view(
        summary,
        token_config,
        fee_schedule,
    )?))
}

#[derive(Debug, Serialize)]
pub(crate) struct MyFundingCapacityView {
    pub(crate) account_id: String,
    pub(crate) instrument_admin: String,
    pub(crate) instrument_id: String,
    pub(crate) unlocked_holdings_minor: i64,
    pub(crate) unlocked_holdings_count: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct FundingCapacityRow {
    unlocked_holdings_minor: i64,
    unlocked_holdings_count: i64,
}

pub(crate) async fn get_my_funding_capacity(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<MyFundingCapacityView>, ApiError> {
    let account_id = authenticate_user_account_id(&state, &headers)?;
    let summary = load_account_summary_row(&state.db, &account_id).await?;

    let funding: FundingCapacityRow = sqlx::query_as(
        r#"
        SELECT
          COALESCE(SUM(amount_minor), 0)::BIGINT AS unlocked_holdings_minor,
          COUNT(*)::BIGINT AS unlocked_holdings_count
        FROM token_holdings
        WHERE active = TRUE
          AND owner_party = $1
          AND instrument_admin = $2
          AND instrument_id = $3
          AND lock_status_class IS NULL
        "#,
    )
    .bind(&summary.owner_party)
    .bind(&summary.instrument_admin)
    .bind(&summary.instrument_id)
    .fetch_one(&state.db)
    .await
    .context("query user funding capacity")?;

    Ok(Json(MyFundingCapacityView {
        account_id: summary.account_id,
        instrument_admin: summary.instrument_admin,
        instrument_id: summary.instrument_id,
        unlocked_holdings_minor: funding.unlocked_holdings_minor,
        unlocked_holdings_count: funding.unlocked_holdings_count,
    }))
}

#[derive(Debug, Serialize)]
pub(crate) struct DepositInstructionMetadataField {
    pub(crate) key: String,
    pub(crate) required: bool,
    pub(crate) value_hint: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct DepositInstructionsView {
    pub(crate) account_id: String,
    pub(crate) instrument_admin: String,
    pub(crate) instrument_id: String,
    pub(crate) recipient_party: String,
    pub(crate) reason_hint: String,
    pub(crate) required_metadata: Vec<DepositInstructionMetadataField>,
    pub(crate) token_config: Option<TokenConfigView>,
    pub(crate) fee_schedule: Option<FeeScheduleView>,
}

pub(crate) async fn get_my_deposit_instructions(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<DepositInstructionsView>, ApiError> {
    let account_id = authenticate_user_account_id(&state, &headers)?;

    let summary = load_account_summary_row(&state.db, &account_id).await?;
    let token_config =
        load_active_token_config(&state.db, &summary.instrument_admin, &summary.instrument_id)
            .await?;
    let fee_schedule =
        load_active_fee_schedule(&state.db, &summary.instrument_admin, &summary.instrument_id)
            .await?;

    let mut required_metadata = vec![DepositInstructionMetadataField {
        key: META_ACCOUNT_ID_KEY.to_string(),
        required: true,
        value_hint: summary.account_id.clone(),
    }];

    let requires_deposit_id_metadata = token_config
        .as_ref()
        .and_then(|cfg| cfg.requires_deposit_id_metadata)
        .unwrap_or(false);
    if requires_deposit_id_metadata {
        required_metadata.push(DepositInstructionMetadataField {
            key: META_DEPOSIT_ID_KEY.to_string(),
            required: true,
            value_hint: "<uuid>".to_string(),
        });
    }

    Ok(Json(DepositInstructionsView {
        account_id: summary.account_id.clone(),
        instrument_admin: summary.instrument_admin,
        instrument_id: summary.instrument_id,
        recipient_party: summary.committee_party,
        reason_hint: summary.account_id,
        required_metadata,
        token_config,
        fee_schedule,
    }))
}

#[derive(Debug, sqlx::FromRow)]
struct DepositPendingRow {
    contract_id: String,
    deposit_id: String,
    instrument_admin: String,
    instrument_id: String,
    lineage_root_instruction_cid: String,
    current_instruction_cid: String,
    step_seq: i64,
    expected_amount_minor: i64,
    metadata: SqlJson<serde_json::Value>,
    reason: String,
    created_at: DateTime<Utc>,
    last_offset: i64,
    latest_status_class: Option<String>,
    latest_output: Option<String>,
    latest_pending_actions: Option<SqlJson<Vec<String>>>,
}

#[derive(Debug, Serialize)]
pub(crate) struct DepositPendingView {
    pub(crate) contract_id: String,
    pub(crate) deposit_id: String,
    pub(crate) instrument_admin: String,
    pub(crate) instrument_id: String,
    pub(crate) lineage_root_instruction_cid: String,
    pub(crate) current_instruction_cid: String,
    pub(crate) step_seq: i64,
    pub(crate) expected_amount_minor: i64,
    pub(crate) metadata: serde_json::Value,
    pub(crate) reason: String,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) last_offset: i64,
    pub(crate) latest_status_class: Option<String>,
    pub(crate) latest_output: Option<String>,
    pub(crate) latest_pending_actions: Option<Vec<String>>,
}

pub(crate) async fn list_my_deposit_pendings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListLimitQuery>,
) -> Result<Json<Vec<DepositPendingView>>, ApiError> {
    let account_id = authenticate_user_account_id(&state, &headers)?;
    let limit = clamp_limit(query.limit);

    let rows: Vec<DepositPendingRow> = sqlx::query_as(
        r#"
        SELECT
          p.contract_id,
          p.deposit_id,
          p.instrument_admin,
          p.instrument_id,
          p.lineage_root_instruction_cid,
          p.current_instruction_cid,
          p.step_seq,
          p.expected_amount_minor,
          p.metadata,
          p.reason,
          p.created_at,
          p.last_offset,
          ti.status_class AS latest_status_class,
          ti.output AS latest_output,
          ti.pending_actions AS latest_pending_actions
        FROM deposit_pendings p
        LEFT JOIN token_transfer_instruction_latest til
          ON til.lineage_root_instruction_cid = p.lineage_root_instruction_cid
        LEFT JOIN token_transfer_instructions ti
          ON ti.contract_id = til.contract_id
        WHERE p.account_id = $1
          AND p.active = TRUE
        ORDER BY p.created_at DESC, p.contract_id DESC
        LIMIT $2
        "#,
    )
    .bind(&account_id)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .context("query my deposit pendings")?;

    let pendings = rows
        .into_iter()
        .map(|row| DepositPendingView {
            contract_id: row.contract_id,
            deposit_id: row.deposit_id,
            instrument_admin: row.instrument_admin,
            instrument_id: row.instrument_id,
            lineage_root_instruction_cid: row.lineage_root_instruction_cid,
            current_instruction_cid: row.current_instruction_cid,
            step_seq: row.step_seq,
            expected_amount_minor: row.expected_amount_minor,
            metadata: row.metadata.0,
            reason: row.reason,
            created_at: row.created_at,
            last_offset: row.last_offset,
            latest_status_class: row.latest_status_class,
            latest_output: row.latest_output,
            latest_pending_actions: row.latest_pending_actions.map(|x| x.0),
        })
        .collect();

    Ok(Json(pendings))
}

#[derive(Debug, sqlx::FromRow)]
struct WithdrawalPendingRow {
    contract_id: String,
    withdrawal_id: String,
    instrument_admin: String,
    instrument_id: String,
    amount_minor: i64,
    lineage_root_instruction_cid: String,
    current_instruction_cid: String,
    step_seq: i64,
    pending_state: String,
    pending_actions: SqlJson<Vec<String>>,
    metadata: SqlJson<serde_json::Value>,
    reason: String,
    created_at: DateTime<Utc>,
    last_offset: i64,
    latest_status_class: Option<String>,
    latest_output: Option<String>,
    latest_pending_actions: Option<SqlJson<Vec<String>>>,
}

#[derive(Debug, Serialize)]
pub(crate) struct WithdrawalPendingView {
    pub(crate) contract_id: String,
    pub(crate) withdrawal_id: String,
    pub(crate) instrument_admin: String,
    pub(crate) instrument_id: String,
    pub(crate) amount_minor: i64,
    pub(crate) lineage_root_instruction_cid: String,
    pub(crate) current_instruction_cid: String,
    pub(crate) step_seq: i64,
    pub(crate) pending_state: String,
    pub(crate) pending_actions: Vec<String>,
    pub(crate) metadata: serde_json::Value,
    pub(crate) reason: String,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) last_offset: i64,
    pub(crate) latest_status_class: Option<String>,
    pub(crate) latest_output: Option<String>,
    pub(crate) latest_pending_actions: Option<Vec<String>>,
}

pub(crate) async fn list_my_withdrawal_pendings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListLimitQuery>,
) -> Result<Json<Vec<WithdrawalPendingView>>, ApiError> {
    let account_id = authenticate_user_account_id(&state, &headers)?;
    let limit = clamp_limit(query.limit);

    let rows: Vec<WithdrawalPendingRow> = sqlx::query_as(
        r#"
        SELECT
          w.contract_id,
          w.withdrawal_id,
          w.instrument_admin,
          w.instrument_id,
          w.amount_minor,
          w.lineage_root_instruction_cid,
          w.current_instruction_cid,
          w.step_seq,
          w.pending_state,
          w.pending_actions,
          w.metadata,
          w.reason,
          w.created_at,
          w.last_offset,
          ti.status_class AS latest_status_class,
          ti.output AS latest_output,
          ti.pending_actions AS latest_pending_actions
        FROM withdrawal_pendings w
        LEFT JOIN token_transfer_instruction_latest til
          ON til.lineage_root_instruction_cid = w.lineage_root_instruction_cid
        LEFT JOIN token_transfer_instructions ti
          ON ti.contract_id = til.contract_id
        WHERE w.account_id = $1
          AND w.active = TRUE
        ORDER BY w.created_at DESC, w.contract_id DESC
        LIMIT $2
        "#,
    )
    .bind(&account_id)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .context("query my withdrawal pendings")?;

    let pendings = rows
        .into_iter()
        .map(|row| WithdrawalPendingView {
            contract_id: row.contract_id,
            withdrawal_id: row.withdrawal_id,
            instrument_admin: row.instrument_admin,
            instrument_id: row.instrument_id,
            amount_minor: row.amount_minor,
            lineage_root_instruction_cid: row.lineage_root_instruction_cid,
            current_instruction_cid: row.current_instruction_cid,
            step_seq: row.step_seq,
            pending_state: row.pending_state,
            pending_actions: row.pending_actions.0,
            metadata: row.metadata.0,
            reason: row.reason,
            created_at: row.created_at,
            last_offset: row.last_offset,
            latest_status_class: row.latest_status_class,
            latest_output: row.latest_output,
            latest_pending_actions: row.latest_pending_actions.map(|x| x.0),
        })
        .collect();

    Ok(Json(pendings))
}

#[derive(Debug, Serialize)]
pub(crate) struct ReceiptView {
    pub(crate) receipt_id: String,
    pub(crate) kind: String,
    pub(crate) status: String,
    pub(crate) amount_minor: i64,
    pub(crate) instrument_admin: String,
    pub(crate) instrument_id: String,
    pub(crate) reference_id: String,
    pub(crate) lineage_root_instruction_cid: String,
    pub(crate) terminal_instruction_cid: String,
    pub(crate) created_at: DateTime<Utc>,
}

#[derive(Debug, sqlx::FromRow)]
struct DepositReceiptRow {
    contract_id: String,
    deposit_id: String,
    instrument_admin: String,
    instrument_id: String,
    credited_minor: i64,
    lineage_root_instruction_cid: String,
    terminal_instruction_cid: String,
    created_at: DateTime<Utc>,
}

#[derive(Debug, sqlx::FromRow)]
struct WithdrawalReceiptRow {
    contract_id: String,
    withdrawal_id: String,
    instrument_admin: String,
    instrument_id: String,
    amount_minor: i64,
    status: String,
    lineage_root_instruction_cid: String,
    terminal_instruction_cid: String,
    created_at: DateTime<Utc>,
}

pub(crate) async fn list_my_receipts(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ListLimitQuery>,
) -> Result<Json<Vec<ReceiptView>>, ApiError> {
    let account_id = authenticate_user_account_id(&state, &headers)?;
    let limit = clamp_limit(query.limit);

    let deposit_rows: Vec<DepositReceiptRow> = sqlx::query_as(
        r#"
        SELECT
          contract_id,
          deposit_id,
          instrument_admin,
          instrument_id,
          credited_minor,
          lineage_root_instruction_cid,
          terminal_instruction_cid,
          created_at
        FROM deposit_receipts
        WHERE account_id = $1
          AND active = TRUE
        ORDER BY created_at DESC, contract_id DESC
        LIMIT $2
        "#,
    )
    .bind(&account_id)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .context("query my deposit receipts")?;

    let withdrawal_rows: Vec<WithdrawalReceiptRow> = sqlx::query_as(
        r#"
        SELECT
          contract_id,
          withdrawal_id,
          instrument_admin,
          instrument_id,
          amount_minor,
          status,
          lineage_root_instruction_cid,
          terminal_instruction_cid,
          created_at
        FROM withdrawal_receipts
        WHERE account_id = $1
          AND active = TRUE
        ORDER BY created_at DESC, contract_id DESC
        LIMIT $2
        "#,
    )
    .bind(&account_id)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .context("query my withdrawal receipts")?;

    let mut receipts = Vec::with_capacity(deposit_rows.len() + withdrawal_rows.len());

    for row in deposit_rows {
        receipts.push(ReceiptView {
            receipt_id: row.contract_id,
            kind: "Deposit".to_string(),
            status: "Credited".to_string(),
            amount_minor: row.credited_minor,
            instrument_admin: row.instrument_admin,
            instrument_id: row.instrument_id,
            reference_id: row.deposit_id,
            lineage_root_instruction_cid: row.lineage_root_instruction_cid,
            terminal_instruction_cid: row.terminal_instruction_cid,
            created_at: row.created_at,
        });
    }

    for row in withdrawal_rows {
        receipts.push(ReceiptView {
            receipt_id: row.contract_id,
            kind: "Withdrawal".to_string(),
            status: row.status,
            amount_minor: row.amount_minor,
            instrument_admin: row.instrument_admin,
            instrument_id: row.instrument_id,
            reference_id: row.withdrawal_id,
            lineage_root_instruction_cid: row.lineage_root_instruction_cid,
            terminal_instruction_cid: row.terminal_instruction_cid,
            created_at: row.created_at,
        });
    }

    receipts.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then_with(|| right.receipt_id.cmp(&left.receipt_id))
    });

    let limit_usize = usize::try_from(limit).map_err(|_| ApiError::internal("invalid limit"))?;
    if receipts.len() > limit_usize {
        receipts.truncate(limit_usize);
    }

    Ok(Json(receipts))
}

#[derive(Debug, sqlx::FromRow)]
struct AdminQuarantineRow {
    contract_id: String,
    instrument_admin: String,
    instrument_id: String,
    holding_cid: String,
    reason: String,
    related_account_id: Option<String>,
    related_deposit_id: Option<String>,
    created_at: DateTime<Utc>,
    last_offset: i64,
    holding_amount_minor: Option<i64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct AdminQuarantineView {
    pub(crate) contract_id: String,
    pub(crate) instrument_admin: String,
    pub(crate) instrument_id: String,
    pub(crate) holding_cid: String,
    pub(crate) reason: String,
    pub(crate) related_account_id: Option<String>,
    pub(crate) related_deposit_id: Option<String>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) last_offset: i64,
    pub(crate) holding_amount_minor: Option<i64>,
}

pub(crate) async fn admin_list_quarantine(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<InstrumentFilterQuery>,
) -> Result<Json<Vec<AdminQuarantineView>>, ApiError> {
    authenticate_admin(&state, &headers)?;
    let limit = clamp_limit(query.limit);

    let rows: Vec<AdminQuarantineRow> = sqlx::query_as(
        r#"
        SELECT
          q.contract_id,
          q.instrument_admin,
          q.instrument_id,
          q.holding_cid,
          q.reason,
          q.related_account_id,
          q.related_deposit_id,
          q.created_at,
          q.last_offset,
          h.amount_minor AS holding_amount_minor
        FROM quarantined_holdings q
        LEFT JOIN token_holdings h
          ON h.contract_id = q.holding_cid
         AND h.active = TRUE
        WHERE q.active = TRUE
          AND ($1::TEXT IS NULL OR q.instrument_admin = $1)
          AND ($2::TEXT IS NULL OR q.instrument_id = $2)
        ORDER BY q.created_at DESC, q.contract_id DESC
        LIMIT $3
        "#,
    )
    .bind(query.instrument_admin)
    .bind(query.instrument_id)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .context("query quarantine queue")?;

    let out = rows
        .into_iter()
        .map(|row| AdminQuarantineView {
            contract_id: row.contract_id,
            instrument_admin: row.instrument_admin,
            instrument_id: row.instrument_id,
            holding_cid: row.holding_cid,
            reason: row.reason,
            related_account_id: row.related_account_id,
            related_deposit_id: row.related_deposit_id,
            created_at: row.created_at,
            last_offset: row.last_offset,
            holding_amount_minor: row.holding_amount_minor,
        })
        .collect();

    Ok(Json(out))
}

#[derive(Debug, sqlx::FromRow)]
struct AdminEscalatedWithdrawalRow {
    contract_id: String,
    owner_party: String,
    account_id: String,
    instrument_admin: String,
    instrument_id: String,
    withdrawal_id: String,
    amount_minor: i64,
    current_instruction_cid: String,
    step_seq: i64,
    pending_actions: SqlJson<Vec<String>>,
    created_at: DateTime<Utc>,
    last_offset: i64,
    latest_status_class: Option<String>,
    latest_output: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct AdminEscalatedWithdrawalView {
    pub(crate) contract_id: String,
    pub(crate) owner_party: String,
    pub(crate) account_id: String,
    pub(crate) instrument_admin: String,
    pub(crate) instrument_id: String,
    pub(crate) withdrawal_id: String,
    pub(crate) amount_minor: i64,
    pub(crate) current_instruction_cid: String,
    pub(crate) step_seq: i64,
    pub(crate) pending_actions: Vec<String>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) last_offset: i64,
    pub(crate) latest_status_class: Option<String>,
    pub(crate) latest_output: Option<String>,
}

pub(crate) async fn admin_list_cancel_escalated(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<InstrumentFilterQuery>,
) -> Result<Json<Vec<AdminEscalatedWithdrawalView>>, ApiError> {
    authenticate_admin(&state, &headers)?;
    let limit = clamp_limit(query.limit);

    let rows: Vec<AdminEscalatedWithdrawalRow> = sqlx::query_as(
        r#"
        SELECT
          w.contract_id,
          w.owner_party,
          w.account_id,
          w.instrument_admin,
          w.instrument_id,
          w.withdrawal_id,
          w.amount_minor,
          w.current_instruction_cid,
          w.step_seq,
          w.pending_actions,
          w.created_at,
          w.last_offset,
          ti.status_class AS latest_status_class,
          ti.output AS latest_output
        FROM withdrawal_pendings w
        LEFT JOIN token_transfer_instruction_latest til
          ON til.lineage_root_instruction_cid = w.lineage_root_instruction_cid
        LEFT JOIN token_transfer_instructions ti
          ON ti.contract_id = til.contract_id
        WHERE w.active = TRUE
          AND w.pending_state = 'CancelEscalated'
          AND ($1::TEXT IS NULL OR w.instrument_admin = $1)
          AND ($2::TEXT IS NULL OR w.instrument_id = $2)
        ORDER BY w.created_at DESC, w.contract_id DESC
        LIMIT $3
        "#,
    )
    .bind(query.instrument_admin)
    .bind(query.instrument_id)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .context("query cancel escalated withdrawals")?;

    let out = rows
        .into_iter()
        .map(|row| AdminEscalatedWithdrawalView {
            contract_id: row.contract_id,
            owner_party: row.owner_party,
            account_id: row.account_id,
            instrument_admin: row.instrument_admin,
            instrument_id: row.instrument_id,
            withdrawal_id: row.withdrawal_id,
            amount_minor: row.amount_minor,
            current_instruction_cid: row.current_instruction_cid,
            step_seq: row.step_seq,
            pending_actions: row.pending_actions.0,
            created_at: row.created_at,
            last_offset: row.last_offset,
            latest_status_class: row.latest_status_class,
            latest_output: row.latest_output,
        })
        .collect();

    Ok(Json(out))
}

#[derive(Debug, sqlx::FromRow)]
struct AdminInventoryRow {
    instrument_admin: String,
    instrument_id: String,
    total_holdings: i64,
    unlocked_holdings: i64,
    locked_holdings: i64,
    lock_expired_holdings: i64,
    reserved_holdings: i64,
    dust_holdings: i64,
    total_amount_minor: i64,
    target_utxo_count_min: Option<i64>,
    target_utxo_count_max: Option<i64>,
    dust_threshold_minor: Option<i64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct AdminInventoryView {
    pub(crate) instrument_admin: String,
    pub(crate) instrument_id: String,
    pub(crate) total_holdings: i64,
    pub(crate) unlocked_holdings: i64,
    pub(crate) locked_holdings: i64,
    pub(crate) lock_expired_holdings: i64,
    pub(crate) reserved_holdings: i64,
    pub(crate) dust_holdings: i64,
    pub(crate) dust_ratio: f64,
    pub(crate) total_amount_minor: i64,
    pub(crate) target_utxo_count_min: Option<i64>,
    pub(crate) target_utxo_count_max: Option<i64>,
    pub(crate) dust_threshold_minor: Option<i64>,
}

pub(crate) async fn admin_treasury_inventory(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<InstrumentFilterQuery>,
) -> Result<Json<Vec<AdminInventoryView>>, ApiError> {
    authenticate_admin(&state, &headers)?;
    let limit = clamp_limit(query.limit);

    let rows: Vec<AdminInventoryRow> = sqlx::query_as(
        r#"
        WITH reserved AS (
          SELECT DISTINCT holding_cid
          FROM treasury_holding_reservations
          WHERE state IN ('Reserved', 'Submitting')
        )
        SELECT
          h.instrument_admin,
          h.instrument_id,
          COUNT(*)::BIGINT AS total_holdings,
          COUNT(*) FILTER (WHERE h.lock_status_class IS NULL)::BIGINT AS unlocked_holdings,
          COUNT(*) FILTER (WHERE h.lock_status_class IS NOT NULL)::BIGINT AS locked_holdings,
          COUNT(*) FILTER (
            WHERE h.lock_status_class IS NOT NULL
              AND h.lock_expires_at IS NOT NULL
              AND h.lock_expires_at < now()
          )::BIGINT AS lock_expired_holdings,
          COUNT(*) FILTER (WHERE r.holding_cid IS NOT NULL)::BIGINT AS reserved_holdings,
          COUNT(*) FILTER (
            WHERE h.amount_minor <= COALESCE(cfg.dust_threshold_minor, 0)
          )::BIGINT AS dust_holdings,
          COALESCE(SUM(h.amount_minor), 0)::BIGINT AS total_amount_minor,
          cfg.target_utxo_count_min,
          cfg.target_utxo_count_max,
          cfg.dust_threshold_minor
        FROM token_holdings h
        LEFT JOIN reserved r
          ON r.holding_cid = h.contract_id
        LEFT JOIN token_configs cfg
          ON cfg.instrument_admin = h.instrument_admin
         AND cfg.instrument_id = h.instrument_id
         AND cfg.active = TRUE
        WHERE h.active = TRUE
          AND h.owner_party = $1
          AND ($2::TEXT IS NULL OR h.instrument_admin = $2)
          AND ($3::TEXT IS NULL OR h.instrument_id = $3)
        GROUP BY
          h.instrument_admin,
          h.instrument_id,
          cfg.target_utxo_count_min,
          cfg.target_utxo_count_max,
          cfg.dust_threshold_minor
        ORDER BY h.instrument_admin, h.instrument_id
        LIMIT $4
        "#,
    )
    .bind(&state.ledger_cfg.committee_party)
    .bind(query.instrument_admin)
    .bind(query.instrument_id)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .context("query treasury inventory")?;

    let out = rows
        .into_iter()
        .map(|row| {
            let dust_ratio = if row.total_holdings == 0 {
                0.0
            } else {
                row.dust_holdings as f64 / row.total_holdings as f64
            };

            AdminInventoryView {
                instrument_admin: row.instrument_admin,
                instrument_id: row.instrument_id,
                total_holdings: row.total_holdings,
                unlocked_holdings: row.unlocked_holdings,
                locked_holdings: row.locked_holdings,
                lock_expired_holdings: row.lock_expired_holdings,
                reserved_holdings: row.reserved_holdings,
                dust_holdings: row.dust_holdings,
                dust_ratio,
                total_amount_minor: row.total_amount_minor,
                target_utxo_count_min: row.target_utxo_count_min,
                target_utxo_count_max: row.target_utxo_count_max,
                dust_threshold_minor: row.dust_threshold_minor,
            }
        })
        .collect();

    Ok(Json(out))
}

#[derive(Debug, sqlx::FromRow)]
struct AdminTreasuryOpRow {
    op_id: String,
    op_type: String,
    instrument_admin: String,
    instrument_id: String,
    owner_party: Option<String>,
    account_id: Option<String>,
    amount_minor: Option<i64>,
    state: String,
    step_seq: i64,
    command_id: String,
    lineage_root_instruction_cid: Option<String>,
    current_instruction_cid: Option<String>,
    last_error: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub(crate) struct AdminTreasuryOpView {
    pub(crate) op_id: String,
    pub(crate) op_type: String,
    pub(crate) instrument_admin: String,
    pub(crate) instrument_id: String,
    pub(crate) owner_party: Option<String>,
    pub(crate) account_id: Option<String>,
    pub(crate) amount_minor: Option<i64>,
    pub(crate) state: String,
    pub(crate) step_seq: i64,
    pub(crate) command_id: String,
    pub(crate) lineage_root_instruction_cid: Option<String>,
    pub(crate) current_instruction_cid: Option<String>,
    pub(crate) last_error: Option<String>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) updated_at: DateTime<Utc>,
}

pub(crate) async fn admin_list_treasury_ops(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<TreasuryOpsQuery>,
) -> Result<Json<Vec<AdminTreasuryOpView>>, ApiError> {
    authenticate_admin(&state, &headers)?;
    let limit = clamp_limit(query.limit);

    let rows: Vec<AdminTreasuryOpRow> = sqlx::query_as(
        r#"
        SELECT
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
          lineage_root_instruction_cid,
          current_instruction_cid,
          last_error,
          created_at,
          updated_at
        FROM treasury_ops
        WHERE ($1::TEXT IS NULL OR state = $1)
          AND ($2::TEXT IS NULL OR op_type = $2)
          AND ($3::TEXT IS NULL OR account_id = $3)
        ORDER BY updated_at DESC, op_id DESC
        LIMIT $4
        "#,
    )
    .bind(query.state)
    .bind(query.op_type)
    .bind(query.account_id)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .context("query treasury ops")?;

    let out = rows
        .into_iter()
        .map(|row| AdminTreasuryOpView {
            op_id: row.op_id,
            op_type: row.op_type,
            instrument_admin: row.instrument_admin,
            instrument_id: row.instrument_id,
            owner_party: row.owner_party,
            account_id: row.account_id,
            amount_minor: row.amount_minor,
            state: row.state,
            step_seq: row.step_seq,
            command_id: row.command_id,
            lineage_root_instruction_cid: row.lineage_root_instruction_cid,
            current_instruction_cid: row.current_instruction_cid,
            last_error: row.last_error,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
        .collect();

    Ok(Json(out))
}

#[derive(Debug, sqlx::FromRow)]
struct AdminDriftRow {
    instrument_admin: String,
    instrument_id: String,
    total_holdings_minor: i64,
    cleared_liabilities_minor: i64,
    pending_withdrawals_minor: i64,
    pending_deposits_minor: i64,
    quarantined_minor: i64,
    available_liquidity_minor: i64,
}

#[derive(Debug, Serialize)]
pub(crate) struct AdminDriftView {
    pub(crate) instrument_admin: String,
    pub(crate) instrument_id: String,
    pub(crate) total_holdings_minor: i64,
    pub(crate) cleared_liabilities_minor: i64,
    pub(crate) pending_withdrawals_minor: i64,
    pub(crate) pending_deposits_minor: i64,
    pub(crate) quarantined_minor: i64,
    pub(crate) available_liquidity_minor: i64,
    pub(crate) implied_obligations_minor: i64,
    pub(crate) coverage_minor: i64,
}

pub(crate) async fn admin_drift_dashboard(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<InstrumentFilterQuery>,
) -> Result<Json<Vec<AdminDriftView>>, ApiError> {
    authenticate_admin(&state, &headers)?;
    let limit = clamp_limit(query.limit);

    let rows: Vec<AdminDriftRow> = sqlx::query_as(
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
        WHERE ($2::TEXT IS NULL OR i.instrument_admin = $2)
          AND ($3::TEXT IS NULL OR i.instrument_id = $3)
        ORDER BY i.instrument_admin, i.instrument_id
        LIMIT $4
        "#,
    )
    .bind(&state.ledger_cfg.committee_party)
    .bind(query.instrument_admin)
    .bind(query.instrument_id)
    .bind(limit)
    .fetch_all(&state.db)
    .await
    .context("query drift dashboard")?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let implied_obligations_i128 = i128::from(row.cleared_liabilities_minor)
            + i128::from(row.pending_withdrawals_minor)
            + i128::from(row.pending_deposits_minor)
            + i128::from(row.quarantined_minor);
        let implied_obligations_minor = i64::try_from(implied_obligations_i128)
            .map_err(|_| ApiError::internal("implied obligations overflow"))?;

        let coverage_i128 = i128::from(row.total_holdings_minor) - implied_obligations_i128;
        let coverage_minor =
            i64::try_from(coverage_i128).map_err(|_| ApiError::internal("coverage overflow"))?;

        out.push(AdminDriftView {
            instrument_admin: row.instrument_admin,
            instrument_id: row.instrument_id,
            total_holdings_minor: row.total_holdings_minor,
            cleared_liabilities_minor: row.cleared_liabilities_minor,
            pending_withdrawals_minor: row.pending_withdrawals_minor,
            pending_deposits_minor: row.pending_deposits_minor,
            quarantined_minor: row.quarantined_minor,
            available_liquidity_minor: row.available_liquidity_minor,
            implied_obligations_minor,
            coverage_minor,
        });
    }

    Ok(Json(out))
}

fn normalize_asset_slot(raw: Option<String>) -> Result<String, ApiError> {
    let normalized = raw
        .unwrap_or_else(|| "asset".to_string())
        .trim()
        .to_ascii_lowercase()
        .replace(['-', ' '], "_");

    if normalized.is_empty() {
        return Err(ApiError::bad_request("slot must be non-empty"));
    }

    let allowed = ["asset", "card_background", "hero_background", "thumbnail"];
    if !allowed.contains(&normalized.as_str()) {
        return Err(ApiError::bad_request(
            "slot must be one of: card_background, hero_background, thumbnail",
        ));
    }

    Ok(normalized)
}

fn normalize_asset_market_slug(raw: Option<String>) -> String {
    let value = raw.unwrap_or_else(|| "market".to_string());
    let mut out = String::new();
    for ch in value.trim().chars() {
        let next = match ch {
            'A'..='Z' => ch.to_ascii_lowercase(),
            'a'..='z' | '0'..='9' | '_' | '-' => ch,
            _ => '-',
        };
        out.push(next);
    }

    let out = out.trim_matches('-');
    if out.is_empty() {
        return "market".to_string();
    }
    out.to_string()
}

fn detect_image_extension(
    content_type: Option<&str>,
    file_name: Option<&str>,
) -> Option<&'static str> {
    if let Some(kind) = content_type.and_then(content_type_to_extension) {
        return Some(kind);
    }
    file_name.and_then(file_name_to_extension)
}

fn content_type_to_extension(content_type: &str) -> Option<&'static str> {
    match content_type.trim().to_ascii_lowercase().as_str() {
        "image/png" => Some("png"),
        "image/jpeg" => Some("jpg"),
        "image/webp" => Some("webp"),
        "image/gif" => Some("gif"),
        "image/avif" => Some("avif"),
        _ => None,
    }
}

fn file_name_to_extension(file_name: &str) -> Option<&'static str> {
    let extension = file_name.rsplit_once('.')?.1.trim().to_ascii_lowercase();
    match extension.as_str() {
        "png" => Some("png"),
        "jpg" | "jpeg" => Some("jpg"),
        "webp" => Some("webp"),
        "gif" => Some("gif"),
        "avif" => Some("avif"),
        _ => None,
    }
}

fn normalize_content_type(content_type: Option<&str>, extension: &str) -> &'static str {
    if let Some(content_type) = content_type {
        match content_type.trim().to_ascii_lowercase().as_str() {
            "image/png" => return "image/png",
            "image/jpeg" => return "image/jpeg",
            "image/webp" => return "image/webp",
            "image/gif" => return "image/gif",
            "image/avif" => return "image/avif",
            _ => {}
        }
    }

    match extension {
        "png" => "image/png",
        "jpg" => "image/jpeg",
        "webp" => "image/webp",
        "gif" => "image/gif",
        "avif" => "image/avif",
        _ => "application/octet-stream",
    }
}

fn clamp_limit(limit: Option<i64>) -> i64 {
    let raw = limit.unwrap_or(DEFAULT_QUERY_LIMIT);
    raw.clamp(1, MAX_QUERY_LIMIT)
}

fn required_header(headers: &HeaderMap, name: &str) -> Result<String, ApiError> {
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

fn optional_header(headers: &HeaderMap, name: &str) -> Result<Option<String>, ApiError> {
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

fn authenticate_user_account_id(state: &AppState, headers: &HeaderMap) -> Result<String, ApiError> {
    if state.auth_keys.api_key_count() == 0 {
        return Err(ApiError::unauthorized("no API keys configured"));
    }

    let admin_key = optional_header(headers, "x-admin-key")?;
    if admin_key.is_some() {
        return Err(ApiError::forbidden(
            "x-admin-key is not allowed on user endpoints",
        ));
    }

    let key = required_header(headers, "x-api-key")?;
    let Some(account_id) = state.auth_keys.account_id_for_api_key(&key) else {
        return Err(ApiError::unauthorized("invalid API key"));
    };

    Ok(account_id)
}

fn authenticate_admin(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    if state.auth_keys.admin_key_count() == 0 {
        return Err(ApiError::unauthorized("no admin API keys configured"));
    }

    let user_key = optional_header(headers, "x-api-key")?;
    if user_key.is_some() {
        return Err(ApiError::forbidden(
            "x-api-key is not allowed on admin endpoints",
        ));
    }

    let key = required_header(headers, "x-admin-key")?;
    if !state.auth_keys.is_admin_key(&key) {
        return Err(ApiError::unauthorized("invalid admin API key"));
    }

    Ok(())
}

pub(crate) fn require_admin(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    authenticate_admin(state, headers)
}

pub(crate) fn require_user_account(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<String, ApiError> {
    authenticate_user_account_id(state, headers)
}

fn reject_mixed_auth_headers(
    user_key: &Option<String>,
    admin_key: &Option<String>,
) -> Result<(), ApiError> {
    if user_key.is_some() && admin_key.is_some() {
        return Err(ApiError::forbidden(
            "x-api-key and x-admin-key cannot be used together",
        ));
    }
    Ok(())
}

fn normalize_market_category(raw: Option<String>, fallback: &str) -> Result<String, ApiError> {
    let Some(category) = raw else {
        return Ok(fallback.to_string());
    };

    let category = category.trim().to_string();
    if category.is_empty() {
        return Err(ApiError::bad_request("category must be non-empty"));
    }
    if category.len() > 64 {
        return Err(ApiError::bad_request("category length must be <= 64"));
    }

    Ok(category)
}

fn normalize_market_resolution_time(
    raw: Option<String>,
    fallback: Option<DateTime<Utc>>,
) -> Result<Option<DateTime<Utc>>, ApiError> {
    let Some(raw) = raw else {
        return Ok(fallback);
    };

    let normalized = raw.trim();
    if normalized.is_empty() {
        return Err(ApiError::bad_request(
            "resolution_time must be a non-empty RFC3339 timestamp",
        ));
    }

    let parsed = DateTime::parse_from_rfc3339(normalized)
        .map_err(|_| ApiError::bad_request("resolution_time must be a valid RFC3339 timestamp"))?;
    Ok(Some(parsed.with_timezone(&Utc)))
}

fn normalize_market_tags(
    raw: Option<Vec<String>>,
    fallback: Vec<String>,
) -> Result<Vec<String>, ApiError> {
    let Some(tags) = raw else {
        return Ok(fallback);
    };

    if tags.len() > 16 {
        return Err(ApiError::bad_request("tags length must be <= 16"));
    }

    let mut normalized = Vec::new();
    let mut seen = HashSet::new();

    for tag in tags {
        let tag = tag.trim().to_string();
        if tag.is_empty() {
            return Err(ApiError::bad_request("tags must not contain empty values"));
        }
        if tag.len() > 32 {
            return Err(ApiError::bad_request(
                "each tag length must be <= 32 characters",
            ));
        }
        if seen.insert(tag.clone()) {
            normalized.push(tag);
        }
    }

    Ok(normalized)
}

fn checked_add3(a: i64, b: i64, c: i64) -> Result<i64, ApiError> {
    let ab = a
        .checked_add(b)
        .ok_or_else(|| ApiError::internal("integer overflow"))?;
    ab.checked_add(c)
        .ok_or_else(|| ApiError::internal("integer overflow"))
}

async fn load_account_summary_row(
    db: &PgPool,
    account_id: &str,
) -> Result<AccountSummaryRow, ApiError> {
    let row: Option<AccountSummaryRow> = sqlx::query_as(
        r#"
        SELECT
          ar.account_id,
          ar.owner_party,
          ar.committee_party,
          ar.instrument_admin,
          ar.instrument_id,
          ar.status AS account_status,
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
    .fetch_optional(db)
    .await
    .context("query account summary")?;

    row.ok_or_else(|| ApiError::not_found("account not found"))
}

async fn load_active_token_config(
    db: &PgPool,
    instrument_admin: &str,
    instrument_id: &str,
) -> Result<Option<TokenConfigView>, ApiError> {
    let row: Option<TokenConfigRow> = sqlx::query_as(
        r#"
        SELECT
          instrument_admin,
          instrument_id,
          symbol,
          decimals,
          deposit_mode,
          inbound_requires_acceptance,
          dust_threshold_minor,
          hard_max_inputs_per_transfer,
          operational_max_inputs_per_transfer,
          target_utxo_count_min,
          target_utxo_count_max,
          withdrawal_fee_headroom_minor,
          unexpected_fee_buffer_minor,
          requires_deposit_id_metadata,
          allowed_deposit_pending_status_classes,
          allowed_withdrawal_pending_status_classes,
          allowed_cancel_pending_status_classes
        FROM token_configs
        WHERE instrument_admin = $1
          AND instrument_id = $2
          AND active = TRUE
        ORDER BY created_at DESC, contract_id DESC
        LIMIT 1
        "#,
    )
    .bind(instrument_admin)
    .bind(instrument_id)
    .fetch_optional(db)
    .await
    .context("query active token config")?;

    Ok(row.map(TokenConfigView::from))
}

async fn load_active_fee_schedule(
    db: &PgPool,
    instrument_admin: &str,
    instrument_id: &str,
) -> Result<Option<FeeScheduleView>, ApiError> {
    let row: Option<FeeScheduleRow> = sqlx::query_as(
        r#"
        SELECT
          instrument_admin,
          instrument_id,
          deposit_policy,
          withdrawal_policy
        FROM fee_schedules
        WHERE instrument_admin = $1
          AND instrument_id = $2
          AND active = TRUE
        ORDER BY created_at DESC, contract_id DESC
        LIMIT 1
        "#,
    )
    .bind(instrument_admin)
    .bind(instrument_id)
    .fetch_optional(db)
    .await
    .context("query active fee schedule")?;

    Ok(row.map(FeeScheduleView::from))
}

fn build_my_summary_view(
    summary: AccountSummaryRow,
    token_config: Option<TokenConfigView>,
    fee_schedule: Option<FeeScheduleView>,
) -> Result<MySummaryView, ApiError> {
    let available_minor = checked_add3(
        summary.cleared_cash_minor,
        summary.delta_pending_trades_minor,
        -summary.locked_open_orders_minor,
    )?;
    let withdrawable_minor = available_minor
        .checked_sub(summary.pending_withdrawals_reserved_minor)
        .ok_or_else(|| ApiError::internal("withdrawable overflow"))?;

    Ok(MySummaryView {
        account_id: summary.account_id,
        owner_party: summary.owner_party,
        committee_party: summary.committee_party,
        instrument_admin: summary.instrument_admin,
        instrument_id: summary.instrument_id,
        account_status: summary.account_status,
        cleared_cash_minor: summary.cleared_cash_minor,
        delta_pending_trades_minor: summary.delta_pending_trades_minor,
        locked_open_orders_minor: summary.locked_open_orders_minor,
        available_minor,
        pending_withdrawals_reserved_minor: summary.pending_withdrawals_reserved_minor,
        withdrawable_minor,
        effective_available_minor: withdrawable_minor,
        token_config,
        fee_schedule,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone as _;

    fn test_fill(fill_sequence: i64, matched_at_ms: i64, price_ticks: i64) -> MarketFillRow {
        MarketFillRow {
            fill_id: format!("fill-{fill_sequence}"),
            fill_sequence,
            market_id: "mkt-1".to_string(),
            outcome: "YES".to_string(),
            maker_order_id: "maker".to_string(),
            taker_order_id: "taker".to_string(),
            price_ticks,
            quantity_minor: 1,
            engine_version: "test".to_string(),
            matched_at: Utc
                .timestamp_millis_opt(matched_at_ms)
                .single()
                .expect("valid timestamp"),
            clearing_epoch: None,
        }
    }

    #[test]
    fn clamp_limit_bounds_values() {
        assert_eq!(clamp_limit(None), DEFAULT_QUERY_LIMIT);
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(1_000_000)), MAX_QUERY_LIMIT);
    }

    #[test]
    fn checked_add3_handles_overflow() {
        assert!(checked_add3(i64::MAX, 1, 0).is_err());
    }

    #[test]
    fn reject_mixed_auth_headers_rejects_dual_headers() {
        let user_key = Some("user-key".to_string());
        let admin_key = Some("admin-key".to_string());
        assert!(reject_mixed_auth_headers(&user_key, &admin_key).is_err());
    }

    #[test]
    fn reject_mixed_auth_headers_accepts_single_scope() {
        let user_key = Some("user-key".to_string());
        let no_admin = None;
        assert!(reject_mixed_auth_headers(&user_key, &no_admin).is_ok());
    }

    #[test]
    fn normalize_market_category_enforces_non_empty_and_len() {
        assert!(normalize_market_category(Some("".to_string()), "General").is_err());
        assert!(normalize_market_category(Some("x".repeat(65)), "General").is_err());
        assert_eq!(
            normalize_market_category(Some("Politics".to_string()), "General").unwrap(),
            "Politics"
        );
    }

    #[test]
    fn normalize_market_tags_dedupes_and_validates() {
        let out = normalize_market_tags(
            Some(vec![
                "usa".to_string(),
                "election".to_string(),
                "usa".to_string(),
            ]),
            Vec::new(),
        )
        .unwrap();
        assert_eq!(out, vec!["usa".to_string(), "election".to_string()]);

        assert!(normalize_market_tags(Some(vec!["".to_string()]), Vec::new()).is_err());
        assert!(normalize_market_tags(Some(vec!["x".repeat(33)]), Vec::new()).is_err());
    }

    #[test]
    fn normalize_market_resolution_time_accepts_rfc3339() {
        let parsed =
            normalize_market_resolution_time(Some("2026-03-18T00:00:00Z".to_string()), None)
                .expect("valid resolution_time");
        assert_eq!(
            parsed.expect("missing parsed resolution time").to_rfc3339(),
            "2026-03-18T00:00:00+00:00"
        );
    }

    #[test]
    fn normalize_market_resolution_time_rejects_invalid_values() {
        assert!(normalize_market_resolution_time(Some("".to_string()), None).is_err());
        assert!(normalize_market_resolution_time(Some("not-a-date".to_string()), None).is_err());
    }

    #[test]
    fn market_chart_range_parse_accepts_supported_values() {
        assert_eq!(
            MarketChartRange::parse(Some("1h")).unwrap(),
            MarketChartRange::OneHour
        );
        assert_eq!(
            MarketChartRange::parse(Some("6H")).unwrap(),
            MarketChartRange::SixHours
        );
        assert_eq!(
            MarketChartRange::parse(Some("1D")).unwrap(),
            MarketChartRange::OneDay
        );
        assert_eq!(
            MarketChartRange::parse(Some("1W")).unwrap(),
            MarketChartRange::OneWeek
        );
        assert_eq!(
            MarketChartRange::parse(Some("1M")).unwrap(),
            MarketChartRange::OneMonth
        );
        assert_eq!(
            MarketChartRange::parse(Some("ALL")).unwrap(),
            MarketChartRange::All
        );
        assert_eq!(
            MarketChartRange::parse(None).unwrap(),
            MarketChartRange::All
        );
    }

    #[test]
    fn market_chart_range_parse_rejects_unknown_values() {
        assert!(MarketChartRange::parse(Some("2H")).is_err());
    }

    #[test]
    fn market_chart_range_fixed_bin_spec_matches_expected() {
        let one_hour = MarketChartRange::OneHour.fixed_bin_spec().expect("spec");
        assert_eq!(one_hour.sample_points, 60);
        assert_eq!(one_hour.bin_size_ms, CHART_BIN_SIZE_1M_MS);

        let six_hours = MarketChartRange::SixHours.fixed_bin_spec().expect("spec");
        assert_eq!(six_hours.sample_points, 360);
        assert_eq!(six_hours.bin_size_ms, CHART_BIN_SIZE_1M_MS);

        let one_day = MarketChartRange::OneDay.fixed_bin_spec().expect("spec");
        assert_eq!(one_day.sample_points, 288);
        assert_eq!(one_day.bin_size_ms, CHART_BIN_SIZE_5M_MS);

        let one_week = MarketChartRange::OneWeek.fixed_bin_spec().expect("spec");
        assert_eq!(one_week.sample_points, 336);
        assert_eq!(one_week.bin_size_ms, CHART_BIN_SIZE_30M_MS);

        let one_month = MarketChartRange::OneMonth.fixed_bin_spec().expect("spec");
        assert_eq!(one_month.sample_points, 240);
        assert_eq!(one_month.bin_size_ms, CHART_BIN_SIZE_3H_MS);

        assert!(MarketChartRange::All.fixed_bin_spec().is_none());
    }

    #[test]
    fn chart_all_bin_size_ms_uses_expected_thresholds() {
        assert_eq!(chart_all_bin_size_ms(0), CHART_BIN_SIZE_1M_MS);
        assert_eq!(
            chart_all_bin_size_ms(CHART_LIVE_6H_MS - 1),
            CHART_BIN_SIZE_1M_MS
        );
        assert_eq!(
            chart_all_bin_size_ms(CHART_LIVE_6H_MS),
            CHART_BIN_SIZE_5M_MS
        );
        assert_eq!(
            chart_all_bin_size_ms(CHART_LIVE_1D_MS - 1),
            CHART_BIN_SIZE_5M_MS
        );
        assert_eq!(
            chart_all_bin_size_ms(CHART_LIVE_1D_MS),
            CHART_BIN_SIZE_30M_MS
        );
        assert_eq!(
            chart_all_bin_size_ms(CHART_LIVE_1W_MS - 1),
            CHART_BIN_SIZE_30M_MS
        );
        assert_eq!(
            chart_all_bin_size_ms(CHART_LIVE_1W_MS),
            CHART_BIN_SIZE_3H_MS
        );
        assert_eq!(
            chart_all_bin_size_ms(CHART_LIVE_1M_MS - 1),
            CHART_BIN_SIZE_3H_MS
        );
        assert_eq!(
            chart_all_bin_size_ms(CHART_LIVE_1M_MS),
            CHART_BIN_SIZE_1D_MS
        );
    }

    #[test]
    fn floor_timestamp_millis_aligns_to_step() {
        let ts = Utc
            .timestamp_millis_opt(1_700_000_123_456)
            .single()
            .expect("valid timestamp");
        let aligned = floor_timestamp_millis(ts, 60_000).expect("align");
        assert_eq!(aligned.timestamp_millis(), 1_700_000_100_000);
    }

    #[test]
    fn sample_points_for_fixed_bins_includes_endpoints() {
        let start_at = Utc
            .timestamp_millis_opt(1_700_000_000_000)
            .single()
            .expect("valid timestamp");
        let end_at = Utc
            .timestamp_millis_opt(1_700_000_180_000)
            .single()
            .expect("valid timestamp");
        let sample_points =
            sample_points_for_fixed_bins(start_at, end_at, CHART_BIN_SIZE_1M_MS).expect("points");
        assert_eq!(sample_points, 4);
    }

    #[test]
    fn clamp_chart_stream_poll_interval_ms_bounds_values() {
        assert_eq!(
            clamp_chart_stream_poll_interval_ms(None),
            CHART_STREAM_POLL_MS_DEFAULT
        );
        assert_eq!(
            clamp_chart_stream_poll_interval_ms(Some(1)),
            CHART_STREAM_POLL_MS_MIN
        );
        assert_eq!(
            clamp_chart_stream_poll_interval_ms(Some(60_000)),
            CHART_STREAM_POLL_MS_MAX
        );
    }

    #[test]
    fn clamp_chart_snapshot_sample_points_bounds_values() {
        assert_eq!(
            clamp_chart_snapshot_sample_points(None),
            CHART_SNAPSHOT_SAMPLE_POINTS_DEFAULT
        );
        assert_eq!(
            clamp_chart_snapshot_sample_points(Some(1)),
            CHART_SNAPSHOT_SAMPLE_POINTS_MIN
        );
        assert_eq!(
            clamp_chart_snapshot_sample_points(Some(100_000)),
            CHART_SNAPSHOT_SAMPLE_POINTS_MAX
        );
    }

    #[test]
    fn build_market_chart_samples_leaves_pre_open_window_empty() {
        let start_at = Utc
            .timestamp_millis_opt(1_700_000_000_000)
            .single()
            .expect("valid timestamp");
        let end_at = Utc
            .timestamp_millis_opt(1_700_000_600_000)
            .single()
            .expect("valid timestamp");
        let fills = vec![test_fill(1, 1_700_000_480_000, 53)];

        let samples =
            build_market_chart_samples(start_at, end_at, 6, None, &fills).expect("samples build");

        assert_eq!(samples.len(), 6);
        assert!(samples[0].price_ticks.is_none());
        assert!(samples[1].price_ticks.is_none());
        assert!(samples[2].price_ticks.is_none());
        assert!(samples[3].price_ticks.is_none());
        assert_eq!(samples[4].price_ticks, Some(53));
        assert_eq!(samples[5].price_ticks, Some(53));
    }

    #[test]
    fn build_market_chart_samples_uses_previous_fill_for_carry() {
        let start_at = Utc
            .timestamp_millis_opt(1_700_000_000_000)
            .single()
            .expect("valid timestamp");
        let end_at = Utc
            .timestamp_millis_opt(1_700_000_600_000)
            .single()
            .expect("valid timestamp");
        let previous_fill = test_fill(0, 1_700_000_000_000 - 1_000, 49);
        let fills = vec![test_fill(1, 1_700_000_480_000, 53)];

        let samples = build_market_chart_samples(start_at, end_at, 6, Some(&previous_fill), &fills)
            .expect("samples build");

        assert_eq!(samples.len(), 6);
        assert_eq!(samples[0].price_ticks, Some(49));
        assert_eq!(samples[3].price_ticks, Some(49));
        assert_eq!(samples[4].price_ticks, Some(53));
        assert_eq!(samples[5].price_ticks, Some(53));
    }

    #[test]
    fn build_market_chart_samples_fixed_bins_defaults_to_mid_price() {
        let start_at = Utc
            .timestamp_millis_opt(1_700_000_000_000)
            .single()
            .expect("valid timestamp");

        let samples = build_market_chart_samples_fixed_bins(
            start_at,
            4,
            60_000,
            Some(CHART_DEFAULT_PRICE_TICKS),
            None,
            &[],
        )
        .expect("samples");

        assert_eq!(samples.len(), 4);
        assert_eq!(samples[0].price_ticks, Some(CHART_DEFAULT_PRICE_TICKS));
        assert_eq!(samples[1].price_ticks, Some(CHART_DEFAULT_PRICE_TICKS));
        assert_eq!(samples[2].price_ticks, Some(CHART_DEFAULT_PRICE_TICKS));
        assert_eq!(samples[3].price_ticks, Some(CHART_DEFAULT_PRICE_TICKS));
    }

    #[test]
    fn build_market_chart_samples_fixed_bins_without_default_uses_null_until_fill() {
        let start_at = Utc
            .timestamp_millis_opt(1_700_000_000_000)
            .single()
            .expect("valid timestamp");
        let fills = vec![test_fill(1, 1_700_000_000_000 + 70_000, 47)];

        let samples =
            build_market_chart_samples_fixed_bins(start_at, 4, 60_000, None, None, &fills)
                .expect("samples");

        assert_eq!(samples.len(), 4);
        assert_eq!(samples[0].price_ticks, None);
        assert_eq!(samples[1].price_ticks, Some(47));
        assert_eq!(samples[2].price_ticks, Some(47));
        assert_eq!(samples[3].price_ticks, Some(47));
    }

    #[test]
    fn build_market_chart_samples_fixed_bins_assigns_fills_to_bin_closes() {
        let start_at = Utc
            .timestamp_millis_opt(1_700_000_000_000)
            .single()
            .expect("valid timestamp");
        let fills = vec![
            test_fill(1, 1_700_000_000_000 + 10_000, 52),
            test_fill(2, 1_700_000_000_000 + 50_000, 53),
            test_fill(3, 1_700_000_000_000 + 70_000, 47),
        ];

        let samples = build_market_chart_samples_fixed_bins(
            start_at,
            4,
            60_000,
            Some(CHART_DEFAULT_PRICE_TICKS),
            None,
            &fills,
        )
        .expect("samples");

        assert_eq!(samples.len(), 4);
        assert_eq!(samples[0].price_ticks, Some(53));
        assert_eq!(samples[1].price_ticks, Some(47));
        assert_eq!(samples[2].price_ticks, Some(47));
        assert_eq!(samples[3].price_ticks, Some(47));
    }

    #[test]
    fn normalize_asset_slot_accepts_supported_values() {
        assert_eq!(
            normalize_asset_slot(Some("card background".to_string())).expect("valid slot"),
            "card_background".to_string()
        );
        assert_eq!(
            normalize_asset_slot(Some("hero-background".to_string())).expect("valid slot"),
            "hero_background".to_string()
        );
        assert_eq!(
            normalize_asset_slot(Some("thumbnail".to_string())).expect("valid slot"),
            "thumbnail".to_string()
        );
    }

    #[test]
    fn normalize_asset_slot_rejects_unknown_values() {
        assert!(normalize_asset_slot(Some("banner".to_string())).is_err());
        assert!(normalize_asset_slot(Some("   ".to_string())).is_err());
    }

    #[test]
    fn normalize_asset_market_slug_normalizes_invalid_characters() {
        assert_eq!(
            normalize_asset_market_slug(Some("  MKT Demo #1 ".to_string())),
            "mkt-demo--1".to_string()
        );
        assert_eq!(
            normalize_asset_market_slug(Some("".to_string())),
            "market".to_string()
        );
    }

    #[test]
    fn detect_image_extension_prefers_content_type_then_file_name() {
        assert_eq!(
            detect_image_extension(Some("image/webp"), Some("file.png")),
            Some("webp")
        );
        assert_eq!(
            detect_image_extension(None, Some("avatar.jpeg")),
            Some("jpg")
        );
        assert_eq!(detect_image_extension(None, Some("asset.txt")), None);
    }
}
