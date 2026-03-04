use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{anyhow, Context as _};
use pebble_trading::{Fill, IncomingOrder, OrderSide};
use sqlx::{Postgres, QueryBuilder, Transaction};

use crate::ApiError;

#[derive(Default, Debug, Clone)]
pub(crate) struct RiskDelta {
    pub delta_pending_minor: i64,
    pub locked_delta_minor: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct MakerOrderUpdate {
    pub(crate) order_id: String,
    pub(crate) new_remaining_minor: i64,
    pub(crate) new_locked_minor: i64,
    pub(crate) new_status: &'static str,
}

#[derive(Debug, Clone)]
pub(crate) struct PositionDelta {
    pub net_quantity_delta_minor: i64,
    pub last_price_ticks: i64,
}

pub(crate) fn wal_offset_to_db_i64(offset: pebble_fluid::wal::WalOffset) -> Result<i64, ApiError> {
    i64::try_from(offset.0)
        .map_err(|_| ApiError::internal(format!("wal offset {} overflows i64", offset.0)))
}

pub(crate) fn advance_projection_cursor(
    current_cursor: i64,
    applied_offsets: &[i64],
) -> Result<i64, ApiError> {
    let mut next_cursor = current_cursor;

    let mut offsets = applied_offsets.to_vec();
    offsets.sort_unstable();

    for offset in &offsets {
        let expected = next_cursor
            .checked_add(1)
            .ok_or_else(|| ApiError::internal("projection cursor overflow"))?;
        if *offset == expected {
            next_cursor = expected;
        } else if *offset <= next_cursor {
            continue;
        } else {
            break;
        }
    }
    Ok(next_cursor)
}

pub(crate) async fn record_wal_projection_offset(
    tx: &mut Transaction<'_, Postgres>,
    wal_offset: pebble_fluid::wal::WalOffset,
) -> Result<(), ApiError> {
    let wal_offset = wal_offset_to_db_i64(wal_offset)?;
    sqlx::query(
        r#"
        INSERT INTO wal_projection_cursor (id, last_projected_offset)
        VALUES (1, 0)
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .execute(&mut **tx)
    .await
    .context("initialize wal_projection_cursor")
    .map_err(ApiError::from)?;

    sqlx::query(
        r#"
        INSERT INTO wal_applied_offsets (wal_offset)
        VALUES ($1)
        ON CONFLICT (wal_offset) DO NOTHING
        "#,
    )
    .bind(wal_offset)
    .execute(&mut **tx)
    .await
    .context("record wal_applied_offset")
    .map_err(ApiError::from)?;

    let current_cursor: i64 = sqlx::query_scalar(
        "SELECT last_projected_offset FROM wal_projection_cursor WHERE id = 1 FOR UPDATE",
    )
    .fetch_optional(&mut **tx)
    .await
    .context("load wal_projection_cursor for update")?
    .ok_or_else(|| ApiError::internal("missing wal_projection_cursor row"))?;

    let applied_offsets: Vec<i64> = sqlx::query_scalar(
        "SELECT wal_offset FROM wal_applied_offsets WHERE wal_offset > $1 ORDER BY wal_offset",
    )
    .bind(current_cursor)
    .fetch_all(&mut **tx)
    .await
    .context("load pending wal_applied_offsets")
    .map_err(ApiError::from)?;

    let new_cursor = advance_projection_cursor(current_cursor, &applied_offsets)?;

    if new_cursor != current_cursor {
        sqlx::query(
            "UPDATE wal_projection_cursor SET last_projected_offset = $1, updated_at = now() WHERE id = 1",
        )
        .bind(new_cursor)
        .execute(&mut **tx)
        .await
        .context("advance wal_projection_cursor")
        .map_err(ApiError::from)?;

        sqlx::query("DELETE FROM wal_applied_offsets WHERE wal_offset <= $1")
            .bind(new_cursor)
            .execute(&mut **tx)
            .await
            .context("prune wal_applied_offsets")
            .map_err(ApiError::from)?;
    }

    Ok(())
}

pub(crate) async fn enforce_and_advance_nonce(
    tx: &mut Transaction<'_, Postgres>,
    account_id: &str,
    nonce: i64,
    validate_nonce_progression: &(dyn Fn(Option<i64>, i64) -> Result<(), ApiError> + Sync),
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

pub(crate) async fn sync_projection_nonce(
    tx: &mut Transaction<'_, Postgres>,
    account_id: &str,
    nonce: i64,
) -> Result<(), ApiError> {
    sqlx::query(
        r#"
        INSERT INTO account_nonces (account_id, last_nonce, updated_at)
        VALUES ($1, $2, now())
        ON CONFLICT (account_id) DO UPDATE
        SET
          last_nonce = GREATEST(account_nonces.last_nonce, EXCLUDED.last_nonce),
          updated_at = now()
        "#,
    )
    .bind(account_id)
    .bind(nonce)
    .execute(&mut **tx)
    .await
    .context("sync projection nonce")?;
    Ok(())
}

pub(crate) async fn reserve_market_sequences(
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

pub(crate) async fn batch_update_maker_orders(
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

pub(crate) async fn batch_insert_fills(
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
          canonical_price_ticks,\
          maker_outcome,\
          maker_side,\
          maker_price_ticks,\
          taker_outcome,\
          taker_side,\
          taker_price_ticks,\
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
            .push_bind(fill.canonical_price_ticks)
            .push_bind(&fill.maker_outcome)
            .push_bind(side_to_db(fill.maker_side))
            .push_bind(fill.maker_price_ticks)
            .push_bind(&fill.taker_outcome)
            .push_bind(side_to_db(fill.taker_side))
            .push_bind(fill.taker_price_ticks)
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

pub(crate) async fn batch_upsert_position_deltas(
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

pub(crate) async fn batch_insert_fill_recorded_events(
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
              "canonical_price_ticks": fill.canonical_price_ticks,
              "maker_outcome": fill.maker_outcome,
              "maker_side": side_to_db(fill.maker_side),
              "maker_price_ticks": fill.maker_price_ticks,
              "taker_outcome": fill.taker_outcome,
              "taker_side": side_to_db(fill.taker_side),
              "taker_price_ticks": fill.taker_price_ticks,
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

pub(crate) async fn insert_taker_order(
    tx: &mut Transaction<'_, Postgres>,
    incoming: &IncomingOrder,
    params: &InsertTakerOrderParams<'_>,
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
          wal_offset,
          submitted_at,
          updated_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,now(),now())
        "#,
    )
    .bind(&incoming.order_id.0)
    .bind(&incoming.market_id.0)
    .bind(&incoming.account_id)
    .bind(params.owner_party)
    .bind(params.instrument_admin)
    .bind(params.instrument_id)
    .bind(&incoming.outcome)
    .bind(params.side)
    .bind(params.order_type)
    .bind(params.tif)
    .bind(incoming.nonce.0)
    .bind(incoming.price_ticks)
    .bind(incoming.quantity_minor)
    .bind(params.remaining_minor)
    .bind(params.locked_minor)
    .bind(params.status)
    .bind(params.engine_version)
    .bind(params.wal_offset.map(wal_offset_to_db_i64).transpose()?)
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

pub(crate) struct InsertTakerOrderParams<'a> {
    pub(crate) owner_party: &'a str,
    pub(crate) instrument_admin: &'a str,
    pub(crate) instrument_id: &'a str,
    pub(crate) side: &'a str,
    pub(crate) order_type: &'a str,
    pub(crate) tif: &'a str,
    pub(crate) engine_version: &'a str,
    pub(crate) remaining_minor: i64,
    pub(crate) locked_minor: i64,
    pub(crate) status: &'a str,
    pub(crate) wal_offset: Option<pebble_fluid::wal::WalOffset>,
}

pub(crate) async fn batch_apply_risk_deltas(
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
            .iter()
            .map(|(account_id, locked_delta_minor)| {
                let existing_locked = existing_locked_by_account.get(*account_id).copied();
                let reason = match existing_locked {
                    None if *locked_delta_minor < 0 => "missing_row_for_negative_locked_delta",
                    None => "missing_row_for_non_negative_locked_delta",
                    Some(current_locked)
                        if i64::checked_add(current_locked, *locked_delta_minor)
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

pub(crate) async fn apply_projection_risk_deltas(
    tx: &mut Transaction<'_, Postgres>,
    instrument_admin: &str,
    instrument_id: &str,
    risk_deltas: &BTreeMap<String, RiskDelta>,
) -> Result<(), ApiError> {
    if risk_deltas.is_empty() {
        return Ok(());
    }

    for (account_id, delta) in risk_deltas {
        if delta.delta_pending_minor == 0 && delta.locked_delta_minor == 0 {
            continue;
        }

        sqlx::query(
            r#"
            INSERT INTO account_risk_state (
              account_id,
              instrument_admin,
              instrument_id,
              delta_pending_trades_minor,
              locked_open_orders_minor,
              updated_at
            )
            VALUES ($1, $2, $3, $4, GREATEST($5, 0), now())
            ON CONFLICT (account_id) DO UPDATE
            SET
              instrument_admin = EXCLUDED.instrument_admin,
              instrument_id = EXCLUDED.instrument_id,
              delta_pending_trades_minor = account_risk_state.delta_pending_trades_minor + $4,
              locked_open_orders_minor = GREATEST(
                0,
                account_risk_state.locked_open_orders_minor + $5
              ),
              updated_at = now()
            "#,
        )
        .bind(account_id)
        .bind(instrument_admin)
        .bind(instrument_id)
        .bind(delta.delta_pending_minor)
        .bind(delta.locked_delta_minor)
        .execute(&mut **tx)
        .await
        .context("apply projection risk delta")?;
    }

    Ok(())
}

pub(crate) async fn insert_trading_event(
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

pub(crate) async fn advisory_lock(
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

fn side_to_db(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "Buy",
        OrderSide::Sell => "Sell",
    }
}

fn is_order_id_unique_violation(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => {
            db_err.code().as_deref() == Some("23505")
                && matches!(
                    db_err.constraint(),
                    Some("orders_pkey") | Some("orders_order_id_key")
                )
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[test]
    fn wal_offset_to_db_i64_rejects_overflow() {
        let offset = pebble_fluid::wal::WalOffset(u64::MAX);
        let err = wal_offset_to_db_i64(offset).unwrap_err();
        assert_eq!(err.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn advance_projection_cursor_skips_non_contiguous_offsets() {
        let updated = advance_projection_cursor(3, &[4, 5, 6, 8]).unwrap();
        assert_eq!(updated, 6);

        let unchanged = advance_projection_cursor(6, &[7, 9]).unwrap();
        assert_eq!(unchanged, 7);
    }

    #[test]
    fn advance_projection_cursor_handles_empty_set() {
        let unchanged = advance_projection_cursor(42, &[]).unwrap();
        assert_eq!(unchanged, 42);
    }

    #[test]
    fn advance_projection_cursor_ignores_offsets_before_cursor() {
        let updated = advance_projection_cursor(10, &[8, 9]).unwrap();
        assert_eq!(updated, 10);
    }

    #[test]
    fn advance_projection_cursor_handles_unsorted_input() {
        let unchanged = advance_projection_cursor(10, &[11, 13, 12]).unwrap();
        assert_eq!(unchanged, 13);
    }

    #[test]
    fn advance_projection_cursor_overflows_if_contiguous_growth_exceeds_i64() {
        let err = advance_projection_cursor(i64::MAX, &[i64::MAX]).unwrap_err();
        assert_eq!(err.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn advance_projection_cursor_handles_duplicates() {
        let updated = advance_projection_cursor(9, &[10, 10, 11, 12, 12]).unwrap();
        assert_eq!(updated, 12);
    }

    #[test]
    fn wal_offset_to_db_i64_accepts_max_i64() {
        let offset = pebble_fluid::wal::WalOffset(i64::MAX as u64);
        let db_offset = wal_offset_to_db_i64(offset).unwrap();
        assert_eq!(db_offset, i64::MAX);
    }

    #[test]
    fn test_stage_b_contiguous_cursor_under_out_of_order_commits() {
        // Offset N+1 applied first while N is still missing: cursor must not advance.
        let cursor = advance_projection_cursor(10, &[12]).unwrap();
        assert_eq!(cursor, 10);

        // Once N arrives, cursor can advance contiguously through both N and N+1.
        let cursor = advance_projection_cursor(cursor, &[11, 12]).unwrap();
        assert_eq!(cursor, 12);
    }
}
