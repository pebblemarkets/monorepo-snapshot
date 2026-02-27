-- M3: reconcile account_risk_state.delta_pending_trades_minor to unapplied fill deltas.
--
-- Bugfix context:
--   delta_pending_trades_minor historically accumulated fill deltas but was not
--   decremented after successful clearing apply, causing persistent double-counting
--   in available/withdrawable calculations.
--
-- This migration performs a one-time repair by recalculating, per account, the
-- net fill delta that remains unapplied to AccountState:
--   pending = fills with clearing_epoch IS NULL OR clearing_epoch > last_applied_epoch.

WITH latest_account_state AS (
  SELECT
    st.account_id,
    st.last_applied_epoch
  FROM account_states st
  JOIN account_state_latest asl
    ON asl.contract_id = st.contract_id
  WHERE st.active = TRUE
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
UPDATE account_risk_state rs
SET
  delta_pending_trades_minor = COALESCE(pfd.pending_delta_minor, 0),
  updated_at = now()
FROM latest_account_state las
LEFT JOIN pending_fill_deltas pfd
  ON pfd.account_id = las.account_id
WHERE rs.account_id = las.account_id
  AND rs.delta_pending_trades_minor <> COALESCE(pfd.pending_delta_minor, 0);
