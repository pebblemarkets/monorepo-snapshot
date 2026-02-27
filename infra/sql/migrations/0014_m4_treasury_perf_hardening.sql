-- M4: treasury query-path hardening.
--
-- These indexes target the high-frequency treasury polling paths:
-- - consolidation candidate/count scans over token_holdings
-- - anti-join checks against deposit_pendings
-- - claim polling ordered by created_at
-- - deposit reconcile join/order over deposit_pendings

-- Eligible holding scans in withdrawal/consolidation flows.
CREATE INDEX IF NOT EXISTS token_holdings_available_filter_idx
  ON token_holdings(owner_party, instrument_admin, instrument_id, last_offset, contract_id, origin_instruction_cid)
  WHERE active = TRUE
    AND lock_status_class IS NULL;

-- Ordered smallest/largest holding selection with LIMIT in reservation queries.
CREATE INDEX IF NOT EXISTS token_holdings_available_amount_idx
  ON token_holdings(owner_party, instrument_admin, instrument_id, amount_minor, contract_id)
  WHERE active = TRUE
    AND lock_status_class IS NULL;

-- Active deposit-pending lineage lookup for anti-joins and reconcile joins.
CREATE INDEX IF NOT EXISTS deposit_pendings_active_lineage_idx
  ON deposit_pendings(lineage_root_instruction_cid)
  WHERE active = TRUE;

-- Active deposit-pending work queue ordering.
CREATE INDEX IF NOT EXISTS deposit_pendings_active_created_idx
  ON deposit_pendings(created_at, contract_id)
  WHERE active = TRUE;

-- Active claim polling by owner in creation order.
CREATE INDEX IF NOT EXISTS token_holding_claims_owner_created_active_idx
  ON token_holding_claims(owner_party, created_at, contract_id)
  WHERE active = TRUE;
