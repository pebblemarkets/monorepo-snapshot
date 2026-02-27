-- M5: disclosure service request/response audit trail.

CREATE TABLE IF NOT EXISTS disclosure_audit_logs (
  id BIGSERIAL PRIMARY KEY,
  requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  requester_type TEXT NOT NULL,
  requester_subject_hash TEXT NOT NULL,
  requester_ip TEXT,
  module_name TEXT NOT NULL,
  entity_name TEXT NOT NULL,
  package_id TEXT,
  contract_id TEXT NOT NULL,
  allowed BOOLEAN NOT NULL,
  outcome TEXT NOT NULL,
  http_status INT NOT NULL,
  request_json JSONB NOT NULL,
  response_json JSONB,
  error_text TEXT
);

CREATE INDEX IF NOT EXISTS disclosure_audit_logs_requested_at_idx
  ON disclosure_audit_logs(requested_at DESC);

CREATE INDEX IF NOT EXISTS disclosure_audit_logs_template_contract_idx
  ON disclosure_audit_logs(module_name, entity_name, contract_id);

CREATE INDEX IF NOT EXISTS disclosure_audit_logs_requester_idx
  ON disclosure_audit_logs(requester_subject_hash, requested_at DESC);
