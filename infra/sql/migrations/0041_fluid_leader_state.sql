CREATE TABLE IF NOT EXISTS fluid_leader_state (
    singleton_key BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton_key),
    leader_epoch BIGINT NOT NULL DEFAULT 0,
    leader_node_id TEXT NOT NULL DEFAULT '',
    leader_repl_addr TEXT NOT NULL DEFAULT '',
    committed_offset BIGINT NOT NULL DEFAULT 0,
    elected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO fluid_leader_state (singleton_key)
VALUES (TRUE)
ON CONFLICT (singleton_key) DO NOTHING;
