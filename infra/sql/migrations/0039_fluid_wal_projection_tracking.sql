CREATE TABLE IF NOT EXISTS wal_projection_cursor (
    id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_projected_offset BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO wal_projection_cursor (id, last_projected_offset)
VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS wal_applied_offsets (
    wal_offset BIGINT PRIMARY KEY
);
