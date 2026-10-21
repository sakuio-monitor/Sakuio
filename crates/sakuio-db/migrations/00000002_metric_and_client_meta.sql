ALTER TABLE clients ADD COLUMN last_seen_at TEXT;
ALTER TABLE clients ADD COLUMN agent_version TEXT;
ALTER TABLE clients ADD COLUMN disabled INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS metric_samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    node_uuid TEXT NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_metric_samples_node_time
    ON metric_samples (node_uuid, observed_at_ms DESC);
