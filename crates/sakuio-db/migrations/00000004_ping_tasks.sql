CREATE TABLE IF NOT EXISTS ping_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    weight INTEGER NOT NULL DEFAULT 0,
    name TEXT NOT NULL,
    clients_json TEXT NOT NULL DEFAULT '[]',
    default_on INTEGER NOT NULL DEFAULT 0,
    type TEXT NOT NULL DEFAULT 'icmp',
    target TEXT NOT NULL,
    interval_secs INTEGER NOT NULL DEFAULT 60,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ping_tasks_weight ON ping_tasks (weight, id);

CREATE TABLE IF NOT EXISTS ping_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_uuid TEXT NOT NULL,
    task_id INTEGER NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    value_ms INTEGER NOT NULL,
    FOREIGN KEY (task_id) REFERENCES ping_tasks (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ping_records_client_time ON ping_records (client_uuid, observed_at_ms);
CREATE INDEX IF NOT EXISTS idx_ping_records_task_time ON ping_records (task_id, observed_at_ms);
