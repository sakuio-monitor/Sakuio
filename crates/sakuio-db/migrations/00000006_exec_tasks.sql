CREATE TABLE IF NOT EXISTS exec_tasks (
    task_id TEXT PRIMARY KEY NOT NULL,
    command TEXT NOT NULL,
    clients_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS exec_task_results (
    task_id TEXT NOT NULL,
    client_uuid TEXT NOT NULL,
    result TEXT NOT NULL DEFAULT '',
    exit_code INTEGER,
    finished_at TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (task_id, client_uuid),
    FOREIGN KEY (task_id) REFERENCES exec_tasks (task_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_exec_task_results_task ON exec_task_results (task_id);
