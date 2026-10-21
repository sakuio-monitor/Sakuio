CREATE TABLE IF NOT EXISTS notification_rules (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    threshold INTEGER NOT NULL DEFAULT 0,
    channels_json TEXT NOT NULL DEFAULT '[]',
    clients_json TEXT NOT NULL DEFAULT '[]',
    template_title TEXT NOT NULL DEFAULT '',
    template_body TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);
