CREATE TABLE IF NOT EXISTS notification_channel_instances (
    id TEXT PRIMARY KEY NOT NULL,
    channel_type TEXT NOT NULL,
    name TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 0,
    config_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL
);

INSERT INTO notification_channel_instances (id, channel_type, name, enabled, config_json, updated_at)
SELECT
    'legacy-' || channel_id,
    channel_id,
    channel_id,
    enabled,
    config_json,
    updated_at
FROM notification_channel_configs;

DROP TABLE notification_channel_configs;
