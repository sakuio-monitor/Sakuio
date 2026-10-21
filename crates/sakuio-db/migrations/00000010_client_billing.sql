-- Client billing & traffic ledger fields.
-- 客户端账单与流量账本字段。
ALTER TABLE clients ADD COLUMN auto_renewal INTEGER NOT NULL DEFAULT 0;
ALTER TABLE clients ADD COLUMN traffic_threshold BIGINT NOT NULL DEFAULT 0;
ALTER TABLE clients ADD COLUMN traffic_stat_mode TEXT NOT NULL DEFAULT 'max';
ALTER TABLE clients ADD COLUMN traffic_used_up BIGINT NOT NULL DEFAULT 0;
ALTER TABLE clients ADD COLUMN traffic_used_down BIGINT NOT NULL DEFAULT 0;
ALTER TABLE clients ADD COLUMN traffic_last_json TEXT;
ALTER TABLE clients ADD COLUMN traffic_cycle_start TEXT;
