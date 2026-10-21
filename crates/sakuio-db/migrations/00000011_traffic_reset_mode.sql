-- Traffic reset schedule mode: cycle (follow billing cycle) / monthly / never.
-- 流量重置方式：跟随计费周期 / 每月 1 日 / 不自动重置。
ALTER TABLE clients ADD COLUMN traffic_reset_mode TEXT NOT NULL DEFAULT 'cycle';
