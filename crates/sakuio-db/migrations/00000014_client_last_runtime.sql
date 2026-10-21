-- Persists the last reported runtime totals so the dashboard can keep
-- showing uptime / total traffic after a board restart (the in-memory
-- snapshot is lost on restart).
-- 持久化最近一次上报的运行时累计值：面板重启后内存快照丢失，
-- 仍可展示断开前的运行时长与总流量。
ALTER TABLE clients ADD COLUMN last_net_total_up INTEGER;
ALTER TABLE clients ADD COLUMN last_net_total_down INTEGER;
ALTER TABLE clients ADD COLUMN last_uptime_secs INTEGER;
