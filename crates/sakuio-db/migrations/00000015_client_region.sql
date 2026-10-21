-- Persists the resolved IP geolocation (country codes) so region flags
-- render immediately after a board restart, without waiting for the GeoIP
-- resolver tick to finish re-parsing every node.
-- 持久化 IP 归属地国家代码：面板重启后旗帜立即可用，
-- 无需等待 GeoIP 解析器重新解析全部节点。
ALTER TABLE clients ADD COLUMN region TEXT NOT NULL DEFAULT '';
ALTER TABLE clients ADD COLUMN region6 TEXT NOT NULL DEFAULT '';
