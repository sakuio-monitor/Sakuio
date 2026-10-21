-- Bandwidth config for the billing tab (value + unit, unit stored as symbol).
-- 带宽配置（数值 + 单位，单位存符号本身）。
ALTER TABLE clients ADD COLUMN bandwidth REAL NOT NULL DEFAULT 0;
ALTER TABLE clients ADD COLUMN bandwidth_unit TEXT NOT NULL DEFAULT 'Mbps';
