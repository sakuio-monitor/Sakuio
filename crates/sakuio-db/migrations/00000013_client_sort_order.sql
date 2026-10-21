-- 客户端手动排序（拖拽列表）。存量行按 created_at, uuid 顺序初始化编号，
-- 之后所有行 sort_order 均非空：列表按纯 sort_order 排序，游标分页按序号过滤。
ALTER TABLE clients ADD COLUMN sort_order REAL;

UPDATE clients SET sort_order = (
    SELECT COUNT(*) + 1 FROM clients c2
    WHERE c2.created_at < clients.created_at
       OR (c2.created_at = clients.created_at AND c2.uuid < clients.uuid)
);
