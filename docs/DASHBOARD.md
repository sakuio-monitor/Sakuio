# 仪表盘

管理端仪表盘使用预聚合快照，不在请求路径扫描 365 天历史。

## 数据层

- `dashboard_fleet_points`
- `dashboard_node_range`
- `dashboard_ping_range`
- `dashboard_meta`
- `admin_dashboard_layouts`
- `ping_rollup_hourly`

`MetricStore` 的 `metric_buckets` 继续保存 1m / 5m / 1h / 1d 明细；后台任务按范围生成快照：

| 范围 | 粒度 | 数据源 |
| --- | --- | --- |
| 24h | 5 分钟 | 5m 桶 |
| 7d | 1 小时 | 1h 桶 |
| 30d | 1 小时 | 1h 桶 |
| 90d | 1 天 | 1d 桶 |
| 365d | 1 天 | 1d 桶 |

仪表盘 API 只读取快照表，因此响应时间与节点数和历史长度基本无关。

## 接口

```text
GET  /api/admin/dashboard?range=24h
GET  /api/admin/dashboard/traffic?range=24h
GET  /api/admin/dashboard/rankings?range=24h
GET  /api/admin/dashboard/ping-quality?range=24h
GET  /api/admin/dashboard/capacity
GET  /api/admin/dashboard/database
GET  /api/admin/dashboard/expiry
GET  /api/admin/dashboard/stream
GET  /api/admin/dashboard/layout
PUT  /api/admin/dashboard/layout
```

- `dashboard` 为聚合首屏接口。
- `stream` 通过 SSE 推送在线数、总数与快照 revision。
- 布局按管理员保存，宽度支持 `compact/wide/full`，高度支持 `short/normal/tall`，并支持 Top N。

## 性能

- 快照读取行数：Fleet ≤ 720，节点排名 1000，Ping 质量 ≤ 30000。
- 应用内 5 秒缓存，避免组件刷新风暴。
- 后台刷新：
  - 24h / 7d / 30d：约 5 分钟
  - 90d / 365d：约 1 小时
- 首次启动或快照为空时返回 `status: warming`，由 SSE revision 触发页面重新加载。

## Ping

`ping_records` 原始明细保留 48 小时，写入时同步更新 `ping_rollup_hourly`。仪表盘质量快照读取小时汇总，按 task/client 聚合 loss、延迟分位数与波动率，原始明细不会进入 HTTP 响应。

## 注意

- 首次升级会创建新表并触发后台快照回填。
- 365 天查询依赖日桶保留策略，请在数据库设置中确保目标指标保留天数。
- SQLite 建议开启 WAL（Sakuio 所有 SQLite 连接已启用）。
