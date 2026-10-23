# 审计日志 / Audit log

管理端 `/admin/logs` 提供审计日志查询，覆盖管理员敏感操作、登录/登出与安装流程。

## 记录内容

每条记录包含：

| 字段 | 说明 |
|------|------|
| `created_at` | UTC 毫秒时间 |
| `actor` / `actor_id` | 管理员用户名 / 用户 ID |
| `action` | 系统语义动作，如 `SERVER_DELETE`、`WEB_TERMINAL_OPEN`、`FILE_UPLOAD`、`THEME_APPLY`、`LOGIN`；**不使用 HTTP 方法作为日志类型** |
| `target` | 操作对象（去掉 `/api/admin/` 前缀的路径） |
| `detail` | 请求体/查询参数脱敏摘要，或非 JSON 请求体大小摘要 |
| `ip` | 客户端 IP（优先 `X-Forwarded-For` / `X-Real-IP`） |
| `user_agent` | 客户端 UA |
| `status` | HTTP 状态码 |
| `method` / `path` | 原始方法/路径 |

### 自动覆盖范围

审计中间件挂在管理员鉴权之后：

- 所有 `/api/admin/**` 的 `POST`/`PUT`/`PATCH`/`DELETE`；
- 敏感读取：数据库信息、备份下载、Web 终端打开、远程文件浏览/下载/读取/搜索；
- Web 终端打开与关闭（含操作者、IP、UA、目标节点）；
- 远程文件上传、下载、编辑、删除、移动、重命名、权限修改、回收站；
- 服务器创建、修改、删除、批量删除、分组、排序、流量重置；
- 远程命令执行；
- 服务器上线/下线（Agent 连接、断开、心跳超时、启动清理），记录节点 IP；
- 登录成功、失败（未知用户、错误密码、2FA 缺失/错误）、登出（显式同步写入）；
- 安装完成/目标库失败（显式同步写入）。

请求体仅对 `Content-Type: application/json` 且不超过 16 KB 的请求采集；密码、token、secret、cookie、验证码等键自动替换为 `***`，查询参数同样脱敏。压缩包/备份等二进制只记录类型与大小。

## 写入路径与性能

```text
request -> audit middleware -> bounded mpsc (8192)
        -> batch writer (64 rows / 250ms)
        -> single multi-row INSERT
```

- 请求路径只做 `try_send`，队列满时丢弃并计数/告警，绝不阻塞业务；
- 合并写入减少数据库往返；
- 登录、登出、安装等关键事件走同步单条写入，避免被队列丢弃。

## 百万到一亿级日志

- 表 `audit_logs` 使用自增 `BIGINT` 主键；
- 索引：`created_at`、`(actor, created_at)`、`(action, created_at)`、`(ip, created_at)`、`(target, created_at)`；
- 前端列表使用现有 `EntityListPage`/`DataTable`，后端采用**游标分页**（`id < cursor`），不使用深 `OFFSET`，也不执行 `COUNT(*)`；
- 过滤条件使用精确 `actor`/`action`/`ip` 和 `target` 前缀搜索，保持索引可用；
- 默认保留 **180 天**，可在「站点设置 → 审计日志」选择 7/14/30/180/365 天或永久保存；
- 缩短保留期后立即异步分批删除过期日志，不阻塞保存请求；
- 后台每 6 小时按 500 行分批清理，避免长事务/大锁。

- 单页最多 200 条，前端默认 20/页；长期运行建议将保留期与业务合规要求结合。

## API

```text
GET /api/admin/logs?limit=50&cursor=<id>&actor=&category=&ip=&q=&from=&to=
```

返回：

```json
{
  "success": true,
  "items": [ ... ],
  "next_cursor": "12345"
}
```

`category` 为用户可见分类：`auth`、`server`、`terminal`、`theme`、`settings`、`database`、`notification`、`ping`；后端映射为对应语义动作集合。

`q` 为 `target` 前缀匹配；`from`/`to` 为 `created_at` 范围（ISO 时间）。日志列表的动作列只显示语义名称，不显示 HTTP 方法；旧记录的 HTTP 方法会统一显示为“其他操作”。

## 实现结构

| 文件 | 职责 |
|------|------|
| `crates/sakuio-db/src/schema.rs` | `audit_logs` 表与索引 |
| `crates/sakuio-db/src/audit.rs` | 批量插入、游标查询、分批清理 |
| `crates/sakuio-server/src/audit/logger.rs` | 有界队列、批处理、保留期任务 |
| `crates/sakuio-server/src/audit/middleware.rs` | 管理端请求审计、IP/UA 提取 |
| `crates/sakuio-server/src/audit/sanitize.rs` | 敏感字段脱敏、长度限制 |
| `crates/sakuio-server/src/audit_api.rs` | 查询 API |
| `crates/sakuio-server/src/geoip_resolver.rs` | 全局持久化 IP Geo 引擎 |
| `crates/sakuio-db/src/geo_cache.rs` | `ip_geo_cache` 持久缓存 |
| `apps/admin/src/pages/LogsPage.tsx` | 管理端列表/筛选/游标分页 |
