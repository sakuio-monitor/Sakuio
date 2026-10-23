# 编码规范 / Coding Standards

## 注释 / Comments

- 对外 API（`pub` 项、trait 方法、配置键）必须 **中英双语**。
- 格式：英文一句 + 中文一句；复杂逻辑块可加行内双语注释。
- 禁止仅写中文或仅写英文的 `pub` 文档注释。

示例：

```rust
/// Returns the transport plugin name (e.g. `websocket`).
/// 返回传输插件名称（如 `websocket`）。
fn name(&self) -> &'static str;
```

## 日志 / Logging

- 使用 **`tracing`**，禁止 `println!` / `eprintln!` 作为运行日志（CLI 用户输出除外）。
- **日志正文一律英文**；结构化字段用 snake_case。
- 分级约定：
  - `error`：请求失败、数据损坏、必须人工介入
  - `warn`：降级、重试、配置忽略（如未启用 Redis）
  - `info`：生命周期（listen、install complete、agent connected/disconnected）
  - `debug`：协议细节、SQL（无敏感参数）
  - `trace`：帧级调试（默认关闭）

示例：

```rust
tracing::info!(node_uuid = %uuid, session_id = %sid, "agent board session established");
```

## 数据库 / Database

- 表结构是**声明式**的：在 `crates/sakuio-db/src/schema.rs` 的 `TABLES` 中增列/增表；启动时按方言渲染并只增不改地补齐。
- SQL 一律写可移植的 `?` 占位符，由 `DbPool` 按后端重写；**禁止**在同一份仓储代码里写 `$1` / 方言分支，除非确属方言能力差异（如 `RETURNING`、`last_insert_id`）。
- 标识符若为保留字（如 MySQL `key`），必须走 `quote_ident`。
- 新增查询必须同时通过 `scripts/test-backends.sh`（SQLite / PostgreSQL / MySQL / MariaDB）。

## 前端 / Frontend

- 前端独立仓库：**`Sakuio-frontend`**（与 `sakuio` 后端并列，勿写进 `sakuio/` 目录树）；包管理 **Bun**（`bun install` / `bun run dev:admin`）。
- UI 文案 **不得硬编码**；走 **i18n**，首版 locale：`zh-CN`。
- 管理端（`apps/admin`）与访客端（`apps/public`）**独立入口**，共享 `packages/i18n` 时可复用 key 前缀。
