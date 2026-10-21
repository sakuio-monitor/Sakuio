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

## 数据库迁移 / Migrations

- Board / 配置库 SQL：**`000000NN_<slug>.sql`**（8 位数字 + 下划线，参考 Tanzaku `tz-board/migrations`）。
- **`migrations/` 目录仅放可执行 `.sql`**；不要 `.down.sql` 或文档，避免被 `sqlx::migrate!()` 一并 embed。
- 新版本只 **新建** 下一个 `NN`，禁止改已存在迁移文件内容。

## 前端 / Frontend

- 前端独立仓库：**`Sakuio-frontend`**（与 `sakuio` 后端并列，勿写进 `sakuio/` 目录树）；包管理 **Bun**（`bun install` / `bun run dev:admin`）。
- UI 文案 **不得硬编码**；走 **i18n**，首版 locale：`zh-CN`。
- 管理端（`apps/admin`）与访客端（`apps/public`）**独立入口**，共享 `packages/i18n` 时可复用 key 前缀。
