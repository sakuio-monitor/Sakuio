# 多后端集成测试 / Multi-backend tests

Sakuio 的主库与指标库都支持 **SQLite / PostgreSQL / MySQL / MariaDB**。SQLite 是默认路径，另外三种由集成测试在真实容器上验证。

## 一键运行

前置：安装 [OrbStack](https://orbstack.dev/)（或 Docker Desktop），确保 `docker` 可用。

```bash
scripts/test-backends.sh
```

脚本会：

1. 启动 `docker-compose.test.yml` 中的 `postgres:17`、`mysql:8.4`、`mariadb:11.4`，等待健康检查；
2. 设置 `SAKUIO_TEST_*_DSN`；
3. 依次运行：
   - `cargo test -p sakuio-db --test backends` — 应用库仓储矩阵；
   - `cargo test -p sakuio-metric --test backends` — 指标库矩阵（独立库、合并库、自定义前缀）；
   - `cargo test -p sakuio-server --test acceptance` — 真实进程 + HTTP 验收（每种主库 × 每种指标库）；
4. 默认 `docker compose down -v` 清理；`KEEP=1 scripts/test-backends.sh` 保留容器。

## 测试矩阵

| 层级 | 覆盖内容 |
|------|----------|
| `sakuio-db/tests/backends.rs` | 建表幂等、`system_settings`（MySQL 保留字 `key`）、客户端写入/排序、`UPDATE … RETURNING`、ping 的 `RETURNING id` / `last_insert_id` |
| `sakuio-metric/tests/backends.rs` | 独立指标库、合并存储、自定义表前缀、桶 upsert 去重、保留策略、清理、容量统计 |
| `sakuio-server/tests/acceptance.rs` | 以各引擎作主库启动真实二进制、登录、数据库信息接口、同 DSN 切换被拒绝、指标库热切换到各引擎并校验表已创建；安装到所选 PG 并重启；SQLite→PG→MySQL 两次主库切换与会话/数据保留 |
| `sakuio-metric/tests/writer_flush.rs` | 维护期间暂存桶在切换前写入目标存储（含未完成窗口） |
| `sakuio-server` 单元测试 | 维护模式 HTTP 门禁：写请求 503、只读与数据库设置端点放行 |

审计日志说明见 [AUDIT.md](./AUDIT.md)；主题系统说明见 [THEMES.md](./THEMES.md)；其单元/集成测试包含 ZIP 路径穿越、根目录/嵌套布局、安装后热切换、删除与站点字段注入。内存基线见 [BENCHMARKS.md](./BENCHMARKS.md)。

## 环境变量

不通过脚本运行时，手动设置以下变量即可跑同一套测试：

```bash
export SAKUIO_TEST_POSTGRES_DSN='postgres://sakuio:sakuio@127.0.0.1:55432/sakuio_test'
export SAKUIO_TEST_MYSQL_DSN='mysql://sakuio:sakuio@127.0.0.1:53306/sakuio_test'
export SAKUIO_TEST_MARIADB_DSN='mysql://sakuio:sakuio@127.0.0.1:53307/sakuio_test'
cargo test -p sakuio-db --test backends -- --test-threads=1
```

未设置的引擎会被跳过，因此无容器时 `cargo test` 仍然只跑 SQLite。

## 服务端运行时变量

| 变量 | 说明 |
|------|------|
| `SAKUIO_DATABASE_DSN` | 临时覆盖 `config.yaml` 中的主库连接 |
| `SAKUIO_LISTEN` | 监听地址，默认 `0.0.0.0:8555`（验收测试用它分配随机端口） |

## 注意

- 测试会 **清空** 对应数据库中的应用表（`reset_database`），只能指向一次性测试库。
- 不要把这些 DSN 指向生产库。
