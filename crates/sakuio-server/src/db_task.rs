//! Robust database task management: a single gate for all database jobs
//! (main-database switch, metrics migration / sync), plus the main-database
//! copy-verify-restart flow.
//! 健壮的数据库任务管理：主库切换、监控库迁移/同步共用同一互斥闸门，
//! 并实现主库「复制 → 校验 → 自动重启」流程。

use futures_util::TryStreamExt;
use sakuio_db::DbPool;
use sakuio_db::{Col, DbKind, Ty, TABLES};
use serde::Serialize;
use std::sync::Arc;

/// Kinds of mutually exclusive database tasks.
/// 互斥的数据库任务类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DbTaskKind {
    MainSwitch,
    MetricsMigrate,
}

impl DbTaskKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MainSwitch => "main_switch",
            Self::MetricsMigrate => "metrics_migrate",
        }
    }
}

/// Task state surfaced to the admin page.
/// 暴露给管理页的任务状态。
#[derive(Debug, Clone, Serialize, Default)]
pub struct DbTaskState {
    pub kind: Option<&'static str>,
    pub running: bool,
    pub phase: String,
    pub processed: u64,
    pub error: Option<String>,
    pub finished_at: Option<String>,
}

/// Concurrency gate + progress holder; only one database task runs at a time.
/// The state lock is a short critical section (never held across awaits).
/// 并发闸门 + 进度持有；同一时间只允许一个数据库任务。
/// 状态锁为短临界区（不会跨 await 持有），故用同步锁，进度回调可直接调用。
#[derive(Clone, Default)]
pub struct DbTaskManager {
    state: Arc<std::sync::Mutex<DbTaskState>>,
}

impl DbTaskManager {
    pub fn snapshot(&self) -> DbTaskState {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Marks a task as running; fails when another task is still running.
    /// 标记任务开始；已有任务未结束时报错。
    pub fn begin(&self, kind: DbTaskKind) -> Result<(), String> {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if s.running {
            return Err("another database task is already running".into());
        }
        *s = DbTaskState {
            kind: Some(kind.as_str()),
            running: true,
            phase: "preparing".into(),
            ..Default::default()
        };
        Ok(())
    }

    pub fn phase(&self, phase: impl Into<String>) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).phase = phase.into();
    }

    pub fn processed(&self, processed: u64) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .processed = processed;
    }

    /// Marks the task finished; `err` = failure message.
    /// 标记任务结束；`err` 非空表示失败。
    pub fn finish(&self, err: Option<String>) {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.running = false;
        s.phase = if err.is_some() {
            "failed".into()
        } else {
            "completed".into()
        };
        s.error = err;
        s.finished_at = Some(chrono::Utc::now().to_rfc3339());
    }
}

/// Typed cell for schema-driven row copying.
/// 按表结构类型化抽取的单元格。
enum Val {
    S(Option<String>),
    I(Option<i32>),
    L(Option<i64>),
    D(Option<f64>),
}

fn read_val(row: &sqlx::any::AnyRow, i: usize, col: &Col) -> Val {
    use sqlx::Row;
    match col.ty {
        Ty::Key | Ty::Text => Val::S(
            row.try_get::<Option<String>, _>(i)
                .or_else(|_| {
                    row.try_get::<Option<i64>, _>(i)
                        .map(|v| v.map(|n| n.to_string()))
                })
                .unwrap_or_default(),
        ),
        Ty::Int => Val::I(
            row.try_get::<Option<i32>, _>(i)
                .or_else(|_| {
                    row.try_get::<Option<i64>, _>(i)
                        .map(|v| v.map(|n| n as i32))
                })
                .unwrap_or_default(),
        ),
        Ty::BigInt | Ty::AutoId => Val::L(
            row.try_get::<Option<i64>, _>(i)
                .or_else(|_| {
                    row.try_get::<Option<i32>, _>(i)
                        .map(|v| v.map(|n| n as i64))
                })
                .unwrap_or_default(),
        ),
        Ty::Double => Val::D(
            row.try_get::<Option<f64>, _>(i)
                .or_else(|_| {
                    row.try_get::<Option<i64>, _>(i)
                        .map(|v| v.map(|n| n as f64))
                })
                .unwrap_or_default(),
        ),
    }
}

fn sql_type(ty: Ty) -> &'static str {
    match ty {
        Ty::Key => "varchar(191)",
        Ty::Text => "text",
        Ty::Int => "integer",
        Ty::BigInt | Ty::AutoId => "bigint",
        Ty::Double => "double precision",
    }
}

async fn count_rows(pool: &DbPool, kind: DbKind, table: &str) -> Result<i64, sqlx::Error> {
    use sqlx::Row;
    let quoted = sakuio_db::quote_ident(kind, table);
    let row = sqlx::query(&format!("SELECT COUNT(*) FROM {quoted}"))
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<i64, _>(0).unwrap_or(0))
}

/// Copies the given tables; `clear` empties them first (reverse-dependency
/// order) so a retried switch never duplicates or conflicts.
/// 复制指定表；`clear` 时先按逆依赖顺序清空，保证重试幂等。
async fn copy_tables(
    source: &DbPool,
    target: &DbPool,
    dialect: DbKind,
    selected: &[&'static sakuio_db::TableDef],
    clear: bool,
    tasks: &DbTaskManager,
) -> Result<(), String> {
    let source_kind = source.kind();
    if clear {
        for table in selected.iter().rev() {
            let quoted = sakuio_db::quote_ident(dialect, table.name);
            sqlx::query(&format!("DELETE FROM {quoted}"))
                .execute(target)
                .await
                .map_err(|e| format!("clear {}: {e}", table.name))?;
        }
    }
    // Rows per target transaction: bounds memory and lock time on large tables.
    // 每个目标事务的行数：控制大表复制的内存与锁持有时间。
    const COPY_BATCH_ROWS: usize = 500;
    for table in selected {
        // Column names must be quoted per side: `key` is reserved in MySQL.
        // 两侧的列名都要按方言引用：`key` 在 MySQL 中是保留字。
        let source_columns = table
            .cols
            .iter()
            .map(|column| sakuio_db::quote_ident(source_kind, column.name))
            .collect::<Vec<_>>()
            .join(", ");
        let target_columns = table
            .cols
            .iter()
            .map(|column| sakuio_db::quote_ident(dialect, column.name))
            .collect::<Vec<_>>()
            .join(", ");
        let source_table = sakuio_db::quote_ident(source_kind, table.name);
        let target_table = sakuio_db::quote_ident(dialect, table.name);
        let select_sql = format!("SELECT {source_columns} FROM {source_table}");
        let mut rows = sqlx::query(&select_sql).fetch(source);
        // PostgreSQL 按目标类型显式 CAST，规避新旧表列类型差异。
        let values_template = table
            .cols
            .iter()
            .enumerate()
            .map(|(i, c)| match dialect {
                DbKind::Postgres => format!("CAST(${} AS {})", i + 1, sql_type(c.ty)),
                _ => "?".to_string(),
            })
            .collect::<Vec<_>>()
            .join(", ");
        let insert_sql =
            format!("INSERT INTO {target_table} ({target_columns}) VALUES ({values_template})");

        let mut tx = target.begin().await.map_err(|e| e.to_string())?;
        let mut copied: u64 = 0;
        let mut in_batch = 0usize;
        while let Some(row) = rows
            .try_next()
            .await
            .map_err(|e| format!("read {}: {e}", table.name))?
        {
            let mut q = sqlx::query(&insert_sql);
            for (i, col) in table.cols.iter().enumerate() {
                q = match read_val(&row, i, col) {
                    Val::S(v) => q.bind(v),
                    Val::I(v) => q.bind(v),
                    Val::L(v) => q.bind(v),
                    Val::D(v) => q.bind(v),
                };
            }
            q.execute(&mut tx)
                .await
                .map_err(|e| format!("copy {}: {e}", table.name))?;
            copied += 1;
            in_batch += 1;
            if in_batch >= COPY_BATCH_ROWS {
                tx.commit()
                    .await
                    .map_err(|e| format!("commit {}: {e}", table.name))?;
                tx = target.begin().await.map_err(|e| e.to_string())?;
                in_batch = 0;
            }
        }
        tx.commit()
            .await
            .map_err(|e| format!("commit {}: {e}", table.name))?;

        // PostgreSQL 身份列需手动推进序列，否则重启后插入主键冲突。
        if dialect == DbKind::Postgres && table.cols.iter().any(|c| c.ty == Ty::AutoId) {
            sqlx::query(&format!(
                "SELECT setval(pg_get_serial_sequence('{}', 'id'), (SELECT COALESCE(MAX({}), 1) FROM {}))",
                table.name,
                sakuio_db::quote_ident(dialect, "id"),
                target_table
            ))
            .execute(target)
            .await
            .map_err(|e| format!("fix sequence {}: {e}", table.name))?;
        }

        tasks.phase(format!("copying {} ({copied} rows)", table.name));
        tasks.processed(copied);
    }
    Ok(())
}

/// Builds physical `TableDef`s for a non-default metric table prefix. The
/// definitions are leaked once per switch, which is negligible for an
/// operator-triggered action.
/// 为非默认指标表前缀构造物理 `TableDef`；每次切换泄漏少量定义，
/// 对运维触发的低频操作可以忽略。
fn prefixed_metric_tables(prefix: &str) -> Vec<&'static sakuio_db::TableDef> {
    let prefix = prefix.trim();
    if prefix.is_empty() || prefix == "metric_" {
        return Vec::new();
    }
    TABLES
        .iter()
        .filter(|table| table.name.starts_with("metric_"))
        .map(|table| {
            let suffix = &table.name["metric_".len()..];
            let name: &'static str = Box::leak(format!("{prefix}{suffix}").into_boxed_str());
            let def: &'static sakuio_db::TableDef = Box::leak(Box::new(sakuio_db::TableDef {
                name,
                cols: table.cols,
                pk: table.pk,
                fks: table.fks,
                indexes: table.indexes,
            }));
            def
        })
        .collect()
}

/// Tables whose row counts differ between the two databases.
/// 两侧行数不一致的表。
async fn verify_tables(
    source: &DbPool,
    target: &DbPool,
    selected: &[&'static sakuio_db::TableDef],
) -> Result<Vec<&'static sakuio_db::TableDef>, String> {
    let mut mismatched = Vec::new();
    for table in selected {
        let src = count_rows(source, source.kind(), table.name)
            .await
            .map_err(|e| format!("count {} on source: {e}", table.name))?;
        let dst = count_rows(target, target.kind(), table.name)
            .await
            .map_err(|e| format!("count {} on target: {e}", table.name))?;
        if src != dst {
            mismatched.push(*table);
            tracing::warn!(
                table = table.name,
                source = src,
                target = dst,
                "table row count mismatch"
            );
        }
    }
    Ok(mismatched)
}

/// Re-executes the current binary so the new main database takes effect.
/// Returns false when spawning failed (caller keeps serving on the old db).
/// 重新执行当前程序，让新主库配置生效。启动失败时返回 false（继续用旧库服务）。
pub(crate) fn spawn_restart() -> bool {
    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(error = %e, "resolve current exe for restart failed");
            return false;
        }
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    match std::process::Command::new(exe).args(args).spawn() {
        Ok(_) => {
            tracing::info!("main database switched, restarting process");
            std::process::exit(0);
        }
        Err(e) => {
            tracing::error!(error = %e, "spawn restart process failed; keep serving on the old database");
            false
        }
    }
}

/// Background flow: copy main database to the new one, verify (with retries
/// for rows written mid-copy), persist the new config, then restart. On any
/// failure the old database stays in use.
/// 后台流程：复制主库 → 校验（对复制期间的增量写入做多轮重试）→ 写入新配置 →
/// 重启；任一步失败则继续用旧库。
pub async fn run_main_switch(
    tasks: &DbTaskManager,
    source: &DbPool,
    target_dsn: &str,
    writer: &sakuio_metric::MetricWriterHandle,
    current_metric_store: Arc<sakuio_metric::MetricStore>,
) -> Result<(), String> {
    // Startup resolves the main DSN from the bootstrap SQLite, so a switch
    // driven by an environment override could never take effect.
    // 启动时从 bootstrap SQLite 解析主库 DSN；被环境变量覆盖时面板切换无法生效。
    if std::env::var_os("SAKUIO_DATABASE_DSN").is_some() {
        return Err(
            "SAKUIO_DATABASE_DSN overrides the main database; unset it before switching from the panel"
                .into(),
        );
    }
    tasks.phase("connecting target");
    let target = sakuio_db::AppDatabase::connect(target_dsn, true)
        .await
        .map_err(|e| format!("connect target database: {e}"))?;
    let dialect = target.kind();

    // Colocated metrics may use a custom table prefix; the target needs those
    // tables before the copy starts.
    // 合并存储可能使用自定义指标表前缀；复制前需在目标库建好对应表。
    let metric_cfg = sakuio_metric::MetricDbConfig::load(source)
        .await
        .map_err(|e| format!("load metric config: {e}"))?;
    let colocated = metric_cfg.dsn.trim().is_empty();
    if colocated {
        sakuio_db::ensure_metric_schema(target.pool(), &metric_cfg.table_prefix)
            .await
            .map_err(|e| format!("ensure metric schema: {e}"))?;
    }

    // Refuse to switch onto the database we are already using: clearing the
    // target tables would delete the live data.
    // 拒绝切换到当前正在使用的库：清空目标表会删掉线上数据。
    if sakuio_db::normalize_dsn(target.dsn()) == sakuio_db::normalize_dsn(source.dsn()) {
        return Err("target database is the current main database".into());
    }
    match sakuio_db::is_same_database(source, target.pool()).await {
        Ok(true) => return Err("target database is the current main database".into()),
        Ok(false) => {}
        Err(e) => {
            // Without a reliable identity check we could clear the live
            // database; refuse instead of guessing.
            // 无法可靠判定目标身份时可能清空线上库，宁可拒绝也不猜测。
            return Err(format!("cannot verify target database identity: {e}"));
        }
    }

    // 复制期间写入会让行数漂移；每次重试都清空并完整重拷，避免只补子表时
    // 持续触发外键失败。最多 4 轮，写流量收敛后校验通过。
    let mut tables: Vec<&'static sakuio_db::TableDef> = TABLES.iter().collect();
    if colocated {
        tables.extend(prefixed_metric_tables(&metric_cfg.table_prefix));
    }
    let mut verified = false;
    let mut last_error: Option<String> = None;
    for pass in 1..=4 {
        tasks.phase(if pass == 1 {
            "copying".to_string()
        } else {
            format!("re-copying (pass {pass})")
        });
        match copy_tables(source, target.pool(), dialect, &tables, true, tasks).await {
            Ok(()) => {
                tasks.phase("verifying");
                match verify_tables(source, target.pool(), &tables).await {
                    Ok(mismatched) if mismatched.is_empty() => {
                        verified = true;
                        break;
                    }
                    Ok(mismatched) => {
                        let names = mismatched
                            .iter()
                            .map(|table| table.name)
                            .collect::<Vec<_>>()
                            .join(", ");
                        tracing::warn!(pass, tables = %names, "row count mismatch, retrying full copy");
                        last_error = Some(format!("tables still mismatched: {names}"));
                    }
                    Err(e) => {
                        tracing::warn!(pass, error = %e, "verification failed, retrying");
                        last_error = Some(e);
                    }
                }
            }
            Err(e) => {
                tracing::warn!(pass, error = %e, "copy pass failed, retrying");
                last_error = Some(e);
            }
        }
        if pass < 4 {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }
    if !verified {
        return Err(format!(
            "main database switch failed after retries: {}",
            last_error.unwrap_or_else(|| "verification failed".into())
        ));
    }

    tasks.phase("saving config");
    let driver = match dialect {
        DbKind::MySql => "mysql",
        DbKind::Postgres => "postgresql",
        DbKind::Sqlite => "sqlite",
    };
    let repo = sakuio_db::SettingsRepository::new(target.pool());
    repo.set_json("app_db_driver", &driver.to_string())
        .await
        .map_err(|e| format!("save app_db_driver: {e}"))?;
    repo.set_json("app_db_dsn", &target.dsn().to_string())
        .await
        .map_err(|e| format!("save app_db_dsn: {e}"))?;

    // Colocated bucket tables live in the new main database; a separate
    // metric database keeps its existing store. Flush in-memory buckets there
    // before the process exits.
    // 合并存储的桶表会随主库迁移；独立指标库仍用原存储。退出前把内存中的
    // 桶写入对应目标。
    let flush_store = if colocated {
        Arc::new(
            sakuio_metric::MetricStore::connect(target.pool(), &metric_cfg)
                .await
                .map_err(|e| format!("open target metric store: {e}"))?,
        )
    } else {
        current_metric_store
    };

    // The next process reads the locator from the bootstrap database, not from
    // the target, so it must be updated before the restart.
    // 下一个进程从 bootstrap 库读取定位信息（而不是目标库），因此重启前必须更新它。
    let bootstrap = sakuio_db::open_bootstrap_db()
        .await
        .map_err(|e| format!("open bootstrap database: {e}"))?;
    let locator = sakuio_db::SettingsRepository::new(bootstrap.pool());
    locator
        .set_json("app_db_driver", &driver.to_string())
        .await
        .map_err(|e| format!("save bootstrap app_db_driver: {e}"))?;
    locator
        .set_json("app_db_dsn", &target.dsn().to_string())
        .await
        .map_err(|e| format!("save bootstrap app_db_dsn: {e}"))?;

    tasks.phase("flushing metrics");
    if let Err(e) = writer.flush_to_store(flush_store).await {
        // Keep the old database active: undo the locator so a later restart
        // does not open a target that is missing the pending buckets.
        // 保持旧库可用：回滚定位信息，避免之后重启打开缺少待落盘桶的目标库。
        let source_driver = match source.kind() {
            DbKind::MySql => "mysql",
            DbKind::Postgres => "postgresql",
            DbKind::Sqlite => "sqlite",
        };
        let _ = locator
            .set_json("app_db_driver", &source_driver.to_string())
            .await;
        let _ = locator
            .set_json("app_db_dsn", &source.dsn().to_string())
            .await;
        bootstrap.shutdown().await;
        return Err(format!("flush in-memory metric buckets: {e}"));
    }
    bootstrap.shutdown().await;

    tasks.phase("restarting");
    target.shutdown().await;
    // 给 HTTP 响应留出刷盘时间再退出进程。
    tokio::time::sleep(std::time::Duration::from_millis(800)).await;
    if !spawn_restart() {
        return Err(
            "new database is verified and config saved, but auto-restart failed; restart manually to apply"
                .into(),
        );
    }
    Ok(())
}
