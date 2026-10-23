//! Admin database settings API: DSNs, sizes, advanced options, and the
//! mutually-exclusive database task flows (main switch / metrics migration).
//! 管理端数据库设置 API：连接串、容量、高级选项，以及互斥的数据库任务流
//! （主库切换 / 监控库迁移与同步）。

use crate::db_task::{DbTaskKind, DbTaskManager, DbTaskState};
use crate::AppState;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use sakuio_db::{DbKind, DbPool};
use sakuio_metric::MetricDbConfig;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::watch;

/// Shared runtime for the monitoring store: hot-swappable store handle,
/// retention policy cache, and the database task manager.
/// 指标存储运行时共享状态：可热切换的存储、保留策略缓存与数据库任务管理器。
pub struct MetricRuntime {
    pub store: watch::Receiver<Arc<sakuio_metric::MetricStore>>,
    pub handle: sakuio_metric::MetricWriterHandle,
    pub tasks: DbTaskManager,
    pub main_kind: DbKind,
    pub main_dsn: String,
}

impl MetricRuntime {
    pub async fn current_store(&self) -> Arc<sakuio_metric::MetricStore> {
        self.store.borrow().clone()
    }

    /// Toggles the process-wide maintenance flag used by write-path guards.
    /// 切换进程级维护标记，供各写路径门禁使用。
    pub fn set_maintenance(on: bool) {
        sakuio_db::set_maintenance(on);
    }
}

/// GET /api/admin/settings/database — full page payload.
/// 数据库页面聚合信息。
pub async fn get_database_info(
    state: State<AppState>,
    _user: crate::auth::AdminUser,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let cfg = MetricDbConfig::load(&state.db)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let store = state.metric.current_store().await;
    let metric_size = store.size_bytes().await.unwrap_or(0);
    let main_size = main_db_size_bytes(&state).await;

    Ok(Json(serde_json::json!({
        "success": true,
        "maintenance": sakuio_db::maintenance_active(),
        "main": {
            "kind": state.metric.main_kind.as_str(),
            "dsn": state.metric.main_dsn,
            "size_bytes": main_size,
            "max_connections": state.db.max_connections(),
        },
        "metrics": {
            "kind": store.kind().as_str(),
            "dsn": cfg.dsn,
            "size_bytes": metric_size,
            "table_prefix": cfg.table_prefix,
            "max_connections": cfg.max_connections,
            "min_idle_connections": cfg.min_idle_connections,
            "bucket_1m_minutes": cfg.bucket_1m_minutes,
            "bucket_5m_minutes": cfg.bucket_5m_minutes,
            "bucket_1h_hours": cfg.bucket_1h_hours,
        },
        "retention": retention_payload(&store).await,
        // 兼容旧字段名；内容为统一任务状态。
        "migration": state.metric.tasks.snapshot(),
    })))
}

async fn retention_payload(store: &sakuio_metric::MetricStore) -> serde_json::Value {
    let days = store.retention_days().await.unwrap_or_default();
    let defs: Vec<serde_json::Value> = sakuio_metric::METRIC_KEYS
        .iter()
        .map(|d| {
            let days = days
                .iter()
                .find(|(k, _)| k == d.key)
                .map(|(_, v)| *v)
                .unwrap_or(90);
            serde_json::json!({
                "key": d.key,
                "kind": d.kind,
                "unit": d.unit,
                "days": days,
            })
        })
        .collect();
    serde_json::json!({ "items": defs })
}

/// PUT /api/admin/settings/database — save advanced options; a DSN change also
/// hot-swaps the monitoring store and starts a background full sync (no restart).
/// 保存高级选项；连接串变更时热切换监控库并后台异步同步旧数据（不重启）。
#[derive(Deserialize)]
pub struct UpdateDatabaseRequest {
    pub dsn: Option<String>,
    pub table_prefix: Option<String>,
    pub max_connections: Option<u32>,
    pub min_idle_connections: Option<u32>,
    pub bucket_1m_minutes: Option<i64>,
    pub bucket_5m_minutes: Option<i64>,
    pub bucket_1h_hours: Option<i64>,
    pub retention: Option<std::collections::HashMap<String, i64>>,
}

pub async fn update_database(
    state: State<AppState>,
    _user: crate::auth::AdminUser,
    Json(body): Json<UpdateDatabaseRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, &'static str)> {
    let old_cfg = MetricDbConfig::load(&state.db)
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "load config failed"))?;
    let mut cfg = old_cfg.clone();
    if let Some(dsn) = &body.dsn {
        cfg.dsn = dsn.trim().to_string();
    }
    if let Some(v) = &body.table_prefix {
        cfg.table_prefix = v.trim().to_string();
    }
    if let Some(v) = body.max_connections {
        cfg.max_connections = v.clamp(1, 10_000);
    }
    if let Some(v) = body.min_idle_connections {
        cfg.min_idle_connections = v.clamp(0, 10_000);
    }
    if let Some(v) = body.bucket_1m_minutes {
        cfg.bucket_1m_minutes = v.clamp(60, i64::MAX / 60_000);
    }
    if let Some(v) = body.bucket_5m_minutes {
        cfg.bucket_5m_minutes = v.clamp(300, i64::MAX / 60_000);
    }
    if let Some(v) = body.bucket_1h_hours {
        cfg.bucket_1h_hours = v.clamp(24, i64::MAX / 3_600_000);
    }

    let dsn_changed = cfg.dsn != old_cfg.dsn;
    let prefix_changed = cfg.table_prefix != old_cfg.table_prefix;
    let pool_changed = cfg.max_connections != old_cfg.max_connections
        || cfg.min_idle_connections != old_cfg.min_idle_connections;
    // A DSN, prefix or pool-size change needs a fresh store; retention-only
    // updates reuse the active one.
    // DSN、前缀或连接池变化需要重建存储；只改保留策略时复用当前存储。
    let reconnect = dsn_changed || prefix_changed || pool_changed;
    let tasks = state.metric.tasks.clone();
    if reconnect {
        // 换监控库/重建连接池也算数据库任务；同一时间只允许一个。
        tasks
            .begin(DbTaskKind::MetricsMigrate)
            .map_err(|_| (StatusCode::CONFLICT, "another database task is running"))?;
    }

    // 测试连接并预热新监控库；失败则不落配置。
    let old_store = state.metric.current_store().await;
    let new_store = if reconnect {
        match sakuio_metric::MetricStore::connect(&state.db, &cfg).await {
            Ok(store) => Some(Arc::new(store)),
            Err(e) => {
                tracing::warn!(error = %e, "metric db connect failed");
                tasks.finish(Some(e.to_string()));
                return Err((StatusCode::BAD_REQUEST, "metric db connect failed"));
            }
        }
    } else {
        None
    };
    let active_store = new_store.as_deref().unwrap_or(old_store.as_ref());

    if let Some(retention) = &body.retention {
        for (key, days) in retention {
            if !sakuio_metric::METRIC_KEYS
                .iter()
                .any(|d| d.key == key.as_str())
            {
                continue;
            }
            if let Err(e) = active_store
                .set_retention_days(key, (*days).clamp(0, 36_500))
                .await
            {
                tracing::warn!(error = %e, "save metric retention failed");
                if reconnect {
                    tasks.finish(Some(e.to_string()));
                }
                return Err((StatusCode::INTERNAL_SERVER_ERROR, "save retention failed"));
            }
        }
    }
    // Copy and verify metric buckets before committing the new configuration.
    // This keeps the previous database active on any failure.
    if let Some(new_store) = &new_store {
        if dsn_changed || prefix_changed {
            let has_data =
                table_exists(old_store.pool(), &format!("{}buckets", old_store.prefix())).await;
            if has_data {
                tasks.phase("copying metrics");
                if let Err(e) = sakuio_metric::run_bucket_sync(
                    old_store.as_ref(),
                    new_store.as_ref(),
                    |n, p| {
                        tasks.processed(n);
                        tasks.phase(p);
                    },
                )
                .await
                {
                    tracing::warn!(error = %e, "metric bucket migration failed");
                    tasks.finish(Some(e.to_string()));
                    return Err((StatusCode::INTERNAL_SERVER_ERROR, "metric migration failed"));
                }
                tasks.phase("verifying metrics");
                let old_table = format!("{}buckets", old_store.prefix());
                let new_table = format!("{}buckets", new_store.prefix());
                let old_count: Result<i64, _> = sqlx::query_scalar(&format!(
                    "SELECT COUNT(*) FROM {old_table}"
                ))
                .fetch_one(old_store.pool())
                .await;
                let new_count: Result<i64, _> = sqlx::query_scalar(&format!(
                    "SELECT COUNT(*) FROM {new_table}"
                ))
                .fetch_one(new_store.pool())
                .await;
                match (old_count, new_count) {
                    (Ok(old), Ok(new)) if new >= old => {}
                    (Ok(old), Ok(new)) => {
                        tasks.finish(Some("metric bucket verification failed".into()));
                        tracing::warn!(old, new, "metric bucket count mismatch");
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "metric bucket verification failed",
                        ));
                    }
                    (Err(e), _) | (_, Err(e)) => {
                        tasks.finish(Some(e.to_string()));
                        return Err((StatusCode::INTERNAL_SERVER_ERROR, "verify metrics failed"));
                    }
                }
            }
        }
    }

    if let Err(e) = cfg.save(&state.db).await {
        tracing::warn!(error = %e, "save metric db config failed");
        if reconnect {
            tasks.finish(Some(e.to_string()));
        }
        return Err((StatusCode::INTERNAL_SERVER_ERROR, "save config failed"));
    }
    if let Err(e) = crate::config::update_metric_database(&cfg.dsn, cfg.max_connections) {
        tracing::warn!(error = %e, "save metric database into config.yaml failed");
        if reconnect {
            tasks.finish(Some(e.to_string()));
        }
        return Err((StatusCode::INTERNAL_SERVER_ERROR, "save config.yaml failed"));
    }

    if let Some(new_store) = new_store {
        state.metric.handle.swap_store((*new_store).clone()).await;
        state.metric.handle.reload_policy(new_store.as_ref()).await;
        old_store.shutdown().await;
        if reconnect {
            tasks.finish(None);
        }
    }

    Ok(Json(serde_json::json!({ "success": true })))
}

/// PUT /api/admin/settings/database/main — switch the main database: test the
/// connection, then copy, verify and auto-restart in the background; any
/// failure keeps the current database in use.
/// 切换主库：先测连，后台复制、校验通过后写配置并自动重启；失败则继续用旧库。
#[derive(Deserialize)]
pub struct UpdateMainDatabaseRequest {
    pub driver: String,
    pub dsn: String,
    pub max_connections: Option<u32>,
}

pub async fn update_main_database(
    state: State<AppState>,
    _user: crate::auth::AdminUser,
    Json(body): Json<UpdateMainDatabaseRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, &'static str)> {
    let driver = body.driver.trim();
    let dsn = body.dsn.trim();
    if !matches!(
        driver,
        "sqlite" | "sqlite3" | "mysql" | "mariadb" | "postgresql" | "postgres"
    ) || dsn.is_empty()
    {
        return Err((StatusCode::BAD_REQUEST, "invalid driver or dsn"));
    }
    let kind = sakuio_db::DbKind::detect(dsn);
    let supplied = match driver.to_ascii_lowercase().as_str() {
        "sqlite" | "sqlite3" => "sqlite",
        "mysql" | "mariadb" => "mysql",
        _ => "postgresql",
    };
    if supplied != sakuio_db::canonical_driver(kind) {
        return Err((StatusCode::BAD_REQUEST, "driver does not match dsn"));
    }
    if std::env::var_os("SAKUIO_DATABASE_DSN").is_some() {
        return Err((
            StatusCode::BAD_REQUEST,
            "SAKUIO_DATABASE_DSN overrides the main database; unset it before switching",
        ));
    }
    if let Some(max) = body.max_connections {
        let max = max.clamp(1, 64);
        let current_max = state.db.max_connections();
        let mut config = crate::config::ServerConfig::load_or_create()
            .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "load config failed"))?;
        config.database.max_connections = max;
        config
            .save()
            .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "save config failed"))?;
        if dsn == state.db.dsn()
            || sakuio_db::normalize_dsn(dsn) == sakuio_db::normalize_dsn(state.db.dsn())
        {
            return Ok(Json(serde_json::json!({
                "success": true,
                "restart_required": max != current_max,
                "unchanged": max == current_max,
            })));
        }
    }
    state
        .metric
        .tasks
        .begin(DbTaskKind::MainSwitch)
        .map_err(|_| (StatusCode::CONFLICT, "another database task is running"))?;

    // 同步测试连接（顺带在目标库建好表结构）；失败立即报错，不进入迁移。
    let probe = match sakuio_db::AppDatabase::connect(dsn, true).await {
        Ok(db) => db,
        Err(e) => {
            tracing::warn!(error = %e, "main db switch probe failed");
            state.metric.tasks.finish(Some(e.to_string()));
            return Err((StatusCode::BAD_REQUEST, "main db connect failed"));
        }
    };
    // Refuse a no-op switch early: the copy step clears the target tables.
    // 提前拒绝原地切换：复制步骤会先清空目标表。
    let same = match sakuio_db::is_same_database(&state.db, probe.pool()).await {
        Ok(same) => same,
        Err(e) => {
            tracing::warn!(error = %e, "cannot compare main database identities");
            probe.shutdown().await;
            state
                .metric
                .tasks
                .finish(Some(format!("cannot verify target database identity: {e}")));
            return Err((
                StatusCode::BAD_REQUEST,
                "cannot verify target database identity",
            ));
        }
    };
    probe.shutdown().await;
    if same {
        state
            .metric
            .tasks
            .finish(Some("target database is the current main database".into()));
        return Err((
            StatusCode::BAD_REQUEST,
            "target database is the current main database",
        ));
    }

    let tasks = state.metric.tasks.clone();
    let main_pool = state.db.clone();
    let dsn = dsn.to_string();
    let writer = state.metric.handle.clone();
    let current_metric_store = state.metric.current_store().await;
    MetricRuntime::set_maintenance(true);
    tokio::spawn(async move {
        // Clears maintenance even if the task panics or the restart spawn
        // fails; the success path exits the process.
        // 即使任务 panic 或重启拉起失败也会解除维护标记；成功路径直接退出进程。
        struct MaintenanceReset;
        impl Drop for MaintenanceReset {
            fn drop(&mut self) {
                sakuio_db::set_maintenance(false);
            }
        }
        let _reset = MaintenanceReset;
        let result = crate::db_task::run_main_switch(
            &tasks,
            &main_pool,
            &dsn,
            &writer,
            current_metric_store,
        )
        .await;
        if let Err(e) = result {
            tracing::error!(error = %e, "main database switch failed");
            // 切换失败：继续用旧库服务，解除维护标记。
            MetricRuntime::set_maintenance(false);
            tasks.finish(Some(e));
        } else {
            tasks.finish(None);
        }
    });
    Ok(Json(serde_json::json!({ "success": true })))
}

/// POST /api/admin/settings/database/migrate — start a metrics migration job.
/// 启动监控数据迁移任务：源库（可选 DSN，留空为主库）→ 当前监控库。
#[derive(Deserialize)]
pub struct MigrateRequest {
    pub source_dsn: Option<String>,
}

pub async fn start_migration(
    state: State<AppState>,
    _user: crate::auth::AdminUser,
    Json(body): Json<MigrateRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, &'static str)> {
    let cfg = MetricDbConfig::load(&state.db)
        .await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "load config failed"))?;
    state
        .metric
        .tasks
        .begin(DbTaskKind::MetricsMigrate)
        .map_err(|_| (StatusCode::CONFLICT, "another database task is running"))?;

    let (source_pool, legacy, _close_after) =
        match sakuio_metric::connect_source(&state.db, body.source_dsn.as_deref(), &cfg).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "resolve migration source failed");
                state.metric.tasks.finish(Some(e.clone()));
                return Err((StatusCode::BAD_REQUEST, "resolve migration source failed"));
            }
        };
    // 源库是旧 metric_samples 所在的主库时走重聚合；否则直搬桶。
    let legacy = legacy || table_exists(&source_pool, "metric_samples").await;
    let target = state.metric.current_store().await;

    let tasks = state.metric.tasks.clone();
    let prefix = cfg.table_prefix.clone();
    tokio::spawn(async move {
        let result = sakuio_metric::run_metrics_migration(
            &target,
            source_pool.clone(),
            legacy,
            prefix,
            |n, p| {
                tasks.processed(n);
                tasks.phase(p);
            },
        )
        .await;
        match result {
            Ok(()) => tasks.finish(None),
            Err(e) => {
                tracing::warn!(error = %e, "metrics migration failed");
                tasks.finish(Some(e));
            }
        }
    });
    Ok(Json(serde_json::json!({ "success": true })))
}

pub async fn migration_status(
    state: State<AppState>,
    _user: crate::auth::AdminUser,
) -> Result<Json<DbTaskState>, StatusCode> {
    Ok(Json(state.metric.tasks.snapshot()))
}

/// Dialect-neutral existence probe: selecting from a missing table errors.
/// 方言无关的表存在探测：表不存在时查询报错。
async fn table_exists(pool: &DbPool, name: &str) -> bool {
    sqlx::query(&format!("SELECT 1 FROM {name} LIMIT 1"))
        .fetch_optional(pool)
        .await
        .is_ok()
}

async fn main_db_size_bytes(state: &AppState) -> u64 {
    let sql = match state.metric.main_kind {
        DbKind::Sqlite => {
            "SELECT CAST((SELECT page_count FROM pragma_page_count) * (SELECT page_size FROM pragma_page_size) AS TEXT)"
        }
        DbKind::MySql => {
            "SELECT CAST(SUM(data_length + index_length) AS CHAR) FROM information_schema.tables WHERE table_schema = DATABASE()"
        }
        DbKind::Postgres => "SELECT CAST(pg_database_size(current_database()) AS TEXT)",
    };
    let row: Option<(Option<String>,)> = sqlx::query_as(sql)
        .fetch_optional(&state.db)
        .await
        .ok()
        .flatten();
    row.and_then(|(v,)| v)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0)
}
