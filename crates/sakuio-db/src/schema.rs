//! Declarative schema: one set of table definitions rendered per dialect.
//! 声明式表结构：一套定义，按 SQLite / MySQL / PostgreSQL 方言渲染执行。

use crate::DbPool;
use sea_query::{
    Alias, ColumnDef, ForeignKey, ForeignKeyAction, Index, IndexCreateStatement, MysqlQueryBuilder,
    PostgresQueryBuilder, SchemaStatementBuilder, SqliteQueryBuilder, Table, TableCreateStatement,
};
use sqlx::Row;

/// Supported database backends.
/// 支持的数据库后端。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbKind {
    Sqlite,
    MySql,
    Postgres,
}

impl DbKind {
    /// Detects the backend from a DSN; empty means local SQLite.
    /// 根据连接串识别后端；留空表示本机 SQLite。
    pub fn detect(dsn: &str) -> Self {
        let dsn = dsn.trim().to_ascii_lowercase();
        if dsn.starts_with("mysql://") || dsn.starts_with("mariadb://") {
            Self::MySql
        } else if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
            Self::Postgres
        } else {
            Self::Sqlite
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::MySql => "mysql",
            Self::Postgres => "postgres",
        }
    }
}

/// Column storage type.
/// 列存储类型。`Key` 为可建索引的定长字符串（MySQL 的 TEXT 不能做主键/索引）。
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Ty {
    Key,
    Text,
    Int,
    BigInt,
    Double,
    /// Auto-increment integer primary key.
    /// 自增整型主键。
    AutoId,
}

#[derive(Clone, Copy)]
pub enum Def {
    None,
    Int(i64),
    Real(f64),
    Str(&'static str),
}

#[derive(Clone, Copy)]
pub struct Col {
    pub name: &'static str,
    pub ty: Ty,
    pub nullable: bool,
    pub default: Def,
}

pub struct Idx {
    pub name: &'static str,
    pub cols: &'static [&'static str],
    pub unique: bool,
}

pub struct Fk {
    pub name: &'static str,
    pub col: &'static str,
    pub ref_table: &'static str,
    pub ref_col: &'static str,
}

pub struct TableDef {
    pub name: &'static str,
    pub cols: &'static [Col],
    pub pk: &'static [&'static str],
    pub fks: &'static [Fk],
    pub indexes: &'static [Idx],
}

const fn req(name: &'static str, ty: Ty) -> Col {
    Col {
        name,
        ty,
        nullable: false,
        default: Def::None,
    }
}

const fn opt(name: &'static str, ty: Ty) -> Col {
    Col {
        name,
        ty,
        nullable: true,
        default: Def::None,
    }
}

const fn def(name: &'static str, ty: Ty, default: Def) -> Col {
    Col {
        name,
        ty,
        nullable: false,
        default,
    }
}

const fn idx(name: &'static str, cols: &'static [&'static str]) -> Idx {
    Idx {
        name,
        cols,
        unique: false,
    }
}

const fn uniq(name: &'static str, cols: &'static [&'static str]) -> Idx {
    Idx {
        name,
        cols,
        unique: true,
    }
}

use Def::{Int as DI, Real as DR, Str as DS};
use Ty::{AutoId, BigInt, Double, Int, Key, Text};

/// All tables in dependency order.
/// 按依赖顺序排列的全部表。
pub static TABLES: &[TableDef] = &[
    TableDef {
        name: "system_settings",
        cols: &[req("key", Key), req("value", Text)],
        pk: &["key"],
        fks: &[],
        indexes: &[],
    },
    TableDef {
        name: "admin_users",
        cols: &[
            req("id", Key),
            req("username", Key),
            req("password_hash", Text),
            req("created_at", Key),
            opt("totp_secret", Text),
            def("totp_enabled", Int, DI(0)),
        ],
        pk: &["id"],
        fks: &[],
        indexes: &[uniq("uq_admin_users_username", &["username"])],
    },
    TableDef {
        name: "admin_sessions",
        cols: &[
            req("id", Key),
            req("user_id", Key),
            req("token_hash", Key),
            req("expires_at", Key),
            req("created_at", Key),
            opt("user_agent", Text),
            opt("ip_address", Key),
        ],
        pk: &["id"],
        fks: &[Fk {
            name: "fk_admin_sessions_user",
            col: "user_id",
            ref_table: "admin_users",
            ref_col: "id",
        }],
        indexes: &[uniq("uq_admin_sessions_token", &["token_hash"])],
    },
    TableDef {
        name: "clients",
        cols: &[
            req("uuid", Key),
            def("name", Key, DS("")),
            req("token", Key),
            def("online", Int, DI(0)),
            opt("basic_info_json", Text),
            req("created_at", Key),
            req("updated_at", Key),
            opt("last_seen_at", Key),
            opt("agent_version", Key),
            def("disabled", Int, DI(0)),
            def("ipv4", Key, DS("")),
            def("ipv6", Key, DS("")),
            def("group_name", Key, DS("")),
            def("private_remark", Text, DS("")),
            def("price", Double, DR(0.0)),
            def("billing_cycle", Int, DI(0)),
            def("currency", Key, DS("$")),
            opt("expired_at", Key),
            def("auto_renewal", Int, DI(0)),
            def("traffic_threshold", BigInt, DI(0)),
            def("traffic_stat_mode", Key, DS("max")),
            def("traffic_used_up", BigInt, DI(0)),
            def("traffic_used_down", BigInt, DI(0)),
            opt("traffic_last_json", Text),
            opt("traffic_cycle_start", Key),
            def("traffic_reset_mode", Key, DS("cycle")),
            def("bandwidth", Double, DR(0.0)),
            def("bandwidth_unit", Key, DS("Mbps")),
            def("report_interval_ms", BigInt, DI(1000)),
            opt("sort_order", Double),
            opt("last_net_total_up", BigInt),
            opt("last_net_total_down", BigInt),
            opt("last_uptime_secs", BigInt),
            def("region", Key, DS("")),
            def("region6", Key, DS("")),
        ],
        pk: &["uuid"],
        fks: &[],
        indexes: &[uniq("uq_clients_token", &["token"])],
    },
    TableDef {
        name: "metric_samples",
        cols: &[
            req("id", AutoId),
            req("node_uuid", Key),
            req("observed_at_ms", BigInt),
            req("payload_json", Text),
            req("created_at", Key),
        ],
        pk: &["id"],
        fks: &[],
        indexes: &[idx(
            "idx_metric_samples_node_time",
            &["node_uuid", "observed_at_ms"],
        )],
    },
    TableDef {
        name: "metric_buckets",
        cols: &[
            req("id", AutoId),
            req("node_uuid", Key),
            req("metric_key", Key),
            req("bucket_seconds", BigInt),
            req("bucket_start_ms", BigInt),
            req("avg_val", Double),
            req("min_val", Double),
            req("max_val", Double),
            req("sample_count", BigInt),
        ],
        pk: &["id"],
        fks: &[],
        indexes: &[
            uniq(
                "uq_metric_bucket",
                &[
                    "node_uuid",
                    "metric_key",
                    "bucket_seconds",
                    "bucket_start_ms",
                ],
            ),
            idx(
                "idx_metric_buckets_time",
                &["bucket_seconds", "bucket_start_ms"],
            ),
            idx(
                "idx_metric_buckets_dashboard",
                &[
                    "bucket_seconds",
                    "metric_key",
                    "bucket_start_ms",
                    "node_uuid",
                ],
            ),
        ],
    },
    TableDef {
        name: "metric_retention",
        cols: &[req("metric_key", Key), def("days", BigInt, DI(90))],
        pk: &["metric_key"],
        fks: &[],
        indexes: &[],
    },
    TableDef {
        name: "ping_tasks",
        cols: &[
            req("id", AutoId),
            def("weight", Int, DI(0)),
            req("name", Key),
            def("clients_json", Text, DS("[]")),
            def("default_on", Int, DI(0)),
            def("type", Key, DS("icmp")),
            req("target", Key),
            def("interval_secs", Int, DI(60)),
            req("created_at", Key),
            req("updated_at", Key),
        ],
        pk: &["id"],
        fks: &[],
        indexes: &[idx("idx_ping_tasks_weight", &["weight", "id"])],
    },
    TableDef {
        name: "ping_records",
        cols: &[
            req("id", AutoId),
            req("client_uuid", Key),
            req("task_id", BigInt),
            req("observed_at_ms", BigInt),
            req("value_ms", BigInt),
        ],
        pk: &["id"],
        fks: &[Fk {
            name: "fk_ping_records_task",
            col: "task_id",
            ref_table: "ping_tasks",
            ref_col: "id",
        }],
        indexes: &[
            idx(
                "idx_ping_records_client_time",
                &["client_uuid", "observed_at_ms"],
            ),
            idx("idx_ping_records_task_time", &["task_id", "observed_at_ms"]),
        ],
    },
    TableDef {
        name: "exec_tasks",
        cols: &[
            req("task_id", Key),
            req("command", Text),
            def("clients_json", Text, DS("[]")),
            req("created_at", Key),
        ],
        pk: &["task_id"],
        fks: &[],
        indexes: &[],
    },
    TableDef {
        name: "exec_task_results",
        cols: &[
            req("task_id", Key),
            req("client_uuid", Key),
            def("result", Text, DS("")),
            opt("exit_code", Int),
            opt("finished_at", Key),
            req("created_at", Key),
        ],
        pk: &["task_id", "client_uuid"],
        fks: &[Fk {
            name: "fk_exec_task_results_task",
            col: "task_id",
            ref_table: "exec_tasks",
            ref_col: "task_id",
        }],
        indexes: &[],
    },
    TableDef {
        name: "notification_channel_instances",
        cols: &[
            req("id", Key),
            req("channel_type", Key),
            req("name", Key),
            def("enabled", Int, DI(0)),
            def("config_json", Text, DS("{}")),
            def("last_used_at", Key, DS("")),
            req("updated_at", Key),
        ],
        pk: &["id"],
        fks: &[],
        indexes: &[],
    },
    TableDef {
        name: "notification_rules",
        cols: &[
            req("id", Key),
            req("name", Key),
            req("event_type", Key),
            def("threshold", Int, DI(0)),
            def("duration_secs", Int, DI(300)),
            def("enabled", Int, DI(1)),
            def("channels_json", Text, DS("[]")),
            def("clients_json", Text, DS("[]")),
            def("template_title", Text, DS("")),
            def("template_body", Text, DS("")),
            req("updated_at", Key),
        ],
        pk: &["id"],
        fks: &[],
        indexes: &[],
    },
    TableDef {
        name: "ip_geo_cache",
        cols: &[
            req("ip", Key),
            def("country_code", Key, DS("")),
            def("location", Key, DS("")),
            req("expires_at", BigInt),
            req("updated_at", Key),
        ],
        pk: &["ip"],
        fks: &[],
        indexes: &[idx("idx_ip_geo_cache_expires", &["expires_at"])],
    },
    TableDef {
        name: "audit_logs",
        cols: &[
            req("id", AutoId),
            req("created_at", Key),
            def("actor", Key, DS("")),
            def("actor_id", Key, DS("")),
            def("action", Key, DS("")),
            def("target", Key, DS("")),
            def("detail", Text, DS("")),
            def("ip", Key, DS("")),
            def("user_agent", Text, DS("")),
            def("status", Int, DI(0)),
            def("method", Key, DS("")),
            def("path", Key, DS("")),
            def("geo_country_code", Key, DS("")),
            def("geo_location", Key, DS("")),
        ],
        pk: &["id"],
        fks: &[],
        indexes: &[
            idx("idx_audit_logs_created", &["created_at"]),
            idx("idx_audit_logs_actor", &["actor", "created_at"]),
            idx("idx_audit_logs_action", &["action", "created_at"]),
            idx("idx_audit_logs_ip", &["ip", "created_at"]),
            idx("idx_audit_logs_target", &["target", "created_at"]),
        ],
    },
    TableDef {
        name: "ping_rollup_hourly",
        cols: &[
            req("task_id", BigInt),
            req("client_uuid", Key),
            req("hour_start_ms", BigInt),
            def("samples", BigInt, DI(0)),
            def("valid", BigInt, DI(0)),
            def("loss_count", BigInt, DI(0)),
            def("sum_ms", Double, DR(0.0)),
            def("sum_sq_ms", Double, DR(0.0)),
            def("min_ms", Double, DR(0.0)),
            def("max_ms", Double, DR(0.0)),
        ],
        pk: &["task_id", "client_uuid", "hour_start_ms"],
        fks: &[],
        indexes: &[idx(
            "idx_ping_rollup_hour",
            &["hour_start_ms", "task_id", "client_uuid"],
        )],
    },
    TableDef {
        name: "auth_providers",
        cols: &[
            req("id", AutoId),
            def("name", Key, DS("")),
            req("driver", Key),
            def("config_json", Text, DS("{}")),
            def("last_validated_at", Key, DS("")),
            def("last_used_at", Key, DS("")),
            req("created_at", Key),
            req("updated_at", Key),
        ],
        pk: &["id"],
        fks: &[],
        indexes: &[idx("idx_auth_providers_driver", &["driver"])],
    },
    TableDef {
        name: "ddns_records",
        cols: &[
            req("id", AutoId),
            def("name", Key, DS("")),
            req("provider_id", BigInt),
            req("client_uuid", Key),
            req("zone_name", Key),
            req("record_name", Key),
            def("record_type", Key, DS("A")),
            def("proxied", Int, DI(0)),
            def("ttl", Int, DI(1)),
            def("enabled", Int, DI(1)),
            def("zone_id", Key, DS("")),
            def("remote_record_id", Key, DS("")),
            def("last_ip", Key, DS("")),
            def("last_error", Text, DS("")),
            opt("last_synced_at", Key),
            req("created_at", Key),
            req("updated_at", Key),
        ],
        pk: &["id"],
        fks: &[Fk {
            name: "fk_ddns_provider",
            col: "provider_id",
            ref_table: "auth_providers",
            ref_col: "id",
        }],
        indexes: &[
            idx("idx_ddns_client", &["client_uuid"]),
            idx("idx_ddns_provider", &["provider_id"]),
        ],
    },
    TableDef {
        name: "dashboard_fleet_points",
        cols: &[
            req("range_code", Key),
            req("bucket_start_ms", BigInt),
            def("up_rate", Double, DR(0.0)),
            def("down_rate", Double, DR(0.0)),
            def("up_cum", Double, DR(0.0)),
            def("down_cum", Double, DR(0.0)),
            def("cpu_avg", Double, DR(0.0)),
            def("mem_used", Double, DR(0.0)),
            def("mem_total", Double, DR(0.0)),
            def("online_nodes", BigInt, DI(0)),
            def("sample_count", BigInt, DI(0)),
        ],
        pk: &["range_code", "bucket_start_ms"],
        fks: &[],
        indexes: &[idx(
            "idx_dashboard_fleet_range",
            &["range_code", "bucket_start_ms"],
        )],
    },
    TableDef {
        name: "dashboard_node_range",
        cols: &[
            req("range_code", Key),
            req("node_uuid", Key),
            def("cpu_avg", Double, DR(0.0)),
            def("cpu_peak", Double, DR(0.0)),
            def("cpu_peak_at", BigInt, DI(0)),
            def("mem_avg", Double, DR(0.0)),
            def("mem_peak", Double, DR(0.0)),
            def("mem_peak_at", BigInt, DI(0)),
            def("mem_total", Double, DR(0.0)),
            def("disk_avg", Double, DR(0.0)),
            def("traffic_up", Double, DR(0.0)),
            def("traffic_down", Double, DR(0.0)),
            def("peak_rate", Double, DR(0.0)),
            def("peak_rate_at", BigInt, DI(0)),
            def("sample_count", BigInt, DI(0)),
        ],
        pk: &["range_code", "node_uuid"],
        fks: &[],
        indexes: &[
            idx("idx_dashboard_node_cpu", &["range_code", "cpu_avg"]),
            idx("idx_dashboard_node_mem", &["range_code", "mem_avg"]),
            idx(
                "idx_dashboard_node_traffic",
                &["range_code", "traffic_up", "traffic_down"],
            ),
        ],
    },
    TableDef {
        name: "dashboard_ping_range",
        cols: &[
            req("range_code", Key),
            req("task_id", BigInt),
            req("client_uuid", Key),
            def("samples", BigInt, DI(0)),
            def("valid", BigInt, DI(0)),
            def("loss", Double, DR(0.0)),
            def("p50_ms", Double, DR(0.0)),
            def("p95_ms", Double, DR(0.0)),
            def("p99_ms", Double, DR(0.0)),
            def("volatility", Double, DR(0.0)),
        ],
        pk: &["range_code", "task_id", "client_uuid"],
        fks: &[],
        indexes: &[
            idx("idx_dashboard_ping_p95", &["range_code", "p95_ms"]),
            idx(
                "idx_dashboard_ping_volatility",
                &["range_code", "volatility"],
            ),
            idx("idx_dashboard_ping_loss", &["range_code", "loss"]),
        ],
    },
    TableDef {
        name: "dashboard_meta",
        cols: &[
            req("range_code", Key),
            def("revision", BigInt, DI(0)),
            def("generated_at", BigInt, DI(0)),
            def("coverage_start", BigInt, DI(0)),
            def("status", Key, DS("warming")),
        ],
        pk: &["range_code"],
        fks: &[],
        indexes: &[],
    },
    TableDef {
        name: "admin_dashboard_layouts",
        cols: &[
            req("admin_id", Key),
            def("layout_json", Text, DS("[]")),
            def("updated_at", BigInt, DI(0)),
        ],
        pk: &["admin_id"],
        fks: &[],
        indexes: &[],
    },
];

fn render<S: SchemaStatementBuilder>(kind: DbKind, stmt: &S) -> String {
    match kind {
        DbKind::Sqlite => stmt.to_string(SqliteQueryBuilder),
        DbKind::MySql => stmt.to_string(MysqlQueryBuilder),
        DbKind::Postgres => stmt.to_string(PostgresQueryBuilder),
    }
}

fn column(kind: DbKind, col: &Col, single_pk: bool) -> ColumnDef {
    let mut c = ColumnDef::new(col.name);
    match col.ty {
        // 191 字符：utf8mb4 下联合索引仍在 InnoDB 键长限制内。
        Ty::Key => {
            c.string_len(191);
        }
        Ty::Text => {
            c.text();
        }
        Ty::Int => {
            c.integer();
        }
        Ty::BigInt => {
            c.big_integer();
        }
        Ty::Double => {
            c.double();
        }
        Ty::AutoId => {
            // SQLite 仅 INTEGER PRIMARY KEY 可 AUTOINCREMENT。
            if kind == DbKind::Sqlite {
                c.integer();
            } else {
                c.big_integer();
            }
            c.auto_increment();
        }
    }
    if !col.nullable {
        c.not_null();
    }
    match col.default {
        Def::None => {}
        Def::Int(v) => {
            c.default(v);
        }
        Def::Real(v) => {
            c.default(v);
        }
        // MySQL 的 TEXT 只接受表达式默认值（8.0.13+）。
        Def::Str(v) if kind == DbKind::MySql && col.ty == Ty::Text => {
            c.extra(format!("DEFAULT ('{}')", v.replace('\'', "''")));
        }
        Def::Str(v) => {
            c.default(v);
        }
    }
    if single_pk {
        c.primary_key();
    }
    c
}

fn create_statement(kind: DbKind, table: &TableDef, physical: &str) -> TableCreateStatement {
    let mut stmt = Table::create();
    stmt.table(Alias::new(physical)).if_not_exists();
    let single_pk = table.pk.len() == 1;
    for col in table.cols {
        stmt.col(column(kind, col, single_pk && table.pk[0] == col.name));
    }
    if table.pk.len() > 1 {
        let mut pk = Index::create();
        for name in table.pk {
            pk.col(*name);
        }
        stmt.primary_key(&mut pk);
    }
    for fk in table.fks {
        stmt.foreign_key(
            ForeignKey::create()
                .name(fk.name)
                .from(Alias::new(physical), fk.col)
                .to(fk.ref_table, fk.ref_col)
                .on_delete(ForeignKeyAction::Cascade),
        );
    }
    stmt
}

fn index_statement(table: &str, index_name: &str, index: &Idx) -> IndexCreateStatement {
    let mut stmt = Index::create();
    stmt.name(index_name.to_string()).table(Alias::new(table));
    for name in index.cols {
        stmt.col(*name);
    }
    if index.unique {
        stmt.unique();
    }
    stmt
}

/// Renders one CREATE TABLE for the dialect (diagnostics / tests).
/// 按方言渲染 CREATE TABLE（诊断与测试用）。
#[allow(dead_code)]
pub fn render_create(kind: DbKind, table: &TableDef) -> String {
    render(kind, &create_statement(kind, table, table.name))
}

/// Ensures every table / index / column exists, additively, on the given pool.
/// `only` 为空时同步全部表；否则仅同步指定表（独立监控库只需桶/保留表）。
/// 对给定连接池幂等确保全部表、索引与列存在（只增不改）。
pub async fn ensure_schema(pool: &DbPool, kind: DbKind, only: &[&str]) -> Result<(), sqlx::Error> {
    for table in TABLES {
        if !only.is_empty() && !only.contains(&table.name) {
            continue;
        }
        ensure_table(pool, kind, table, table.name, "").await?;
    }
    Ok(())
}

/// Ensures the monitoring bucket/retention tables using `prefix`
/// (`metric_` by default, `prefix + "buckets"` physically).
/// 按 `prefix` 确保监控桶/保留表（默认 `metric_`，物理表名为前缀 + 后缀）。
pub async fn ensure_metric_schema(pool: &DbPool, prefix: &str) -> Result<(), sqlx::Error> {
    let prefix = validate_prefix(prefix)?;
    let kind = pool.kind();
    for table in TABLES.iter().filter(|t| t.name.starts_with("metric_")) {
        let suffix = &table.name["metric_".len()..];
        let physical = format!("{prefix}{suffix}");
        ensure_table(pool, kind, table, &physical, &prefix).await?;
    }
    Ok(())
}

fn validate_prefix(prefix: &str) -> Result<String, sqlx::Error> {
    let prefix = prefix.trim();
    let valid = !prefix.is_empty()
        && prefix.len() <= 32
        && prefix
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && prefix
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_');
    if valid {
        Ok(prefix.to_string())
    } else {
        Err(sqlx::Error::Configuration(
            format!("invalid table prefix: {prefix:?}").into(),
        ))
    }
}

async fn ensure_table(
    pool: &DbPool,
    kind: DbKind,
    table: &TableDef,
    physical: &str,
    index_prefix: &str,
) -> Result<(), sqlx::Error> {
    if !table_exists(pool, kind, physical).await? {
        let ddl = render(kind, &create_statement(kind, table, physical));
        sqlx::query(&ddl).execute(pool).await?;
    } else {
        let existing = existing_columns(pool, kind, physical).await?;
        for col in table.cols {
            if existing.iter().any(|c| c.eq_ignore_ascii_case(col.name)) {
                continue;
            }
            // 存量表只能补可空或带默认值的列。
            if !col.nullable && matches!(col.default, Def::None) {
                continue;
            }
            // sea-query 语句含非 Send 的标识符，须在 await 前渲染成字符串。
            let ddl = render(
                kind,
                &Table::alter()
                    .table(Alias::new(physical))
                    .add_column(column(kind, col, false))
                    .to_owned(),
            );
            sqlx::query(&ddl).execute(pool).await?;
        }
    }
    for index in table.indexes {
        let index_name = if index_prefix.is_empty() {
            index.name.to_string()
        } else {
            format!("{index_prefix}{}", index.name)
        };
        if !index_exists(pool, kind, physical, &index_name).await? {
            let ddl = render(kind, &index_statement(physical, &index_name, index));
            sqlx::query(&ddl).execute(pool).await?;
        }
    }
    Ok(())
}

async fn count(pool: &DbPool, sql: &str) -> Result<i64, sqlx::Error> {
    let row = sqlx::query(sql).fetch_one(pool).await?;
    Ok(row.try_get::<i64, _>(0).unwrap_or(0))
}

/// Identifiers here come only from the static table list, so inlining is safe
/// and avoids dialect-specific bind placeholders.
/// 标识符均来自静态表定义，直接内联可规避各方言占位符差异。
async fn table_exists(pool: &DbPool, kind: DbKind, name: &str) -> Result<bool, sqlx::Error> {
    let sql = match kind {
        DbKind::Sqlite => {
            format!("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '{name}'")
        }
        DbKind::MySql => format!(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '{name}'"
        ),
        DbKind::Postgres => format!(
            "SELECT COUNT(*)::bigint FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = '{name}'"
        ),
    };
    Ok(count(pool, &sql).await? > 0)
}

async fn existing_columns(
    pool: &DbPool,
    kind: DbKind,
    table: &str,
) -> Result<Vec<String>, sqlx::Error> {
    let (sql, col) = match kind {
        DbKind::Sqlite => (format!("PRAGMA table_info(\"{table}\")"), 1),
        DbKind::MySql => (
            format!(
                "SELECT CAST(column_name AS CHAR) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = '{table}'"
            ),
            0,
        ),
        DbKind::Postgres => (
            format!(
                "SELECT column_name::text FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = '{table}'"
            ),
            0,
        ),
    };
    let rows = sqlx::query(&sql).fetch_all(pool).await?;
    Ok(rows
        .iter()
        .filter_map(|r| r.try_get::<String, _>(col).ok())
        .collect())
}

async fn index_exists(
    pool: &DbPool,
    kind: DbKind,
    table: &str,
    name: &str,
) -> Result<bool, sqlx::Error> {
    let sql = match kind {
        DbKind::Sqlite => {
            format!("SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = '{name}'")
        }
        DbKind::MySql => format!(
            "SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = '{table}' AND index_name = '{name}'"
        ),
        DbKind::Postgres => format!(
            "SELECT COUNT(*)::bigint FROM pg_indexes WHERE schemaname = current_schema() AND indexname = '{name}'"
        ),
    };
    Ok(count(pool, &sql).await? > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Diagnostic dump: `SAKUIO_DUMP_DDL=1 cargo test -p sakuio-db dumps_ddl -- --nocapture`.
    /// 诊断输出：按方言打印全部建表语句，便于人工核对。
    #[test]
    fn dumps_ddl() {
        if std::env::var("SAKUIO_DUMP_DDL").is_err() {
            return;
        }
        for kind in [DbKind::Sqlite, DbKind::MySql, DbKind::Postgres] {
            for table in TABLES {
                println!("--- {} {}", kind.as_str(), table.name);
                println!("{}", render_create(kind, table));
            }
        }
    }
}
