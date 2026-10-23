//! Audit log storage with keyset pagination and bounded retention cleanup.
//! 审计日志存储：游标分页（避免深 OFFSET）与分批保留清理。

use crate::{DbPool, DbResult};
use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use sqlx::Row;

/// One persisted audit record.
/// 一条已持久化的审计记录。
#[derive(Debug, Clone, Serialize)]
pub struct AuditLogRow {
    pub id: i64,
    pub created_at: String,
    pub actor: String,
    pub actor_id: String,
    pub action: String,
    pub target: String,
    pub detail: String,
    pub ip: String,
    pub user_agent: String,
    pub status: i64,
    pub method: String,
    pub path: String,
    pub geo_country_code: String,
    pub geo_location: String,
}

/// A record waiting to be written.
/// 待写入的审计记录。
#[derive(Debug, Clone)]
pub struct NewAuditLog {
    pub created_at: String,
    pub actor: String,
    pub actor_id: String,
    pub action: String,
    pub target: String,
    pub detail: String,
    pub ip: String,
    pub user_agent: String,
    pub status: i64,
    pub method: String,
    pub path: String,
    pub geo_country_code: String,
    pub geo_location: String,
}

/// Keyset filter; `before_id` walks backwards from the newest rows.
/// 游标过滤；`before_id` 从最新记录向前翻页。
#[derive(Debug, Clone, Default)]
pub struct AuditLogFilter {
    pub before_id: Option<i64>,
    pub limit: i64,
    pub actor: String,
    pub action: String,
    pub actions: Vec<String>,
    pub ip: String,
    pub query: String,
    pub from: String,
    pub to: String,
}

fn clip(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        value.to_string()
    } else {
        value.chars().take(max_chars).collect()
    }
}

pub struct AuditLogRepository<'a> {
    pool: &'a DbPool,
}

impl<'a> AuditLogRepository<'a> {
    pub fn new(pool: &'a DbPool) -> Self {
        Self { pool }
    }

    /// Inserts up to `MAX_BATCH` rows in one statement.
    /// 单条语句批量写入，降低高频审计的数据库往返开销。
    pub async fn insert_batch(&self, rows: &[NewAuditLog]) -> DbResult<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let columns = "(created_at, actor, actor_id, action, target, detail, ip, user_agent, status, method, path, geo_country_code, geo_location)";
        let placeholders = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        let values = std::iter::repeat_n(placeholders, rows.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("INSERT INTO audit_logs {columns} VALUES {values}");
        let mut query = sqlx::query(&sql);
        for row in rows {
            query = query
                .bind(clip(&row.created_at, 40))
                .bind(clip(&row.actor, 191))
                .bind(clip(&row.actor_id, 191))
                .bind(clip(&row.action, 191))
                .bind(clip(&row.target, 191))
                .bind(clip(&row.detail, 4096))
                .bind(clip(&row.ip, 191))
                .bind(&row.user_agent)
                .bind(row.status)
                .bind(clip(&row.method, 32))
                .bind(clip(&row.path, 191))
                .bind(clip(&row.geo_country_code, 191))
                .bind(clip(&row.geo_location, 191));
        }
        query.execute(self.pool).await?;
        Ok(())
    }

    /// Reads one page (newest first). Returns `limit + 1` rows so callers can
    /// decide `has_more` without a COUNT over a huge table.
    /// 读取一页（由新到旧），返回 `limit + 1` 行以判断是否还有更多。
    pub async fn list(&self, filter: &AuditLogFilter) -> DbResult<Vec<AuditLogRow>> {
        let mut sql = String::from(
            "SELECT id, created_at, actor, actor_id, action, target, detail, ip, user_agent, status, method, path, geo_country_code, geo_location \
             FROM audit_logs WHERE 1 = 1",
        );
        if filter.before_id.is_some() {
            sql.push_str(" AND id < ?");
        }
        if !filter.actor.trim().is_empty() {
            sql.push_str(" AND actor = ?");
        }
        if !filter.action.trim().is_empty() {
            sql.push_str(" AND action = ?");
        }
        if !filter.actions.is_empty() {
            let placeholders = std::iter::repeat_n("?", filter.actions.len())
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(" AND action IN ({placeholders})"));
        }
        if !filter.ip.trim().is_empty() {
            sql.push_str(" AND ip = ?");
        }
        if !filter.query.trim().is_empty() {
            // Prefix search stays index-friendly; avoid `%...%` scans on a
            // table that may hold 100M+ rows.
            sql.push_str(" AND target LIKE ?");
        }
        if !filter.from.trim().is_empty() {
            sql.push_str(" AND created_at >= ?");
        }
        if !filter.to.trim().is_empty() {
            sql.push_str(" AND created_at <= ?");
        }
        sql.push_str(" ORDER BY id DESC LIMIT ?");

        let mut query = sqlx::query(&sql);
        if let Some(before_id) = filter.before_id {
            query = query.bind(before_id);
        }
        if !filter.actor.trim().is_empty() {
            query = query.bind(filter.actor.trim());
        }
        if !filter.action.trim().is_empty() {
            query = query.bind(filter.action.trim());
        }
        for action in &filter.actions {
            query = query.bind(action);
        }
        if !filter.ip.trim().is_empty() {
            query = query.bind(filter.ip.trim());
        }
        if !filter.query.trim().is_empty() {
            query = query.bind(format!("{}%", filter.query.trim()));
        }
        if !filter.from.trim().is_empty() {
            query = query.bind(filter.from.trim());
        }
        if !filter.to.trim().is_empty() {
            query = query.bind(filter.to.trim());
        }
        query = query.bind(filter.limit.clamp(1, 500) + 1);

        let rows = query.fetch_all(self.pool).await?;
        Ok(rows
            .into_iter()
            .map(|row| AuditLogRow {
                id: row.try_get("id").unwrap_or_default(),
                created_at: row.try_get("created_at").unwrap_or_default(),
                actor: row.try_get("actor").unwrap_or_default(),
                actor_id: row.try_get("actor_id").unwrap_or_default(),
                action: row.try_get("action").unwrap_or_default(),
                target: row.try_get("target").unwrap_or_default(),
                detail: row.try_get("detail").unwrap_or_default(),
                ip: row.try_get("ip").unwrap_or_default(),
                user_agent: row.try_get("user_agent").unwrap_or_default(),
                status: row.try_get("status").unwrap_or_default(),
                method: row.try_get("method").unwrap_or_default(),
                path: row.try_get("path").unwrap_or_default(),
                geo_country_code: row.try_get("geo_country_code").unwrap_or_default(),
                geo_location: row.try_get("geo_location").unwrap_or_default(),
            })
            .collect())
    }

    /// Deletes one bounded chunk older than `before`; returns the deleted count.
    /// 分批删除早于 `before` 的记录，返回删除数量。
    pub async fn delete_before(&self, before: &str, limit: i64) -> DbResult<u64> {
        let ids =
            sqlx::query("SELECT id FROM audit_logs WHERE created_at < ? ORDER BY id ASC LIMIT ?")
                .bind(before)
                .bind(limit.clamp(1, 500))
                .fetch_all(self.pool)
                .await?;
        if ids.is_empty() {
            return Ok(0);
        }
        let ids: Vec<i64> = ids
            .iter()
            .filter_map(|row| row.try_get::<i64, _>("id").ok())
            .collect();
        if ids.is_empty() {
            return Ok(0);
        }
        let placeholders = std::iter::repeat_n("?", ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("DELETE FROM audit_logs WHERE id IN ({placeholders})");
        let mut query = sqlx::query(&sql);
        for id in &ids {
            query = query.bind(id);
        }
        let result = query.execute(self.pool).await?;
        Ok(result.rows_affected())
    }
}

/// Records a system-generated event (agent online/offline, watchdog, etc.).
/// 记录系统事件（Agent 上下线、看门狗等）。
pub async fn record_system_event(
    pool: &DbPool,
    action: &str,
    target: &str,
    detail: &str,
    ip: &str,
    geo_country_code: &str,
    geo_location: &str,
) -> DbResult<()> {
    let entry = NewAuditLog {
        created_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        actor: "system".into(),
        actor_id: String::new(),
        action: action.into(),
        target: target.into(),
        detail: detail.into(),
        ip: ip.to_string(),
        user_agent: String::new(),
        status: 200,
        method: String::new(),
        path: String::new(),
        geo_country_code: geo_country_code.to_string(),
        geo_location: geo_location.to_string(),
    };
    AuditLogRepository::new(pool).insert_batch(&[entry]).await
}
