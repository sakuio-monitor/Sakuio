//! Structured audit logging for sensitive admin operations.
//! 管理端敏感操作的结构化审计日志。

mod action;
mod logger;
mod middleware;
mod sanitize;

pub use action::{category_actions, classify};
pub use logger::{now, prune_older_than, AuditLogger};
pub use middleware::{audit_admin_layer, client_ip, set_trust_proxy_headers};

use crate::geoip_resolver::GeoEngine;
use sakuio_db::{AuditLogRepository, DbPool, NewAuditLog};

use crate::AppState;

/// Writes a critical login/logout event synchronously so it is never lost in
/// the bounded queue.
/// 同步写入关键登录/登出事件，避免被有界队列丢弃。
pub async fn record_auth_event(
    state: &AppState,
    actor: &str,
    actor_id: &str,
    action: &str,
    target: &str,
    detail: &str,
    ip: &str,
    user_agent: &str,
    status: i64,
    method: &str,
    path: &str,
) {
    record_auth_event_to(
        &state.geo, &state.db, actor, actor_id, action, target, detail, ip, user_agent, status,
        method, path,
    )
    .await;
}

/// Queues an agent-facing event without blocking the WebSocket handshake.
/// Repeated failures from one source are collapsed to one record per minute.
pub fn record_agent_event(
    state: &AppState,
    actor_id: &str,
    action: &str,
    target: &str,
    detail: &str,
    ip: &str,
    user_agent: &str,
    status: i64,
    path: &str,
) -> bool {
    let key = if action == "AGENT_LINK_OPEN" {
        format!("open:{actor_id}")
    } else {
        format!("{action}:{ip}")
    };
    if !agent_event_allowed(&key, std::time::Duration::from_secs(60)) {
        return false;
    }
    state.audit.record(NewAuditLog {
        created_at: now(),
        actor: "agent".to_string(),
        actor_id: actor_id.to_string(),
        action: action.to_string(),
        target: target.to_string(),
        detail: detail.to_string(),
        ip: ip.to_string(),
        user_agent: user_agent.to_string(),
        status,
        method: "GET".to_string(),
        path: path.to_string(),
        geo_country_code: String::new(),
        geo_location: String::new(),
    });
    true
}

fn agent_event_allowed(key: &str, interval: std::time::Duration) -> bool {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    use std::time::Instant;
    static LAST: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();
    let table = LAST.get_or_init(|| Mutex::new(HashMap::new()));
    let Ok(mut table) = table.lock() else {
        return true;
    };
    let now = Instant::now();
    if table.len() > 4096 {
        table.retain(|_, at| now.duration_since(*at) < std::time::Duration::from_secs(300));
    }
    if table.get(key).is_some_and(|at| now.duration_since(*at) < interval) {
        return false;
    }
    table.insert(key.to_string(), now);
    true
}

/// Writes a non-auth operational event (agent, background job, remote action).
pub async fn record_event(
    state: &AppState,
    actor: &str,
    actor_id: &str,
    action: &str,
    target: &str,
    detail: &str,
    ip: &str,
    user_agent: &str,
    status: i64,
    method: &str,
    path: &str,
) {
    record_auth_event(
        state, actor, actor_id, action, target, detail, ip, user_agent, status, method, path,
    )
    .await;
}

/// Writes a critical event to an explicit pool (used when installing into a
/// different database than the bootstrap DB).
/// 写入指定连接池（安装到非 bootstrap 数据库时使用）。
pub async fn record_auth_event_to(
    geo: &GeoEngine,
    pool: &DbPool,
    actor: &str,
    actor_id: &str,
    action: &str,
    target: &str,
    detail: &str,
    ip: &str,
    user_agent: &str,
    status: i64,
    method: &str,
    path: &str,
) {
    let mut entry = NewAuditLog {
        created_at: now(),
        actor: actor.to_string(),
        actor_id: actor_id.to_string(),
        action: action.to_string(),
        target: target.to_string(),
        detail: detail.to_string(),
        ip: ip.to_string(),
        user_agent: user_agent.to_string(),
        status,
        method: method.to_string(),
        path: path.to_string(),
        geo_country_code: String::new(),
        geo_location: String::new(),
    };
    if !ip.is_empty() && !sakuio_notify::ipgeo::is_private_ip(ip) {
        if let Some(info) = geo.lookup_current(ip).await {
            entry.geo_country_code = info.country_code;
            entry.geo_location = info.location;
        }
    }
    record_entry(pool, entry).await;
}

async fn record_entry(pool: &DbPool, entry: NewAuditLog) {
    if let Err(error) = AuditLogRepository::new(pool).insert_batch(&[entry]).await {
        tracing::error!(error = %error, "critical audit event write failed");
    }
}
