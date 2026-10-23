//! Admin audit-log query API with keyset pagination.
//! 管理端审计日志查询 API（游标分页）。

use crate::AppState;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use sakuio_db::AuditLogFilter;
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
pub struct IpGeoQuery {
    pub ip: String,
}

/// GET `/api/admin/logs/ip-geo?ip=...` — location for one address.
/// 单个 IP 的归属地；与节点旗帜共用 geoip_resolver 缓存。
pub async fn ip_geo(
    State(state): State<AppState>,
    _user: crate::auth::AdminUser,
    Query(query): Query<IpGeoQuery>,
) -> Response {
    let ip = query.ip.trim();
    if ip.is_empty() || ip.len() > 64 || ip.parse::<std::net::IpAddr>().is_err() {
        return Json(json!({
            "success": true,
            "ip": ip,
            "country_code": "",
            "location": "",
        }))
        .into_response();
    }
    match crate::geoip_resolver::lookup_ip(&state, ip).await {
        Some(info) => Json(json!({
            "success": true,
            "ip": ip,
            "country_code": info.country_code,
            "location": info.location,
        }))
        .into_response(),
        None => Json(json!({
            "success": true,
            "ip": ip,
            "country_code": "",
            "location": "",
        }))
        .into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub struct AuditQuery {
    pub cursor: Option<String>,
    pub limit: Option<i64>,
    pub actor: Option<String>,
    pub action: Option<String>,
    pub category: Option<String>,
    pub ip: Option<String>,
    pub q: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
}

pub async fn list_logs(
    State(state): State<AppState>,
    _user: crate::auth::AdminUser,
    Query(query): Query<AuditQuery>,
) -> Response {
    let limit = query.limit.unwrap_or(50).clamp(10, 200);
    let before_id = query
        .cursor
        .as_deref()
        .and_then(|cursor| cursor.parse::<i64>().ok());
    let filter = AuditLogFilter {
        before_id,
        limit,
        actor: query.actor.unwrap_or_default(),
        action: query.action.unwrap_or_default(),
        actions: query
            .category
            .as_deref()
            .and_then(crate::audit::category_actions)
            .map(|actions| actions.iter().map(|action| action.to_string()).collect())
            .unwrap_or_default(),
        ip: query.ip.unwrap_or_default(),
        query: query.q.unwrap_or_default(),
        from: query.from.unwrap_or_default(),
        to: query.to.unwrap_or_default(),
    };
    match sakuio_db::AuditLogRepository::new(&state.db)
        .list(&filter)
        .await
    {
        Ok(mut rows) => {
            let has_more = rows.len() as i64 > limit;
            if has_more {
                rows.truncate(limit as usize);
            }
            let next_cursor = if has_more {
                rows.last().map(|row| row.id.to_string())
            } else {
                None
            };
            Json(json!({
                "success": true,
                "items": rows,
                "next_cursor": next_cursor,
            }))
            .into_response()
        }
        Err(error) => {
            tracing::warn!(error = %error, "query audit logs failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "success": false, "message": "query audit logs failed" })),
            )
                .into_response()
        }
    }
}
