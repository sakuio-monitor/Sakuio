//! Admin API audit middleware.
//! 管理端 API 审计中间件。

use axum::{
    body::{to_bytes, Body},
    extract::{ConnectInfo, Request, State},
    http::{header, HeaderMap, Method},
    middleware::Next,
    response::Response,
};
use sakuio_db::NewAuditLog;
use std::net::SocketAddr;

use crate::audit::sanitize;
use crate::auth::AdminUser;
use crate::AppState;

const MAX_CAPTURED_BODY: usize = 16 * 1024;
const MAX_USER_AGENT: usize = 512;

fn should_audit(method: &Method, path: &str) -> bool {
    if !path.starts_with("/api/admin/") || path.starts_with("/api/admin/logs") {
        return false;
    }
    if matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    ) {
        return true;
    }
    // Sensitive reads/downloads and webshell upgrades are audited too.
    method == Method::GET
        && (path == "/api/admin/settings/database"
            || path.ends_with("/settings/backup")
            || path.ends_with("/logs/export")
            || (path.contains("/nodes/") && (path.ends_with("/shell") || path.contains("/fs/"))))
}

/// Best-effort client address; proxy headers are preferred like the login flow.
/// 尽力获取客户端地址；与登录流程一致，优先代理头。
pub fn client_ip(headers: &HeaderMap, addr: Option<SocketAddr>) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|value| value.to_str().ok())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        })
        .unwrap_or_else(|| addr.map(|addr| addr.ip().to_string()).unwrap_or_default())
}

fn request_ip(request: &Request) -> String {
    let addr = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0);
    client_ip(request.headers(), addr)
}

fn user_agent(headers: &HeaderMap) -> String {
    headers
        .get(header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .chars()
        .take(MAX_USER_AGENT)
        .collect()
}

fn target_from_path(path: &str) -> String {
    path.strip_prefix("/api/admin/")
        .unwrap_or(path)
        .trim_end_matches('/')
        .to_string()
}

pub async fn audit_admin_layer(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let action = crate::audit::classify(&method, &path);
    if !should_audit(&method, &path) {
        return next.run(request).await;
    }

    let query = request.uri().query().unwrap_or("").to_string();
    let actor = request.extensions().get::<AdminUser>().cloned();
    let ip = request_ip(&request);
    let agent = user_agent(request.headers());
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();
    let content_length = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());

    let (parts, body) = request.into_parts();
    let capture_json = content_type.contains("application/json")
        && content_length.is_some_and(|length| length <= MAX_CAPTURED_BODY as u64);
    let (body, mut content) = if capture_json {
        match to_bytes(body, MAX_CAPTURED_BODY).await {
            Ok(bytes) => {
                let detail = sanitize::redact_json(&bytes).unwrap_or_else(|| "{}".into());
                (Body::from(bytes), detail)
            }
            Err(_) => (
                Body::empty(),
                "[request body exceeded audit capture limit]".to_string(),
            ),
        }
    } else {
        (body, sanitize::body_summary(&content_type, content_length))
    };
    if !query.is_empty() {
        let query = sanitize::redact_query(&query);
        if !query.is_empty() {
            content = if content.is_empty() {
                format!("query: {query}")
            } else {
                format!("query: {query}; {content}")
            };
        }
    }

    let response = next.run(Request::from_parts(parts, body)).await;
    let status = response.status().as_u16() as i64;
    let entry = NewAuditLog {
        created_at: crate::audit::now(),
        actor: actor
            .as_ref()
            .map(|user| user.username.clone())
            .unwrap_or_default(),
        actor_id: actor
            .as_ref()
            .map(|user| user.id.clone())
            .unwrap_or_default(),
        action: action.to_string(),
        target: target_from_path(&path),
        detail: content,
        ip,
        user_agent: agent,
        status,
        method: method.as_str().to_string(),
        path,
        geo_country_code: String::new(),
        geo_location: String::new(),
    };
    state.audit.record(entry);
    response
}
