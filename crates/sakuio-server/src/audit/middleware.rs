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
    if !path.starts_with("/api/admin/") {
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
            || path.starts_with("/api/admin/logs")
            || path.starts_with("/api/admin/pprof/")
            || path.ends_with("/themes/:short/preview")
            || (path.contains("/themes/") && path.ends_with("/preview"))
            || (path.contains("/nodes/") && (path.ends_with("/shell") || path.contains("/fs/"))))
}

/// Best-effort client address. Reverse-proxy headers are checked in priority
/// order and every candidate is parsed as an IP so a spoofed/garbage header
/// cannot become an audit key.
pub fn client_ip(headers: &HeaderMap, addr: Option<SocketAddr>) -> String {
    // Only trust forwarding headers from a local/private hop. A direct public
    // client must not be able to choose its own audit IP.
    let trusted_hop = addr.is_some_and(|addr| match addr.ip() {
        std::net::IpAddr::V4(ip) => ip.is_loopback() || ip.is_private() || ip.is_link_local(),
        std::net::IpAddr::V6(ip) => {
            ip.is_loopback() || ip.is_unique_local() || ip.is_unicast_link_local()
        }
    });
    if trusted_hop {
        for name in [
            "eo-connecting-ip",
            "cf-connecting-ip",
            "true-client-ip",
            "fastly-client-ip",
            "x-client-ip",
            "x-cluster-client-ip",
            "x-original-forwarded-for",
            "x-forwarded-for",
            "x-forwarded",
            "x-real-ip",
            "x-proxy-user-ip",
            "x-azure-clientip",
            "x-appengine-remote-addr",
            "x-original-ip",
            "forwarded",
        ] {
            if let Some(value) = headers.get(name).and_then(|v| v.to_str().ok()) {
                let candidate = if name == "forwarded" {
                    let first = value.split(',').next().unwrap_or(value);
                    first
                        .split(';')
                        .find_map(|part| part.trim().strip_prefix("for="))
                        .unwrap_or(first)
                } else {
                    value.split(',').next().unwrap_or(value)
                };
                if let Some(ip) = parse_forwarded_ip(candidate) {
                    return ip;
                }
            }
        }
    }
    addr.map(|addr| addr.ip().to_string()).unwrap_or_default()
}

fn parse_forwarded_ip(value: &str) -> Option<String> {
    let mut value = value.trim().trim_matches('"');
    if let Some(rest) = value.strip_prefix('[') {
        let end = rest.find(']')?;
        let ip = &rest[..end];
        return ip.parse::<std::net::IpAddr>().ok().map(|ip| ip.to_string());
    }
    if value.matches(':').count() == 1 {
        if let Some((host, port)) = value.rsplit_once(':') {
            if port.chars().all(|c| c.is_ascii_digit()) {
                value = host;
            }
        }
    }
    value
        .parse::<std::net::IpAddr>()
        .ok()
        .map(|ip| ip.to_string())
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

#[cfg(test)]
mod client_ip_tests {
    use super::*;
    use axum::http::HeaderValue;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn proxy_addr() -> Option<SocketAddr> {
        Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 12345))
    }

    #[test]
    fn reads_cloudflare_and_forwarded_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("cf-connecting-ip", HeaderValue::from_static("203.0.113.9"));
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("198.51.100.7, 10.0.0.1"),
        );
        assert_eq!(client_ip(&headers, proxy_addr()), "203.0.113.9");
    }

    #[test]
    fn strips_forwarded_ports_and_brackets() {
        let mut headers = HeaderMap::new();
        headers.insert("x-real-ip", HeaderValue::from_static("203.0.113.9:443"));
        assert_eq!(client_ip(&headers, proxy_addr()), "203.0.113.9");
        headers.insert("x-real-ip", HeaderValue::from_static("[2001:db8::1]:443"));
        assert_eq!(client_ip(&headers, proxy_addr()), "2001:db8::1");
    }

    #[test]
    fn reads_common_cdn_and_proxy_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("fastly-client-ip", HeaderValue::from_static("203.0.113.20"));
        assert_eq!(client_ip(&headers, proxy_addr()), "203.0.113.20");

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-original-forwarded-for",
            HeaderValue::from_static("203.0.113.21, 198.51.100.10"),
        );
        assert_eq!(client_ip(&headers, proxy_addr()), "203.0.113.21");

        let mut headers = HeaderMap::new();
        headers.insert(
            "forwarded",
            HeaderValue::from_static("for=\"[2001:db8::2]:443\";proto=https"),
        );
        assert_eq!(client_ip(&headers, proxy_addr()), "2001:db8::2");
    }

    #[test]
    fn prefers_forwarded_client_over_cdn_real_ip() {
        let mut headers = HeaderMap::new();
        headers.insert("x-real-ip", HeaderValue::from_static("198.51.100.10"));
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("203.0.113.9, 198.51.100.10"),
        );
        assert_eq!(client_ip(&headers, proxy_addr()), "203.0.113.9");

        headers.insert("eo-connecting-ip", HeaderValue::from_static("203.0.113.11"));
        assert_eq!(client_ip(&headers, proxy_addr()), "203.0.113.11");
    }

    #[test]
    fn ignores_headers_from_untrusted_public_hop() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", HeaderValue::from_static("203.0.113.9"));
        let public = Some(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
            12345,
        ));
        assert_eq!(client_ip(&headers, public), "8.8.8.8");
    }
}
