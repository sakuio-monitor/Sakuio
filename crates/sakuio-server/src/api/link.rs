use crate::AppState;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use sakuio_db::ClientRepository;
use sakuio_transport_ws::ws_upgrade;
use std::net::SocketAddr;

/// GET `/api/agent/link` — WebSocket upgrade entry.
/// GET `/api/agent/link`：WebSocket 升级入口。
pub async fn agent_link(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let repo = ClientRepository::new(&state.db);
    let audit_ip = crate::audit::client_ip(&headers, Some(addr));
    let user_agent = headers
        .get(axum::http::header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .chars()
        .take(512)
        .collect::<String>();
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
        .unwrap_or("")
        .to_string();
    if token.is_empty() {
        crate::audit::record_agent_event(
            &state, "", "AGENT_AUTH_FAILED", "", "missing bearer token",
            &audit_ip, &user_agent, 401, "/api/agent/link",
        );
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "message": "token required" })),
        )
            .into_response();
    }
    let mut resolved = repo.uuid_by_token(&token).await;
    let mut resolved_token = token.clone();

    // Auto-discovery: an unknown shared key registers a new node automatically.
    if matches!(resolved, Ok(None)) {
        let settings = crate::site_settings::load(&state).await;
        if !settings.auto_discovery_key.is_empty() && token == settings.auto_discovery_key {
            let ip = crate::audit::client_ip(&headers, Some(addr));
            match auto_register(&state, &ip).await {
                Ok((uuid, new_token)) => {
                    resolved_token = new_token;
                    resolved = Ok(Some(uuid));
                }
                Err(e) => {
                    crate::audit::record_agent_event(
                        &state, "", "AGENT_AUTODISCOVERY_REJECTED", "", &e,
                        &audit_ip, &user_agent, 500, "/api/agent/link",
                    );
                    tracing::warn!(ip = %ip, error = %e, "auto discovery registration failed");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({ "message": "auto discovery failed" })),
                    )
                        .into_response();
                }
            }
        }
    }

    match resolved {
        Ok(Some(uuid)) => {
            if repo.is_disabled(&uuid).await.unwrap_or(true) {
                crate::audit::record_agent_event(
                    &state, &uuid, "AGENT_AUTH_FAILED", &uuid, "client disabled",
                    &audit_ip, &user_agent, 403, "/api/agent/link",
                );
                return (
                    StatusCode::FORBIDDEN,
                    Json(serde_json::json!({ "message": "client disabled" })),
                )
                    .into_response();
            }
            crate::audit::record_agent_event(
                &state, &uuid, "AGENT_LINK_OPEN", &uuid, "websocket accepted",
                &audit_ip, &user_agent, 101, "/api/agent/link",
            );
            ws_upgrade(ws, State(state.link), resolved_token)
                .await
                .into_response()
        }
        Ok(None) => {
            crate::audit::record_agent_event(
                &state, "", "AGENT_AUTH_FAILED", "", "unknown node token",
                &audit_ip, &user_agent, 401, "/api/agent/link",
            );
            (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "message": "unknown token" })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "message": e.to_string() })),
        )
            .into_response(),
    }
}

/// Registers (or reuses) a node for a remote IP connecting with the discovery key.
/// Returns `(uuid, token)`; the token is delivered to the agent via `Welcome`.
async fn auto_register(state: &AppState, ip: &str) -> Result<(String, String), String> {
    // The discovery key identifies a board, not a machine. Each connection that
    // has not yet persisted a node token must become its own node; deduplicating
    // by public IP merged different machines behind the same NAT into one node.
    let repo = ClientRepository::new(&state.db);
    let (uuid, token) = repo
        .insert(&sakuio_db::NewClient {
            name: String::new(),
            token: None,
        })
        .await
        .map_err(|e| e.to_string())?;
    crate::audit::record_agent_event(
        state, &uuid, "AGENT_AUTODISCOVERY_REGISTER", &uuid,
        "auto discovery registration succeeded", ip, "", 201, "/api/agent/link",
    );
    tracing::info!(ip = %ip, node_uuid = %uuid, "auto discovery registered new node");
    Ok((uuid, token))
}
