use crate::AppState;
use axum::extract::{ConnectInfo, Query, State};
use axum::extract::ws::WebSocketUpgrade;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use sakuio_db::ClientRepository;
use sakuio_transport_ws::{ws_upgrade, LinkQuery};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Re-registration window: the same remote IP reusing the discovery key within
/// this period joins its already-created node instead of creating a duplicate.
const AUTO_DISCOVERY_DEDUP_WINDOW: Duration = Duration::from_secs(10 * 60);

fn auto_discovery_bindings() -> &'static Mutex<HashMap<String, (String, Instant)>> {
    static BINDINGS: OnceLock<Mutex<HashMap<String, (String, Instant)>>> = OnceLock::new();
    BINDINGS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// GET `/api/agent/link` — WebSocket upgrade entry.
/// GET `/api/agent/link`：WebSocket 升级入口。
pub async fn agent_link(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Query(query): Query<LinkQuery>,
) -> impl IntoResponse {
    let repo = ClientRepository::new(&state.db);
    let token = query.token.trim().to_string();
    if token.is_empty() {
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
            let ip = addr.ip().to_string();
            match auto_register(&state, &ip).await {
                Ok((uuid, new_token)) => {
                    resolved_token = new_token;
                    resolved = Ok(Some(uuid));
                }
                Err(e) => {
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
                return (
                    StatusCode::FORBIDDEN,
                    Json(serde_json::json!({ "message": "client disabled" })),
                )
                    .into_response();
            }
            let link_query = LinkQuery {
                token: resolved_token,
            };
            ws_upgrade(ws, State(state.link), Query(link_query))
                .await
                .into_response()
        }
        Ok(None) => (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "message": "unknown token" })),
        )
            .into_response(),
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
    // Take a snapshot under the lock; the guard must not be held across awaits.
    let cached_uuid = {
        let bindings = auto_discovery_bindings()
            .lock()
            .map_err(|_| "lock poisoned".to_string())?;
        bindings.get(ip).and_then(|(uuid, at)| {
            (at.elapsed() < AUTO_DISCOVERY_DEDUP_WINDOW).then(|| uuid.clone())
        })
    };
    if let Some(uuid) = cached_uuid {
        let repo = ClientRepository::new(&state.db);
        if let Ok(Some(token)) = repo.token_by_uuid(&uuid).await {
            tracing::info!(ip = %ip, node_uuid = %uuid, "auto discovery rebind");
            return Ok((uuid, token));
        }
    }
    let repo = ClientRepository::new(&state.db);
    let (uuid, token) = repo
        .insert(&sakuio_db::NewClient {
            name: String::new(),
            token: None,
        })
        .await
        .map_err(|e| e.to_string())?;
    {
        let mut bindings = auto_discovery_bindings()
            .lock()
            .map_err(|_| "lock poisoned".to_string())?;
        bindings.insert(ip.to_string(), (uuid.clone(), Instant::now()));
        bindings.retain(|_, (_, at)| at.elapsed() < AUTO_DISCOVERY_DEDUP_WINDOW);
    }
    tracing::info!(ip = %ip, node_uuid = %uuid, "auto discovery registered new node");
    Ok((uuid, token))
}
