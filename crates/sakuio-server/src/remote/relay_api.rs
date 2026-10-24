use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::IntoResponse;

use crate::AppState;
use sakuio_db::ClientRepository;

pub async fn agent_relay_ws(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Path(relay_id): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // Never put the node token in the query string: proxies and access logs
    // would persist it. Agents send Authorization: Bearer <node token>.
    let Some(token) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let token = token.to_string();
    let db = state.db.clone();
    let relay = state.relay.clone();
    ws.on_upgrade(move |socket| async move {
        let Some(node_uuid) = ClientRepository::new(&db)
            .uuid_by_token(&token)
            .await
            .ok()
            .flatten()
        else {
            return;
        };
        // A valid token alone is not enough: the relay must have been created
        // for this exact node, otherwise another agent could hijack a session.
        let _ = relay.attach_agent_for(&relay_id, &node_uuid, socket).await;
    })
}
