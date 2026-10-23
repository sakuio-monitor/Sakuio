use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use serde::Deserialize;

use crate::AppState;
use sakuio_db::ClientRepository;

#[derive(Deserialize)]
pub struct RelayQuery {
    pub token: String,
}

pub async fn agent_relay_ws(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Path(relay_id): Path<String>,
    Query(q): Query<RelayQuery>,
) -> impl IntoResponse {
    let db = state.db.clone();
    let relay = state.relay.clone();
    ws.on_upgrade(move |socket| async move {
        let Some(node_uuid) = ClientRepository::new(&db)
            .uuid_by_token(q.token.trim())
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
