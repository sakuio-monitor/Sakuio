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
        if ClientRepository::new(&db)
            .uuid_by_token(q.token.trim())
            .await
            .ok()
            .flatten()
            .is_none()
        {
            return;
        }
        relay.attach_agent(&relay_id, socket).await;
    })
}
