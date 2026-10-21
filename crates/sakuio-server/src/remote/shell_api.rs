use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::response::Response;
use serde::Deserialize;
use uuid::Uuid;

use crate::auth::AdminUser;
use crate::remote::build_relay_spec;
use crate::AppState;
use sakuio_db::ClientRepository;
use sakuio_protocol::RemoteShellOpen;

#[derive(Deserialize)]
pub struct ShellQuery {
    pub cols: Option<u16>,
    pub rows: Option<u16>,
}

pub async fn shell_ws(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    _user: AdminUser,
    Path(uuid): Path<String>,
    Query(q): Query<ShellQuery>,
) -> Response {
    ws.on_upgrade(move |socket| shell_session(state, uuid, q, socket))
}

async fn shell_session(
    state: AppState,
    uuid: String,
    q: ShellQuery,
    browser: WebSocket,
) {
    let relay_id = Uuid::new_v4().to_string();
    state.relay.register(relay_id.clone()).await;

    let token = match ClientRepository::new(&state.db).token_by_uuid(&uuid).await {
        Ok(Some(t)) => t,
        _ => return,
    };

    let relay = build_relay_spec(&relay_id, &token);
    let open = RemoteShellOpen {
        relay,
        cols: q.cols.unwrap_or(120),
        rows: q.rows.unwrap_or(32),
    };
    if state
        .remote
        .push_shell(
            state.board.as_ref(),
            &state.command_bus,
            &uuid,
            serde_json::to_value(&open).unwrap(),
        )
        .await
        .is_err()
    {
        return;
    }

    state.relay.attach_client(&relay_id, browser).await;
}
