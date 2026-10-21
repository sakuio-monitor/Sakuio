use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::auth::AdminUser;
use crate::remote::build_relay_spec;
use crate::AppState;
use sakuio_core::RemoteError;
use sakuio_db::{ClientRepository, ExecRepository};
use sakuio_protocol::RemoteExecOpen;

#[derive(Clone, Default)]
pub struct ExecStreamRegistry {
    inner: Arc<RwLock<HashMap<(String, String), String>>>,
}

impl ExecStreamRegistry {
    pub async fn put(&self, task_id: &str, client_uuid: &str, relay_id: String) {
        let mut guard = self.inner.write().await;
        guard.insert((task_id.to_string(), client_uuid.to_string()), relay_id);
    }

    pub async fn relay_id(&self, task_id: &str, client_uuid: &str) -> Option<String> {
        let guard = self.inner.read().await;
        guard
            .get(&(task_id.to_string(), client_uuid.to_string()))
            .cloned()
    }

    pub async fn remove(&self, task_id: &str, client_uuid: &str) {
        let mut guard = self.inner.write().await;
        guard.remove(&(task_id.to_string(), client_uuid.to_string()));
    }
}

#[derive(Deserialize)]
pub struct ExecRequest {
    pub command: String,
    pub clients: Vec<String>,
    #[serde(default, rename = "2fa_code")]
    pub two_fa_code: Option<String>,
}

pub async fn exec_task(
    State(state): State<AppState>,
    _user: AdminUser,
    Json(body): Json<ExecRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let command = body.command.trim();
    if command.is_empty() || body.clients.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let task_id = Uuid::new_v4().to_string();
    let exec_repo = ExecRepository::new(&state.db);
    exec_repo
        .create_task(&task_id, command, &body.clients)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let clients_repo = ClientRepository::new(&state.db);
    let mut dispatched = Vec::new();

    for uuid in &body.clients {
        exec_repo
            .insert_pending_result(&task_id, uuid)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let token = match clients_repo.token_by_uuid(uuid).await {
            Ok(Some(t)) => t,
            _ => {
                let _ = exec_repo
                    .finish_result(&task_id, uuid, "Unknown client", -1)
                    .await;
                continue;
            }
        };

        let relay_id = Uuid::new_v4().to_string();
        state.relay.register(relay_id.clone()).await;
        state
            .exec_streams
            .put(&task_id, uuid, relay_id.clone())
            .await;

        let open = RemoteExecOpen {
            relay: build_relay_spec(&relay_id, &token, &state.relay_public_host),
            command: command.to_string(),
            task_id: task_id.clone(),
            client_uuid: uuid.clone(),
        };

        match state
            .remote
            .push_exec(
                state.board.as_ref(),
                &state.command_bus,
                uuid,
                open,
            )
            .await
        {
            Ok(()) => dispatched.push(uuid.clone()),
            Err(RemoteError::Offline) => {
                let _ = exec_repo
                    .finish_result(&task_id, uuid, "Client offline", -1)
                    .await;
                state.exec_streams.remove(&task_id, uuid).await;
            }
            Err(_) => {
                let _ = exec_repo
                    .finish_result(&task_id, uuid, "Failed to dispatch exec", -1)
                    .await;
                state.exec_streams.remove(&task_id, uuid).await;
            }
        }
    }

    Ok(Json(serde_json::json!({
        "success": true,
        "task_id": task_id,
        "clients": dispatched,
    })))
}

pub async fn exec_task_results(
    State(state): State<AppState>,
    _user: AdminUser,
    Path(task_id): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let exec_repo = ExecRepository::new(&state.db);
    let results = exec_repo
        .list_results(&task_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(serde_json::json!({
        "success": true,
        "results": results,
    })))
}

pub async fn exec_stream_ws(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    _user: AdminUser,
    Path((task_id, uuid)): Path<(String, String)>,
) -> Response {
    ws.on_upgrade(move |socket| exec_stream_session(state, task_id, uuid, socket))
}

async fn exec_stream_session(state: AppState, task_id: String, uuid: String, browser: WebSocket) {
    let Some(relay_id) = state.exec_streams.relay_id(&task_id, &uuid).await else {
        return;
    };
    state.relay.attach_client(&relay_id, browser).await;
    state.exec_streams.remove(&task_id, &uuid).await;
}
