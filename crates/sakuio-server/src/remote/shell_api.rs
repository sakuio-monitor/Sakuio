use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{header, HeaderMap};
use axum::response::Response;
use serde::Deserialize;
use std::net::SocketAddr;
use uuid::Uuid;

use crate::auth::AdminUser;
use crate::remote::build_relay_spec;
use crate::AppState;
use sakuio_db::{ClientRepository, NewAuditLog};
use sakuio_protocol::RemoteShellOpen;

#[derive(Deserialize)]
pub struct ShellQuery {
    pub cols: Option<u16>,
    pub rows: Option<u16>,
}

pub async fn shell_ws(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    user: AdminUser,
    Path(uuid): Path<String>,
    Query(q): Query<ShellQuery>,
) -> Response {
    let ip = crate::audit::client_ip(&headers, Some(addr));
    let agent = headers
        .get(header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .chars()
        .take(512)
        .collect::<String>();
    ws.on_upgrade(move |socket| shell_session(state, user, uuid, q, ip, agent, socket))
}

struct ShellAudit {
    state: AppState,
    user: AdminUser,
    uuid: String,
    ip: String,
    agent: String,
    geo_country_code: String,
    geo_location: String,
}

impl Drop for ShellAudit {
    fn drop(&mut self) {
        self.state.audit.record(NewAuditLog {
            created_at: crate::audit::now(),
            actor: self.user.username.clone(),
            actor_id: self.user.id.clone(),
            action: "WEB_TERMINAL_CLOSE".into(),
            target: self.uuid.clone(),
            detail: "web terminal session closed".into(),
            ip: self.ip.clone(),
            user_agent: self.agent.clone(),
            status: 200,
            method: String::new(),
            path: format!("/api/admin/nodes/{}/shell", self.uuid),
            geo_country_code: self.geo_country_code.clone(),
            geo_location: self.geo_location.clone(),
        });
    }
}

async fn shell_session(
    state: AppState,
    user: AdminUser,
    uuid: String,
    q: ShellQuery,
    ip: String,
    agent: String,
    browser: WebSocket,
) {
    let geo = state.geo.lookup_current(&ip).await;
    let _audit = ShellAudit {
        state: state.clone(),
        user,
        uuid: uuid.clone(),
        ip,
        agent,
        geo_country_code: geo
            .as_ref()
            .map(|info| info.country_code.clone())
            .unwrap_or_default(),
        geo_location: geo
            .as_ref()
            .map(|info| info.location.clone())
            .unwrap_or_default(),
    };
    let relay_id = Uuid::new_v4().to_string();
    state.relay.register(relay_id.clone(), &uuid).await;

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
