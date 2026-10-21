use crate::session::WsBoardSession;
use async_trait::async_trait;
use axum::{
    extract::{
        ws::{WebSocket, WebSocketUpgrade},
        Query, State,
    },
    response::IntoResponse,
};
use sakuio_board::{
    BoardFrame, BoardSession, BoardSessionHandler, BoardStateStore, BoardTransportRegistry,
    TransportPlugin, TransportError,
};
use sakuio_core::{flush_pending_ping_commands, AgentCommandBus, new_session_id};
use sakuio_db::ClientRepository;
use sakuio_protocol::{TelemetryBatch};
use serde::Deserialize;
use sqlx::SqlitePool;
use std::sync::Arc;

/// Default HTTP path for the agent WebSocket link.
/// Agent WebSocket 链路的默认 HTTP 路径。
pub const WS_LINK_PATH: &str = "/api/agent/link";

/// Shared state for the WebSocket upgrade handler.
/// WebSocket 升级处理器共享状态。
#[derive(Clone)]
pub struct WsLinkState {
    pub db: SqlitePool,
    pub handler: Arc<dyn BoardSessionHandler>,
    pub command_bus: AgentCommandBus,
    pub board: Arc<dyn BoardStateStore>,
}

/// Query parameters for the agent WebSocket URL.
/// Agent WebSocket URL 的查询参数。
#[derive(Debug, Deserialize)]
pub struct LinkQuery {
    pub token: String,
}

/// WebSocket transport plugin entry point.
/// WebSocket 传输插件入口。
pub struct WsBoardTransportPlugin;

#[async_trait]
impl TransportPlugin for WsBoardTransportPlugin {
    fn name(&self) -> &'static str {
        "websocket"
    }

    fn register(&self, _registry: &mut BoardTransportRegistry) -> Result<(), TransportError> {
        tracing::debug!(transport = "websocket", "transport plugin registered (server mounts via router)");
        Ok(())
    }
}

/// Upgrades HTTP to WebSocket and runs the SakuLink session loop.
/// 将 HTTP 升级为 WebSocket 并运行 SakuLink 会话循环。
pub async fn ws_upgrade(
    ws: WebSocketUpgrade,
    State(state): State<WsLinkState>,
    Query(query): Query<LinkQuery>,
) -> impl IntoResponse {
    let token = query.token;
    let db = state.db.clone();
    let handler = state.handler.clone();
    let command_bus = state.command_bus.clone();
    let board = state.board.clone();
    ws.on_upgrade(move |socket| async move {
        if let Err(e) = run_link_session(db, handler, command_bus, board, token, socket).await {
            tracing::warn!(error = %e, "agent link session ended with error");
        }
    })
}

async fn run_link_session(
    db: SqlitePool,
    handler: Arc<dyn BoardSessionHandler>,
    command_bus: AgentCommandBus,
    board: Arc<dyn BoardStateStore>,
    token: String,
    socket: WebSocket,
) -> Result<(), String> {
    let clients = ClientRepository::new(&db);
    let token = token.trim();
    let Some(node_uuid) = clients
        .uuid_by_token(token)
        .await
        .map_err(|e| e.to_string())?
    else {
        tracing::warn!(
            token_len = token.len(),
            "agent link rejected: unknown token"
        );
        return Err("unknown token".into());
    };
    if clients
        .is_disabled(&node_uuid)
        .await
        .map_err(|e| e.to_string())?
    {
        tracing::warn!(node_uuid = %node_uuid, "agent link rejected: client disabled");
        return Err("client disabled".into());
    }

    let session_id = new_session_id();
    let mut session = WsBoardSession::new(session_id.clone(), socket);
    handler
        .on_connect(node_uuid.clone(), session_id.clone())
        .await?;
    flush_pending_ping_commands(board.as_ref(), &command_bus, &node_uuid).await;
    let mut cmd_rx = command_bus.register(&node_uuid).await;

    loop {
        tokio::select! {
            outbound = cmd_rx.recv() => {
                match outbound {
                    Some(msg) => {
                        session.send(BoardFrame::Control(msg)).await?;
                    }
                    None => break,
                }
            }
            frame = session.recv() => {
                let Some(frame) = frame? else {
                    break;
                };
                match frame {
                    BoardFrame::Control(msg) => {
                        let replies = handler
                            .on_control(&node_uuid, &session_id, msg)
                            .await?;
                        for reply in replies {
                            session
                                .send(BoardFrame::Control(reply))
                                .await?;
                        }
                    }
                    BoardFrame::Telemetry(payload) => {
                        let batch: TelemetryBatch =
                            cbor4ii::serde::from_reader(payload.body.as_slice())
                                .map_err(|e| e.to_string())?;
                        if let Some(ack) = handler
                            .on_telemetry(&node_uuid, &session_id, payload.batch_seq, batch)
                            .await?
                        {
                            session.send(BoardFrame::Control(ack)).await?;
                        }
                    }
                }
            }
        }
    }

    command_bus.unregister(&node_uuid).await;
    handler.on_disconnect(node_uuid, session_id).await;
    Ok(())
}
