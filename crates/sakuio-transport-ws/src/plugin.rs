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
use sakuio_protocol::{ControlMessage, TelemetryBatch};
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

/// Translates low-level websocket / IO errors into operator-friendly reasons.
/// 把底层 websocket / IO 错误翻译成运维可读的断开原因。
fn friendly_link_error(raw: &str) -> String {
    let lower = raw.to_lowercase();
    if lower.contains("connection reset without closing handshake") {
        "agent exited or network dropped (connection reset, no closing handshake)".into()
    } else if lower.contains("reset by peer") {
        "connection reset by peer (agent exited or network dropped)".into()
    } else if lower.contains("refused") {
        "connection refused".into()
    } else if lower.contains("timed out") || lower.contains("timeout") {
        "connection timed out".into()
    } else {
        raw.to_string()
    }
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

    // Run the event loop, then always unregister + fire on_disconnect so the
    // node is marked offline even when the loop exits with an error (e.g.
    // connection reset without a closing handshake).
    // 先跑事件循环，随后无论正常结束还是报错都注销并触发 on_disconnect，
    // 保证节点一定被标记离线（如未握手直接断开的连接重置）。
    let outcome = link_event_loop(
        &handler,
        &mut session,
        &mut cmd_rx,
        &node_uuid,
        &session_id,
    )
    .await;

    command_bus.unregister(&node_uuid).await;
    handler.on_disconnect(node_uuid.clone(), session_id).await;

    match outcome {
        Ok(()) => Ok(()),
        Err(e) => {
            tracing::warn!(
                node_uuid = %node_uuid,
                reason = %friendly_link_error(&e),
                "agent disconnected unexpectedly"
            );
            Err(e)
        }
    }
}

/// Session event loop: outbound commands, inbound frames, telemetry ingest.
/// 会话事件循环：下发命令、接收帧、遥测入库。
async fn link_event_loop(
    handler: &Arc<dyn BoardSessionHandler>,
    session: &mut WsBoardSession,
    cmd_rx: &mut tokio::sync::mpsc::UnboundedReceiver<ControlMessage>,
    node_uuid: &str,
    session_id: &str,
) -> Result<(), String> {
    loop {
        tokio::select! {
            outbound = cmd_rx.recv() => {
                match outbound {
                    Some(msg) => {
                        session.send(BoardFrame::Control(msg)).await?;
                    }
                    None => return Ok(()),
                }
            }
            frame = session.recv() => {
                let Some(frame) = frame? else {
                    return Ok(());
                };
                match frame {
                    BoardFrame::Control(msg) => {
                        let replies = handler
                            .on_control(node_uuid, session_id, msg)
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
                            .on_telemetry(node_uuid, session_id, payload.batch_seq, batch)
                            .await?
                        {
                            session.send(BoardFrame::Control(ack)).await?;
                        }
                    }
                }
            }
        }
    }
}
