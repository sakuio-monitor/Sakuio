use async_trait::async_trait;
use axum::extract::ws::{Message, WebSocket};
use futures_util::StreamExt;
use sakuio_board::{BoardFrame, BoardSession};
use sakuio_protocol::{decode_sktl, encode_sktl, ControlMessage, TelemetryBatch};

/// WebSocket-backed board session.
/// 基于 WebSocket 的板级会话。
pub struct WsBoardSession {
    session_id: String,
    socket: WebSocket,
}

impl WsBoardSession {
    /// Wraps an upgraded WebSocket with a server-assigned session id.
    /// 用服务端分配的 session id 包装已升级的 WebSocket。
    pub fn new(session_id: String, socket: WebSocket) -> Self {
        Self { session_id, socket }
    }
}

#[async_trait]
impl BoardSession for WsBoardSession {
    async fn send(&mut self, frame: BoardFrame) -> Result<(), String> {
        let msg = match frame {
            BoardFrame::Control(c) => {
                let bytes = c.to_json_bytes().map_err(|e| e.to_string())?;
                Message::Text(String::from_utf8(bytes).map_err(|e| e.to_string())?)
            }
            BoardFrame::Telemetry(payload) => {
                let batch: TelemetryBatch = cbor4ii::serde::from_reader(payload.body.as_slice())
                    .map_err(|e| e.to_string())?;
                let raw = encode_sktl(payload.batch_seq, &batch).map_err(|e| e.to_string())?;
                Message::Binary(raw)
            }
        };
        self.socket.send(msg).await.map_err(|e| e.to_string())
    }

    async fn recv(&mut self) -> Result<Option<BoardFrame>, String> {
        loop {
            let Some(next) = self.socket.next().await else {
                return Ok(None);
            };
            let msg = next.map_err(|e| e.to_string())?;
            match msg {
                Message::Text(text) => {
                    let bytes = text.as_bytes();
                    let control = ControlMessage::from_json_bytes(bytes).map_err(|e| e.to_string())?;
                    return Ok(Some(BoardFrame::Control(control)));
                }
                Message::Binary(bin) => {
                    let (batch_seq, batch) = decode_sktl(&bin).map_err(|e| e.to_string())?;
                    let mut body = Vec::new();
                    cbor4ii::serde::to_writer(&mut body, &batch).map_err(|e| e.to_string())?;
                    return Ok(Some(BoardFrame::Telemetry(sakuio_protocol::SkltPayload {
                        batch_seq,
                        body,
                    })));
                }
                Message::Ping(payload) => {
                    self.socket
                        .send(Message::Pong(payload))
                        .await
                        .map_err(|e| e.to_string())?;
                    continue;
                }
                Message::Pong(_) | Message::Close(_) => return Ok(None),
            }
        }
    }

    fn session_id(&self) -> &str {
        &self.session_id
    }
}
