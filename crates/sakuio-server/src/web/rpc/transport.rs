use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;

use crate::AppState;

use super::dispatch::dispatch;
use super::types::{
    error_response, parse_request_body, success_response, JsonRpcRequest,
};

/// GET `/api/rpc2` — WebSocket JSON-RPC.
pub async fn rpc2_get(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| rpc2_websocket_loop(state, socket))
}

async fn rpc2_websocket_loop(state: AppState, mut socket: WebSocket) {
    while let Some(msg) = socket.next().await {
        let Ok(msg) = msg else { break };
        let text = match msg {
            Message::Text(t) => t.to_string(),
            Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
            Message::Close(_) => break,
            _ => continue,
        };
        let req: JsonRpcRequest = match serde_json::from_str(&text) {
            Ok(r) => r,
            Err(e) => {
                let err = error_response(None, -32700, &format!("bad request: {e}"));
                let _ = socket.send(Message::Text(err.to_string().into())).await;
                continue;
            }
        };
        let id = req.id.clone();
        if let Some((code, msg)) = req.validate() {
            let _ = socket
                .send(Message::Text(error_response(id, code, msg).to_string().into()))
                .await;
            continue;
        }
        if !req.has_id() {
            continue;
        }
        let resp = match dispatch(&state, &req.method, req.params.as_ref()).await {
            Ok(result) => success_response(id, result),
            Err((code, msg)) => error_response(id, code, msg),
        };
        if socket
            .send(Message::Text(resp.to_string().into()))
            .await
            .is_err()
        {
            break;
        }
    }
}

/// POST `/api/rpc2` — HTTP JSON-RPC, single or batch.
pub async fn rpc2_post(State(state): State<AppState>, body: Bytes) -> impl IntoResponse {
    let requests = match parse_request_body(&body) {
        Ok(r) => r,
        Err((code, msg)) => return Json(error_response(None, code, msg)),
    };
    let mut responses: Vec<Value> = Vec::new();
    for req in requests {
        let id = req.id.clone();
        if !req.has_id() {
            continue;
        }
        if let Some((code, msg)) = req.validate() {
            responses.push(error_response(id, code, msg));
            continue;
        }
        let resp = match dispatch(&state, &req.method, req.params.as_ref()).await {
            Ok(result) => success_response(id, result),
            Err((code, msg)) => error_response(id, code, msg),
        };
        responses.push(resp);
    }
    if responses.len() == 1 {
        Json(responses.into_iter().next().unwrap())
    } else {
        Json(Value::Array(responses))
    }
}
