use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::{
    sse::{Event, KeepAlive},
    IntoResponse, Sse,
};
use futures_util::{stream, StreamExt};
use sakuio_board::BoardStateStore;
use serde_json::json;

use crate::web::rpc::data::{load_public_clients, parse_basic, v2_report_from_snapshot};
use crate::AppState;

/// GET `/api/clients` — realtime WebSocket (`get` / `get <uuid>`).
pub async fn clients_websocket(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| clients_ws_loop(state, socket))
}

async fn clients_ws_loop(state: AppState, mut socket: WebSocket) {
    while let Some(msg) = socket.next().await {
        let Ok(msg) = msg else { break };
        let text = match msg {
            Message::Text(t) => t.to_string(),
            Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
            Message::Close(_) => break,
            _ => continue,
        };
        let filter_uuid = parse_clients_ws_message(&text);
        if filter_uuid.is_err() {
            let err = json!({ "status": "error", "error": "Invalid message" });
            if socket
                .send(Message::Text(err.to_string().into()))
                .await
                .is_err()
            {
                break;
            }
            continue;
        }
        let filter_uuid = filter_uuid.unwrap();
        let payload = match build_clients_ws_payload(&state, filter_uuid.as_deref()).await {
            Ok(p) => p,
            Err(_) => {
                let err = json!({ "status": "error", "error": "Internal error" });
                if socket
                    .send(Message::Text(err.to_string().into()))
                    .await
                    .is_err()
                {
                    break;
                }
                continue;
            }
        };
        if socket
            .send(Message::Text(payload.to_string().into()))
            .await
            .is_err()
        {
            break;
        }
    }
}

fn parse_clients_ws_message(text: &str) -> Result<Option<String>, ()> {
    let message = text.trim();
    if message == "get" {
        return Ok(None);
    }
    if let Some(rest) = message.strip_prefix("get ") {
        let uuid = rest.trim();
        if uuid.is_empty() {
            return Err(());
        }
        return Ok(Some(uuid.to_string()));
    }
    Err(())
}

pub(crate) async fn build_clients_ws_payload(
    state: &AppState,
    filter_uuid: Option<&str>,
) -> Result<serde_json::Value, i32> {
    let rows = load_public_clients(state).await?;
    let mut online: Vec<String> = Vec::new();
    let mut data = serde_json::Map::new();

    for row in rows {
        if row.disabled {
            continue;
        }
        if let Some(u) = filter_uuid {
            if row.uuid != u {
                continue;
            }
        }
        let presence = state
            .board
            .resolve_presence(&row.uuid)
            .await
            .ok()
            .flatten()
            .is_some();
        let snap = state
            .board
            .get_latest_snapshot(&row.uuid)
            .await
            .ok()
            .flatten();
        let is_online = presence || snap.as_ref().is_some_and(|s| s.online) || row.online;
        if is_online {
            online.push(row.uuid.clone());
        }
        if let Some(snap) = snap {
            let basic = parse_basic(&row);
            let mut report = v2_report_from_snapshot(&snap, &basic);
            if let Some(obj) = report.as_object_mut() {
                obj.remove("uuid");
            }
            data.insert(row.uuid.clone(), report);
        }
    }

    Ok(json!({
        "status": "success",
        "data": {
            "online": online,
            "data": data,
        }
    }))
}

/// GET `/api/clients/sse` — realtime SSE stream using the same payload as
/// the compatibility WebSocket. Existing APIs are unchanged.
pub async fn clients_sse(
    State(state): State<AppState>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, std::convert::Infallible>>> {
    let stream = stream::unfold(state, |state| async move {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let payload = build_clients_ws_payload(&state, None)
            .await
            .unwrap_or_else(|_| json!({ "status": "error", "error": "Internal error" }));
        Some((
            Ok(Event::default().event("nodes").data(payload.to_string())),
            state,
        ))
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}
