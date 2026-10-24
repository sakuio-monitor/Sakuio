//! Small in-memory per-IP limiter for the public JSON-RPC endpoint.

use axum::{
    extract::{ConnectInfo, Request},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

const WINDOW: Duration = Duration::from_secs(60);
const LIMIT: u32 = 120;

fn table() -> &'static Mutex<HashMap<SocketAddr, (Instant, u32)>> {
    static TABLE: OnceLock<Mutex<HashMap<SocketAddr, (Instant, u32)>>> = OnceLock::new();
    TABLE.get_or_init(|| Mutex::new(HashMap::new()))
}

pub async fn limit_rpc(request: Request, next: Next) -> Response {
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| *addr);
    if let Some(peer) = peer {
        let now = Instant::now();
        let allowed = if let Ok(mut guard) = table().lock() {
            guard.retain(|_, (start, _)| now.duration_since(*start) < WINDOW * 2);
            let entry = guard.entry(peer).or_insert((now, 0));
            if now.duration_since(entry.0) >= WINDOW {
                *entry = (now, 0);
            }
            entry.1 = entry.1.saturating_add(1);
            entry.1 <= LIMIT
        } else {
            false
        };
        if !allowed {
            return (StatusCode::TOO_MANY_REQUESTS, "too many requests").into_response();
        }
    }
    next.run(request).await
}
