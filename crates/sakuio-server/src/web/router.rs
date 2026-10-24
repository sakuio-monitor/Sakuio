use axum::routing::get;
use axum::Router;

use crate::AppState;

use super::clients_ws::{clients_sse, clients_websocket};
use super::respond::api_error;
use super::rpc::{
    limit_rpc,
    api_nodes, api_public, api_public_ping_records, api_public_ping_tasks, api_recent,
    api_records_load, api_version, rpc2_get, rpc2_post,
};

async fn ping() -> &'static str {
    "pong"
}

async fn oauth_not_configured() -> axum::response::Response {
    api_error(
        axum::http::StatusCode::NOT_IMPLEMENTED,
        "oauth not configured",
    )
}

/// Public visitor / theme HTTP routes.
pub fn public_api_router() -> Router<AppState> {
    Router::new()
        .route("/ping", get(ping))
        .route("/api/oauth", get(oauth_not_configured))
        .route("/api/oauth_callback", get(oauth_not_configured))
        .route("/api/clients", get(clients_websocket))
        .route("/api/clients/sse", get(clients_sse))
        .route("/api/nodes", get(api_nodes))
        .route("/api/public", get(api_public))
        .route("/api/version", get(api_version))
        .route("/api/recent/:uuid", get(api_recent))
        .route("/api/records/load", get(api_records_load))
        .route("/api/records/ping", get(api_public_ping_records))
        .route("/api/task/ping", get(api_public_ping_tasks))
        .route("/api/rpc2", get(rpc2_get).post(rpc2_post).layer(axum::middleware::from_fn(limit_rpc)))
}
