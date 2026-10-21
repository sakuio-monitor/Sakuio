use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};

/// Theme REST envelope: `{ status, message, data? }`.
pub fn api_success(data: Value) -> Response {
    Json(json!({
        "status": "success",
        "message": "",
        "data": data,
    }))
    .into_response()
}

pub fn api_error(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(json!({
            "status": "error",
            "message": message,
        })),
    )
        .into_response()
}

pub fn rpc_http_status(code: i32) -> StatusCode {
    match code {
        -32700 | -32600 | -32602 => StatusCode::BAD_REQUEST,
        -32041 | -32042 => StatusCode::UNAUTHORIZED,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
