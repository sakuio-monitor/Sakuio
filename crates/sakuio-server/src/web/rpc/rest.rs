use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use serde::Deserialize;
use serde_json::Value;

use crate::AppState;
use crate::web::respond::{api_error, api_success, rpc_http_status};

use super::dispatch::dispatch;

async fn call_rpc(
    state: &AppState,
    method: &str,
    params: Value,
) -> Result<Value, (i32, &'static str)> {
    if params.is_null() {
        dispatch(state, method, None).await
    } else {
        dispatch(state, method, Some(&params)).await
    }
}

async fn bind_standard(
    State(state): State<AppState>,
    method: &'static str,
    params: Value,
) -> impl IntoResponse {
    match call_rpc(&state, method, params).await {
        Ok(data) => api_success(data),
        Err((code, msg)) => api_error(rpc_http_status(code), msg),
    }
}

pub async fn api_me(State(state): State<AppState>) -> impl IntoResponse {
    match dispatch(&state, "public:getMe", None).await {
        Ok(v) => axum::Json(v).into_response(),
        Err((code, msg)) => api_error(rpc_http_status(code), msg),
    }
}

pub async fn api_public(state: State<AppState>) -> impl IntoResponse {
    bind_standard(state, "public:getPublicSettings", Value::Null).await
}

pub async fn api_version(state: State<AppState>) -> impl IntoResponse {
    bind_standard(state, "public:getVersion", Value::Null).await
}

pub async fn api_nodes(state: State<AppState>) -> impl IntoResponse {
    bind_standard(state, "public:getNodesInformation", Value::Null).await
}

pub async fn api_recent(
    state: State<AppState>,
    Path(uuid): Path<String>,
) -> impl IntoResponse {
    bind_standard(
        state,
        "public:getClientRecentRecords",
        serde_json::json!({ "uuid": uuid }),
    )
    .await
}

#[derive(Deserialize)]
pub struct RecordsLoadQuery {
    pub uuid: String,
    pub load_type: Option<String>,
    pub hours: Option<String>,
}

pub async fn api_records_load(
    state: State<AppState>,
    Query(q): Query<RecordsLoadQuery>,
) -> impl IntoResponse {
    bind_standard(
        state,
        "public:getRecordsByUUID",
        serde_json::json!({
            "uuid": q.uuid,
            "load_type": q.load_type.unwrap_or_default(),
            "hours": q.hours.unwrap_or_else(|| "4".into()),
        }),
    )
    .await
}

#[derive(Deserialize)]
pub struct RecordsPingQuery {
    pub uuid: Option<String>,
    pub task_id: Option<String>,
    pub hours: Option<String>,
}

pub async fn api_public_ping_records(
    state: State<AppState>,
    Query(q): Query<RecordsPingQuery>,
) -> impl IntoResponse {
    bind_standard(
        state,
        "public:getPingRecords",
        serde_json::json!({
            "uuid": q.uuid.unwrap_or_default(),
            "task_id": q.task_id.unwrap_or_default(),
            "hours": q.hours.unwrap_or_default(),
        }),
    )
    .await
}

pub async fn api_public_ping_tasks(state: State<AppState>) -> impl IntoResponse {
    bind_standard(state, "public:getPublicPingTasks", Value::Null).await
}

pub async fn admin_ping_list(state: State<AppState>) -> impl IntoResponse {
    bind_standard(state, "admin:getAllPingTasks", Value::Null).await
}

pub async fn admin_ping_add(
    state: State<AppState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let params = parse_json_body(&body);
    bind_standard(state, "admin:addPingTask", params).await
}

pub async fn admin_ping_delete(
    state: State<AppState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let params = parse_json_body(&body);
    bind_standard(state, "admin:deletePingTask", params).await
}

pub async fn admin_ping_edit(
    state: State<AppState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let params = parse_json_body(&body);
    bind_standard(state, "admin:editPingTask", params).await
}

pub async fn admin_ping_order(
    state: State<AppState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let params = parse_json_body(&body);
    bind_standard(state, "admin:orderPingTask", params).await
}

fn parse_json_body(body: &axum::body::Bytes) -> Value {
    if body.is_empty() {
        return Value::Null;
    }
    serde_json::from_slice(body).unwrap_or(Value::Null)
}
