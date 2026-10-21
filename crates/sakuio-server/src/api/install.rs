use crate::AppState;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use sakuio_db::{test_config_database, CompleteInstallRequest, InstallService, TestDbRequest};

/// GET `/api/install/status`.
/// GET `/api/install/status`。
pub async fn install_status(State(state): State<AppState>) -> impl IntoResponse {
    let service = InstallService::new(&state.db);
    match service.status().await {
        Ok(status) => Json(status).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "install status query failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "message": e.to_string() })),
            )
                .into_response()
        }
    }
}

/// POST `/api/install/test-db`.
/// POST `/api/install/test-db`。
pub async fn install_test_db(
    State(state): State<AppState>,
    Json(body): Json<TestDbRequest>,
) -> Response {
    {
        let gate = state.install_gate.read().await;
        if !*gate {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "message": "install not active" })),
            )
                .into_response();
        }
    }
    match test_config_database(&body).await {
        Ok(()) => Json(serde_json::json!({ "ok": true })).into_response(),
        Err(e) => {
            tracing::warn!(error = %e, "install db test failed");
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "message": e.to_string() })),
            )
                .into_response()
        }
    }
}

/// POST `/api/install/complete`.
/// POST `/api/install/complete`。
pub async fn install_complete(
    State(state): State<AppState>,
    Json(body): Json<CompleteInstallRequest>,
) -> Response {
    {
        let gate = state.install_gate.read().await;
        if !*gate {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "message": "install not active" })),
            )
                .into_response();
        }
    }
    let service = InstallService::new(&state.db);
    match service.complete(body).await {
        Ok(()) => {
            let mut gate = state.install_gate.write().await;
            *gate = false;
            tracing::info!("install wizard deactivated");
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => {
            tracing::warn!(error = %e, "install complete rejected");
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "message": e.to_string() })),
            )
                .into_response()
        }
    }
}
