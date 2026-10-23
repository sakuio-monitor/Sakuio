use crate::AppState;
use axum::{
    extract::{ConnectInfo, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use sakuio_db::{
    complete_install, test_config_database, AppDatabase, CompleteInstallRequest, InstallService,
    TestDbRequest,
};
use std::net::SocketAddr;

/// GET `/api/install/status`.
/// GET `/api/install/status`。
pub async fn install_status(State(state): State<AppState>) -> impl IntoResponse {
    let service = InstallService::new(&state.db);
    let configured = crate::config::ServerConfig::load_or_create()
        .ok()
        .is_some_and(|config| config.database.is_configured() && config.database.dsn() == state.db.dsn());
    match service.status().await {
        Ok(status) => Json(serde_json::json!({
            "state": status.state,
            "required": status.required,
            "database_configured": configured,
        }))
        .into_response(),
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

/// POST `/api/install/database` — validate and persist the database before the
/// administrator/site steps. The process restarts so the next wizard steps run
/// against the real database, not the in-memory setup runtime.
pub async fn install_database(
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
    let dsn = body.app_db_dsn.trim();
    if dsn.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "message": "database DSN is required" })),
        )
            .into_response();
    }
    let target = match AppDatabase::connect(dsn, true).await {
        Ok(target) => target,
        Err(e) => {
            tracing::warn!(error = %e, "install database connection failed");
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "message": e.to_string() })),
            )
                .into_response();
        }
    };
    if let Err(e) = crate::config::update_database(target.dsn(), 16) {
        target.shutdown().await;
        tracing::error!(error = %e, "write install database config failed");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "message": format!("save config.yaml failed: {e}") })),
        )
            .into_response();
    }
    target.shutdown().await;
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        if !crate::db_task::spawn_restart() {
            tracing::error!("database configured, but auto-restart failed; restart manually");
        }
    });
    Json(serde_json::json!({ "ok": true, "restart": true })).into_response()
}

/// POST `/api/install/complete`.
/// POST `/api/install/complete`。
pub async fn install_complete(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
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
    let configured = crate::config::ServerConfig::load_or_create()
        .ok()
        .is_some_and(|config| config.database.is_configured() && config.database.dsn() == state.db.dsn());
    if !configured {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "message": "configure the database first" })),
        )
            .into_response();
    }
    let site_name = body.sitename.clone();
    let site_description = body.description.clone();
    let audit_username = body.username.clone();
    let audit_driver = body
        .app_db_driver
        .clone()
        .unwrap_or_else(|| "sqlite".into());
    let audit_ip = crate::audit::client_ip(&headers, Some(addr));
    let audit_agent = headers
        .get(header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .chars()
        .take(512)
        .collect::<String>();

    // The database was configured and the process restarted in the previous
    // step; write the remaining site/admin data directly into the live DB.
    let target = &state.db;
    match complete_install(target, target, body).await {
        Ok(outcome) => {
            // Persist the site name/description into the selected database so
            // theme HTML injection and settings survive the install restart.
            // 把站点名称/描述写入所选数据库，保证主题注入与重启后设置一致。
            let mut settings = crate::site_settings::load_from_pool(target).await;
            if let Some(name) = site_name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty())
            {
                settings.site_name = name.to_string();
            }
            if let Some(description) = site_description {
                settings.site_description = description;
            }
            if let Err(e) = crate::site_settings::save_to_pool(target, &settings).await {
                tracing::warn!(error = %e, "save initial site settings failed");
            }
            crate::audit::record_auth_event_to(
                &state.geo,
                target,
                &audit_username,
                "",
                "INSTALL",
                &audit_username,
                &format!(
                    "installation completed; driver={audit_driver}; restart_required={}",
                    outcome.restart_required
                ),
                &audit_ip,
                &audit_agent,
                200,
                "POST",
                "/api/install/complete",
            )
            .await;
            let mut gate = state.install_gate.write().await;
            *gate = false;
            tracing::info!("install wizard deactivated");
            drop(gate);
            if outcome.restart_required {
                // Reply first, then re-exec so the new database takes effect.
                // 先返回响应，再重启进程使新数据库生效。
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(800)).await;
                    if !crate::db_task::spawn_restart() {
                        tracing::error!(
                            "install saved to the selected database, but auto-restart failed; \
                             restart manually"
                        );
                    }
                });
                Json(serde_json::json!({ "ok": true, "restart": true })).into_response()
            } else {
                StatusCode::NO_CONTENT.into_response()
            }
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
