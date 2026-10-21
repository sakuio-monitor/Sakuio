//! Site settings: public site info, CORS / WS origin checks, private site,
//! custom head/body/favicon, and backup download / restore.
//! 站点设置：站点信息、CORS 与 WS Origin 校验、私有站点、自定义内容与备份。

use axum::body::Bytes;
use axum::extract::{Path, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Redirect, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::auth::AdminUser;
use crate::AppState;
use sakuio_db::{AdminAuthRepository, SettingsRepository};

pub const SETTINGS_KEY: &str = "site_settings";
pub const SHARE_COOKIE: &str = "sakuio_share";
const DB_PATH: &str = "./data/sakuio.db";
const RESTORE_PATH: &str = "./data/sakuio.db.restore";

/// Everything configurable on the site settings page, stored as one JSON blob.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct SiteSettings {
    pub site_name: String,
    pub site_description: String,
    pub cors_origin_check_enabled: bool,
    pub cors_allowed_origins: Vec<String>,
    pub ws_origin_check_enabled: bool,
    pub ws_allowed_origins: Vec<String>,
    pub ssrf_protection: bool,
    pub show_partial_ip: bool,
    pub agent_connect_address: String,
    pub private_site: bool,
    pub share_token: String,
    pub custom_head: String,
    pub custom_body: String,
    pub favicon: String,
    pub auto_discovery_key: String,
    #[serde(default = "default_geoip_enabled")]
    pub geoip_enabled: bool,
    #[serde(default = "default_geoip_provider")]
    pub geoip_provider: String,
    pub geoip_mmdb_url: String,
    #[serde(default)]
    pub auto_discovery_generated: bool,
}

fn default_geoip_enabled() -> bool {
    true
}

fn default_geoip_provider() -> String {
    sakuio_notify::ipgeo::PROVIDER_IP9.into()
}

impl Default for SiteSettings {
    fn default() -> Self {
        Self {
            site_name: "Sakuio".into(),
            site_description: "Sakuio server monitoring".into(),
            cors_origin_check_enabled: false,
            cors_allowed_origins: Vec::new(),
            ws_origin_check_enabled: false,
            ws_allowed_origins: Vec::new(),
            ssrf_protection: false,
            show_partial_ip: false,
            agent_connect_address: String::new(),
            private_site: false,
            share_token: String::new(),
            custom_head: String::new(),
            custom_body: String::new(),
            favicon: String::new(),
            auto_discovery_key: String::new(),
            geoip_enabled: default_geoip_enabled(),
            geoip_provider: default_geoip_provider(),
            geoip_mmdb_url: String::new(),
            auto_discovery_generated: false,
        }
    }
}

/// Startup bootstrap: generates the auto-discovery key on first launch only.
/// 自动发现密钥只在首次启动（尚无设置记录或未生成过）时生成，之后留空即表示禁用。
pub async fn bootstrap_defaults(state: &AppState) {
    let mut settings = load(state).await;
    if !settings.auto_discovery_generated {
        // 一次性迁移：老库默认的 geojs 切到 ip9。
        if settings.geoip_provider == sakuio_notify::ipgeo::PROVIDER_GEOJS {
            settings.geoip_provider = default_geoip_provider();
        }
        if settings.auto_discovery_key.is_empty() {
            settings.auto_discovery_key = format!(
                "{}{}",
                uuid::Uuid::new_v4().simple(),
                uuid::Uuid::new_v4().simple()
            );
            tracing::info!("auto discovery enabled with a generated key");
        }
        settings.auto_discovery_generated = true;
        if let Err(e) = save(state, &settings).await {
            tracing::error!(error = ?e, "failed to persist auto discovery key");
        }
    }
}

/// Loads site settings from `system_settings`, falling back to defaults.
pub async fn load(state: &AppState) -> SiteSettings {
    SettingsRepository::new(&state.db)
        .get_json::<SiteSettings>(SETTINGS_KEY)
        .await
        .ok()
        .flatten()
        .unwrap_or_default()
}

async fn save(state: &AppState, settings: &SiteSettings) -> Result<(), StatusCode> {
    SettingsRepository::new(&state.db)
        .set_json(SETTINGS_KEY, settings)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

fn normalize_origin(origin: &str) -> String {
    origin.trim().trim_end_matches('/').to_lowercase()
}

/// True when `origin` matches the request host (same-origin) or the allow list.
fn origin_allowed(origin: &str, host_header: Option<&str>, allow_list: &[String]) -> bool {
    let origin = normalize_origin(origin);
    if let Some(host) = host_header {
        let host = host.trim().to_lowercase();
        let host_with_scheme = format!("http://{host}");
        let host_with_tls_scheme = format!("https://{host}");
        if origin == host_with_scheme || origin == host_with_tls_scheme {
            return true;
        }
    }
    allow_list
        .iter()
        .any(|item| normalize_origin(item) == origin && !origin.is_empty())
}

fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let raw = headers.get(header::COOKIE)?.to_str().ok()?;
    let prefix = format!("{name}=");
    for part in raw.split(';') {
        let part = part.trim();
        if let Some(value) = part.strip_prefix(&prefix) {
            let value = value.trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

/// CORS / WebSocket Origin guard for all public routes.
pub async fn origin_guard(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let settings = load(&state).await;
    if !settings.cors_origin_check_enabled && !settings.ws_origin_check_enabled {
        return next.run(request).await;
    }
    let headers = request.headers();
    let Some(origin) = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) else {
        return next.run(request).await;
    };
    let is_ws = headers
        .get("sec-websocket-version")
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    let enabled = if is_ws {
        settings.ws_origin_check_enabled
    } else {
        settings.cors_origin_check_enabled
    };
    if !enabled {
        return next.run(request).await;
    }
    let allow_list = if is_ws {
        &settings.ws_allowed_origins
    } else {
        &settings.cors_allowed_origins
    };
    let host = headers.get(header::HOST).and_then(|v| v.to_str().ok());
    if origin_allowed(origin, host, allow_list) {
        next.run(request).await
    } else {
        StatusCode::FORBIDDEN.into_response()
    }
}

/// Private-site gate: public data routes require an admin session or share cookie.
pub async fn private_site_layer(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    let settings = load(&state).await;
    if !settings.private_site {
        return next.run(request).await;
    }
    let path = request.uri().path();
    let open_path = path == "/ping"
        || path == "/api/version"
        || path == "/api/public"
        || path == "/api/oauth"
        || path == "/api/oauth_callback"
        || path.starts_with("/api/share/")
        || path.starts_with("/api/install");
    if open_path {
        return next.run(request).await;
    }
    if let Some(token) = cookie_value(request.headers(), crate::auth::SESSION_COOKIE) {
        let logged_in = AdminAuthRepository::new(&state.db)
            .resolve_session(&token)
            .await
            .ok()
            .flatten()
            .is_some();
        if logged_in {
            return next.run(request).await;
        }
    }
    if !settings.share_token.is_empty()
        && cookie_value(request.headers(), SHARE_COOKIE).as_deref() == Some(settings.share_token.as_str())
    {
        return next.run(request).await;
    }
    StatusCode::UNAUTHORIZED.into_response()
}

/// GET `/api/share/:token` — validates a temporary share link and drops a cookie.
pub async fn share_entry(
    State(state): State<AppState>,
    jar: axum_extra::extract::cookie::CookieJar,
    Path(token): Path<String>,
) -> Response {
    let settings = load(&state).await;
    if settings.share_token.is_empty() || !settings.private_site {
        return StatusCode::NOT_FOUND.into_response();
    }
    if token != settings.share_token {
        return StatusCode::FORBIDDEN.into_response();
    }
    let cookie = axum_extra::extract::cookie::Cookie::build((SHARE_COOKIE, token))
        .path("/")
        .http_only(true)
        .max_age(cookie::time::Duration::seconds(7 * 24 * 3600))
        .build();
    (jar.add(cookie), Redirect::to("/")).into_response()
}

/// GET `/api/admin/settings/site`.
pub async fn get_site_settings(State(state): State<AppState>, _admin: AdminUser) -> Response {
    let settings = load(&state).await;
    Json(json!({ "settings": settings })).into_response()
}

/// PUT `/api/admin/settings/site`.
pub async fn put_site_settings(
    State(state): State<AppState>,
    _admin: AdminUser,
    Json(body): Json<SiteSettingsBody>,
) -> Response {
    let mut settings = load(&state).await;
    if let Some(v) = body.site_name {
        settings.site_name = v.trim().to_string();
    }
    if let Some(v) = body.site_description {
        settings.site_description = v;
    }
    if let Some(v) = body.cors_origin_check_enabled {
        settings.cors_origin_check_enabled = v;
    }
    if let Some(v) = body.cors_allowed_origins {
        settings.cors_allowed_origins = v;
    }
    if let Some(v) = body.ws_origin_check_enabled {
        settings.ws_origin_check_enabled = v;
    }
    if let Some(v) = body.ws_allowed_origins {
        settings.ws_allowed_origins = v;
    }
    if let Some(v) = body.ssrf_protection {
        settings.ssrf_protection = v;
    }
    if let Some(v) = body.show_partial_ip {
        settings.show_partial_ip = v;
    }
    if let Some(v) = body.agent_connect_address {
        settings.agent_connect_address = v.trim().trim_end_matches('/').to_string();
    }
    if let Some(v) = body.private_site {
        settings.private_site = v;
    }
    if let Some(v) = body.custom_head {
        settings.custom_head = v;
    }
    if let Some(v) = body.custom_body {
        settings.custom_body = v;
    }
    if let Some(v) = body.favicon {
        settings.favicon = v;
    }
    if let Some(v) = body.auto_discovery_key {
        settings.auto_discovery_key = v.trim().to_string();
    }
    if let Some(v) = body.geoip_enabled {
        settings.geoip_enabled = v;
    }
    if let Some(v) = body.geoip_provider {
        settings.geoip_provider = v.trim().to_string();
    }
    if let Some(v) = body.geoip_mmdb_url {
        settings.geoip_mmdb_url = v.trim().to_string();
    }
    sakuio_notify::ipgeo::clear_cache();
    match save(&state, &settings).await {
        Ok(()) => Json(json!({ "settings": settings })).into_response(),
        Err(status) => api_error_db(status),
    }
}

/// POST `/api/admin/settings/site/share-token` — regenerate the share token.
pub async fn regenerate_share_token(State(state): State<AppState>, _admin: AdminUser) -> Response {
    let mut settings = load(&state).await;
    settings.share_token = uuid::Uuid::new_v4().simple().to_string();
    match save(&state, &settings).await {
        Ok(()) => Json(json!({ "share_token": settings.share_token })).into_response(),
        Err(status) => api_error_db(status),
    }
}

/// GET `/api/admin/settings/backup` — download a SQLite backup via `VACUUM INTO`.
pub async fn download_backup(State(state): State<AppState>, _admin: AdminUser) -> Response {
    let ts = chrono::Utc::now().format("%Y%m%d-%H%M%S");
    let path = format!("./data/sakuio-backup-{ts}.db");
    let sql = format!("VACUUM INTO '{}'", path.replace('\'', "''"));
    if let Err(e) = sqlx::query(&sql).execute(&state.db).await {
        tracing::error!("backup vacuum failed: {e}");
        return api_error_db(StatusCode::INTERNAL_SERVER_ERROR);
    }
    let bytes = match tokio::fs::read(&path).await {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("backup read failed: {e}");
            return api_error_db(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };
    let _ = tokio::fs::remove_file(&path).await;
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/octet-stream"),
            (
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"sakuio-backup.db\"",
            ),
        ],
        bytes,
    )
        .into_response()
}

/// POST `/api/admin/settings/restore` — upload a backup; hot-applied immediately.
/// 流程：关闭连接池 → 换库文件 → 重新拉起进程。
pub async fn restore_backup(State(state): State<AppState>, _admin: AdminUser, body: Bytes) -> Response {
    if body.is_empty() || !body.starts_with(b"SQLite format 3\x00") {
        return api_error_db(StatusCode::BAD_REQUEST);
    }
    if tokio::fs::write(RESTORE_PATH, &body).await.is_err() {
        return api_error_db(StatusCode::INTERNAL_SERVER_ERROR);
    }
    // 先返回响应，再热应用并重启进程。
    let state_for_restart = state.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        apply_restore_and_restart(state_for_restart).await;
    });
    Json(json!({
        "restored": true,
        "message": "backup applied; server is restarting",
    }))
    .into_response()
}

/// Closes the pool, swaps in the restored database file and re-executes the server.
async fn apply_restore_and_restart(state: AppState) {
    state.db.close().await;
    let db_path = std::path::Path::new(DB_PATH);
    let bak_path = std::path::Path::new("./data/sakuio.db.bak");
    let _ = tokio::fs::remove_file("./data/sakuio.db-wal").await;
    let _ = tokio::fs::remove_file("./data/sakuio.db-shm").await;
    let _ = tokio::fs::rename(db_path, bak_path).await;
    if let Err(e) = tokio::fs::rename(RESTORE_PATH, db_path).await {
        tracing::error!("failed to swap restored database: {e}");
        let _ = tokio::fs::rename(bak_path, db_path).await;
        std::process::exit(1);
    }
    tracing::info!("database restored, restarting server process");
    if let Ok(exe) = std::env::current_exe() {
        let _ = tokio::process::Command::new(exe)
            .args(std::env::args_os().skip(1))
            .envs(std::env::vars_os())
            .spawn();
    }
    std::process::exit(0);
}

/// Startup hook: applies a staged restore file before the pool opens.
pub fn apply_staged_restore() {
    let restore_path = std::path::Path::new(RESTORE_PATH);
    if !restore_path.exists() {
        return;
    }
    let db_path = std::path::Path::new(DB_PATH);
    let bak_path = std::path::Path::new("./data/sakuio.db.bak");
    if db_path.exists() {
        let _ = std::fs::rename(db_path, bak_path);
    }
    match std::fs::rename(restore_path, db_path) {
        Ok(()) => tracing::info!("restored database backup, previous file kept as sakuio.db.bak"),
        Err(e) => tracing::error!("failed to apply staged restore: {e}"),
    }
}

fn api_error_db(status: StatusCode) -> Response {
    (
        status,
        Json(json!({ "status": "error", "message": "database error" })),
    )
        .into_response()
}

/// POST `/api/admin/settings/geoip/update` — download / refresh the local MMDB database.
pub async fn update_geoip_db(State(state): State<AppState>, _admin: AdminUser) -> Response {
    let settings = load(&state).await;
    let url = if settings.geoip_mmdb_url.trim().is_empty() {
        None
    } else {
        Some(settings.geoip_mmdb_url.as_str())
    };
    match sakuio_notify::geoip::download_mmdb(url.unwrap_or(sakuio_notify::geoip::DEFAULT_MMDB_URL))
        .await
    {
        Ok(bytes) => {
            sakuio_notify::ipgeo::clear_cache();
            Json(json!({ "updated": true, "bytes": bytes })).into_response()
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(json!({ "status": "error", "message": e })),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
pub struct GeoIpTestBody {
    #[serde(default)]
    pub ip: Option<String>,
}

/// POST `/api/admin/settings/geoip/test` — resolve a location with the configured provider.
pub async fn test_geoip(
    State(state): State<AppState>,
    _admin: AdminUser,
    Json(body): Json<GeoIpTestBody>,
) -> Response {
    let settings = load(&state).await;
    let mut ip = body.ip.unwrap_or_default().trim().to_string();
    if ip.is_empty() {
        // No IP given: probe the server's own public egress address.
        match sakuio_notify::geoip::public_ip().await {
            Ok(public) => ip = public,
            Err(e) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    Json(json!({ "status": "error", "message": e })),
                )
                    .into_response()
            }
        }
    }
    let location = sakuio_notify::ipgeo::lookup_with(&settings.geoip_provider, &ip)
        .await
        .unwrap_or_default();
    Json(json!({ "ip": ip, "location": location })).into_response()
}

/// Partial update payload for `PUT /api/admin/settings/site`.
#[derive(Deserialize)]
pub struct SiteSettingsBody {
    pub site_name: Option<String>,
    pub site_description: Option<String>,
    pub cors_origin_check_enabled: Option<bool>,
    pub cors_allowed_origins: Option<Vec<String>>,
    pub ws_origin_check_enabled: Option<bool>,
    pub ws_allowed_origins: Option<Vec<String>>,
    pub ssrf_protection: Option<bool>,
    pub show_partial_ip: Option<bool>,
    pub agent_connect_address: Option<String>,
    pub private_site: Option<bool>,
    pub custom_head: Option<String>,
    pub custom_body: Option<String>,
    pub favicon: Option<String>,
    pub auto_discovery_key: Option<String>,
    pub geoip_enabled: Option<bool>,
    pub geoip_provider: Option<String>,
    pub geoip_mmdb_url: Option<String>,
}
