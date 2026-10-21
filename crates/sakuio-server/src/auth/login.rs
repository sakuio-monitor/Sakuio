use axum::{
    extract::{ConnectInfo, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use axum_extra::extract::cookie::{Cookie, CookieJar, SameSite};
use serde_json::json;
use std::net::SocketAddr;

use crate::web::respond::{api_error, api_success};
use crate::AppState;
use sakuio_db::{login_requires_2fa, AdminAuthRepository, LoginRequest};

use super::{user_me_json, SESSION_COOKIE};

const SESSION_MAX_AGE_SECS: i64 = 30 * 24 * 3600;

fn login_needs_2fa_error() -> axum::response::Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "status": "error",
            "message": "2FA code is required",
            "needs_2fa": true,
        })),
    )
        .into_response()
}

pub async fn login(
    State(state): State<AppState>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    jar: CookieJar,
    Json(body): Json<LoginRequest>,
) -> impl IntoResponse {
    if *state.install_gate.read().await {
        return api_error(StatusCode::FORBIDDEN, "installation required");
    }
    let repo = AdminAuthRepository::new(&state.db);
    let Some((user_id, hash, totp_enabled, totp_secret)) =
        repo.find_by_username(&body.username).await.ok().flatten()
    else {
        return api_error(StatusCode::UNAUTHORIZED, "Invalid credentials");
    };
    if !AdminAuthRepository::verify_password(&body.password, &hash) {
        return api_error(StatusCode::UNAUTHORIZED, "Invalid credentials");
    }
    if login_requires_2fa(totp_enabled, &totp_secret) {
        let code = body.two_fa_code.as_deref();
        let secret = totp_secret.as_deref().unwrap_or("");
        if code.is_none() || code == Some("") {
            return login_needs_2fa_error();
        }
        if !AdminAuthRepository::verify_totp(secret, code.unwrap()) {
            return api_error(StatusCode::UNAUTHORIZED, "Invalid 2FA code");
        }
    }
    let token = match repo
        .create_session(&user_id, None, None)
        .await
    {
        Ok(t) => t,
        Err(_) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, "session failed"),
    };
    let cookie = Cookie::build((SESSION_COOKIE, token))
        .path("/")
        .http_only(true)
        .same_site(SameSite::Lax)
        .max_age(cookie::time::Duration::seconds(SESSION_MAX_AGE_SECS))
        .build();
    let user = repo
        .resolve_session(cookie.value())
        .await
        .ok()
        .flatten()
        .unwrap_or(sakuio_db::AdminSessionUser {
            id: user_id,
            username: body.username.clone(),
            totp_enabled: login_requires_2fa(totp_enabled, &totp_secret),
        });
    let state_for_notify = state.clone();
    let username = body.username.clone();
    let login_ip = headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(',').next())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| addr.ip().to_string());
    tokio::spawn(async move {
        let provider = crate::site_settings::load(&state_for_notify)
            .await
            .geoip_provider;
        let ip_location = sakuio_notify::ipgeo::lookup_with(&provider, &login_ip)
            .await
            .unwrap_or_else(|| "未知".into());
        crate::notification_api::dispatch_event(
            &state_for_notify,
            crate::notification_api::EVENT_LOGIN,
            "",
            "",
            &[
                ("username", username),
                ("ip", login_ip),
                ("ip_location", ip_location),
            ],
        )
        .await;
    });
    (jar.add(cookie), api_success(user_me_json(&user))).into_response()
}

pub async fn logout(State(state): State<AppState>, jar: CookieJar) -> impl IntoResponse {
    if let Some(c) = jar.get(SESSION_COOKIE) {
        let _ = AdminAuthRepository::new(&state.db)
            .delete_session(c.value())
            .await;
    }
    let cleared = Cookie::build((SESSION_COOKIE, ""))
        .path("/")
        .max_age(cookie::time::Duration::seconds(0))
        .http_only(true)
        .build();
    (jar.add(cleared), api_success(json!({ "logged_out": true }))).into_response()
}

pub async fn session_me(
    State(state): State<AppState>,
    jar: CookieJar,
) -> impl IntoResponse {
    let repo = AdminAuthRepository::new(&state.db);
    let Some(token) = jar.get(SESSION_COOKIE).map(|c| c.value()) else {
        return api_success(sakuio_db::guest_me_json());
    };
    match repo.resolve_session(token).await {
        Ok(Some(user)) => api_success(user_me_json(&user)),
        Ok(None) => api_success(sakuio_db::guest_me_json()),
        Err(_) => api_error(StatusCode::INTERNAL_SERVER_ERROR, "db error"),
    }
}
