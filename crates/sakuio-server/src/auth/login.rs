use axum::{
    extract::{ConnectInfo, State},
    http::{header, HeaderMap, StatusCode},
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
    let audit_ip = crate::audit::client_ip(&headers, Some(addr));
    let audit_agent = headers
        .get(header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .chars()
        .take(512)
        .collect::<String>();
    if *state.install_gate.read().await {
        return api_error(StatusCode::FORBIDDEN, "installation required");
    }
    let repo = AdminAuthRepository::new(&state.db);
    let Some((user_id, hash, totp_enabled, totp_secret)) =
        repo.find_by_username(&body.username).await.ok().flatten()
    else {
        crate::audit::record_auth_event(
            &state,
            &body.username,
            "",
            "LOGIN_FAILED",
            &body.username,
            "unknown user",
            &audit_ip,
            &audit_agent,
            401,
            "POST",
            "/api/login",
        )
        .await;
        return api_error(StatusCode::UNAUTHORIZED, "Invalid credentials");
    };
    if !AdminAuthRepository::verify_password(&body.password, &hash) {
        crate::audit::record_auth_event(
            &state,
            &body.username,
            &user_id,
            "LOGIN_FAILED",
            &body.username,
            "invalid password",
            &audit_ip,
            &audit_agent,
            401,
            "POST",
            "/api/login",
        )
        .await;
        return api_error(StatusCode::UNAUTHORIZED, "Invalid credentials");
    }
    if login_requires_2fa(totp_enabled, &totp_secret) {
        let code = body.two_fa_code.as_deref();
        let secret = totp_secret.as_deref().unwrap_or("");
        if code.is_none() || code == Some("") {
            crate::audit::record_auth_event(
                &state,
                &body.username,
                &user_id,
                "LOGIN_CHALLENGE",
                &body.username,
                "2FA code required",
                &audit_ip,
                &audit_agent,
                401,
                "POST",
                "/api/login",
            )
            .await;
            return login_needs_2fa_error();
        }
        if !AdminAuthRepository::verify_totp(secret, code.unwrap()) {
            crate::audit::record_auth_event(
                &state,
                &body.username,
                &user_id,
                "LOGIN_FAILED",
                &body.username,
                "invalid 2FA code",
                &audit_ip,
                &audit_agent,
                401,
                "POST",
                "/api/login",
            )
            .await;
            return api_error(StatusCode::UNAUTHORIZED, "Invalid 2FA code");
        }
    }
    let token = match repo.create_session(&user_id, None, None).await {
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
    crate::audit::record_auth_event(
        &state,
        &user.username,
        &user.id,
        "LOGIN",
        &body.username,
        "login success",
        &audit_ip,
        &audit_agent,
        200,
        "POST",
        "/api/login",
    )
    .await;
    let state_for_notify = state.clone();
    let username = body.username.clone();
    let login_ip = audit_ip.clone();
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

pub async fn logout(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    jar: CookieJar,
) -> impl IntoResponse {
    let ip = crate::audit::client_ip(&headers, Some(addr));
    let agent = headers
        .get(header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .chars()
        .take(512)
        .collect::<String>();
    let user = crate::auth::RequireAdmin::require(&state, &jar).await.ok();
    if let Some(c) = jar.get(SESSION_COOKIE) {
        let _ = AdminAuthRepository::new(&state.db)
            .delete_session(c.value())
            .await;
    }
    if let Some(user) = user {
        crate::audit::record_auth_event(
            &state,
            &user.username,
            &user.id,
            "LOGOUT",
            &user.username,
            "logout",
            &ip,
            &agent,
            200,
            "GET",
            "/api/logout",
        )
        .await;
    }
    let cleared = Cookie::build((SESSION_COOKIE, ""))
        .path("/")
        .max_age(cookie::time::Duration::seconds(0))
        .http_only(true)
        .build();
    (jar.add(cleared), api_success(json!({ "logged_out": true }))).into_response()
}

pub async fn session_me(State(state): State<AppState>, jar: CookieJar) -> impl IntoResponse {
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
