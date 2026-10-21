use async_trait::async_trait;
use axum::{
    extract::{FromRequestParts, Request, State},
    http::{header, request::Parts, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use axum_extra::extract::CookieJar;

use crate::AppState;
use sakuio_db::AdminAuthRepository;

use super::SESSION_COOKIE;

#[derive(Clone, Debug)]
pub struct AdminUser {
    pub id: String,
    pub username: String,
    pub totp_enabled: bool,
}

#[derive(Clone, Copy, Default)]
pub struct RequireAdmin;

impl RequireAdmin {
    pub async fn require_token(state: &AppState, token: &str) -> Result<AdminUser, StatusCode> {
        if token.is_empty() {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let repo = AdminAuthRepository::new(&state.db);
        let user = repo
            .resolve_session(token)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .ok_or(StatusCode::UNAUTHORIZED)?;
        Ok(AdminUser {
            id: user.id,
            username: user.username,
            totp_enabled: user.totp_enabled,
        })
    }

    pub async fn require(state: &AppState, jar: &CookieJar) -> Result<AdminUser, StatusCode> {
        let token = jar
            .get(SESSION_COOKIE)
            .map(|c| c.value())
            .unwrap_or("");
        Self::require_token(state, token).await
    }
}

fn session_token_from_cookie_header(raw: &str) -> Option<String> {
    let prefix = format!("{SESSION_COOKIE}=");
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

#[async_trait]
impl<S> FromRequestParts<S> for AdminUser
where
    S: Send + Sync,
{
    type Rejection = StatusCode;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<AdminUser>()
            .cloned()
            .ok_or(StatusCode::UNAUTHORIZED)
    }
}

pub async fn admin_auth_layer(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Response {
    let token = request
        .headers()
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(session_token_from_cookie_header)
        .unwrap_or_default();

    match RequireAdmin::require_token(&state, &token).await {
        Ok(user) => {
            request.extensions_mut().insert(user);
            next.run(request).await
        }
        Err(status) => status.into_response(),
    }
}
