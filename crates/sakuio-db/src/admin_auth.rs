use crate::DbPool;
use crate::{DbError, DbResult};
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use rand::rngs::OsRng;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use totp_rs::{Algorithm, Secret, TOTP};

const SESSION_DAYS: i64 = 30;

#[derive(Debug, Clone, Serialize)]
pub struct AdminUserRow {
    pub id: String,
    pub username: String,
    pub totp_enabled: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdminSessionUser {
    pub id: String,
    pub username: String,
    pub totp_enabled: bool,
}

pub struct AdminAuthRepository<'a> {
    pool: &'a DbPool,
}

impl<'a> AdminAuthRepository<'a> {
    pub fn new(pool: &'a DbPool) -> Self {
        Self { pool }
    }

    pub async fn find_by_username(
        &self,
        username: &str,
    ) -> DbResult<Option<(String, String, bool, Option<String>)>> {
        let row: Option<(String, String, i64, Option<String>)> = sqlx::query_as(
            "SELECT id, password_hash, totp_enabled, totp_secret FROM admin_users WHERE username = ? LIMIT 1",
        )
        .bind(username.trim())
        .fetch_optional(self.pool)
        .await?;
        Ok(row.map(|(id, hash, en, sec)| (id, hash, en != 0, sec)))
    }

    pub async fn user_by_id(&self, id: &str) -> DbResult<Option<AdminUserRow>> {
        let row: Option<(String, String, i64)> = sqlx::query_as(
            "SELECT id, username, totp_enabled FROM admin_users WHERE id = ? LIMIT 1",
        )
        .bind(id)
        .fetch_optional(self.pool)
        .await?;
        Ok(row.map(|(id, username, en)| AdminUserRow {
            id,
            username,
            totp_enabled: en != 0,
        }))
    }

    pub fn verify_password(password: &str, stored_hash: &str) -> bool {
        let parsed = match PasswordHash::new(stored_hash) {
            Ok(p) => p,
            Err(_) => return false,
        };
        Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok()
    }

    pub fn verify_totp(secret_b32: &str, code: &str) -> bool {
        let secret = match Secret::Encoded(secret_b32.to_string()).to_bytes() {
            Ok(b) => b,
            Err(_) => return false,
        };
        let totp = match TOTP::new(
            Algorithm::SHA1,
            6,
            1,
            30,
            secret,
            Some("Sakuio".into()),
            "admin".into(),
        ) {
            Ok(t) => t,
            Err(_) => return false,
        };
        totp.check_current(code.trim()).unwrap_or(false)
    }

    pub fn hash_password(password: &str) -> DbResult<String> {
        let salt = SaltString::generate(&mut OsRng);
        Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map(|hash| hash.to_string())
            .map_err(|e| DbError::Message(e.to_string()))
    }

    pub async fn update_username(&self, user_id: &str, username: &str) -> DbResult<()> {
        sqlx::query("UPDATE admin_users SET username = ? WHERE id = ?")
            .bind(username)
            .bind(user_id)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn update_password_hash(&self, user_id: &str, password_hash: &str) -> DbResult<()> {
        sqlx::query("UPDATE admin_users SET password_hash = ? WHERE id = ?")
            .bind(password_hash)
            .bind(user_id)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn username_exists(&self, username: &str, except_user_id: &str) -> DbResult<bool> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT id FROM admin_users WHERE username = ? AND id <> ? LIMIT 1")
                .bind(username)
                .bind(except_user_id)
                .fetch_optional(self.pool)
                .await?;
        Ok(row.is_some())
    }

    pub async fn create_session(
        &self,
        user_id: &str,
        user_agent: Option<&str>,
        ip: Option<&str>,
    ) -> DbResult<String> {
        let mut raw = [0u8; 32];
        OsRng.fill_bytes(&mut raw);
        let token = hex::encode(raw);
        let token_hash = hash_token(&token);
        let id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now();
        let expires = now + chrono::Duration::days(SESSION_DAYS);
        sqlx::query(
            "INSERT INTO admin_sessions (id, user_id, token_hash, expires_at, created_at, user_agent, ip_address)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&id)
        .bind(user_id)
        .bind(&token_hash)
        .bind(expires.to_rfc3339())
        .bind(now.to_rfc3339())
        .bind(user_agent)
        .bind(ip)
        .execute(self.pool)
        .await?;
        Ok(token)
    }

    pub async fn resolve_session(&self, token: &str) -> DbResult<Option<AdminSessionUser>> {
        let token_hash = hash_token(token);
        let now = chrono::Utc::now().to_rfc3339();
        let row: Option<(String, String, String, i64, Option<String>)> = sqlx::query_as(
            "SELECT s.user_id, u.username, s.id, u.totp_enabled, u.totp_secret
             FROM admin_sessions s
             JOIN admin_users u ON u.id = s.user_id
             WHERE s.token_hash = ? AND s.expires_at > ?
             LIMIT 1",
        )
        .bind(&token_hash)
        .bind(&now)
        .fetch_optional(self.pool)
        .await?;
        Ok(
            row.map(|(id, username, _sid, en, secret)| AdminSessionUser {
                id,
                username,
                totp_enabled: login_requires_2fa(en != 0, &secret),
            }),
        )
    }

    /// Revokes every session for a user after password/TOTP/username changes.
    pub async fn delete_sessions_for_user(&self, user_id: &str) -> DbResult<()> {
        sqlx::query("DELETE FROM admin_sessions WHERE user_id = ?")
            .bind(user_id)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_session(&self, token: &str) -> DbResult<()> {
        let token_hash = hash_token(token);
        sqlx::query("DELETE FROM admin_sessions WHERE token_hash = ?")
            .bind(token_hash)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn totp_secret(&self, user_id: &str) -> DbResult<Option<String>> {
        let row: Option<(Option<String>,)> =
            sqlx::query_as("SELECT totp_secret FROM admin_users WHERE id = ?")
                .bind(user_id)
                .fetch_optional(self.pool)
                .await?;
        Ok(row.and_then(|(s,)| s))
    }

    pub async fn set_totp_pending(&self, user_id: &str, secret: &str) -> DbResult<()> {
        sqlx::query("UPDATE admin_users SET totp_secret = ?, totp_enabled = 0 WHERE id = ?")
            .bind(secret)
            .bind(user_id)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn enable_totp(&self, user_id: &str) -> DbResult<()> {
        sqlx::query("UPDATE admin_users SET totp_enabled = 1 WHERE id = ?")
            .bind(user_id)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn disable_totp(&self, user_id: &str) -> DbResult<()> {
        sqlx::query("UPDATE admin_users SET totp_enabled = 0, totp_secret = NULL WHERE id = ?")
            .bind(user_id)
            .execute(self.pool)
            .await?;
        Ok(())
    }

    pub async fn verify_sensitive_2fa(
        &self,
        user_id: &str,
        code: Option<&str>,
    ) -> Result<(), Sensitive2faError> {
        let row: Option<(i64, Option<String>)> =
            sqlx::query_as("SELECT totp_enabled, totp_secret FROM admin_users WHERE id = ?")
                .bind(user_id)
                .fetch_optional(self.pool)
                .await
                .map_err(|_| Sensitive2faError::Internal)?;

        let Some((enabled, secret)) = row else {
            return Err(Sensitive2faError::Unauthorized);
        };
        if enabled == 0 {
            return Ok(());
        }
        let secret = secret
            .filter(|s| !s.is_empty())
            .ok_or(Sensitive2faError::Internal)?;
        let code = code
            .filter(|c| !c.trim().is_empty())
            .ok_or(Sensitive2faError::Required)?;
        if !Self::verify_totp(&secret, code) {
            return Err(Sensitive2faError::Invalid);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sensitive2faError {
    Required,
    Invalid,
    Unauthorized,
    Internal,
}

impl Sensitive2faError {
    pub fn message(self) -> &'static str {
        match self {
            Self::Required => "2FA code is required",
            Self::Invalid => "Invalid 2FA code",
            Self::Unauthorized => "Unauthorized",
            Self::Internal => "Internal error",
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
    #[serde(default, rename = "2fa_code")]
    pub two_fa_code: Option<String>,
}

fn hash_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn generate_totp_secret() -> String {
    let mut bytes = [0u8; 20];
    OsRng.fill_bytes(&mut bytes);
    Secret::Raw(bytes.to_vec()).to_encoded().to_string()
}

pub fn totp_provisioning_uri(username: &str, secret_b32: &str) -> DbResult<String> {
    let secret = Secret::Encoded(secret_b32.to_string())
        .to_bytes()
        .map_err(|e| DbError::Message(e.to_string()))?;
    let totp = TOTP::new(
        Algorithm::SHA1,
        6,
        1,
        30,
        secret,
        Some("Sakuio".into()),
        username.to_string(),
    )
    .map_err(|e| DbError::Message(e.to_string()))?;
    Ok(totp.get_url())
}

/// True only when admin explicitly enabled 2FA and a secret is stored.
pub fn login_requires_2fa(totp_enabled: bool, totp_secret: &Option<String>) -> bool {
    totp_enabled && totp_secret.as_ref().is_some_and(|s| !s.trim().is_empty())
}

pub fn guest_me_json() -> serde_json::Value {
    serde_json::json!({
        "username": "Guest",
        "logged_in": false,
        "2fa_enabled": false,
    })
}
