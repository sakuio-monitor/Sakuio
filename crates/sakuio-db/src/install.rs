use crate::{settings::SettingsRepository, DbError, DbResult, AppDatabase};
use argon2::{
    password_hash::{PasswordHasher, SaltString},
    Argon2,
};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use uuid::Uuid;

/// Install wizard status payload.
/// 安装向导状态响应体。
#[derive(Debug, Clone, Serialize)]
pub struct InstallStatus {
    pub state: String,
    pub required: bool,
}

/// POST `/api/install/complete` body (subset of DESIGN §3.2).
/// POST `/api/install/complete` 请求体（DESIGN §3.2 子集）。
#[derive(Debug, Deserialize)]
pub struct CompleteInstallRequest {
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub sitename: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub app_db_driver: Option<String>,
    #[serde(default)]
    pub app_db_dsn: Option<String>,
    #[serde(default)]
    pub metric_colocation: Option<String>,
}

/// Orchestrates first-time setup against the app database.
/// 针对应用库编排首次安装流程。
pub struct InstallService<'a> {
    pool: &'a SqlitePool,
}

impl<'a> InstallService<'a> {
    /// Creates the service for the given pool.
    /// 为指定连接池创建安装服务。
    pub fn new(pool: &'a SqlitePool) -> Self {
        Self { pool }
    }

    /// Returns install status for the UI.
    /// 返回供 UI 使用的安装状态。
    pub async fn status(&self) -> DbResult<InstallStatus> {
        let settings = SettingsRepository::new(self.pool);
        let installed = settings.is_installed().await?;
        Ok(InstallStatus {
            state: if installed {
                "completed".into()
            } else {
                "ready".into()
            },
            required: !installed,
        })
    }

    /// Completes install: admin user, settings, metric colocation flags.
    /// 完成安装：管理员账号、系统设置与指标库同库/分库标记。
    pub async fn complete(&self, req: CompleteInstallRequest) -> DbResult<()> {
        let settings = SettingsRepository::new(self.pool);
        if settings.is_installed().await? {
            return Err(DbError::Message("installation already completed".into()));
        }
        validate_credentials(&req.username, &req.password)?;

        let password_hash = hash_password(&req.password)?;
        let admin_id = Uuid::new_v4().to_string();
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO admin_users (id, username, password_hash, created_at)
             VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(&admin_id)
        .bind(&req.username)
        .bind(&password_hash)
        .bind(&now)
        .execute(self.pool)
        .await?;

        settings
            .set_json("sitename", &req.sitename.clone().unwrap_or_else(|| "Sakuio".into()))
            .await?;
        settings
            .set_json(
                "description",
                &req.description.clone().unwrap_or_default(),
            )
            .await?;
        let colocation = req
            .metric_colocation
            .clone()
            .unwrap_or_else(|| "same".into());
        settings.set_json("metric_colocation", &colocation).await?;

        let driver = req
            .app_db_driver
            .clone()
            .unwrap_or_else(|| "sqlite".into());
        let dsn = req
            .app_db_dsn
            .clone()
            .unwrap_or_else(|| "./data/sakuio.db".into());
        settings.set_json("app_db_driver", &driver).await?;
        settings.set_json("app_db_dsn", &dsn).await?;
        settings.set_json("installation_state", &"ready").await?;

        tracing::info!(username = %req.username, "installation completed");
        Ok(())
    }
}

/// Opens the default SQLite database used before install completes.
/// 在安装完成前使用的默认 SQLite 连接。
pub async fn open_bootstrap_db() -> DbResult<AppDatabase> {
    AppDatabase::connect_sqlite("./data/sakuio.db", true).await
}

fn validate_credentials(username: &str, password: &str) -> DbResult<()> {
    if username.trim().is_empty() {
        return Err(DbError::Message("username required".into()));
    }
    if password.len() < 8 {
        return Err(DbError::Message("password must be at least 8 characters".into()));
    }
    Ok(())
}

fn hash_password(password: &str) -> DbResult<String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| DbError::Message(e.to_string()))
}
