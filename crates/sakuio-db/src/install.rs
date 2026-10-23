use crate::{
    normalize_dsn, settings::SettingsRepository, AppDatabase, DbError, DbKind, DbPool, DbResult,
};
use argon2::{
    password_hash::{PasswordHasher, SaltString},
    Argon2,
};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Install wizard status payload.
/// 安装向导状态响应体。
#[derive(Debug, Clone, Serialize)]
pub struct InstallStatus {
    pub state: String,
    pub required: bool,
}

/// Result of a completed install.
/// 安装完成后的结果。
#[derive(Debug, Clone, Serialize)]
pub struct InstallOutcome {
    /// True when the process must restart to open the selected database.
    /// 需要重启进程才能打开所选数据库时为 true。
    pub restart_required: bool,
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
    pool: &'a DbPool,
}

impl<'a> InstallService<'a> {
    /// Creates the service for the given pool.
    /// 为指定连接池创建安装服务。
    pub fn new(pool: &'a DbPool) -> Self {
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
}

/// Canonical driver label for a database kind (matches the settings values).
/// 数据库类型对应的规范驱动名（与设置值一致）。
pub fn canonical_driver(kind: DbKind) -> &'static str {
    match kind {
        DbKind::Sqlite => "sqlite",
        DbKind::MySql => "mysql",
        DbKind::Postgres => "postgresql",
    }
}

/// Completes install into `target`, using `bootstrap` as the locator database
/// read at startup. The locator only stores `app_db_driver` / `app_db_dsn`;
/// all user data is written to `target`.
/// 把安装数据写入 `target`；`bootstrap` 仅保存启动时读取的定位信息
/// （`app_db_driver` / `app_db_dsn`），用户数据全部落在目标库。
pub async fn complete_install(
    target: &DbPool,
    bootstrap: &DbPool,
    req: CompleteInstallRequest,
) -> DbResult<InstallOutcome> {
    validate_credentials(&req.username, &req.password)?;
    validate_driver(req.app_db_driver.as_deref(), target.kind())?;

    let settings = SettingsRepository::new(target);
    if settings.is_installed().await? {
        return Err(DbError::Message("installation already completed".into()));
    }

    let password_hash = hash_password(&req.password)?;
    let admin_id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO admin_users (id, username, password_hash, created_at)
         VALUES (?, ?, ?, ?)",
    )
    .bind(&admin_id)
    .bind(&req.username)
    .bind(&password_hash)
    .bind(&now)
    .execute(target)
    .await?;

    let sitename = req.sitename.clone().unwrap_or_else(|| "Sakuio".into());
    let description = req.description.clone().unwrap_or_default();
    let colocation = req
        .metric_colocation
        .clone()
        .unwrap_or_else(|| "same".into());
    let driver = canonical_driver(target.kind());
    let dsn = target.dsn().to_string();

    settings.set_json("sitename", &sitename).await?;
    settings.set_json("description", &description).await?;
    settings.set_json("metric_colocation", &colocation).await?;
    settings
        .set_json("installation_state", &"completed")
        .await?;
    settings.set_json("app_db_driver", &driver).await?;
    settings.set_json("app_db_dsn", &dsn).await?;

    // The locator must live in the bootstrap database because startup reads it
    // before it knows where the real database is.
    // 启动时需要先读定位信息，因此它必须写在 bootstrap 库。
    let bootstrap_settings = SettingsRepository::new(bootstrap);
    let same_database = normalize_dsn(bootstrap.dsn()) == normalize_dsn(target.dsn());
    if same_database {
        bootstrap_settings
            .set_json("app_db_driver", &driver)
            .await?;
        bootstrap_settings.set_json("app_db_dsn", &dsn).await?;
        bootstrap_settings
            .set_json("installation_state", &"completed")
            .await?;
    } else {
        bootstrap_settings
            .set_json("app_db_driver", &driver)
            .await?;
        bootstrap_settings.set_json("app_db_dsn", &dsn).await?;
    }

    tracing::info!(
        username = %req.username,
        driver,
        same_database,
        "installation completed"
    );
    Ok(InstallOutcome {
        restart_required: !same_database,
    })
}

/// Opens the app database before install completes (defaults to local SQLite).
/// 在安装完成前打开应用库（默认本机 SQLite）。
pub async fn open_bootstrap_db() -> DbResult<AppDatabase> {
    AppDatabase::connect("./data/sakuio.db", true).await
}

fn validate_driver(driver: Option<&str>, kind: DbKind) -> DbResult<()> {
    let Some(driver) = driver.map(str::trim).filter(|d| !d.is_empty()) else {
        return Ok(());
    };
    let expected = canonical_driver(kind);
    let supplied = match driver.to_ascii_lowercase().as_str() {
        "postgresql" | "postgres" => "postgresql",
        "mariadb" | "mysql" => "mysql",
        "sqlite" | "sqlite3" => "sqlite",
        other => {
            return Err(DbError::Message(format!(
                "unsupported database driver: {other}"
            )))
        }
    };
    if supplied != expected {
        return Err(DbError::Message(format!(
            "driver {driver} does not match DSN backend {expected}"
        )));
    }
    Ok(())
}

fn validate_credentials(username: &str, password: &str) -> DbResult<()> {
    if username.trim().is_empty() {
        return Err(DbError::Message("username required".into()));
    }
    if password.len() < 8 {
        return Err(DbError::Message(
            "password must be at least 8 characters".into(),
        ));
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
