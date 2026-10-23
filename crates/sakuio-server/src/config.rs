//! Persistent server configuration stored next to the executable.
//! 可执行文件同目录下的持久化服务配置。

use anyhow::{Context, Result};
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use url::Url;

const CONFIG_NAME: &str = "config.yaml";
const DEFAULT_LISTEN: &str = "0.0.0.0:8555";
const DEFAULT_MAIN_MAX: u32 = 16;
const DEFAULT_METRIC_MAX: u32 = 25;

fn default_main_max() -> u32 {
    DEFAULT_MAIN_MAX
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseSettings {
    /// sqlite, postgres, mysql, or unconfigured.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub driver: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub host: String,
    #[serde(default, skip_serializing_if = "is_zero_u16")]
    pub port: u16,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub database: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub user: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub password: String,
    /// SQLite file path, relative to the service working directory.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub path: String,
    #[serde(default = "default_main_max")]
    pub max_connections: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub min_idle_connections: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricDatabaseSettings {
    /// "same" (colocated with the main database) or "separate".
    pub mode: String,
    #[serde(flatten)]
    pub connection: DatabaseSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub listen: String,
    /// Main application database. `driver: unconfigured` means setup mode.
    pub database: DatabaseSettings,
    /// Metrics are always explicitly declared as same or separate.
    pub metric_database: MetricDatabaseSettings,
}

impl Default for DatabaseSettings {
    fn default() -> Self {
        Self {
            driver: "unconfigured".into(),
            host: String::new(),
            port: 0,
            database: String::new(),
            user: String::new(),
            password: String::new(),
            path: String::new(),
            max_connections: DEFAULT_MAIN_MAX,
            min_idle_connections: 0,
        }
    }
}

impl Default for MetricDatabaseSettings {
    fn default() -> Self {
        Self {
            mode: "same".into(),
            connection: DatabaseSettings {
                driver: String::new(),
                max_connections: DEFAULT_METRIC_MAX,
                ..DatabaseSettings::default()
            },
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: DEFAULT_LISTEN.into(),
            database: DatabaseSettings::default(),
            metric_database: MetricDatabaseSettings::default(),
        }
    }
}

fn is_zero_u16(value: &u16) -> bool {
    *value == 0
}

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

impl DatabaseSettings {
    pub fn is_configured(&self) -> bool {
        let driver = self.driver.trim().to_ascii_lowercase();
        matches!(driver.as_str(), "sqlite" | "sqlite3" | "postgres" | "postgresql" | "mysql" | "mariadb")
    }

    pub fn dsn(&self) -> String {
        let driver = self.driver.trim().to_ascii_lowercase();
        match driver.as_str() {
            "sqlite" | "sqlite3" => {
                let path = self.path.trim();
                if path.is_empty() {
                    "sqlite://data/sakuio.db?mode=rwc".into()
                } else if path.contains("://") {
                    path.to_string()
                } else {
                    format!("sqlite://{}?mode=rwc", path.trim_start_matches("./"))
                }
            }
            "postgres" | "postgresql" => {
                self.network_dsn("postgres", 5432)
            }
            "mysql" | "mariadb" => {
                self.network_dsn("mysql", 3306)
            }
            _ => String::new(),
        }
    }

    fn network_dsn(&self, scheme: &str, default_port: u16) -> String {
        let user = utf8_percent_encode(self.user.trim(), NON_ALPHANUMERIC).to_string();
        let password = utf8_percent_encode(&self.password, NON_ALPHANUMERIC).to_string();
        let host = if self.host.trim().is_empty() { "127.0.0.1" } else { self.host.trim() };
        let port = if self.port == 0 { default_port } else { self.port };
        let database = self.database.trim();
        format!("{scheme}://{user}:{password}@{host}:{port}/{database}")
    }

    pub fn from_dsn(dsn: &str, max_connections: u32) -> Self {
        let dsn = dsn.trim();
        let driver = match sakuio_db::DbKind::detect(dsn) {
            sakuio_db::DbKind::Sqlite => "sqlite",
            sakuio_db::DbKind::Postgres => "postgres",
            sakuio_db::DbKind::MySql => "mysql",
        };
        let mut result = Self {
            driver: driver.into(),
            max_connections,
            ..Self::default()
        };
        if driver == "sqlite" {
            result.path = dsn
                .trim_start_matches("sqlite://")
                .split('?')
                .next()
                .unwrap_or("data/sakuio.db")
                .to_string();
            if result.path.is_empty() {
                result.path = "data/sakuio.db".into();
            }
            return result;
        }
        if let Ok(url) = Url::parse(dsn) {
            result.host = url.host_str().unwrap_or("127.0.0.1").to_string();
            result.port = url.port().unwrap_or_else(|| if driver == "mysql" { 3306 } else { 5432 });
            result.database = url.path().trim_start_matches('/').to_string();
            result.user = percent_decode_str(url.username()).decode_utf8_lossy().to_string();
            result.password = url
                .password()
                .map(|value| percent_decode_str(value).decode_utf8_lossy().to_string())
                .unwrap_or_default();
        }
        result
    }
}

impl MetricDatabaseSettings {
    pub fn is_same(&self) -> bool {
        self.mode.trim().is_empty() || self.mode.trim().eq_ignore_ascii_case("same")
    }

    pub fn dsn(&self) -> String {
        if self.is_same() {
            String::new()
        } else {
            self.connection.dsn()
        }
    }

    pub fn max_connections(&self) -> u32 {
        self.connection.max_connections
    }
}

impl ServerConfig {
    pub fn path() -> Result<PathBuf> {
        let cwd = std::env::current_dir().context("resolve working directory")?;
        let cwd_config = cwd.join(CONFIG_NAME);
        if cwd_config.exists() {
            return Ok(cwd_config);
        }
        let exe = std::env::current_exe().context("resolve executable path")?;
        let dir = exe.parent().context("resolve executable directory")?;
        Ok(dir.join(CONFIG_NAME))
    }

    pub fn load_or_create() -> Result<Self> {
        let path = Self::path()?;
        if path.exists() {
            let raw = fs::read_to_string(&path)
                .with_context(|| format!("read {}", path.display()))?;
            let config: Self = serde_yaml::from_str(&raw)
                .with_context(|| format!("parse {}", path.display()))?;
            return Ok(config);
        }
        let config = Self::default();
        config.save_to(&path)?;
        Ok(config)
    }

    pub fn save(&self) -> Result<()> {
        self.save_to(&Self::path()?)
    }

    fn save_to(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create {}", parent.display()))?;
        }
        let raw = serde_yaml::to_string(self).context("serialize config.yaml")?;
        let tmp = path.with_extension(format!("yaml.tmp.{}", std::process::id()));
        fs::write(&tmp, raw).with_context(|| format!("write {}", tmp.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600));
        }
        fs::rename(&tmp, path).with_context(|| format!("replace {}", path.display()))?;
        Ok(())
    }

    pub fn set_database(&mut self, dsn: &str, max_connections: u32) {
        self.database = DatabaseSettings::from_dsn(dsn, max_connections);
    }

    pub fn set_metric_database(&mut self, dsn: &str, max_connections: u32) {
        if dsn.trim().is_empty() {
            self.metric_database = MetricDatabaseSettings::default();
        } else {
            self.metric_database = MetricDatabaseSettings {
                mode: "separate".into(),
                connection: DatabaseSettings::from_dsn(dsn, max_connections),
            };
        }
    }
}

pub fn update_database(dsn: &str, max_connections: u32) -> Result<()> {
    let mut config = ServerConfig::load_or_create()?;
    config.set_database(dsn, max_connections);
    config.save()
}

pub fn update_metric_database(dsn: &str, max_connections: u32) -> Result<()> {
    let mut config = ServerConfig::load_or_create()?;
    config.set_metric_database(dsn, max_connections);
    config.save()
}
