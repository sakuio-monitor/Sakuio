//! Hot-swappable Komari-compatible theme runtime: install, apply, delete,
//! update and static serving without restarting the process.
//! 可热切换的 Komari 兼容主题运行时：安装、应用、删除、更新与静态服务，
//! 全程无需重启进程。

mod archive;
mod configuration;
mod jobs;
mod manifest;
mod market;
mod store;

pub use jobs::{ThemeJob, ThemeJobEvent, ThemeJobs};
pub use manifest::ThemeManifest;
pub use market::{MarketTheme, DEFAULT_MARKET_URL};
pub use store::{InstalledTheme, ThemeSource, ThemeStore};

use crate::theme::store::valid_short;
use sakuio_db::{DbPool, SettingsRepository};
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};

const SETTINGS_KEY: &str = "theme";
const DEFAULT_SHORT: &str = "default";
const MAX_ARCHIVE_BYTES: usize = 128 << 20;

const DEFAULT_INDEX: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Komari Monitor</title>
<meta name="description" content="A simple server monitor tool.">
</head>
<body>
<main style="max-width:720px;margin:12vh auto;padding:0 24px;font-family:system-ui,sans-serif">
<h1>Sakuio</h1>
<p>A simple server monitor tool.</p>
<p>No theme is installed yet. Open <a href="/admin">/admin</a> → Themes to install one from the Komari market or upload a theme ZIP.</p>
</main>
</body>
</html>
"#;

/// One row in the admin theme list.
/// 管理端主题列表的一行。
#[derive(Debug, Clone, Serialize)]
pub struct ThemeSummary {
    pub short: String,
    pub name: String,
    pub description: String,
    pub version: String,
    pub author: String,
    pub url: String,
    pub preview: String,
    pub preview_url: Option<String>,
    pub active: bool,
    pub installed: bool,
    pub installed_version: Option<String>,
    pub source: Option<String>,
    pub configuration_type: String,
    pub config_items: usize,
    pub configuration_name: Value,
    pub configuration_icon: String,
}

/// A resolved static file.
/// 解析后的静态文件。
pub struct ThemeFile {
    pub bytes: Vec<u8>,
    pub mime: String,
}

/// Strings injected into `dist/index.html`, mirroring Komari's replacements.
/// 注入 `dist/index.html` 的站点字段，与 Komari 的替换行为一致。
#[derive(Debug, Clone, Default)]
pub struct SiteInject {
    pub title: String,
    pub description: String,
    pub custom_head: String,
    pub custom_body: String,
}

fn is_github_url(url: &str) -> bool {
    url::Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(str::to_ascii_lowercase))
        .is_some_and(|host| {
            host == "github.com"
                || host == "raw.githubusercontent.com"
                || host.ends_with(".githubusercontent.com")
        })
}

pub struct ThemeService {
    store: ThemeStore,
    market: market::MarketClient,
    active: RwLock<String>,
    pool: DbPool,
    jobs: ThemeJobs,
    download_slots: Arc<Semaphore>,
}

impl ThemeService {
    pub async fn new(pool: DbPool, root: impl Into<PathBuf>) -> Result<Self, String> {
        let store = ThemeStore::new(root)?;
        let settings = SettingsRepository::new(&pool);
        let stored: Option<String> = settings.get_json(SETTINGS_KEY).await.ok().flatten();
        let active = stored
            .filter(|short| short == DEFAULT_SHORT || store.exists(short))
            .unwrap_or_else(|| DEFAULT_SHORT.to_string());
        if active != DEFAULT_SHORT && !store.exists(&active) {
            tracing::warn!(theme = %active, "configured theme is missing; falling back to default");
        }
        let market = market::MarketClient::new(DEFAULT_MARKET_URL)?;
        Ok(Self {
            store,
            market,
            active: RwLock::new(active),
            pool,
            jobs: ThemeJobs::default(),
            // Bound concurrent downloads so several large themes cannot
            // exhaust memory/bandwidth at once; extra jobs wait in `queued`.
            // 限制并发下载数量，避免多个大主题同时耗尽内存/带宽。
            download_slots: Arc::new(Semaphore::new(3)),
        })
    }

    pub async fn active(&self) -> String {
        self.active.read().await.clone()
    }

    pub fn market_url(&self) -> &str {
        self.market.catalog_url()
    }

    /// Fetch a catalog from an optional alternate source. The default source
    /// remains the built-in Komari market; custom sources are validated here.
    pub async fn market_list_from(
        &self,
        source: Option<String>,
        force: bool,
    ) -> Result<Vec<MarketTheme>, String> {
        match source
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
        {
            Some(url) => market::MarketClient::new(url)?.catalog(force).await,
            None => self.market.catalog(force).await,
        }
    }

    pub fn jobs(&self) -> &ThemeJobs {
        &self.jobs
    }

    /// Lists installed themes. The internal `default` fallback is not exposed
    /// as a selectable theme, so the built-in placeholder never appears next to
    /// an installed theme.
    pub async fn list(&self) -> Vec<ThemeSummary> {
        let active = self.active().await;
        let mut rows = Vec::new();
        for theme in self.store.list() {
            rows.push(self.summary(&theme, &active));
        }
        rows
    }

    fn summary(&self, theme: &InstalledTheme, active: &str) -> ThemeSummary {
        let source = self.store.source(&theme.manifest.short);
        let preview = theme.manifest.preview.trim().to_string();
        let preview_url = if preview.is_empty() {
            None
        } else if preview.starts_with("http://") || preview.starts_with("https://") {
            Some(preview.clone())
        } else if self.read_preview(&theme.manifest.short).is_some() {
            Some(format!(
                "/api/admin/themes/{}/preview?v={}",
                theme.manifest.short, theme.manifest.version
            ))
        } else {
            None
        };
        ThemeSummary {
            short: theme.manifest.short.clone(),
            name: theme.manifest.display_name(),
            description: theme.manifest.display_description(),
            version: theme.manifest.version.clone(),
            author: theme.manifest.display_author(),
            url: theme.manifest.url.clone(),
            preview,
            preview_url,
            active: theme.manifest.short == active,
            installed: true,
            installed_version: Some(theme.manifest.version.clone()),
            source: source.market_short,
            configuration_type: configuration::configuration_type(&theme.manifest.configuration),
            configuration_name: theme
                .manifest
                .configuration
                .get("name")
                .cloned()
                .unwrap_or(Value::Null),
            configuration_icon: theme
                .manifest
                .configuration
                .get("icon")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            config_items: configuration::managed_items(&theme.manifest.configuration)
                .map(|items| {
                    items
                        .iter()
                        .filter(|item| {
                            !item.key.is_empty() && item.kind != "title" && item.kind != "textbox"
                        })
                        .count()
                })
                .unwrap_or(0),
        }
    }

    /// Applies an installed theme (or the built-in default) immediately.
    /// 立即应用已安装主题（或内置默认主题）。
    pub async fn apply(&self, short: &str) -> Result<(), String> {
        if short == DEFAULT_SHORT {
            return self.persist_active(DEFAULT_SHORT).await;
        }
        self.store.load(short)?;
        self.persist_active(short).await
    }

    async fn persist_active(&self, short: &str) -> Result<(), String> {
        let settings = SettingsRepository::new(&self.pool);
        settings
            .set_json(SETTINGS_KEY, &short)
            .await
            .map_err(|e| format!("save active theme: {e}"))?;
        *self.active.write().await = short.to_string();
        tracing::info!(theme = short, "active theme changed");
        Ok(())
    }

    pub async fn delete(&self, short: &str) -> Result<(), String> {
        if short == DEFAULT_SHORT {
            return Err("the built-in default theme cannot be deleted".into());
        }
        if !valid_short(short) {
            return Err("invalid theme name".into());
        }
        if self.active().await == short {
            return Err("cannot delete the active theme; apply another theme first".into());
        }
        tokio::task::spawn_blocking({
            let root = self.store.root().to_path_buf();
            let short = short.to_string();
            move || ThemeStore::new(root)?.delete(&short)
        })
        .await
        .map_err(|e| format!("theme delete task: {e}"))??;
        if let Err(error) = SettingsRepository::new(&self.pool)
            .delete(&Self::config_key(short))
            .await
        {
            tracing::warn!(theme = short, error = %error, "delete theme configuration failed");
        }
        Ok(())
    }

    /// Installs an uploaded ZIP; the active theme is unaffected.
    /// 安装上传的 ZIP；当前主题保持不变。
    pub async fn install_upload(&self, bytes: Vec<u8>) -> Result<ThemeSummary, String> {
        if bytes.len() > MAX_ARCHIVE_BYTES {
            return Err(format!(
                "theme archive is too large (max {MAX_ARCHIVE_BYTES} bytes)"
            ));
        }
        let root = self.store.root().to_path_buf();
        let manifest =
            tokio::task::spawn_blocking(move || ThemeStore::new(root)?.install_bytes(&bytes))
                .await
                .map_err(|e| format!("theme install task: {e}"))??;
        let theme = self.store.load(&manifest.short)?;
        Ok(self.summary(&theme, &self.active().await))
    }

    pub async fn market_list(&self, force: bool) -> Result<Vec<MarketTheme>, String> {
        self.market.catalog(force).await
    }

    /// Starts an asynchronous market install; returns immediately with a job
    /// whose progress is streamed over SSE.
    /// 启动异步市场安装并立即返回任务句柄，进度通过 SSE 推送。
    pub async fn start_market_install(
        self: &Arc<Self>,
        short: String,
        source: Option<String>,
        proxy: Option<String>,
        force_catalog: bool,
    ) -> ThemeJob {
        let job = self.jobs.create_or_reuse(&short, "install").await;
        let service = self.clone();
        let task = job.clone();
        tokio::spawn(async move {
            let market = match source
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
            {
                Some(url) => match market::MarketClient::new(url) {
                    Ok(client) => client,
                    Err(error) => {
                        task.fail(error);
                        return;
                    }
                },
                None => {
                    // The default market client is owned by the service; use
                    // the service path when no custom source was supplied.
                    if let Err(error) = service
                        .run_market_install(&task, &short, force_catalog, proxy.clone())
                        .await
                    {
                        tracing::warn!(theme = %short, error = %error, "theme market install failed");
                        task.fail(error);
                    }
                    return;
                }
            };
            if let Err(error) = service
                .run_market_install_with(&task, &short, force_catalog, &market, proxy.clone())
                .await
            {
                tracing::warn!(theme = %short, error = %error, "theme market install failed");
                task.fail(error);
            }
        });
        job
    }

    async fn run_market_install(
        &self,
        job: &ThemeJob,
        short: &str,
        force_catalog: bool,
        proxy: Option<String>,
    ) -> Result<(), String> {
        self.run_market_install_with(job, short, force_catalog, &self.market, proxy)
            .await
    }

    async fn run_market_install_with(
        &self,
        job: &ThemeJob,
        short: &str,
        force_catalog: bool,
        market: &market::MarketClient,
        proxy: Option<String>,
    ) -> Result<(), String> {
        job.set_phase("resolving");
        let mut entry = market
            .find(short, force_catalog)
            .await?
            .ok_or_else(|| format!("theme {short} not found in market"))?;
        if let Some(proxy) = proxy
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if is_github_url(&entry.download) {
                entry.download = format!("{}/{}", proxy.trim_end_matches('/'), entry.download);
            }
        }
        if entry.installable == Some(false) {
            return Err(format!("theme {short} is not installable"));
        }
        job.set_phase("queued");
        let _slot = self
            .download_slots
            .acquire()
            .await
            .map_err(|e| format!("download queue closed: {e}"))?;
        let bytes = self.download_market(job, &entry, market).await?;
        job.set_phase("installing");
        let manifest = self.install_bytes_blocking(bytes).await?;
        self.store.save_source(
            &manifest.short,
            &ThemeSource {
                market_short: Some(entry.short.clone()),
                download: Some(entry.download.clone()),
                sha256: Some(entry.sha256.clone()),
                source_url: Some(entry.url.clone()),
                installed_version: Some(manifest.version.clone()),
            },
        )?;
        job.finish(None);
        Ok(())
    }

    /// Starts an asynchronous update (market version or manifest URL).
    /// 启动异步更新（市场版本或清单 URL）。
    pub async fn start_update(
        self: &Arc<Self>,
        short: String,
        force_catalog: bool,
        proxy: Option<String>,
    ) -> ThemeJob {
        let job = self.jobs.create_or_reuse(&short, "update").await;
        let service = self.clone();
        let task = job.clone();
        tokio::spawn(async move {
            if let Err(error) = service
                .run_update(&task, &short, force_catalog, proxy)
                .await
            {
                tracing::warn!(theme = %short, error = %error, "theme update failed");
                task.fail(error);
            }
        });
        job
    }

    async fn run_update(
        &self,
        job: &ThemeJob,
        short: &str,
        force_catalog: bool,
        proxy: Option<String>,
    ) -> Result<(), String> {
        let theme = self.store.load(short)?;
        let source = self.store.source(short);
        if let Some(market_short) = source.market_short.as_deref() {
            job.set_phase("resolving");
            let mut entry = self
                .market
                .find(market_short, force_catalog)
                .await?
                .ok_or_else(|| format!("theme {market_short} is no longer in the market"))?;
            if let Some(proxy) = proxy
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                if is_github_url(&entry.download) {
                    entry.download = format!("{}/{}", proxy.trim_end_matches('/'), entry.download);
                }
            }
            if !entry.version.trim().is_empty()
                && !version_is_newer(&entry.version, &theme.manifest.version)
            {
                job.finish(Some(false));
                return Ok(());
            }
            job.set_phase("queued");
            let _slot = self
                .download_slots
                .acquire()
                .await
                .map_err(|e| format!("download queue closed: {e}"))?;
            let bytes = self.download_market(job, &entry, &self.market).await?;
            job.set_phase("installing");
            let manifest = self.install_bytes_blocking(bytes).await?;
            self.store.save_source(
                &manifest.short,
                &ThemeSource {
                    market_short: Some(entry.short.clone()),
                    download: Some(entry.download.clone()),
                    sha256: Some(entry.sha256.clone()),
                    source_url: Some(entry.url.clone()),
                    installed_version: Some(manifest.version.clone()),
                },
            )?;
            job.finish(Some(true));
            return Ok(());
        }

        job.set_phase("queued");
        let _slot = self
            .download_slots
            .acquire()
            .await
            .map_err(|e| format!("download queue closed: {e}"))?;
        let bytes = self.download_update(job, &theme.manifest).await?;
        job.set_phase("installing");
        self.install_bytes_blocking(bytes).await?;
        job.finish(Some(true));
        Ok(())
    }

    async fn download_market(
        &self,
        job: &ThemeJob,
        entry: &MarketTheme,
        market: &market::MarketClient,
    ) -> Result<Vec<u8>, String> {
        let mut attempt = 0;
        loop {
            job.set_phase("downloading");
            let progress_target = job.clone();
            let result = market
                .download_with_progress(entry, move |progress| {
                    progress_target.set_progress(
                        progress.downloaded,
                        progress.total,
                        progress.speed_bps,
                    );
                })
                .await;
            match result {
                Ok(bytes) => return Ok(bytes),
                Err(error) if attempt == 0 && is_retryable_download_error(&error) => {
                    attempt += 1;
                    job.set_phase("retrying");
                    tracing::warn!(theme = %entry.short, error = %error, "theme download failed; retrying once");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn download_update(
        &self,
        job: &ThemeJob,
        manifest: &ThemeManifest,
    ) -> Result<Vec<u8>, String> {
        let mut attempt = 0;
        loop {
            job.set_phase("downloading");
            let progress_target = job.clone();
            let result = self
                .market
                .download_update_with_progress(manifest, move |progress| {
                    progress_target.set_progress(
                        progress.downloaded,
                        progress.total,
                        progress.speed_bps,
                    );
                })
                .await;
            match result {
                Ok(bytes) => return Ok(bytes),
                Err(error) if attempt == 0 && is_retryable_download_error(&error) => {
                    attempt += 1;
                    job.set_phase("retrying");
                    tracing::warn!(error = %error, "theme update download failed; retrying once");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn install_bytes_blocking(&self, bytes: Vec<u8>) -> Result<ThemeManifest, String> {
        let root = self.store.root().to_path_buf();
        tokio::task::spawn_blocking(move || ThemeStore::new(root)?.install_bytes(&bytes))
            .await
            .map_err(|e| format!("theme install task: {e}"))?
    }

    /// Reads a file from the active theme's `dist/`. Returns `None` for missing
    /// assets; SPA routes fall back to `index.html`.
    /// 读取当前主题 `dist/` 下的文件；SPA 路由回退到 `index.html`。
    pub async fn read_active(&self, relative: &str) -> Option<ThemeFile> {
        let active = self.active().await;
        if active == DEFAULT_SHORT {
            let path = relative.trim_start_matches('/');
            if path.is_empty() || !has_extension(path) {
                return Some(default_index_file());
            }
            return None;
        }
        let path = relative.trim_start_matches('/');
        if path.is_empty() {
            return self.read_index(&active).await;
        }
        if let Some(bytes) = self.store.read_dist_file(&active, path) {
            return Some(ThemeFile {
                bytes,
                mime: mime_for(path),
            });
        }
        if has_extension(path) {
            return None;
        }
        self.read_index(&active).await
    }

    /// Reads the admin SPA bundled with the active Komari theme.
    /// Komari themes ship `dist/admin-app/`; serve it below `/admin`.
    pub async fn read_admin_asset(&self, relative: &str) -> Option<ThemeFile> {
        let active = self.active().await;
        if active == DEFAULT_SHORT {
            return None;
        }
        let relative = relative.trim_start_matches('/');
        if relative.split('/').any(|segment| segment.starts_with('.')) {
            return None;
        }
        let path = if relative.is_empty() {
            "dist/admin-app/index.html".to_string()
        } else {
            format!("dist/admin-app/{relative}")
        };
        let bytes = self.store.read_file(&active, &path)?;
        Some(ThemeFile {
            bytes,
            mime: mime_for(&path),
        })
    }

    pub async fn read_theme_asset(&self, short: &str, relative: &str) -> Option<ThemeFile> {
        if short != DEFAULT_SHORT && !valid_short(short) {
            return None;
        }
        if short == DEFAULT_SHORT {
            return None;
        }
        let path = relative.trim_start_matches('/');
        if path.split('/').any(|segment| segment.starts_with('.')) {
            return None;
        }
        let bytes = self.store.read_root_file(short, path)?;
        Some(ThemeFile {
            bytes,
            mime: mime_for(path),
        })
    }

    async fn read_index(&self, short: &str) -> Option<ThemeFile> {
        let bytes = self.store.read_dist_file(short, "index.html")?;
        Some(ThemeFile {
            bytes,
            mime: "text/html; charset=utf-8".into(),
        })
    }

    /// Reads an installed theme's preview image for the admin gallery.
    /// 读取已安装主题的预览图供管理端展示。
    pub fn read_preview(&self, short: &str) -> Option<ThemeFile> {
        if short == DEFAULT_SHORT {
            return None;
        }
        let theme = self.store.load(short).ok()?;
        let preview = theme.manifest.preview.trim().trim_start_matches('/');
        if preview.is_empty() || preview.starts_with("http://") || preview.starts_with("https://") {
            return None;
        }
        let bytes = self
            .store
            .read_root_file(short, preview)
            .or_else(|| self.store.read_dist_file(short, preview))?;
        Some(ThemeFile {
            bytes,
            mime: mime_for(preview),
        })
    }

    fn config_key(short: &str) -> String {
        format!("theme_config:{short}")
    }

    /// Returns the declared configuration and effective values for a theme.
    /// 返回主题声明的配置项与当前生效值。
    pub async fn config_view(&self, short: &str) -> Result<Value, String> {
        if short == DEFAULT_SHORT {
            return Ok(json!({
                "type": configuration::MANAGED,
                "items": [],
                "values": {},
                "name": Value::Null,
            }));
        }
        let theme = self.store.load(short)?;
        let declaration = theme.manifest.configuration.clone();
        let kind = configuration::configuration_type(&declaration);
        let items = configuration::managed_items(&declaration)?;
        let saved: Option<Value> = SettingsRepository::new(&self.pool)
            .get_json(&Self::config_key(short))
            .await
            .ok()
            .flatten();
        let saved = saved
            .and_then(|value| match value {
                Value::Object(map) => Some(map),
                _ => None,
            })
            .unwrap_or_default();
        let values = configuration::merged_values(&items, &saved);
        Ok(json!({
            "type": kind,
            "items": items,
            "values": values,
            "name": declaration.get("name").cloned().unwrap_or(Value::Null),
        }))
    }

    /// Validates and persists managed values; the active theme sees them on
    /// the next public settings request without a restart.
    /// 校验并保存托管配置；当前主题在下一次公开设置请求时立即生效，无需重启。
    pub async fn save_config(&self, short: &str, values: Value) -> Result<Value, String> {
        if short == DEFAULT_SHORT {
            return Err("the built-in default theme has no managed configuration".into());
        }
        let theme = self.store.load(short)?;
        let declaration = theme.manifest.configuration;
        let declaration_name = declaration.get("name").cloned().unwrap_or(Value::Null);
        if configuration::configuration_type(&declaration) != configuration::MANAGED {
            return Err("this theme does not use managed configuration".into());
        }
        let items = configuration::managed_items(&declaration)?;
        let Value::Object(input) = values else {
            return Err("configuration values must be a JSON object".into());
        };
        let normalized = configuration::normalize_values(&items, &input)?;
        SettingsRepository::new(&self.pool)
            .set_json(&Self::config_key(short), &normalized)
            .await
            .map_err(|e| format!("save theme configuration: {e}"))?;
        let display = configuration::merged_values(&items, &normalized);
        tracing::info!(theme = short, "theme configuration saved");
        Ok(json!({
            "type": configuration::MANAGED,
            "name": declaration_name,
            "items": items,
            "values": display,
        }))
    }

    /// Returns `theme_settings` for the public RPC response.
    /// 返回公开 RPC 响应中的 `theme_settings`。
    pub async fn theme_settings(&self, short: &str) -> Value {
        let mut output = Map::new();
        output.insert("dataUpdateInterval".into(), Value::from(3));
        output.insert("rpcTransportMode".into(), Value::String("websocket".into()));
        if short == DEFAULT_SHORT {
            return Value::Object(output);
        }
        let Ok(theme) = self.store.load(short) else {
            return Value::Object(output);
        };
        let declaration = theme.manifest.configuration;
        let Ok(items) = configuration::managed_items(&declaration) else {
            return Value::Object(output);
        };
        let saved: Option<Value> = SettingsRepository::new(&self.pool)
            .get_json(&Self::config_key(short))
            .await
            .ok()
            .flatten();
        let saved = saved
            .and_then(|value| match value {
                Value::Object(map) => Some(map),
                _ => None,
            })
            .unwrap_or_default();
        for (key, value) in configuration::merged_values(&items, &saved) {
            output.insert(key, value);
        }
        Value::Object(output)
    }

    pub async fn has_theme(&self, short: &str) -> bool {
        short == DEFAULT_SHORT || self.store.exists(short)
    }
}

/// Applies Komari-compatible title/description/head/body replacements.
/// 按 Komari 的规则替换标题、描述、头部与尾部注入。
pub fn inject_index(html: &str, inject: &SiteInject) -> String {
    let mut output = html.to_string();
    let title = if inject.title.trim().is_empty() {
        "Sakuio"
    } else {
        inject.title.trim()
    };
    output = output.replace(
        "<title>Komari Monitor</title>",
        &format!("<title>{title}</title>"),
    );
    output = output.replace(
        "A simple server monitor tool.",
        if inject.description.trim().is_empty() {
            "Sakuio server monitoring"
        } else {
            inject.description.trim()
        },
    );
    if !inject.custom_head.trim().is_empty() {
        output = output.replace("</head>", &format!("{}</head>", inject.custom_head));
    }
    if !inject.custom_body.trim().is_empty() {
        output = output.replace("</body>", &format!("{}</body>", inject.custom_body));
    }
    output
}

fn default_index_file() -> ThemeFile {
    ThemeFile {
        bytes: DEFAULT_INDEX.as_bytes().to_vec(),
        mime: "text/html; charset=utf-8".into(),
    }
}

/// Network-level errors worth one clean retry; integrity failures are not.
/// 值得重试一次的网络错误；完整性校验失败不重试。
fn is_retryable_download_error(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    !error.contains("sha-256")
        && (error.contains("timed out")
            || error.contains("connection")
            || error.contains("connect")
            || error.contains("error sending request")
            || error.contains("reset")
            || error.contains("broken pipe")
            || error.contains("http 429")
            || error.contains("http 5"))
}

/// Compares dotted numeric versions; non-numeric segments compare as strings.
/// 比较点分数字版本；非数字段按字符串比较。
fn version_is_newer(remote: &str, installed: &str) -> bool {
    let parse = |value: &str| -> Vec<String> {
        value
            .trim()
            .trim_start_matches('v')
            .split(['.', '-', '+'])
            .map(|part| part.to_string())
            .collect()
    };
    let remote = parse(remote);
    let installed = parse(installed);
    for (left, right) in remote.iter().zip(installed.iter()) {
        match (left.parse::<u64>(), right.parse::<u64>()) {
            (Ok(left), Ok(right)) => {
                if left != right {
                    return left > right;
                }
            }
            _ => {
                if left != right {
                    return left > right;
                }
            }
        }
    }
    remote.len() > installed.len()
}

fn has_extension(path: &str) -> bool {
    Path::new(path)
        .extension()
        .is_some_and(|extension| !extension.is_empty())
}

fn mime_for(path: &str) -> String {
    mime_guess::from_path(path)
        .first_or_octet_stream()
        .essence_str()
        .to_string()
        + if mime_guess::from_path(path)
            .first()
            .is_some_and(|mime| mime.type_() == "text" || mime.subtype() == "javascript")
        {
            "; charset=utf-8"
        } else {
            ""
        }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn demo_zip(short: &str) -> Vec<u8> {
        let mut buffer = std::io::Cursor::new(Vec::new());
        {
            let mut writer = zip::ZipWriter::new(&mut buffer);
            let options = zip::write::SimpleFileOptions::default();
            writer.start_file("komari-theme.json", options).unwrap();
            writer
                .write_all(
                    format!(r#"{{"name":"Demo","short":"{short}","version":"1.0.0"}}"#).as_bytes(),
                )
                .unwrap();
            writer.start_file("dist/index.html", options).unwrap();
            writer
                .write_all(
                    b"<html><head><title>Komari Monitor</title></head><body>demo</body></html>",
                )
                .unwrap();
            writer.finish().unwrap();
        }
        buffer.into_inner()
    }

    #[tokio::test]
    async fn install_apply_hot_serve_and_delete() {
        let dir = std::env::temp_dir().join(format!("sakuio-theme-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("app.db");
        let app = sakuio_db::AppDatabase::connect(
            &format!("sqlite://{}?mode=rwc", db_path.display()),
            true,
        )
        .await
        .expect("app db");
        let service = ThemeService::new(app.pool().clone(), dir.join("themes"))
            .await
            .expect("theme service");
        assert_eq!(service.active().await, "default");

        let summary = service
            .install_upload(demo_zip("demo"))
            .await
            .expect("install");
        assert_eq!(summary.short, "demo");
        assert_eq!(service.active().await, "default");

        service.apply("demo").await.expect("apply");
        assert_eq!(service.active().await, "demo");
        let page = service.read_active("").await.expect("active index");
        assert!(String::from_utf8_lossy(&page.bytes).contains("<title>Komari Monitor</title>"));
        let asset = service
            .read_active("index.html")
            .await
            .expect("explicit index");
        assert!(asset.mime.starts_with("text/html"));

        service.apply("default").await.expect("apply default");
        service.delete("demo").await.expect("delete");
        assert!(!service.has_theme("demo").await);

        drop(service);
        app.shutdown().await;
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn injects_site_text() {
        let html =
            "<head><title>Komari Monitor</title></head><body>A simple server monitor tool.</body>";
        let output = inject_index(
            html,
            &SiteInject {
                title: "My Panel".into(),
                description: "status".into(),
                custom_head: "<meta name=x>".into(),
                custom_body: "<script></script>".into(),
            },
        );
        assert!(output.contains("<title>My Panel</title>"));
        assert!(output.contains("status"));
        assert!(output.contains("<meta name=x></head>"));
        assert!(output.contains("<script></script></body>"));
    }

    #[test]
    fn mime_detection() {
        assert!(mime_for("assets/app.js").contains("javascript"));
        assert!(mime_for("assets/a.css").contains("text/css"));
        assert_eq!(mime_for("favicon.ico"), "image/x-icon");
    }
}
