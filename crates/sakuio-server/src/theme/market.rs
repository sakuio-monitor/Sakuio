//! Komari theme market client (catalog, download, SHA-256 verification).
//! Komari 主题市场客户端（目录、下载、SHA-256 校验）。

use crate::theme::manifest::{localized_text, ThemeManifest};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::net::IpAddr;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

pub const DEFAULT_MARKET_URL: &str =
    "https://raw.githubusercontent.com/komari-monitor/theme-market/main/v1.json";
const CATALOG_MAX_BYTES: u64 = 2 << 20;
const PACKAGE_MAX_BYTES: u64 = 100 << 20;
const CACHE_TTL: Duration = Duration::from_secs(600);

/// One market catalog entry (fields mirror Komari's `v1.json`).
/// 市场目录条目（字段与 Komari `v1.json` 一致）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketTheme {
    #[serde(default)]
    pub name: serde_json::Value,
    pub short: String,
    #[serde(default)]
    pub description: serde_json::Value,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub author: serde_json::Value,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub preview: String,
    #[serde(default)]
    pub download: String,
    #[serde(default)]
    pub sha256: String,
    #[serde(default)]
    pub installable: Option<bool>,
}

impl MarketTheme {
    pub fn display_name(&self) -> String {
        localized_text(&self.name)
    }
    pub fn display_description(&self) -> String {
        localized_text(&self.description)
    }
    pub fn display_author(&self) -> String {
        localized_text(&self.author)
    }
}

#[derive(Debug, Deserialize)]
struct MarketCatalog {
    #[serde(default)]
    themes: Vec<MarketTheme>,
}

struct CachedCatalog {
    fetched_at: Instant,
    themes: Vec<MarketTheme>,
}

/// Streaming download progress (bytes and instantaneous speed).
/// 流式下载进度（字节数与瞬时速度）。
#[derive(Debug, Clone, Copy)]
pub struct DownloadProgress {
    pub downloaded: u64,
    pub total: Option<u64>,
    pub speed_bps: u64,
}

pub struct MarketClient {
    catalog_url: String,
    client: reqwest::Client,
    cache: Mutex<Option<CachedCatalog>>,
}

impl MarketClient {
    pub fn new(catalog_url: impl Into<String>) -> Result<Self, String> {
        let catalog_url = catalog_url.into();
        validate_url(&catalog_url)?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .user_agent(concat!("sakuio/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| format!("build theme market client: {e}"))?;
        Ok(Self {
            catalog_url,
            client,
            cache: Mutex::new(None),
        })
    }

    pub fn catalog_url(&self) -> &str {
        &self.catalog_url
    }

    pub async fn catalog(&self, force: bool) -> Result<Vec<MarketTheme>, String> {
        {
            let cache = self.cache.lock().await;
            if !force {
                if let Some(cached) = cache.as_ref() {
                    if cached.fetched_at.elapsed() < CACHE_TTL {
                        return Ok(cached.themes.clone());
                    }
                }
            }
        }
        let bytes = self.get_bytes(&self.catalog_url, CATALOG_MAX_BYTES).await?;
        let catalog: MarketCatalog = serde_json::from_slice(&bytes)
            .map_err(|e| format!("parse theme market catalog: {e}"))?;
        for theme in &catalog.themes {
            if let Err(error) = validate_url(&theme.download) {
                tracing::warn!(short = %theme.short, %error, "skip market theme with unsafe download URL");
            }
        }
        let mut cache = self.cache.lock().await;
        *cache = Some(CachedCatalog {
            fetched_at: Instant::now(),
            themes: catalog.themes.clone(),
        });
        Ok(catalog.themes)
    }

    pub async fn find(&self, short: &str, force: bool) -> Result<Option<MarketTheme>, String> {
        Ok(self
            .catalog(force)
            .await?
            .into_iter()
            .find(|theme| theme.short == short))
    }

    /// Downloads with streaming progress; SHA-256 is verified before returning.
    /// 带流式进度下载；返回前校验 SHA-256。
    pub async fn download_with_progress<F>(
        &self,
        theme: &MarketTheme,
        on_progress: F,
    ) -> Result<Vec<u8>, String>
    where
        F: FnMut(DownloadProgress) + Send,
    {
        if theme.download.trim().is_empty() {
            return Err("market theme has no download URL".into());
        }
        let bytes = self
            .get_bytes_with_progress(&theme.download, PACKAGE_MAX_BYTES, on_progress)
            .await?;
        if !theme.sha256.trim().is_empty() {
            verify_sha256(&bytes, &theme.sha256).map_err(|error| {
                format!(
                    "{error}; the market metadata may be stale, try refreshing the market or report the theme"
                )
            })?;
        }
        Ok(bytes)
    }

    /// Resolves an update package from a theme's manifest URL. Supports direct
    /// ZIP URLs and GitHub repositories (latest release asset).
    /// 从主题清单的 URL 解析更新包：支持直接 ZIP 与 GitHub 仓库（最新 release）。
    /// Same as [`download_update`] with streaming progress for the final asset.
    /// 与 [`download_update`] 相同，但对最终资源提供流式进度。
    pub async fn download_update_with_progress<F>(
        &self,
        manifest: &ThemeManifest,
        on_progress: F,
    ) -> Result<Vec<u8>, String>
    where
        F: FnMut(DownloadProgress) + Send,
    {
        let source = manifest.url.trim();
        if source.is_empty() {
            return Err("theme has no update source (url is empty)".into());
        }
        let parsed = url::Url::parse(source).map_err(|e| format!("invalid theme url: {e}"))?;
        validate_url(source)?;
        if is_github_repo(&parsed) {
            let segments: Vec<_> = parsed
                .path_segments()
                .map(|segments| segments.filter(|part| !part.is_empty()).collect())
                .unwrap_or_default();
            if segments.len() >= 2 {
                let api = format!(
                    "https://api.github.com/repos/{}/{}/releases/latest",
                    segments[0],
                    segments[1].trim_end_matches(".git")
                );
                let raw = self.get_bytes(&api, CATALOG_MAX_BYTES).await?;
                let release: serde_json::Value = serde_json::from_slice(&raw)
                    .map_err(|e| format!("parse GitHub release: {e}"))?;
                let assets = release
                    .get("assets")
                    .and_then(|value| value.as_array())
                    .ok_or_else(|| "GitHub release has no assets".to_string())?;
                let download = assets
                    .iter()
                    .find(|asset| {
                        asset
                            .get("browser_download_url")
                            .and_then(|value| value.as_str())
                            .is_some_and(|url| url.ends_with(".zip"))
                    })
                    .or_else(|| assets.first())
                    .and_then(|asset| asset.get("browser_download_url"))
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| "GitHub release has no asset download URL".to_string())?;
                return self
                    .get_bytes_with_progress(download, PACKAGE_MAX_BYTES, on_progress)
                    .await;
            }
        }
        if parsed.path().ends_with(".zip") {
            return self
                .get_bytes_with_progress(source, PACKAGE_MAX_BYTES, on_progress)
                .await;
        }
        Err("theme url is neither a ZIP archive nor a GitHub repository".into())
    }

    async fn get_bytes(&self, url: &str, limit: u64) -> Result<Vec<u8>, String> {
        self.get_bytes_with_progress(url, limit, |_| {}).await
    }

    async fn get_bytes_with_progress<F>(
        &self,
        url: &str,
        limit: u64,
        mut on_progress: F,
    ) -> Result<Vec<u8>, String>
    where
        F: FnMut(DownloadProgress) + Send,
    {
        validate_url(url)?;
        let mut response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| format!("download {url}: {e}"))?;
        if !response.status().is_success() {
            return Err(format!("download {url}: HTTP {}", response.status()));
        }
        let total = response.content_length();
        if total.is_some_and(|length| length > limit) {
            return Err(format!("download {url}: response exceeds {limit} bytes"));
        }
        let mut data = Vec::with_capacity(total.unwrap_or(0).min(limit) as usize);
        let mut last_emit = Instant::now();
        let mut last_bytes = 0u64;
        let mut speed_bps = 0u64;
        on_progress(DownloadProgress {
            downloaded: 0,
            total,
            speed_bps,
        });
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| format!("read {url}: {e}"))?
        {
            data.extend_from_slice(&chunk);
            if data.len() as u64 > limit {
                return Err(format!("download {url}: response exceeds {limit} bytes"));
            }
            let now = Instant::now();
            let elapsed = now.duration_since(last_emit);
            if elapsed >= Duration::from_millis(200) {
                speed_bps =
                    ((data.len() as u64 - last_bytes) as f64 / elapsed.as_secs_f64()) as u64;
                last_emit = now;
                last_bytes = data.len() as u64;
                on_progress(DownloadProgress {
                    downloaded: last_bytes,
                    total,
                    speed_bps,
                });
            }
        }
        on_progress(DownloadProgress {
            downloaded: data.len() as u64,
            total,
            speed_bps,
        });
        Ok(data)
    }
}

fn is_github_repo(url: &url::Url) -> bool {
    matches!(url.host_str(), Some("github.com") | Some("www.github.com"))
        && !url.path().ends_with(".zip")
}

fn verify_sha256(bytes: &[u8], expected: &str) -> Result<(), String> {
    let expected = expected.trim().to_ascii_lowercase();
    if expected.len() != 64 || !expected.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err("market entry has an invalid SHA-256 value".into());
    }
    let actual = hex::encode(Sha256::digest(bytes));
    if actual != expected {
        return Err(format!(
            "theme package SHA-256 mismatch (expected {expected}, got {actual})"
        ));
    }
    Ok(())
}

/// Accepts only HTTP(S) URLs without credentials or literal private hosts.
/// 仅允许无凭据、非字面私网地址的 HTTP(S) URL。
pub fn validate_url(raw: &str) -> Result<(), String> {
    let parsed = url::Url::parse(raw).map_err(|e| format!("invalid URL {raw:?}: {e}"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err("only http(s) URLs are allowed".into());
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err("URLs with embedded credentials are not allowed".into());
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| "URL is missing a host".to_string())?;
    let normalized = host.trim_matches(['[', ']']);
    if normalized.eq_ignore_ascii_case("localhost") || normalized.ends_with(".localhost") {
        return Err("loopback hosts are not allowed".into());
    }
    if let Ok(ip) = normalized.parse::<IpAddr>() {
        if is_private_ip(ip) {
            return Err("private/loopback IPs are not allowed".into());
        }
    }
    Ok(())
}

fn is_private_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_unspecified()
                || ip.is_broadcast()
        }
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local() || ip.is_unspecified(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unsafe_market_urls() {
        assert!(validate_url("http://127.0.0.1/a.zip").is_err());
        assert!(validate_url("http://localhost/a.zip").is_err());
        assert!(validate_url("file:///tmp/a.zip").is_err());
        assert!(validate_url("https://user:pass@example.com/a.zip").is_err());
        assert!(validate_url("https://example.com/a.zip").is_ok());
    }

    #[test]
    fn verifies_sha256() {
        let bytes = b"theme";
        let digest = hex::encode(Sha256::digest(bytes));
        assert!(verify_sha256(bytes, &digest).is_ok());
        assert!(verify_sha256(bytes, &"0".repeat(64)).is_err());
    }
}

#[cfg(test)]
mod network_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires network access to GitHub"]
    async fn downloads_github_release_update() {
        let client = MarketClient::new(DEFAULT_MARKET_URL).unwrap();
        let manifest = ThemeManifest {
            url: "https://github.com/stqfdyr/komari-theme-adhesive-note".into(),
            ..ThemeManifest::default()
        };
        let bytes = client
            .download_update_with_progress(&manifest, |_| {})
            .await
            .unwrap();
        assert!(bytes.len() > 1024);
    }
}
