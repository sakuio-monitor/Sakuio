//! Filesystem theme store compatible with Komari's `data/theme/<short>` layout.
//! 与 Komari `data/theme/<short>` 布局兼容的文件系统主题仓库。

use crate::theme::archive::{commit_staging, extract_to_staging, safe_join};
use crate::theme::manifest::{manifest_name, ThemeManifest};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Where an installed theme came from; used by the update flow.
/// 已安装主题的来源信息，供更新流程使用。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ThemeSource {
    #[serde(default)]
    pub market_short: Option<String>,
    #[serde(default)]
    pub download: Option<String>,
    #[serde(default)]
    pub sha256: Option<String>,
    #[serde(default)]
    pub source_url: Option<String>,
    #[serde(default)]
    pub installed_version: Option<String>,
}

/// Installed theme metadata.
/// 已安装主题的元数据。
#[derive(Debug, Clone)]
pub struct InstalledTheme {
    pub manifest: ThemeManifest,
}

pub struct ThemeStore {
    root: PathBuf,
}

impl ThemeStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, String> {
        let root = root.into();
        std::fs::create_dir_all(&root)
            .map_err(|e| format!("create theme directory {}: {e}", root.display()))?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn theme_dir(&self, short: &str) -> Option<PathBuf> {
        if !valid_short(short) {
            return None;
        }
        Some(self.root.join(short))
    }

    pub fn exists(&self, short: &str) -> bool {
        self.theme_dir(short)
            .is_some_and(|dir| manifest_name(&dir).is_some())
    }

    /// Lists installed themes, skipping invalid or half-written directories.
    /// 列出已安装主题，跳过无效或未写完的目录。
    pub fn list(&self) -> Vec<InstalledTheme> {
        let Ok(entries) = std::fs::read_dir(&self.root) else {
            return Vec::new();
        };
        let mut themes = Vec::new();
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with('.') || !entry.path().is_dir() {
                continue;
            }
            match self.load(&name) {
                Ok(theme) => themes.push(theme),
                Err(error) => tracing::warn!(theme = %name, error = %error, "skip invalid theme"),
            }
        }
        themes.sort_by(|a, b| a.manifest.short.cmp(&b.manifest.short));
        themes
    }

    pub fn load(&self, short: &str) -> Result<InstalledTheme, String> {
        let dir = self
            .theme_dir(short)
            .ok_or_else(|| format!("invalid theme name: {short}"))?;
        let name = manifest_name(&dir).ok_or_else(|| {
            format!("theme {short} is missing sakuio-theme.json or komari-theme.json")
        })?;
        let raw = std::fs::read(dir.join(name)).map_err(|e| format!("read {short}/{name}: {e}"))?;
        let manifest: ThemeManifest =
            serde_json::from_slice(&raw).map_err(|e| format!("parse {short}/{name}: {e}"))?;
        manifest.validate()?;
        if manifest.short != short {
            return Err(format!(
                "theme directory {short} does not match manifest short {}",
                manifest.short
            ));
        }
        Ok(InstalledTheme { manifest })
    }

    pub fn source(&self, short: &str) -> ThemeSource {
        let Some(dir) = self.theme_dir(short) else {
            return ThemeSource::default();
        };
        std::fs::read(dir.join(".sakuio-source.json"))
            .ok()
            .and_then(|raw| serde_json::from_slice(&raw).ok())
            .unwrap_or_default()
    }

    pub fn save_source(&self, short: &str, source: &ThemeSource) -> Result<(), String> {
        let dir = self
            .theme_dir(short)
            .ok_or_else(|| format!("invalid theme name: {short}"))?;
        let raw = serde_json::to_vec_pretty(source).map_err(|e| e.to_string())?;
        std::fs::write(dir.join(".sakuio-source.json"), raw)
            .map_err(|e| format!("write theme source metadata: {e}"))
    }

    /// Installs (or replaces) a theme from ZIP bytes.
    /// 从 ZIP 字节安装（或替换）主题。
    pub fn install_bytes(&self, bytes: &[u8]) -> Result<ThemeManifest, String> {
        let (manifest, staging) = extract_to_staging(&self.root, bytes)?;
        if let Err(error) = commit_staging(&self.root, &manifest, &staging) {
            let _ = std::fs::remove_dir_all(&staging);
            return Err(error);
        }
        Ok(manifest)
    }

    pub fn delete(&self, short: &str) -> Result<(), String> {
        let dir = self
            .theme_dir(short)
            .ok_or_else(|| "invalid theme name".to_string())?;
        if !dir.is_dir() {
            return Err("theme not found".into());
        }
        std::fs::remove_dir_all(&dir).map_err(|e| format!("delete theme: {e}"))
    }

    /// Reads a file relative to the theme root (used by `/themes/:id/*`).
    /// 读取主题根目录下的文件（供 `/themes/:id/*` 使用）。
    pub fn read_root_file(&self, short: &str, relative: &str) -> Option<Vec<u8>> {
        self.read_file(short, relative)
    }

    /// Reads a file relative to `dist/` (used by the active theme's SPA assets).
    /// 读取 `dist/` 下的文件（供当前主题的 SPA 资源使用）。
    pub fn read_dist_file(&self, short: &str, relative: &str) -> Option<Vec<u8>> {
        let relative = relative.trim_start_matches('/');
        self.read_file(short, &format!("dist/{relative}"))
    }

    pub fn read_file(&self, short: &str, relative: &str) -> Option<Vec<u8>> {
        let dir = self.theme_dir(short)?;
        let relative = relative.trim_start_matches('/');
        let candidate = safe_join(&dir, relative)?;
        let base = std::fs::canonicalize(&dir).ok()?;
        let real = std::fs::canonicalize(&candidate).ok()?;
        if !real.starts_with(&base) || !real.is_file() {
            return None;
        }
        std::fs::read(real).ok()
    }
}

pub fn valid_short(short: &str) -> bool {
    !short.is_empty()
        && short.len() <= 64
        && short != "default"
        && !short.starts_with('.')
        && short
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}
