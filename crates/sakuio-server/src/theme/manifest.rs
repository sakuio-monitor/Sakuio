//! Theme manifest support (`sakuio-theme.json` and legacy `komari-theme.json`).
//! 主题清单：支持 Sakuio 自有命名与 Komari 兼容命名。

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Maximum manifest size accepted from an archive.
/// 压缩包中主题清单允许的最大字节数。
pub const MAX_MANIFEST_BYTES: u64 = 1 << 20;

/// Preferred Sakuio manifest name; the legacy Komari name remains supported.
pub const SAKUIO_MANIFEST: &str = "sakuio-theme.json";
pub const KOMARI_MANIFEST: &str = "komari-theme.json";
pub const MANIFEST_NAMES: [&str; 2] = [SAKUIO_MANIFEST, KOMARI_MANIFEST];

pub fn manifest_name(dir: &std::path::Path) -> Option<&'static str> {
    MANIFEST_NAMES
        .into_iter()
        .find(|name| dir.join(name).is_file())
}

/// Raw theme manifest.
/// 原始主题清单。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ThemeManifest {
    #[serde(default)]
    pub name: Value,
    #[serde(default)]
    pub short: String,
    #[serde(default)]
    pub description: Value,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub author: Value,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub preview: String,
    /// Komari managed/raw configuration declaration; preserved verbatim.
    /// Komari 的托管/原始配置声明，原样保留。
    #[serde(default)]
    pub configuration: Value,
}

impl ThemeManifest {
    /// Returns the localized display name.
    /// 返回本地化的显示名称。
    pub fn display_name(&self) -> String {
        localized_text(&self.name)
    }

    pub fn display_description(&self) -> String {
        localized_text(&self.description)
    }

    pub fn display_author(&self) -> String {
        localized_text(&self.author)
    }

    /// Rejects manifests that could escape the theme root or are unusable.
    /// 拒绝可能逃离主题目录或不可用的清单。
    pub fn validate(&self) -> Result<(), String> {
        let short = self.short.trim();
        if short.is_empty() {
            return Err("theme manifest: short is required".into());
        }
        if short.len() > 64
            || !short
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            return Err(
                "theme manifest: short may only contain letters, digits, '_' and '-'".into(),
            );
        }
        if short.eq_ignore_ascii_case("default") {
            return Err("theme manifest: 'default' is reserved".into());
        }
        if self.display_name().trim().is_empty() {
            return Err("theme manifest: name is required".into());
        }
        Ok(())
    }
}

/// Extracts text from a string or a Komari localized-text object.
/// 从字符串或 Komari 多语言对象中提取文本（优先中文/英文）。
pub fn localized_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.trim().to_string(),
        Value::Object(map) => {
            for key in ["zh-CN", "zh_CN", "zh", "en", "ja"] {
                if let Some(Value::String(text)) = map.get(key) {
                    if !text.trim().is_empty() {
                        return text.trim().to_string();
                    }
                }
            }
            map.values()
                .find_map(|item| item.as_str().map(str::trim))
                .filter(|text| !text.is_empty())
                .unwrap_or_default()
                .to_string()
        }
        _ => String::new(),
    }
}
