//! Latest published agent release within the board compatibility range.
//! 查询 GitHub 上已发布且仍被当前面板支持的 Agent 版本。

use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const REPO: &str = "sakuio-monitor/Sakuio-agent";
const CACHE_TTL: Duration = Duration::from_secs(300);

static CACHE: OnceLock<Mutex<Option<(String, Option<String>, Instant)>>> = OnceLock::new();

fn cache() -> &'static Mutex<Option<(String, Option<String>, Instant)>> {
    CACHE.get_or_init(|| Mutex::new(None))
}

fn parse(value: &str) -> Option<(u64, u64, u64)> {
    let core = value.trim().trim_start_matches(['v', 'V']);
    let core = core.split(['-', '+']).next()?;
    let mut parts = core.split('.');
    Some((
        parts.next()?.parse().ok()?,
        parts.next().unwrap_or("0").parse().ok()?,
        parts.next().unwrap_or("0").parse().ok()?,
    ))
}

/// Highest non-prerelease GitHub release that does not exceed `max_version`.
/// 返回不高于面板兼容区间上界的最高正式发布版本；没有符合条件的版本时返回 None。
pub async fn latest_supported(max_version: &str) -> Result<Option<String>, String> {
    if let Some((cached_max, version, at)) = cache().lock().ok().and_then(|guard| guard.clone()) {
        if cached_max == max_version && at.elapsed() < CACHE_TTL {
            return Ok(version);
        }
    }
    let Some(max) = parse(max_version) else {
        return Err(format!("面板兼容版本上界格式错误：{max_version}"));
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()
        .map_err(|error| format!("创建 GitHub 客户端失败：{error}"))?;
    let response = client
        .get(format!(
            "https://api.github.com/repos/{REPO}/releases?per_page=100"
        ))
        .header("Accept", "application/vnd.github+json")
        .header("User-Agent", "sakuio-server")
        .send()
        .await
        .map_err(|error| format!("查询 Agent Release 失败：{error}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "查询 Agent Release 失败：HTTP {}",
            response.status()
        ));
    }
    let releases: Vec<serde_json::Value> = response
        .json()
        .await
        .map_err(|error| format!("解析 Agent Release 失败：{error}"))?;
    let mut best: Option<((u64, u64, u64), String)> = None;
    for release in releases {
        if release.get("draft").and_then(serde_json::Value::as_bool) == Some(true)
            || release
                .get("prerelease")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
        {
            continue;
        }
        let Some(tag) = release.get("tag_name").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let Some(version) = parse(tag).filter(|version| *version <= max) else {
            continue;
        };
        if best.as_ref().is_none_or(|(current, _)| version > *current) {
            best = Some((
                version,
                tag.trim().trim_start_matches(['v', 'V']).to_string(),
            ));
        }
    }
    let version = best.map(|(_, tag)| tag);
    if let Ok(mut guard) = cache().lock() {
        *guard = Some((max_version.to_string(), version.clone(), Instant::now()));
    }
    Ok(version)
}
