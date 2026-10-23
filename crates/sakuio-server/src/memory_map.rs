//! Process RSS decomposition from OS memory maps.
//! 通过系统内存映射拆分进程 RSS，避免出现“未归类”大块。

#[derive(Debug, Clone, Default)]
pub struct MemoryMapStats {
    pub heap: u64,
    pub stacks: u64,
    pub code: u64,
    pub anonymous: u64,
    pub files: u64,
    pub database: u64,
    pub kernel: u64,
}

impl MemoryMapStats {
    pub fn total(&self) -> u64 {
        self.heap
            .saturating_add(self.stacks)
            .saturating_add(self.code)
            .saturating_add(self.anonymous)
            .saturating_add(self.files)
            .saturating_add(self.database)
            .saturating_add(self.kernel)
    }
}

pub fn read() -> Option<MemoryMapStats> {
    #[cfg(target_os = "linux")]
    {
        linux_smaps()
    }
    #[cfg(target_os = "macos")]
    {
        macos_vmmap()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

#[cfg(target_os = "linux")]
fn linux_smaps() -> Option<MemoryMapStats> {
    let text = std::fs::read_to_string("/proc/self/smaps").ok()?;
    let mut stats = MemoryMapStats::default();
    let mut current_path: Option<String> = None;
    for line in text.lines() {
        if !line.starts_with(' ') && !line.starts_with("Rss:") {
            current_path = Some(
                line.split_whitespace()
                    .skip(5)
                    .collect::<Vec<_>>()
                    .join(" "),
            );
        } else if let Some(value) = line.strip_prefix("Rss:") {
            let kb = value
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse::<u64>()
                .unwrap_or(0);
            let bytes = kb.saturating_mul(1024);
            let path = current_path.as_deref().unwrap_or("");
            if path == "[heap]" {
                stats.heap += bytes;
            } else if path.starts_with("[stack") {
                stats.stacks += bytes;
            } else if path.ends_with(".db") || path.ends_with("-wal") || path.ends_with("-shm") {
                stats.database += bytes;
            } else if path.contains(".so") || path.contains(".dylib") || path.starts_with('/') {
                stats.code += bytes;
            } else if path.is_empty() || path.starts_with("[anon") {
                stats.anonymous += bytes;
            } else {
                stats.files += bytes;
            }
        }
    }
    Some(stats)
}

#[cfg(target_os = "macos")]
fn macos_vmmap() -> Option<MemoryMapStats> {
    let output = std::process::Command::new("/usr/bin/vmmap")
        .args(["-summary", &std::process::id().to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut stats = MemoryMapStats::default();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('=') || trimmed.starts_with("REGION") {
            continue;
        }
        let tokens: Vec<&str> = trimmed.split_whitespace().collect();
        let Some(first_size) = tokens.iter().position(|token| is_size(token)) else {
            continue;
        };
        if first_size < 2 || tokens.len() <= first_size + 1 || !is_size(tokens[first_size + 1]) {
            continue;
        }
        let name = tokens[..first_size].join(" ");
        if name == "TOTAL" || name.starts_with("MALLOC ZONE") {
            continue;
        }
        let resident = parse_size(tokens[first_size + 1]);
        if name.starts_with("MALLOC") {
            stats.heap += resident;
        } else if name.starts_with("Stack") {
            stats.stacks += resident;
        } else if name.starts_with("__") || name.starts_with("MALLOC") {
            stats.code += resident;
        } else if name.starts_with("Kernel")
            || name.starts_with("page table")
            || name.starts_with("shared memory")
            || name.starts_with("Memory Tag")
            || name.starts_with("unused")
        {
            stats.kernel += resident;
        } else if name.starts_with("mapped file") {
            stats.files += resident;
        } else {
            stats.anonymous += resident;
        }
    }
    Some(stats)
}

#[cfg(target_os = "macos")]
fn is_size(token: &str) -> bool {
    token.ends_with('K') || token.ends_with('M') || token.ends_with('G')
}

#[cfg(target_os = "macos")]
fn parse_size(token: &str) -> u64 {
    let (number, unit) = token.split_at(token.len().saturating_sub(1));
    let value = number.parse::<f64>().unwrap_or(0.0);
    let multiplier = match unit {
        "G" => 1024.0 * 1024.0 * 1024.0,
        "M" => 1024.0 * 1024.0,
        _ => 1024.0,
    };
    (value * multiplier) as u64
}
