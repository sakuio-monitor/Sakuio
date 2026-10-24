use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    // GitHub Actions exposes GITHUB_SHA even when git metadata is unavailable
    // inside the build environment. Prefer it over a local git invocation.
    let commit = std::env::var("GITHUB_SHA")
        .ok()
        .map(|sha| sha.chars().take(7).collect::<String>())
        .filter(|sha| !sha.is_empty())
        .or_else(|| {
            Command::new("git")
                .args(["rev-parse", "--short=7", "HEAD"])
                .output()
                .ok()
                .filter(|output| output.status.success())
                .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=SAKUIO_BUILD_EPOCH={epoch}");
    println!("cargo:rustc-env=SAKUIO_BUILD_COMMIT={commit}");
    println!("cargo:rerun-if-changed=build.rs");
}
