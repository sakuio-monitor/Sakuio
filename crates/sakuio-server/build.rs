use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    println!("cargo:rustc-env=SAKUIO_BUILD_EPOCH={epoch}");
    println!("cargo:rerun-if-changed=build.rs");
}
