use std::{
    collections::HashMap,
    path::Path,
};

fn main() {
    check_migration_versions();
}

/// Ensures each embedded migration has a unique numeric prefix and is forward-only SQL.
/// 确保每条内嵌迁移的数字前缀唯一，且仅为正向 SQL 文件。
fn check_migration_versions() {
    let dir = Path::new("migrations");
    println!("cargo:rerun-if-changed={}", dir.display());

    let mut seen = HashMap::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        println!("cargo:rerun-if-changed=migrations/{name}");

        if name.ends_with(".down.sql") {
            panic!(
                "migrations/{name}: down migrations are not embedded; use forward-only 000000NN_name.sql"
            );
        }
        if !name.ends_with(".sql") {
            panic!(
                "migrations/{name}: only executable .sql files belong in migrations/ (see board crate layout)"
            );
        }

        let Some((version, _rest)) = name.split_once('_') else {
            panic!(
                "migrations/{name}: expected name like 00000001_schema.sql (8-digit version + underscore + slug)"
            );
        };
        if version.len() != 8 || !version.chars().all(|c| c.is_ascii_digit()) {
            panic!(
                "migrations/{name}: version prefix must be 8 digits, got `{version}`"
            );
        }
        if let Some(prev) = seen.insert(version.to_string(), name.clone()) {
            panic!("duplicate sqlx migration version {version}: {prev} and {name}");
        }
    }
}
