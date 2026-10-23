//! Safe extraction and validation of Komari theme ZIP archives.
//! Komari 主题 ZIP 的安全解压与校验。

use crate::theme::manifest::{ThemeManifest, MANIFEST_NAMES, MAX_MANIFEST_BYTES};
use std::io::{Cursor, Read, Write};
use std::path::{Component, Path, PathBuf};
use zip::ZipArchive;

/// Archive limits, matching Komari's defaults.
/// 压缩包限制，与 Komari 默认值一致。
pub const MAX_FILES: usize = 10_000;
pub const MAX_FILE_BYTES: u64 = 128 << 20;
pub const MAX_TOTAL_BYTES: u64 = 512 << 20;

/// Extracts `archive` into a fresh temporary directory next to `root`.
/// Returns the validated manifest and the extracted staging directory.
/// 把压缩包解压到 `root` 旁的临时目录，返回校验后的清单与暂存目录。
pub fn extract_to_staging(root: &Path, archive: &[u8]) -> Result<(ThemeManifest, PathBuf), String> {
    let mut zip =
        ZipArchive::new(Cursor::new(archive)).map_err(|e| format!("open theme archive: {e}"))?;
    if zip.len() > MAX_FILES {
        return Err(format!(
            "theme archive has too many files (max {MAX_FILES})"
        ));
    }

    // Determine an optional single top-level directory and locate the manifest.
    // 识别可选的单层顶层目录，并定位清单文件。
    let mut prefix: Option<String> = None;
    let mut manifest_name: Option<String> = None;
    let mut total = 0u64;
    for index in 0..zip.len() {
        let file = zip
            .by_index(index)
            .map_err(|e| format!("read theme archive entry: {e}"))?;
        let Some(path) = file.enclosed_name() else {
            return Err(format!(
                "unsafe path in theme archive: {}",
                entry_name(&file)
            ));
        };
        if path.components().any(|c| matches!(c, Component::ParentDir)) {
            return Err(format!(
                "unsafe path in theme archive: {}",
                entry_name(&file)
            ));
        }
        if is_symlink(&file) {
            return Err(format!(
                "symlinks are not allowed in theme archives: {}",
                entry_name(&file)
            ));
        }
        if file.is_dir() {
            continue;
        }
        total = total.saturating_add(file.size());
        if file.size() > MAX_FILE_BYTES || total > MAX_TOTAL_BYTES {
            return Err("theme archive exceeds the extraction size limit".into());
        }
        let normalized = path.to_string_lossy().into_owned();
        let parts: Vec<&str> = normalized
            .split('/')
            .filter(|part| !part.is_empty() && *part != ".")
            .collect();
        match parts.as_slice() {
            [file] if MANIFEST_NAMES.contains(file) => {
                manifest_name = Some((*file).to_string());
                prefix = Some(String::new());
            }
            [dir, file] if *dir != "__MACOSX" && MANIFEST_NAMES.contains(file) => {
                if prefix.as_deref().is_some_and(|existing| existing != *dir) {
                    return Err("theme archive contains multiple top-level directories".into());
                }
                prefix = Some((*dir).to_string());
                manifest_name = Some(format!("{dir}/{file}"));
            }
            _ => {}
        }
    }
    let (Some(prefix), Some(_manifest_name)) = (prefix, manifest_name) else {
        return Err("sakuio-theme.json or komari-theme.json not found at the archive root".into());
    };
    if prefix.is_empty() {
        // Root layout: no nested directories are stripped.
    } else if prefix.contains('/') || prefix == "." || prefix == ".." {
        return Err("invalid top-level directory in theme archive".into());
    }

    let staging = root.join(format!(".staging-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&staging).map_err(|e| format!("create theme staging dir: {e}"))?;

    let result = (|| -> Result<ThemeManifest, String> {
        let mut extracted_total = 0u64;
        for index in 0..zip.len() {
            let mut file = zip
                .by_index(index)
                .map_err(|e| format!("read theme archive entry: {e}"))?;
            let Some(path) = file.enclosed_name() else {
                return Err(format!(
                    "unsafe path in theme archive: {}",
                    entry_name(&file)
                ));
            };
            let mut relative = path_to_slash(&path);
            if !prefix.is_empty() {
                let stripped = relative
                    .strip_prefix(&format!("{prefix}/"))
                    .map(str::to_string);
                let Some(stripped) = stripped else {
                    continue;
                };
                relative = stripped;
            }
            if relative.is_empty() || relative == ".." {
                continue;
            }
            let target = safe_join(&staging, &relative)
                .ok_or_else(|| format!("unsafe path in theme archive: {}", entry_name(&file)))?;
            if file.is_dir() {
                std::fs::create_dir_all(&target)
                    .map_err(|e| format!("create theme dir {relative}: {e}"))?;
                continue;
            }
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("create theme dir {relative}: {e}"))?;
            }
            let mut out = std::fs::File::create(&target)
                .map_err(|e| format!("create theme file {relative}: {e}"))?;
            let copied = std::io::copy(&mut file.by_ref().take(MAX_FILE_BYTES + 1), &mut out)
                .map_err(|e| format!("extract theme file {relative}: {e}"))?;
            out.flush()
                .map_err(|e| format!("flush theme file {relative}: {e}"))?;
            if copied > MAX_FILE_BYTES {
                return Err(format!("theme file {relative} exceeds the size limit"));
            }
            extracted_total = extracted_total.saturating_add(copied);
            if extracted_total > MAX_TOTAL_BYTES {
                return Err("theme archive exceeds the extraction size limit".into());
            }
        }

        let name = crate::theme::manifest::manifest_name(&staging)
            .ok_or_else(|| "theme manifest not found after extraction".to_string())?;
        let manifest_path = staging.join(name);
        let metadata = std::fs::metadata(&manifest_path)
            .map_err(|_| format!("{name} not found after extraction"))?;
        if metadata.len() > MAX_MANIFEST_BYTES {
            return Err(format!("{name} exceeds the size limit"));
        }
        let raw = std::fs::read(&manifest_path).map_err(|e| format!("read {name}: {e}"))?;
        let manifest: ThemeManifest =
            serde_json::from_slice(&raw).map_err(|e| format!("parse {name}: {e}"))?;
        manifest.validate()?;
        if !staging.join("dist/index.html").is_file() {
            return Err("theme archive is missing dist/index.html".into());
        }
        Ok(manifest)
    })();

    match result {
        Ok(manifest) => Ok((manifest, staging)),
        Err(error) => {
            let _ = std::fs::remove_dir_all(&staging);
            Err(error)
        }
    }
}

/// Atomically replaces `root/<short>` with a validated staging directory.
/// 用校验后的暂存目录原子替换 `root/<short>`。
pub fn commit_staging(root: &Path, manifest: &ThemeManifest, staging: &Path) -> Result<(), String> {
    let target = root.join(&manifest.short);
    let backup = root.join(format!(".old-{}", uuid::Uuid::new_v4()));
    if target.exists() {
        std::fs::rename(&target, &backup).map_err(|e| format!("move existing theme aside: {e}"))?;
    }
    if let Err(error) = std::fs::rename(staging, &target) {
        if backup.exists() {
            let _ = std::fs::rename(&backup, &target);
        }
        return Err(format!("activate extracted theme: {error}"));
    }
    if backup.exists() {
        let _ = std::fs::remove_dir_all(&backup);
    }
    Ok(())
}

fn entry_name<R: Read + ?Sized>(file: &zip::read::ZipFile<'_, R>) -> String {
    file.name()
        .map(|name| name.into_owned())
        .unwrap_or_else(|_| "<invalid>".to_string())
}

fn is_symlink<R: Read + ?Sized>(file: &zip::read::ZipFile<'_, R>) -> bool {
    file.unix_mode()
        .is_some_and(|mode| mode & 0o170000 == 0o120000)
}

fn path_to_slash(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(value.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

pub fn safe_join(base: &Path, relative: &str) -> Option<PathBuf> {
    if relative.is_empty() || relative.starts_with('/') || relative.contains('\\') {
        return None;
    }
    let mut path = base.to_path_buf();
    for component in Path::new(relative).components() {
        match component {
            Component::Normal(value) => path.push(value),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    Some(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zip::write::SimpleFileOptions;

    fn archive(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut buffer = Cursor::new(Vec::new());
        {
            let mut writer = zip::ZipWriter::new(&mut buffer);
            let options = SimpleFileOptions::default();
            for (name, bytes) in entries {
                writer.start_file(*name, options).unwrap();
                writer.write_all(bytes).unwrap();
            }
            writer.finish().unwrap();
        }
        buffer.into_inner()
    }

    const MANIFEST: &str = r#"{"name":"Demo","short":"demo","version":"1.0.0"}"#;

    #[test]
    fn rejects_path_traversal() {
        let root = std::env::temp_dir().join(format!("sakuio-theme-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let bytes = archive(&[
            ("../evil.sh", b"x"),
            ("komari-theme.json", MANIFEST.as_bytes()),
        ]);
        assert!(extract_to_staging(&root, &bytes).is_err());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn accepts_root_and_nested_layouts() {
        let root = std::env::temp_dir().join(format!("sakuio-theme-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let root_layout = archive(&[
            ("komari-theme.json", MANIFEST.as_bytes()),
            ("dist/index.html", b"<html></html>"),
        ]);
        let (manifest, staging) = extract_to_staging(&root, &root_layout).unwrap();
        assert_eq!(manifest.short, "demo");
        commit_staging(&root, &manifest, &staging).unwrap();
        assert!(root.join("demo/dist/index.html").is_file());

        let nested = archive(&[
            ("demo-1.0/komari-theme.json", MANIFEST.as_bytes()),
            ("demo-1.0/dist/index.html", b"<html></html>"),
        ]);
        let (manifest, staging) = extract_to_staging(&root, &nested).unwrap();
        commit_staging(&root, &manifest, &staging).unwrap();
        assert!(root.join("demo/dist/index.html").is_file());
        let _ = std::fs::remove_dir_all(root);
    }
}
