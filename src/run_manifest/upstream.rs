
pub fn discover_upstream_manifests_from_inputs(inputs: &[PathBuf]) -> Vec<UpstreamManifest> {
    let mut dirs: Vec<PathBuf> = inputs
        .iter()
        .filter_map(|path| path.parent().map(Path::to_path_buf))
        .collect();
    dirs.sort();
    dirs.dedup();
    dirs.into_iter()
        .map(|dir| upstream_manifest_for_directory(&dir))
        .filter(|m| m.exists)
        .collect()
}

pub fn upstream_manifest_for_directory(dir: &Path) -> UpstreamManifest {
    let path = manifest_path_for_directory(dir);
    let mut fingerprint = None;
    let mut operation = None;
    let exists = path.exists();
    if exists {
        if let Ok(bytes) = fs::read(&path) {
            if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                fingerprint = value
                    .get("manifest_fingerprint")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                operation = value
                    .get("operation")
                    .and_then(Value::as_str)
                    .map(str::to_string);
            }
        }
    }
    UpstreamManifest {
        path: path_to_stable_string(&path),
        exists,
        fingerprint,
        operation,
    }
}
