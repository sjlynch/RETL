
fn file_identity_with_context(
    path: &Path,
    kind: Option<String>,
    month: Option<String>,
) -> FileIdentity {
    let metadata = fs::metadata(path).ok();
    let modified = metadata
        .as_ref()
        .and_then(|m| m.modified().ok())
        .map(system_time_parts);
    FileIdentity {
        path: path_to_stable_string(path),
        kind,
        month,
        exists: metadata.is_some(),
        size_bytes: metadata.as_ref().map(|m| m.len()),
        modified_unix_secs: modified.map(|(secs, _)| secs),
        modified_nanos: modified.map(|(_, nanos)| nanos),
        digest: None,
    }
}
