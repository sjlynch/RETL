
fn attach_file_identity(path: &Path) -> AttachFileIdentity {
    let stable_path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let metadata = fs::metadata(path).ok();
    let modified = metadata
        .as_ref()
        .and_then(|m| m.modified().ok())
        .map(system_time_parts);
    let (modified_unix_secs, modified_nanos) = match modified {
        Some((secs, nanos)) => (Some(secs), Some(nanos)),
        None => (None, None),
    };

    AttachFileIdentity {
        path: stable_path.to_string_lossy().into_owned(),
        exists: metadata.is_some(),
        len: metadata.as_ref().map(|m| m.len()),
        modified_unix_secs,
        modified_nanos,
    }
}

fn attach_shard_set_fingerprint(
    shards: Option<&HashMap<YearMonth, PathBuf>>,
) -> AttachShardSetFingerprint {
    let index_present = shards.is_some();
    let mut entries = Vec::new();
    if let Some(shards) = shards {
        entries.extend(
            shards
                .iter()
                .map(|(ym, path)| (*ym, attach_file_identity(path))),
        );
    }
    entries.sort_by(|(a_ym, a_id), (b_ym, b_id)| {
        a_ym.cmp(b_ym).then_with(|| a_id.path.cmp(&b_id.path))
    });

    let mut hash = fnv1a_offset_basis();
    update_digest_bool(&mut hash, index_present);
    update_digest_u64(&mut hash, entries.len() as u64);
    for (ym, identity) in &entries {
        update_digest_str(&mut hash, &ym.to_string());
        update_file_identity_digest(&mut hash, identity);
    }

    AttachShardSetFingerprint {
        index_present,
        shards: entries.len() as u64,
        digest: format!("{hash:016x}"),
    }
}

fn parent_payload_fingerprint(spec: &ParentPayloadSpec) -> ParentPayloadFingerprint {
    ParentPayloadFingerprint {
        payload_format_version: spec.payload_format_version(),
        full_record: spec.is_full_record(),
        fields: spec.fields().to_vec(),
    }
}

fn attach_parent_cache_fingerprint(parents: &ParentMaps) -> AttachParentCacheFingerprint {
    AttachParentCacheFingerprint {
        payload: parent_payload_fingerprint(&parents.payload_spec),
        comment_shards: attach_shard_set_fingerprint(parents.comment_shards.as_ref()),
        submission_shards: attach_shard_set_fingerprint(parents.submission_shards.as_ref()),
    }
}

fn attach_resolution_range(
    start: Option<YearMonth>,
    end: Option<YearMonth>,
) -> AttachResolutionRange {
    AttachResolutionRange {
        start: start.map(|ym| ym.to_string()),
        end: end.map(|ym| ym.to_string()),
    }
}

fn build_attach_fingerprint(
    input: &Path,
    parent_cache: &AttachParentCacheFingerprint,
    resolution_range: &AttachResolutionRange,
) -> AttachFingerprint {
    AttachFingerprint {
        version: ATTACH_FINGERPRINT_VERSION,
        attach_format_version: ATTACH_FORMAT_VERSION,
        input: attach_file_identity(input),
        resolution_range: resolution_range.clone(),
        parent_cache: parent_cache.clone(),
    }
}

fn attach_fingerprint_matches(sidecar_path: &Path, expected: &AttachFingerprint) -> bool {
    match crate::util::open_with_default_backoff(sidecar_path) {
        Ok(f) => match serde_json::from_reader::<_, AttachFingerprint>(BufReader::new(f)) {
            Ok(actual) if &actual == expected => true,
            Ok(_) => {
                tracing::debug!(path=%sidecar_path.display(), "resume: attach fingerprint changed, rebuilding");
                false
            }
            Err(e) => {
                tracing::warn!(path=%sidecar_path.display(), error=%e, "resume: attach fingerprint sidecar is unreadable, rebuilding");
                false
            }
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::debug!(path=%sidecar_path.display(), "resume: attach fingerprint sidecar missing, rebuilding");
            false
        }
        Err(e) => {
            tracing::warn!(path=%sidecar_path.display(), error=%e, "resume: attach fingerprint sidecar cannot be opened, rebuilding");
            false
        }
    }
}

fn write_attach_fingerprint_atomic(
    staging_dir: &Path,
    sidecar_path: &Path,
    fingerprint: &AttachFingerprint,
    write_buf_bytes: usize,
) -> Result<()> {
    if sidecar_path.file_name().is_none() {
        return Err(anyhow::anyhow!(
            "attach fingerprint sidecar has no file name: {}",
            sidecar_path.display()
        ));
    }
    crate::util::create_dir_all_with_default_backoff(staging_dir)
        .with_context(|| format!("create staging dir {}", staging_dir.display()))?;
    if let Some(parent) = sidecar_path.parent().filter(|p| !p.as_os_str().is_empty()) {
        crate::util::create_dir_all_with_default_backoff(parent)
            .with_context(|| format!("create sidecar parent {}", parent.display()))?;
    }

    write_json_pretty_atomic(staging_dir, sidecar_path, write_buf_bytes, fingerprint).with_context(
        || {
            format!(
                "write attach fingerprint sidecar {}",
                sidecar_path.display()
            )
        },
    )
}
