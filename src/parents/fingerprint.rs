
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

fn update_digest_bytes(hash: &mut u64, bytes: &[u8]) {
    fnv1a_update(hash, bytes);
}

fn update_digest_bool(hash: &mut u64, value: bool) {
    update_digest_bytes(hash, &[u8::from(value)]);
}

fn update_digest_u32(hash: &mut u64, value: u32) {
    update_digest_bytes(hash, &value.to_le_bytes());
}

fn update_digest_u64(hash: &mut u64, value: u64) {
    update_digest_bytes(hash, &value.to_le_bytes());
}

fn update_digest_i64(hash: &mut u64, value: i64) {
    update_digest_bytes(hash, &value.to_le_bytes());
}

fn update_digest_str(hash: &mut u64, value: &str) {
    update_digest_u64(hash, value.len() as u64);
    update_digest_bytes(hash, value.as_bytes());
}

fn update_digest_option_u64(hash: &mut u64, value: Option<u64>) {
    update_digest_bool(hash, value.is_some());
    if let Some(value) = value {
        update_digest_u64(hash, value);
    }
}

fn update_digest_option_i64(hash: &mut u64, value: Option<i64>) {
    update_digest_bool(hash, value.is_some());
    if let Some(value) = value {
        update_digest_i64(hash, value);
    }
}

fn update_digest_option_u32(hash: &mut u64, value: Option<u32>) {
    update_digest_bool(hash, value.is_some());
    if let Some(value) = value {
        update_digest_u32(hash, value);
    }
}

fn update_file_identity_digest(hash: &mut u64, identity: &AttachFileIdentity) {
    update_digest_str(hash, &identity.path);
    update_digest_bool(hash, identity.exists);
    update_digest_option_u64(hash, identity.len);
    update_digest_option_i64(hash, identity.modified_unix_secs);
    update_digest_option_u32(hash, identity.modified_nanos);
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

fn finish_unordered_digest_string(entries: u64, sum: u64, xor: u64) -> String {
    let mut hash = fnv1a_offset_basis();
    update_digest_u64(&mut hash, entries);
    update_digest_u64(&mut hash, sum);
    update_digest_u64(&mut hash, xor);
    format!("{hash:016x}")
}

fn finish_unordered_digest(entries: usize, sum: u64, xor: u64) -> AttachMapDigest {
    AttachMapDigest {
        entries: entries as u64,
        digest: finish_unordered_digest_string(entries as u64, sum, xor),
    }
}

fn attach_comment_map_digest(map: &HashMap<String, String>) -> AttachMapDigest {
    let mut sum = 0u64;
    let mut xor = 0u64;
    for (id, body) in map {
        let mut entry_hash = fnv1a_offset_basis();
        update_digest_str(&mut entry_hash, id);
        update_digest_str(&mut entry_hash, body);
        sum = sum.wrapping_add(entry_hash);
        xor ^= entry_hash.rotate_left((entry_hash & 63) as u32);
    }
    finish_unordered_digest(map.len(), sum, xor)
}

fn attach_submission_map_digest(map: &HashMap<String, (String, String)>) -> AttachMapDigest {
    let mut sum = 0u64;
    let mut xor = 0u64;
    for (id, (title, selftext)) in map {
        let mut entry_hash = fnv1a_offset_basis();
        update_digest_str(&mut entry_hash, id);
        update_digest_str(&mut entry_hash, title);
        update_digest_str(&mut entry_hash, selftext);
        sum = sum.wrapping_add(entry_hash);
        xor ^= entry_hash.rotate_left((entry_hash & 63) as u32);
    }
    finish_unordered_digest(map.len(), sum, xor)
}

fn parent_payload_fingerprint(spec: &ParentPayloadSpec) -> ParentPayloadFingerprint {
    ParentPayloadFingerprint {
        payload_format_version: spec.payload_format_version(),
        full_record: spec.is_full_record(),
        fields: spec.fields().to_vec(),
    }
}

fn update_unordered_id_digest(count: &mut u64, sum: &mut u64, xor: &mut u64, id: &str) {
    let mut entry_hash = fnv1a_offset_basis();
    update_digest_str(&mut entry_hash, id);
    *count += 1;
    *sum = sum.wrapping_add(entry_hash);
    *xor ^= entry_hash.rotate_left((entry_hash & 63) as u32);
}

fn fingerprint_mem_id_set(kind: &str, ids: &AHashSet<String>) -> ParentIdSetFingerprint {
    let mut count = 0u64;
    let mut sum = 0u64;
    let mut xor = 0u64;
    for id in ids {
        update_unordered_id_digest(&mut count, &mut sum, &mut xor, id);
    }
    ParentIdSetFingerprint {
        kind: kind.to_string(),
        storage: "memory".to_string(),
        ids: count,
        digest: finish_unordered_digest_string(count, sum, xor),
        shard_count: 0,
        backing_shards: Vec::new(),
    }
}

fn fingerprint_empty_id_set(kind: &str) -> ParentIdSetFingerprint {
    ParentIdSetFingerprint {
        kind: kind.to_string(),
        storage: "empty".to_string(),
        ids: 0,
        digest: finish_unordered_digest_string(0, 0, 0),
        shard_count: 0,
        backing_shards: Vec::new(),
    }
}

fn fingerprint_id_shard_file(path: &Path) -> Result<(u64, u64, u64)> {
    let f = crate::util::open_with_default_backoff(path)
        .with_context(|| format!("open parent-id shard {}", path.display()))?;
    let mut r = BufReader::new(f);
    let mut count = 0u64;
    let mut sum = 0u64;
    let mut xor = 0u64;
    let mut id = String::new();
    loop {
        let n = read_line_capped(&mut r, &mut id, DEFAULT_MAX_LINE_BYTES, path)
            .with_context(|| format!("read parent-id shard {}", path.display()))?;
        if n == 0 {
            break;
        }
        if !id.is_empty() {
            update_unordered_id_digest(&mut count, &mut sum, &mut xor, &id);
        }
    }
    Ok((count, sum, xor))
}

fn fingerprint_sharded_id_set(kind: &str, shards: &IdShards) -> Result<ParentIdSetFingerprint> {
    let mut total_count = 0u64;
    let mut total_sum = 0u64;
    let mut total_xor = 0u64;
    let mut backing_shards = Vec::with_capacity(shards.count);

    for idx in 0..shards.count {
        let path = shards.path_for(idx);
        let metadata = fs::metadata(&path)
            .with_context(|| format!("stat parent-id shard {}", path.display()))?;
        let (count, sum, xor) = fingerprint_id_shard_file(&path)?;
        total_count += count;
        total_sum = total_sum.wrapping_add(sum);
        total_xor ^= xor;
        backing_shards.push(ParentIdShardFingerprint {
            index: idx as u64,
            ids: count,
            digest: finish_unordered_digest_string(count, sum, xor),
            len: metadata.len(),
        });
    }

    Ok(ParentIdSetFingerprint {
        kind: kind.to_string(),
        storage: "sharded".to_string(),
        ids: total_count,
        digest: finish_unordered_digest_string(total_count, total_sum, total_xor),
        shard_count: shards.count as u64,
        backing_shards,
    })
}

fn fingerprint_mixed_id_set(
    kind: &str,
    mem: &AHashSet<String>,
    shards: &IdShards,
) -> Result<ParentIdSetFingerprint> {
    let mem_fp = fingerprint_mem_id_set(kind, mem);
    let shard_fp = fingerprint_sharded_id_set(kind, shards)?;
    let ids = mem_fp.ids + shard_fp.ids;

    let mut hash = fnv1a_offset_basis();
    update_digest_str(&mut hash, &mem_fp.digest);
    update_digest_str(&mut hash, &shard_fp.digest);

    Ok(ParentIdSetFingerprint {
        kind: kind.to_string(),
        storage: "mixed".to_string(),
        ids,
        digest: finish_unordered_digest_string(ids, hash, 0),
        shard_count: shard_fp.shard_count,
        backing_shards: shard_fp.backing_shards,
    })
}

fn parent_id_set_fingerprint(
    kind: &str,
    mem: Option<&AHashSet<String>>,
    sharded: Option<&IdShards>,
) -> Result<ParentIdSetFingerprint> {
    match (mem, sharded) {
        (Some(ids), Some(shards)) if !ids.is_empty() => fingerprint_mixed_id_set(kind, ids, shards),
        (_, Some(shards)) => fingerprint_sharded_id_set(kind, shards),
        (Some(ids), None) => Ok(fingerprint_mem_id_set(kind, ids)),
        (None, None) => Ok(fingerprint_empty_id_set(kind)),
    }
}

fn parent_ids_fingerprint(ids: &ParentIds) -> Result<ParentIdsFingerprint> {
    Ok(ParentIdsFingerprint {
        t1: parent_id_set_fingerprint("t1", ids.t1_ids_mem.as_ref(), ids.t1_ids_sharded.as_ref())?,
        t3: parent_id_set_fingerprint("t3", ids.t3_ids_mem.as_ref(), ids.t3_ids_sharded.as_ref())?,
    })
}

fn attach_parent_cache_fingerprint(parents: &ParentMaps) -> AttachParentCacheFingerprint {
    AttachParentCacheFingerprint {
        payload: parent_payload_fingerprint(&parents.payload_spec),
        comment_shards: attach_shard_set_fingerprint(parents.comment_shards.as_ref()),
        submission_shards: attach_shard_set_fingerprint(parents.submission_shards.as_ref()),
        eager_comments: attach_comment_map_digest(&parents.comments),
        eager_submissions: attach_submission_map_digest(&parents.submissions),
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

fn resolver_fingerprint_path(final_dest: &Path) -> PathBuf {
    let mut sidecar = final_dest.as_os_str().to_os_string();
    sidecar.push(RESOLVER_SIDECAR_SUFFIX);
    PathBuf::from(sidecar)
}

fn resolver_kind_label(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Comment => "comments",
        FileKind::Submission => "submissions",
    }
}

fn build_resolver_fingerprint(
    job: &FileJob,
    parent_ids: &ParentIdsFingerprint,
    resolution_range: &AttachResolutionRange,
    payload_spec: &ParentPayloadSpec,
) -> ResolverFingerprint {
    ResolverFingerprint {
        version: RESOLVER_FINGERPRINT_VERSION,
        resolver_format_version: RESOLVER_FORMAT_VERSION,
        source: ResolverSourceFingerprint {
            kind: resolver_kind_label(job.kind).to_string(),
            month: job.ym.to_string(),
            file: attach_file_identity(&job.path),
        },
        resolution_range: resolution_range.clone(),
        parent_ids: parent_ids.clone(),
        payload: parent_payload_fingerprint(payload_spec),
    }
}

fn resolver_fingerprint_matches(sidecar_path: &Path, expected: &ResolverFingerprint) -> bool {
    match crate::util::open_with_default_backoff(sidecar_path) {
        Ok(f) => match serde_json::from_reader::<_, ResolverFingerprint>(BufReader::new(f)) {
            Ok(actual) if &actual == expected => true,
            Ok(_) => {
                tracing::debug!(path=%sidecar_path.display(), "resume: parent resolver fingerprint changed, rebuilding");
                false
            }
            Err(e) => {
                tracing::warn!(path=%sidecar_path.display(), error=%e, "resume: parent resolver fingerprint sidecar is unreadable, rebuilding");
                false
            }
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::debug!(path=%sidecar_path.display(), "resume: parent resolver fingerprint sidecar missing, rebuilding");
            false
        }
        Err(e) => {
            tracing::warn!(path=%sidecar_path.display(), error=%e, "resume: parent resolver fingerprint sidecar cannot be opened, rebuilding");
            false
        }
    }
}

fn write_resolver_fingerprint_atomic(
    sidecar_path: &Path,
    fingerprint: &ResolverFingerprint,
    write_buf_bytes: usize,
) -> Result<()> {
    let sidecar_parent = sidecar_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let staging_dir = ensure_staging_dir(&sidecar_parent).with_context(|| {
        format!(
            "ensure resolver sidecar staging dir under {}",
            sidecar_parent.display()
        )
    })?;

    write_json_pretty_atomic(&staging_dir, sidecar_path, write_buf_bytes, fingerprint).with_context(
        || {
            format!(
                "write resolver fingerprint sidecar {}",
                sidecar_path.display()
            )
        },
    )
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
