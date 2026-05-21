
fn attach_inprogress_exists(staging_dir: &Path, final_dest: &Path) -> Result<bool> {
    let file_name = final_dest
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "final_dest has no UTF-8 file name: {}",
                final_dest.display()
            )
        })?;
    if !staging_dir.exists() {
        return Ok(false);
    }
    for entry in fs::read_dir(staging_dir)
        .with_context(|| format!("read staging dir {}", staging_dir.display()))?
    {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with(file_name) && name.ends_with(INPROGRESS_EXT) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn attach_fingerprint_path(final_dest: &Path) -> PathBuf {
    let mut sidecar = final_dest.as_os_str().to_os_string();
    sidecar.push(ATTACH_SIDECAR_SUFFIX);
    PathBuf::from(sidecar)
}

fn is_owned_spool_part_name(name: &str) -> bool {
    let ym = name
        .strip_prefix("part_RC_")
        .or_else(|| name.strip_prefix("part_RS_"))
        .and_then(|rest| rest.strip_suffix(".jsonl"));
    ym.is_some_and(|ym| ym.parse::<YearMonth>().is_ok())
}

fn owned_attach_output_base_name(name: &str) -> Option<String> {
    if is_owned_spool_part_name(name) {
        return Some(name.to_string());
    }
    let base = name.strip_suffix(ATTACH_SIDECAR_SUFFIX)?;
    is_owned_spool_part_name(base).then(|| base.to_string())
}

fn attach_output_basenames(inputs: &[(usize, PathBuf)]) -> Result<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    for (_idx, input) in inputs {
        let name = input.file_name().ok_or_else(|| {
            anyhow::anyhow!(
                "attach_parents input path has no file name: {}",
                input.display()
            )
        })?;
        names.insert(name.to_string_lossy().to_string());
    }
    Ok(names)
}

fn prune_stale_attach_outputs(out_dir: &Path, keep_basenames: &BTreeSet<String>) -> Result<()> {
    if !out_dir.exists() {
        return Ok(());
    }
    for entry in crate::util::read_dir_with_default_backoff(out_dir)
        .with_context(|| format!("read_dir {}", out_dir.display()))?
    {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(base_name) = owned_attach_output_base_name(name) else {
            continue;
        };
        if keep_basenames.contains(&base_name) {
            continue;
        }
        crate::util::remove_with_short_backoff(&path)
            .with_context(|| format!("remove stale parent attach output {}", path.display()))?;
    }
    Ok(())
}

/// References needed to attach parents to a single input file. Constructed
/// once per `attach_parents_jsonls_parallel_with_stats` call and shared across
/// workers — every field is borrowed from `ParentMaps` (or the empty payload
/// fallback) so cheap to copy.
struct AttachFileCtx<'a> {
    legacy_payload: bool,
    parents_c_eager: &'a HashMap<String, String>,
    parents_s_eager: &'a HashMap<String, (String, String)>,
    empty_parent_payloads: &'a HashMap<String, ParentPayload>,
    comment_shards: Option<&'a HashMap<YearMonth, PathBuf>>,
    submission_shards: Option<&'a HashMap<YearMonth, PathBuf>>,
}

/// Per-worker FIFO caches keyed by parent kind / payload mode. See
/// `WorkerShardCache` for why eviction is plain FIFO with no bump-on-hit.
struct AttachWorkerCaches {
    legacy_comments: WorkerShardCache<String>,
    legacy_submissions: WorkerShardCache<(String, String)>,
    full_comments: WorkerShardCache<ParentPayload>,
    full_submissions: WorkerShardCache<ParentPayload>,
}

impl AttachWorkerCaches {
    fn new() -> Self {
        Self {
            legacy_comments: WorkerShardCache::<String>::new(COMMENT_SHARD_CACHE_CAP),
            legacy_submissions: WorkerShardCache::<(String, String)>::new(
                SUBMISSION_SHARD_CACHE_CAP,
            ),
            full_comments: WorkerShardCache::<ParentPayload>::new(COMMENT_SHARD_CACHE_CAP),
            full_submissions: WorkerShardCache::<ParentPayload>::new(SUBMISSION_SHARD_CACHE_CAP),
        }
    }
}

/// Look up `parent_id` in the appropriate shard (or eager) map and return the
/// `ParentPayload` to splice under the child record's `"parent"` key, or
/// `None` if the parent could not be resolved. Collapses the four near-
/// identical `t1_`/`t3_` × legacy/full-record branches that used to live
/// inline in `attach_parents_jsonls_parallel_with_stats`.
fn resolve_parent_into_value(
    parent_id: &str,
    own_ym: Option<YearMonth>,
    ctx: &AttachFileCtx<'_>,
    caches: &mut AttachWorkerCaches,
) -> Result<Option<ParentPayload>> {
    if ctx.legacy_payload {
        if let Some(rest) = parent_id.strip_prefix("t1_") {
            if let Some(body) = load_shard_value(
                ctx.parents_c_eager,
                ctx.comment_shards,
                &mut caches.legacy_comments,
                rest,
                own_ym,
            )? {
                let mut payload = ParentPayload::new();
                payload.insert("kind".into(), Value::String("comment".into()));
                payload.insert("id".into(), Value::String(rest.to_string()));
                payload.insert("body".into(), Value::String(body));
                return Ok(Some(payload));
            }
        } else if let Some(rest) = parent_id.strip_prefix("t3_") {
            if let Some((title, selftext)) = load_shard_value(
                ctx.parents_s_eager,
                ctx.submission_shards,
                &mut caches.legacy_submissions,
                rest,
                own_ym,
            )? {
                let mut payload = ParentPayload::new();
                payload.insert("kind".into(), Value::String("submission".into()));
                payload.insert("id".into(), Value::String(rest.to_string()));
                payload.insert("title".into(), Value::String(title));
                payload.insert("selftext".into(), Value::String(selftext));
                return Ok(Some(payload));
            }
        }
        return Ok(None);
    }

    if let Some(rest) = parent_id.strip_prefix("t1_") {
        if let Some(mut payload) = load_shard_value(
            ctx.empty_parent_payloads,
            ctx.comment_shards,
            &mut caches.full_comments,
            rest,
            own_ym,
        )? {
            payload.insert("kind".into(), Value::String("comment".into()));
            payload.insert("id".into(), Value::String(rest.to_string()));
            return Ok(Some(payload));
        }
    } else if let Some(rest) = parent_id.strip_prefix("t3_") {
        if let Some(mut payload) = load_shard_value(
            ctx.empty_parent_payloads,
            ctx.submission_shards,
            &mut caches.full_submissions,
            rest,
            own_ym,
        )? {
            payload.insert("kind".into(), Value::String("submission".into()));
            payload.insert("id".into(), Value::String(rest.to_string()));
            return Ok(Some(payload));
        }
    }
    Ok(None)
}

/// Inner per-file body for parent-attach: read JSONL from `in_path`, tally
/// diagnostics, splice resolved parents onto comment-shaped records, and
/// write each (possibly augmented) record to `w`. Returns the file's stats
/// and diagnostics; `files_scanned` is always 1.
fn attach_parents_for_one_file<W: std::io::Write + ?Sized>(
    in_path: &Path,
    w: &mut W,
    ctx: &AttachFileCtx<'_>,
    is_rc_spool_part: bool,
) -> Result<(ParentAttachStats, ParentAttachDiagnostics)> {
    let mut file_stats = ParentAttachStats::default();
    let mut diagnostics = ParentAttachDiagnostics {
        files_scanned: 1,
        ..Default::default()
    };
    let mut caches = AttachWorkerCaches::new();

    let f = crate::util::open_with_default_backoff(in_path)?;
    let mut r = BufReader::new(f);
    let mut line_buf = String::new();
    let mut line_no: u64 = 0;

    loop {
        let n = read_line_capped(&mut r, &mut line_buf, DEFAULT_MAX_LINE_BYTES, in_path)
            .with_context(|| {
                format!(
                    "read parent attach input {} at line {}",
                    in_path.display(),
                    line_no + 1
                )
            })?;
        if n == 0 {
            break;
        }
        line_no += 1;
        if line_buf.is_empty() {
            continue;
        }
        let mut v: Value = serde_json::from_str(&line_buf).with_context(|| {
            format!(
                "malformed JSON in parent attach input {} at line {}",
                in_path.display(),
                line_no
            )
        })?;

        let has_body = v.get("body").is_some();
        let parent_id_value = v.get("parent_id");
        let has_parent_id = parent_id_value.is_some();
        // A `parent_id` is usable only if it is a string prefixed `t1_`/`t3_`;
        // anything else (unprefixed, numeric, null) is skipped by
        // `is_comment_record_for_parent_attach` without being tallied.
        let has_prefixed_parent_id = parent_id_value
            .and_then(|x| x.as_str())
            .is_some_and(|s| s.starts_with("t1_") || s.starts_with("t3_"));
        let has_link_id = v.get("link_id").is_some();
        diagnostics.parsed_records += 1;
        if is_rc_spool_part {
            diagnostics.rc_records += 1;
        }
        if has_body {
            diagnostics.records_with_body += 1;
        }
        if has_parent_id {
            diagnostics.records_with_parent_id += 1;
        }
        if has_parent_id && !has_prefixed_parent_id {
            diagnostics.records_with_unprefixed_parent_id += 1;
        }
        if has_link_id {
            diagnostics.records_with_link_id += 1;
        }

        if is_comment_record_for_parent_attach(&v) {
            diagnostics.comment_shaped_records += 1;
            // Own-month is derived from the consuming record's `created_utc`;
            // used to pick the most-likely parent shard before falling back to
            // other months.
            let own_ym = v
                .get("created_utc")
                .and_then(|x| x.as_i64())
                .map(ym_from_epoch);
            let parent_id = v
                .get("parent_id")
                .and_then(|x| x.as_str())
                .map(|s| s.to_string());
            if let Some(parent_id) = parent_id {
                let resolved = resolve_parent_into_value(&parent_id, own_ym, ctx, &mut caches)?;
                if let Some(payload) = resolved {
                    if let Some(map) = v.as_object_mut() {
                        map.insert("parent".into(), Value::Object(payload));
                    }
                    file_stats.resolved += 1;
                } else {
                    file_stats.unresolved += 1;
                }
            }
        }

        serde_json::to_writer(&mut *w, &v)?;
        w.write_all(b"\n")?;
    }

    Ok((file_stats, diagnostics))
}

/// Build the `RunManifestInput` emitted by
/// `attach_parents_jsonls_parallel_with_stats`. Field names and the
/// `options` JSON shape are part of the manifest compatibility surface — do
/// not reorder or rename without a migration.
fn build_attach_manifest(
    manifest_inputs: &[PathBuf],
    out_paths: &[PathBuf],
    stats: ParentAttachStats,
    diagnostics: ParentAttachDiagnostics,
    manifest_start: RunManifestStart,
    opts: &crate::config::ETLOptions,
    payload_spec: &ParentPayloadSpec,
    resume: bool,
) -> RunManifestInput {
    let mut counts = BTreeMap::new();
    counts.insert("input_files".to_string(), manifest_inputs.len() as u64);
    counts.insert("attached_files".to_string(), out_paths.len() as u64);
    counts.insert("parents_resolved".to_string(), stats.resolved);
    counts.insert("parents_unresolved".to_string(), stats.unresolved);
    counts.insert("records_scanned".to_string(), diagnostics.parsed_records);
    counts.insert(
        "records_with_unprefixed_parent_id".to_string(),
        diagnostics.records_with_unprefixed_parent_id,
    );

    let mut manifest = RunManifestInput::new("parents.attach_parents_jsonls_parallel");
    manifest.start = manifest_start;
    manifest.api_operation = Some("RedditETL::attach_parents_jsonls_parallel".to_string());
    manifest.options = serde_json::json!({
        "resume": resume,
        "resolution_range": {
            "start": opts.start.map(|ym| ym.to_string()),
            "end": opts.end.map(|ym| ym.to_string()),
        },
        "parent_payload": {
            "full_record": payload_spec.is_full_record(),
            "fields": payload_spec.fields(),
        },
        "base_dir": path_to_stable_string(&opts.base_dir),
        "comments_dir": path_to_stable_string(&opts.comments_dir),
        "submissions_dir": path_to_stable_string(&opts.submissions_dir),
        "parallelism": opts.parallelism,
        "file_concurrency": opts.file_concurrency,
        "emit_manifest": opts.emit_manifest,
    });
    manifest.inputs = file_identities(manifest_inputs);
    manifest.output_format = "jsonl-directory".to_string();
    manifest.counts = counts;
    manifest.upstream_manifests = discover_upstream_manifests_from_inputs(manifest_inputs);
    manifest
}
