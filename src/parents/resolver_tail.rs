
fn validate_jsonl_file(path: &Path) -> bool {
    let Ok(f) = crate::util::open_with_default_backoff(path) else {
        return false;
    };
    let mut r = BufReader::new(f);
    let mut buf = String::new();
    loop {
        match read_line_capped(&mut r, &mut buf, DEFAULT_MAX_LINE_BYTES, path) {
            Ok(0) => return true,
            Ok(_) => {
                if buf.trim().is_empty() {
                    continue;
                }
                if serde_json::from_str::<Value>(&buf).is_err() {
                    return false;
                }
            }
            Err(_) => return false,
        }
    }
}

fn looks_like_rc_spool_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.starts_with("part_RC_") || name.starts_with("RC_"))
        .unwrap_or(false)
}

fn diagnose_initial_attach_shape(inputs: &[(usize, PathBuf)], read_buf: usize) -> Result<()> {
    let Some((_, first_input)) = inputs.first() else {
        return Ok(());
    };

    let f = crate::util::open_with_default_backoff(first_input)
        .with_context(|| format!("open initial parent attach input {}", first_input.display()))?;
    let mut r = BufReader::with_capacity(read_buf, f);
    let mut buf = String::with_capacity(64 * 1024);
    let mut line_number = 0u64;
    let mut shape = ParentAttachInitialShape::default();

    while shape.parsed_records < ATTACH_INITIAL_DIAGNOSTIC_SAMPLE_RECORDS {
        let n = read_line_capped(&mut r, &mut buf, DEFAULT_MAX_LINE_BYTES, first_input)
            .with_context(|| {
                format!(
                    "read initial parent attach input {} near line {}",
                    first_input.display(),
                    line_number + 1
                )
            })?;
        if n == 0 {
            break;
        }
        line_number += 1;
        if buf.trim().is_empty() {
            continue;
        }
        let v: Value = serde_json::from_str(&buf).with_context(|| {
            format!(
                "malformed JSON in initial parent attach input {} at line {}",
                first_input.display(),
                line_number
            )
        })?;
        shape.observe(&v);
    }

    if shape.no_legacy_comment_shape()
        && (shape.records_with_parent_id > 0 || looks_like_rc_spool_path(first_input))
    {
        let observed_shape = shape.observed_shape();
        tracing::error!(
            path = %first_input.display(),
            sampled_records = shape.parsed_records,
            records_with_body = shape.records_with_body,
            records_with_parent_id = shape.records_with_parent_id,
            records_with_link_id = shape.records_with_link_id,
            records_with_prefixed_parent_id = shape.records_with_prefixed_parent_id,
            "parents attach initial sample found no records with both `body` and `parent_id` ({observed_shape}); using relaxed parent_id-based matching for records whose `parent_id` starts with `t1_`/`t3_`; if this spool was produced with --whitelist/.whitelist_fields, include body,parent_id,link_id to retain child comment text and parent-resolution context"
        );
    }

    Ok(())
}

fn warn_if_no_comment_shaped_records(diagnostics: ParentAttachDiagnostics) {
    if diagnostics.files_scanned == 0
        || diagnostics.parsed_records == 0
        || diagnostics.comment_shaped_records > 0
    {
        return;
    }

    tracing::warn!(
        scanned_files = diagnostics.files_scanned,
        resume_skipped_files = diagnostics.resume_skipped_files,
        records = diagnostics.parsed_records,
        rc_records = diagnostics.rc_records,
        records_with_body = diagnostics.records_with_body,
        records_with_parent_id = diagnostics.records_with_parent_id,
        records_with_link_id = diagnostics.records_with_link_id,
        "parents attach found zero records with a usable `parent_id`; comment records must include a `parent_id` starting with `t1_` or `t3_`, and parent resolution also benefits from `link_id`; if the spool was produced with --whitelist/.whitelist_fields, include parent_id,link_id (and body if you need the child comment text) or the output will be a no-op copy"
    );
}

fn load_shard_value<V>(
    eager_values: &HashMap<String, V>,
    shards: Option<&HashMap<YearMonth, PathBuf>>,
    cache: &mut WorkerShardCache<V>,
    key: &str,
    own_ym: Option<YearMonth>,
) -> Result<Option<V>>
where
    V: serde::de::DeserializeOwned + Clone,
{
    if let Some(v) = eager_values.get(key) {
        return Ok(Some(v.clone()));
    }
    let Some(idx) = shards else {
        return Ok(None);
    };

    if let Some(ym) = own_ym {
        if let Some(p) = idx.get(&ym) {
            if let Some(v) = cache.get(p, key)? {
                return Ok(Some(v));
            }
        }
    }
    for (ym, p) in idx {
        if Some(*ym) == own_ym {
            continue;
        }
        if let Some(v) = cache.get(p, key)? {
            return Ok(Some(v));
        }
    }
    Ok(None)
}
