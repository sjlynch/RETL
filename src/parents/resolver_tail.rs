
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

/// Fraction of parsed records carrying a `parent_id` field that fails the
/// `t1_`/`t3_` prefix test above which `warn_if_malformed_parent_ids` fires.
/// Such records are silently un-attachable — neither resolved nor counted as
/// `unresolved` — so even a low rate is worth naming explicitly.
const MALFORMED_PARENT_ID_WARN_RATE: f64 = 0.005;

/// Warn the months covered by the planned `files` set against the contiguous
/// resolution window, naming any holes. A child whose `parent_id` lives in a
/// missing month resolves as `unresolved`; the generic unresolved-rate
/// warning's "use a larger --window-months" remedy is actively misleading
/// when the real cause is a gap *inside* the window (a month absent from
/// `data_dir`). `missing_month_diagnostics` already clamps to the discovered
/// corpus min/max, so a request that simply extends past the corpus does not
/// produce noise.
fn warn_resolver_window_gaps(
    discovered: &Discovered,
    start: Option<YearMonth>,
    end: Option<YearMonth>,
) {
    for diag in missing_month_diagnostics(discovered, crate::config::Sources::Both, start, end) {
        let missing = format_year_month_ranges(&diag.months);
        tracing::warn!(
            source = diag.kind.long_label(),
            window_start = %diag.range_start,
            window_end = %diag.range_end,
            missing_month_count = diag.months.len(),
            missing_months = %missing,
            "parents resolver: corpus file(s) missing inside the resolution window; any child whose `parent_id` falls in a gap month resolves as unresolved — this is a hole in the corpus directory, not a too-small window, so a larger --window-months will not help"
        );
    }
}

/// Warn when a non-trivial fraction of parsed records carry a `parent_id`
/// field that is not a string prefixed with `t1_`/`t3_` (unprefixed, numeric,
/// or null). Such records fail `is_comment_record_for_parent_attach`, so they
/// are silently skipped — never resolved, never counted as `unresolved` — and
/// would otherwise leave a user unable to explain a low resolution rate.
fn warn_if_malformed_parent_ids(diagnostics: ParentAttachDiagnostics) {
    if diagnostics.parsed_records == 0 || diagnostics.records_with_unprefixed_parent_id == 0 {
        return;
    }
    let rate =
        diagnostics.records_with_unprefixed_parent_id as f64 / diagnostics.parsed_records as f64;
    if rate <= MALFORMED_PARENT_ID_WARN_RATE {
        return;
    }
    let rate_pct = format!("{:.2}%", rate * 100.0);
    tracing::warn!(
        records = diagnostics.parsed_records,
        records_with_unprefixed_parent_id = diagnostics.records_with_unprefixed_parent_id,
        unprefixed_parent_id_rate = %rate_pct,
        "parents attach skipped records whose `parent_id` is not a string prefixed with `t1_`/`t3_` (unprefixed, numeric, or null); these are neither resolved nor counted as unresolved, so a low resolution rate may reflect malformed parent_id values in the spool rather than missing parent corpus"
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
