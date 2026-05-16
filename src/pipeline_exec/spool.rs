
fn remove_matching_files(dir: &Path, mut should_remove: impl FnMut(&str) -> bool) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    let entries = crate::util::read_dir_with_default_backoff(dir)
        .with_context(|| format!("read_dir {}", dir.display()))?;
    for entry in entries {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if should_remove(name) {
            crate::util::remove_with_short_backoff(&path)
                .with_context(|| format!("remove stale RETL-owned output {}", path.display()))?;
        }
    }
    Ok(())
}

fn planned_job_keys(files: &[FileJob]) -> HashSet<String> {
    files.iter().map(export_part_key).collect()
}

fn spool_key_from_part_name(name: &str) -> Option<String> {
    let (prefix, key_prefix) = if name.starts_with("part_RC_") {
        ("part_RC_", "RC")
    } else if name.starts_with("part_RS_") {
        ("part_RS_", "RS")
    } else {
        return None;
    };
    let ym = name.strip_prefix(prefix)?.strip_suffix(".jsonl")?;
    ym.parse::<YearMonth>().ok()?;
    Some(format!("{key_prefix}_{ym}"))
}

fn clear_spool_resume_parts(out_dir: &Path) -> Result<()> {
    remove_matching_files(out_dir, |name| spool_key_from_part_name(name).is_some())
}

fn prune_spool_outputs_except(out_dir: &Path, keep_keys: &HashSet<String>) -> Result<()> {
    remove_matching_files(out_dir, |name| match spool_key_from_part_name(name) {
        Some(key) => !keep_keys.contains(&key),
        None => false,
    })
}

fn clear_extract_resume_parts(tmp_dir: &Path) -> Result<()> {
    remove_matching_files(tmp_dir, |name| {
        name.starts_with(".part_") && name.ends_with(".jsonl")
    })
}

struct MonthJobCtx<'a> {
    out_dir: &'a Path,
    staging_dir: &'a Path,
    targets: Option<&'a Vec<String>>,
    query: &'a QuerySpec,
    whitelist: &'a Option<Vec<String>>,
    pb: Option<&'a ProgressBar>,
    bounds: Option<DateBounds>,
    read_buf: usize,
    write_buf: usize,
    human_ts: bool,
    whitelist_tracker: Option<&'a WhitelistMatchTracker>,
    record_limit: Option<&'a RecordLimit>,
    resume: bool,
    completed_keys: &'a HashSet<String>,
    allow_partial: bool,
    partial_reporter: Option<&'a crate::config::PartialReadReporter>,
}

/// Load `_progress.json`, validate it against the current fingerprint and each
/// entry against the on-disk file size, drop mismatches with a warning, and
/// return the surviving entries plus a snapshot of completed keys.
fn load_and_validate_manifest(
    out_dir: &Path,
    fingerprint: &str,
    planned_keys: &HashSet<String>,
) -> Result<(HashMap<String, MonthEntry>, HashSet<String>)> {
    let manifest = crate::progress_manifest::load(out_dir);
    if manifest.fingerprint.as_deref() != Some(fingerprint) && !manifest.months.is_empty() {
        tracing::warn!(
            path=%crate::progress_manifest::manifest_path(out_dir).display(),
            stored=?manifest.fingerprint,
            current=%fingerprint,
            "resume manifest fingerprint does not match current query/config; discarding spool parts"
        );
        clear_spool_resume_parts(out_dir)?;
        return Ok((HashMap::new(), HashSet::new()));
    }
    let mut keep: HashMap<String, MonthEntry> = HashMap::new();
    for (k, v) in manifest.months {
        if !planned_keys.contains(&k) {
            tracing::info!(key=%k, "dropping progress manifest entry outside current spool plan");
            continue;
        }
        let final_name = format!("part_{}.jsonl", k);
        let final_path = out_dir.join(&final_name);
        match fs::metadata(&final_path) {
            Ok(meta) if meta.len() == v.size => {
                keep.insert(k, v);
            }
            _ => {
                tracing::info!(key=%k, "dropping stale progress manifest entry; month will be re-run");
            }
        }
    }
    let completed_keys: HashSet<String> = keep.keys().cloned().collect();
    Ok((keep, completed_keys))
}

/// Per-month closure body: skip if the month is already published (resume
/// fast-path), otherwise atomically write the spool output via
/// `write_jsonl_atomic`. Returns `Ok(None)` on resume-skip or a tolerated zstd
/// decode/partial-scan skip (already logged); `Ok(Some(MonthResult))` on a
/// successful publish whose entry the caller should commit to the manifest.
/// Non-decode output, malformed-JSON, and publish failures are fatal.
fn process_month(job: &FileJob, ctx: &MonthJobCtx<'_>) -> Result<Option<MonthResult>> {
    let (file_prefix, key_prefix) = match job.kind {
        FileKind::Comment => (FILE_PREFIX_RC, "RC"),
        FileKind::Submission => (FILE_PREFIX_RS, "RS"),
    };
    let out_path = ctx
        .out_dir
        .join(format!("{}_{}.jsonl", file_prefix, job.ym));
    let key = crate::progress_manifest::month_key(key_prefix, job.ym);

    if ctx.record_limit.is_some_and(|limit| limit.is_exhausted()) {
        return Ok(None);
    }

    // Resume fast-path: skip the month entirely if it's already in the
    // manifest (and the on-disk file matched at load time). Still bump the
    // progress bar by the input file's compressed size so the user sees the
    // work accounted for.
    if ctx.resume && ctx.completed_keys.contains(&key) {
        if let Some(pb) = ctx.pb {
            let sz = fs::metadata(&job.path).map(|m| m.len()).unwrap_or(0);
            pb.inc(sz);
        }
        return Ok(None);
    }

    let n = match write_jsonl_atomic(ctx.staging_dir, &out_path, ctx.write_buf, |w| {
        let result = stream_job_with_partial_policy(
            job,
            w,
            ctx.targets,
            ctx.query,
            ctx.whitelist,
            ctx.pb.cloned(),
            ctx.bounds,
            ctx.read_buf,
            ctx.human_ts,
            ctx.whitelist_tracker,
            ctx.allow_partial,
            ctx.partial_reporter,
            ctx.record_limit,
        )?;
        complete_stream_job(job, result)
    }) {
        Ok(n) => n,
        Err(e) if ctx.allow_partial && is_partial_scan_error(&e) => {
            tracing::warn!(path=%job.path.display(), output=%out_path.display(), error=%e, "Skipping month after zstd decode error; staged spool output was discarded and resume will retry it");
            return Ok(None);
        }
        Err(e) => return Err(e),
    };

    Ok(Some(MonthResult {
        out_path,
        key,
        lines: n,
    }))
}

/// Atomically commit a month's manifest entry after a successful publish.
/// `write_jsonl_atomic` only returns Ok once the rename landed, so the size
/// stat is taken from the published path. Resume-enabled callers fail the run
/// immediately if this commit fails: the data file may remain published, but
/// returning success would incorrectly claim a durable checkpoint.
fn commit_entry_to_manifest(acc: &ManifestAccumulator, result: MonthResult) -> Result<()> {
    let size = fs::metadata(&result.out_path).map(|m| m.len()).unwrap_or(0);
    let entry = MonthEntry {
        size,
        lines: result.lines,
        sha256: None,
    };
    acc.commit(result.key, entry)
}

fn ensure_resume_manifest_durable(
    accumulator: Option<&ManifestAccumulator>,
    operation: &str,
) -> Result<()> {
    if let Some(error) = accumulator.and_then(ManifestAccumulator::last_save_error) {
        anyhow::bail!(
            "{operation} resume progress manifest is not durable; earlier save failed: {error}"
        );
    }
    Ok(())
}
