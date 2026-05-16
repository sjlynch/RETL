
fn checkpoint_part_path(dir: &Path, key: &str) -> PathBuf {
    dir.join(format!("part_{}.jsonl", key))
}

fn scan_checkpoint_dir(work_dir: &Path, fingerprint: &str) -> PathBuf {
    work_dir
        .join("scan_checkpoints")
        .join(stable_hash_component(fingerprint.as_bytes()))
}

fn load_and_validate_scan_checkpoint_manifest(
    dir: &Path,
    fingerprint: &str,
) -> Result<(HashMap<String, MonthEntry>, HashSet<String>)> {
    let manifest = crate::progress_manifest::load(dir);
    if manifest.fingerprint.as_deref() != Some(fingerprint) && !manifest.months.is_empty() {
        tracing::warn!(
            path=%crate::progress_manifest::manifest_path(dir).display(),
            stored=?manifest.fingerprint,
            current=%fingerprint,
            "scan checkpoint fingerprint does not match current query/config/corpus; discarding cached month parts"
        );
        clear_spool_resume_parts(dir)?;
        return Ok((HashMap::new(), HashSet::new()));
    }

    let mut keep = HashMap::<String, MonthEntry>::new();
    for (key, expected) in manifest.months {
        let part_path = checkpoint_part_path(dir, &key);
        match validate_jsonl_part(&part_path) {
            Ok(actual) if actual.size == expected.size && actual.lines == expected.lines => {
                keep.insert(key, actual);
            }
            Ok(actual) => {
                tracing::info!(
                    key=%key,
                    path=%part_path.display(),
                    expected_size=expected.size,
                    actual_size=actual.size,
                    expected_lines=expected.lines,
                    actual_lines=actual.lines,
                    "dropping stale scan checkpoint entry; month will be rebuilt"
                );
                let _ = crate::util::remove_with_short_backoff(&part_path);
            }
            Err(e) => {
                tracing::info!(
                    key=%key,
                    path=%part_path.display(),
                    error=%e,
                    "dropping invalid scan checkpoint entry; month will be rebuilt"
                );
                let _ = crate::util::remove_with_short_backoff(&part_path);
            }
        }
    }
    let completed_keys = keep.keys().cloned().collect();
    Ok((keep, completed_keys))
}

fn materialize_scan_checkpoint(
    etl: &RedditETL,
    query: &QuerySpec,
    show_progress: bool,
    limit: Option<u64>,
) -> Result<ScanCheckpoint> {
    let files = plan_pipeline_files(etl, Some(query))?;
    warn_if_unfiltered_undated_query(etl, query, &files);

    let work_dir = etl.ensure_work_dir()?;
    let fingerprint =
        build_resume_fingerprint(etl, query, ANALYTICS_CHECKPOINT_OPERATION, limit, &files)?;
    let checkpoint_dir = scan_checkpoint_dir(&work_dir, &fingerprint);
    crate::util::create_dir_all_with_default_backoff(&checkpoint_dir)
        .with_context(|| format!("creating scan checkpoint dir {}", checkpoint_dir.display()))?;
    let staging_dir = ensure_staging_dir(&checkpoint_dir)?;
    sweep_stale_inprogress(&checkpoint_dir, true)?;

    let (initial_months, completed_keys) =
        load_and_validate_scan_checkpoint_manifest(&checkpoint_dir, &fingerprint)?;
    crate::progress_manifest::save(&checkpoint_dir, &initial_months, Some(&fingerprint))?;
    let resumed_lines = committed_line_count(&initial_months);
    let accumulator =
        ManifestAccumulator::new(&checkpoint_dir, initial_months, Some(fingerprint.clone()));

    let total_bytes = total_compressed_size(&files);
    let pb = if show_progress && etl.opts.progress {
        Some(make_progress_bar_labeled(
            total_bytes,
            etl.opts.progress_label.as_deref(),
        ))
    } else {
        None
    };

    let parts = Mutex::new(Vec::<PathBuf>::new());
    {
        let mut guard = parts.lock().unwrap();
        for key in &completed_keys {
            guard.push(checkpoint_part_path(&checkpoint_dir, key));
        }
    }

    let total_written = AtomicU64::new(0);
    let targets = resolve_target_subs_from(&etl.opts.subreddit, &query.subreddits);
    let targets_ref = targets.as_ref();
    let bounds = bounds_tuple(etl.opts.start, etl.opts.end);
    let read_buf = etl.opts.read_buffer_bytes;
    let write_buf = etl.opts.write_buffer_bytes;
    let record_limit = record_limit_from_with_claimed(limit, resumed_lines);
    let no_whitelist: Option<Vec<String>> = None;

    crate::concurrency::for_each_file_limited(
        &files,
        etl.opts.file_concurrency,
        |job| -> Result<()> {
            let ctx = MonthJobCtx {
                out_dir: &checkpoint_dir,
                staging_dir: &staging_dir,
                targets: targets_ref,
                query,
                whitelist: &no_whitelist,
                pb: pb.as_ref(),
                bounds,
                read_buf,
                write_buf,
                human_ts: false,
                whitelist_tracker: None,
                record_limit: record_limit.as_deref(),
                resume: true,
                completed_keys: &completed_keys,
                allow_partial: etl.opts.allow_partial,
                partial_reporter: Some(&etl.opts.partial_read_reporter),
            };
            let outcome = process_month(job, &ctx)?;
            if let Some(month) = outcome {
                total_written.fetch_add(month.lines, Ordering::Relaxed);
                parts.lock().unwrap().push(month.out_path.clone());
                commit_entry_to_manifest(&accumulator, month).context(
                    "failed to durably update scan checkpoint progress manifest after publishing month part",
                )?;
            }
            Ok(())
        },
    )?;

    if let Some(pb) = pb {
        pb.finish_with_message("done");
    }
    ensure_resume_manifest_durable(Some(&accumulator), "scan checkpoint")?;

    let mut parts = parts.into_inner().unwrap();
    parts.sort();
    parts.dedup();
    Ok(ScanCheckpoint {
        parts,
        matched_records: resumed_lines + total_written.load(Ordering::Relaxed),
    })
}

fn for_each_checkpoint_record<F>(parts: &[PathBuf], read_buf: usize, on_record: F) -> Result<()>
where
    F: Sync + Send + Fn(&MinimalRecord, &str) -> Result<()>,
{
    parts.par_iter().try_for_each(|path| -> Result<()> {
        let file = crate::util::open_with_default_backoff(path)
            .with_context(|| format!("opening scan checkpoint part {}", path.display()))?;
        let mut reader = BufReader::with_capacity(read_buf.max(8 * 1024), file);
        let mut buf = String::new();
        let mut line_number = 0_u64;
        loop {
            let n = crate::ndjson::read_line_capped(
                &mut reader,
                &mut buf,
                crate::ndjson::DEFAULT_MAX_LINE_BYTES,
                path,
            )
            .with_context(|| {
                format!(
                    "reading scan checkpoint part {} near line {}",
                    path.display(),
                    line_number + 1
                )
            })?;
            if n == 0 {
                break;
            }
            if buf.is_empty() {
                continue;
            }
            line_number += 1;
            let min =
                parse_minimal(&buf).map_err(|e| malformed_json_error(path, line_number, e))?;
            on_record(&min, &buf)?;
        }
        Ok(())
    })
}

fn copy_checkpoint_parts_to_jsonl(
    parts: &[PathBuf],
    out_path: &Path,
    write_buf: usize,
) -> Result<()> {
    let file = crate::util::create_with_default_backoff(out_path)
        .with_context(|| format!("creating matched checkpoint copy {}", out_path.display()))?;
    let mut writer = BufWriter::with_capacity(write_buf.max(8 * 1024), file);
    for part in parts {
        let file = crate::util::open_with_default_backoff(part)
            .with_context(|| format!("opening scan checkpoint part {}", part.display()))?;
        let mut reader = BufReader::new(file);
        std::io::copy(&mut reader, &mut writer)
            .with_context(|| format!("copying scan checkpoint part {}", part.display()))?;
    }
    writer.flush()?;
    Ok(())
}
