// Corpus scan-to-CSV/TSV extraction orchestration.
// Included into `pipeline_exec` by `mod.rs`; no public module boundary.

#[allow(clippy::too_many_arguments)]
fn extract_tabular_common(
    etl: &RedditETL,
    query: &QuerySpec,
    targets: Option<&Vec<String>>,
    out_path: &Path,
    fields: &[String],
    opts: TabularExportOptions,
    format: TabularFormat,
    limit: Option<u64>,
) -> Result<()> {
    let parallelism = etl.opts.parallelism;
    with_thread_pool(parallelism, || {
        let manifest_start = RunManifestStart::now();
        let files = plan_pipeline_files(etl, Some(query))?;
        warn_if_unfiltered_undated_query(etl, query, &files);

        let work_dir = etl.ensure_work_dir()?;
        let tmp_dir = extract_scratch_dir(&work_dir, format.tmp_dir_name(), out_path, false, None)?;
        crate::util::create_dir_all_with_default_backoff(&tmp_dir).with_context(|| {
            format!(
                "creating {} extract work dir {}",
                format.label(),
                tmp_dir.display()
            )
        })?;
        // Reclaim the per-month `.part_*.{csv,tsv}part` scratch on *every*
        // exit path — normal return, an early `Err` (notably the strict-
        // whitelist mismatch from `tracker.finalize()` below, which used to
        // return before any cleanup and leak a full projected per-month copy
        // on every `--strict-whitelist` failure), and a panic-unwind.
        let _scratch_guard = crate::util::ScratchGuard::new(tmp_dir.clone());
        let staging_dir = ensure_staging_dir(&tmp_dir)?;
        sweep_stale_inprogress(&tmp_dir, true)?;

        let selectors = parse_tabular_field_selectors(fields)?;
        let whitelist_tracker = Some(Arc::new(WhitelistMatchTracker::new(
            etl.opts.strict_whitelist,
            fields.iter().cloned(),
        )));
        let record_limit = record_limit_from(limit);
        let total_bytes = total_compressed_size(&files);
        let pb = if etl.opts.progress {
            Some(make_progress_bar_labeled(
                total_bytes,
                etl.opts.progress_label.as_deref(),
            ))
        } else {
            None
        };

        let bounds = bounds_tuple(etl.opts.start, etl.opts.end);
        let read_buf = etl.opts.read_buffer_bytes;
        let write_buf = etl.opts.write_buffer_bytes;
        let output_records = AtomicU64::new(0);

        crate::concurrency::for_each_file_limited(
            &files,
            etl.opts.file_concurrency,
            |job| -> Result<()> {
                if record_limit
                    .as_ref()
                    .is_some_and(|limit| limit.is_exhausted())
                {
                    return Ok(());
                }
                let key = export_part_key(job);
                let tmp_file = tabular_part_path(&tmp_dir, &key, format);
                let lines = match write_jsonl_atomic(&staging_dir, &tmp_file, write_buf, |w| {
                    let result = stream_tabular_job(
                        job,
                        w,
                        targets,
                        query,
                        fields,
                        &selectors,
                        format,
                        pb.clone(),
                        bounds,
                        read_buf,
                        whitelist_tracker.as_deref(),
                        etl.opts.allow_partial,
                        Some(&etl.opts.partial_read_reporter),
                        record_limit.as_deref(),
                    )?;
                    complete_stream_job(job, result)
                }) {
                    Ok(lines) => lines,
                    Err(e) if etl.opts.allow_partial && is_partial_scan_error(&e) => {
                        tracing::warn!(path=%job.path.display(), part=%tmp_file.display(), error=%e, "Skipping tabular export month after zstd decode error; staged part was discarded");
                        return Ok(());
                    }
                    Err(e) => return Err(e),
                };
                output_records.fetch_add(lines, Ordering::Relaxed);
                if lines == 0 {
                    let _ = crate::util::remove_with_short_backoff(&tmp_file);
                }
                Ok(())
            },
        )?;

        if let Some(tracker) = &whitelist_tracker {
            tracker.finalize()?;
        }
        if let Some(pb) = pb {
            pb.finish_with_message("done");
        }
        stitch_tabular_parts(&tmp_dir, out_path, fields, opts, format, write_buf)?;
        let manifest = scan_manifest_input(
            manifest_start,
            &format!("scan.extract_to_{}", format.label()),
            format.label(),
            etl,
            query,
            &files,
            limit,
            manifest_counts(&[("records_written", output_records.load(Ordering::Relaxed))]),
            None,
            None,
            serde_json::json!({
                "tabular_fields": fields,
                "header": opts.header,
            }),
        );
        maybe_write_run_manifest(
            etl.opts.emit_manifest,
            manifest,
            ManifestDestination::File(out_path.to_path_buf()),
        )?;
        // `_scratch_guard` removes `tmp_dir` when this closure scope ends.
        Ok(())
    })
}

/// Warn when an extract's `fields` argument overrides a previously configured
/// field whitelist.
///
/// `extract_to_csv`/`extract_to_tsv` install `fields` as the whitelist
/// unconditionally. If the caller already set one via
/// `RedditETL::whitelist_fields` / `ScanPlan::whitelist_fields`, that earlier
/// choice is silently dropped — two ways to say "which fields" with no signal
/// which won. Surface the override so it is not a silent footgun.
fn warn_tabular_whitelist_override(
    preset: &[String],
    authoritative: &[String],
    format: TabularFormat,
) {
    if preset != authoritative {
        tracing::warn!(
            preset = ?preset,
            authoritative = ?authoritative,
            "extract_to_{} ignores the field whitelist set via \
             RedditETL/ScanPlan::whitelist_fields; the `fields` argument is \
             authoritative and overrides it",
            format.label(),
        );
    }
}

impl ScanPlan {
    /// Export matching records as CSV. `fields` is the fixed top-level schema;
    /// missing fields render as empty cells.
    ///
    /// `fields` is authoritative: it always becomes the projection/whitelist
    /// for this export. Any whitelist previously configured via
    /// [`RedditETL::whitelist_fields`] / [`ScanPlan::whitelist_fields`] is
    /// overridden (a `tracing::warn!` is emitted if the two differ).
    pub fn extract_to_csv<I, S>(
        self,
        out_path: &Path,
        fields: I,
        opts: TabularExportOptions,
    ) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.extract_to_tabular(out_path, fields, opts, TabularFormat::Csv)
    }

    /// Export matching records as TSV. `fields` is the fixed top-level schema;
    /// missing fields render as empty cells. Values containing a literal tab
    /// are rejected because TSV has no standard escaping convention.
    ///
    /// `fields` is authoritative: it always becomes the projection/whitelist
    /// for this export. Any whitelist previously configured via
    /// [`RedditETL::whitelist_fields`] / [`ScanPlan::whitelist_fields`] is
    /// overridden (a `tracing::warn!` is emitted if the two differ).
    pub fn extract_to_tsv<I, S>(
        self,
        out_path: &Path,
        fields: I,
        opts: TabularExportOptions,
    ) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.extract_to_tabular(out_path, fields, opts, TabularFormat::Tsv)
    }

    fn extract_to_tabular<I, S>(
        self,
        out_path: &Path,
        fields: I,
        opts: TabularExportOptions,
        format: TabularFormat,
    ) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        if self.etl.opts.human_readable_timestamps {
            anyhow::bail!(
                "--human-timestamps is not supported for CSV/TSV export; use --format jsonl/json/spool/zst/partitioned-jsonl or omit the flag"
            );
        }
        if self.etl.opts.resume {
            anyhow::bail!(
                "--resume is not supported for CSV/TSV export; use --format jsonl/json/spool/zst/partitioned-jsonl or omit the flag"
            );
        }
        let fields = normalize_tabular_fields(fields)?;
        let mut scan = self;
        if let Some(preset) = &scan.etl.opts.whitelist_fields {
            warn_tabular_whitelist_override(preset, &fields, format);
        }
        scan.etl.opts.whitelist_fields = Some(fields.clone());
        let plan = scan.build()?;
        log_pseudo_user_filter(&plan.query);
        let targets = resolve_target_subs_from(&plan.etl.opts.subreddit, &plan.query.subreddits);
        extract_tabular_common(
            &plan.etl,
            &plan.query,
            targets.as_ref(),
            out_path,
            &fields,
            opts,
            format,
            plan.limit,
        )
    }
}
