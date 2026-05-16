
fn dedupe_key_label(key: &KeyExtractor) -> String {
    match key {
        KeyExtractor::JsonPointer(ptr) => format!("json:{ptr}"),
        KeyExtractor::AuthorLowerFast => "author".to_string(),
        KeyExtractor::SubredditLowerFast => "subreddit".to_string(),
        KeyExtractor::ByValue(_) => "<custom>".to_string(),
    }
}

fn warn_dedupe_key_drops(key: &KeyExtractor, summary: &DedupeKeySummary) {
    if summary.matched_records == 0 || summary.key_extractions_failed == 0 {
        return;
    }
    let drop_rate = summary.key_drop_rate();
    if drop_rate > DEDUPE_KEY_DROP_WARN_RATE {
        let key_spec = dedupe_key_label(key);
        tracing::warn!(
            key = %key_spec,
            matched_records = summary.matched_records,
            key_extractions_failed = summary.key_extractions_failed,
            drop_rate,
            "dedupe dropped {} matching record(s) without an extractable key ({:.2}% of matches); use --strict-key to fail instead",
            summary.key_extractions_failed,
            drop_rate * 100.0,
        );
    }
}

fn dedupe_cfg_from_options(opts: &ETLOptions) -> DedupeCfg {
    crate::config::warn_if_inflight_pair_pathological(opts.inflight_bytes, opts.inflight_groups);
    let mut cfg = DedupeCfg::from(opts);
    if cfg.inflight_bytes > 0 {
        let per_flush_mb = (cfg.inflight_bytes / 2 / (1024 * 1024)).max(1);
        cfg.min_buf_mb = cfg.min_buf_mb.min(per_flush_mb);
        cfg.max_buf_mb = cfg.max_buf_mb.min(per_flush_mb.max(cfg.min_buf_mb));
    }
    cfg
}

fn warn_dedupe_zero_keys(key: &KeyExtractor, input_records: u64) {
    match key {
        KeyExtractor::JsonPointer(ptr) => {
            let key_spec = format!("json:{ptr}");
            tracing::warn!(
                key = %key_spec,
                input_records,
                "--key {key_spec} matched nothing; check the pointer; the value may not be a scalar"
            );
        }
        KeyExtractor::AuthorLowerFast => tracing::warn!(
            key = "author",
            input_records,
            "--key author extracted zero keys from matching input records"
        ),
        KeyExtractor::SubredditLowerFast => tracing::warn!(
            key = "subreddit",
            input_records,
            "--key subreddit extracted zero keys from matching input records"
        ),
        KeyExtractor::ByValue(_) => tracing::warn!(
            key = "<custom>",
            input_records,
            "custom dedupe key extractor extracted zero keys from matching input records"
        ),
    }
}

impl ScanPlan {
    /// Write the distinct keys from matching records as one key per line.
    ///
    /// The scan/filter pass first spools matching raw JSONL records into the
    /// configured work directory, then reuses the public sorted-run dedupe
    /// engine (`build_runs_sorted` + `merge_runs_sorted`) with the supplied
    /// [`KeyExtractor`]. Built-in `author` and `subreddit` extractors keep the
    /// `MinimalRecord` fast path; `json:/pointer` extractors parse full JSON
    /// only for key extraction. Matching records whose key extractor returns
    /// `Ok(None)` are omitted from the output; use
    /// [`ScanPlan::dedupe_keys_to_lines_with_stats`] to inspect that count.
    pub fn dedupe_keys_to_lines(self, key: &KeyExtractor, out_path: &Path) -> Result<u64> {
        Ok(self
            .dedupe_keys_to_lines_with_stats(key, out_path)?
            .unique_keys)
    }

    /// Like [`ScanPlan::dedupe_keys_to_lines`], but returns matched-record,
    /// unique-key, and missing-key counts for diagnostics.
    pub fn dedupe_keys_to_lines_with_stats(
        self,
        key: &KeyExtractor,
        out_path: &Path,
    ) -> Result<DedupeKeySummary> {
        let plan = self.build()?;
        let parallelism = plan.etl.opts.parallelism;
        with_thread_pool(parallelism, || {
            let manifest_start = RunManifestStart::now();
            let files = plan_pipeline_files(&plan.etl, Some(&plan.query))?;
            let work_dir = plan.etl.ensure_work_dir()?;
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let tmp_dir = work_dir.join(format!("dedupe_{}_{}", std::process::id(), unique));
            crate::util::create_dir_all_with_default_backoff(&tmp_dir)
                .with_context(|| format!("creating dedupe work dir {}", tmp_dir.display()))?;

            let result = (|| -> Result<DedupeKeySummary> {
                let input = tmp_dir.join("matched.ndjson");
                let matched_total = if plan.etl.opts.resume {
                    let checkpoint = materialize_scan_checkpoint(
                        &plan.etl,
                        &plan.query,
                        /*show_progress=*/ true,
                        plan.limit,
                    )?;
                    copy_checkpoint_parts_to_jsonl(
                        &checkpoint.parts,
                        &input,
                        plan.etl.opts.write_buffer_bytes,
                    )?;
                    checkpoint.matched_records
                } else {
                    let f = crate::util::create_with_default_backoff(&input)
                        .with_context(|| format!("create {}", input.display()))?;
                    let writer = Mutex::new(BufWriter::with_capacity(
                        plan.etl.opts.write_buffer_bytes,
                        f,
                    ));
                    let matched_records = AtomicU64::new(0);

                    scan_records(
                        &plan.etl,
                        &plan.query,
                        /*show_progress=*/ true,
                        plan.limit,
                        |_min, _kind, line| {
                            matched_records.fetch_add(1, Ordering::Relaxed);
                            let mut w = writer
                                .lock()
                                .map_err(|_| anyhow!("dedupe writer lock poisoned"))?;
                            w.write_all(line.as_bytes())?;
                            w.write_all(b"\n")?;
                            Ok(())
                        },
                    )?;
                    writer
                        .into_inner()
                        .map_err(|_| anyhow!("dedupe writer lock poisoned"))?
                        .flush()?;
                    matched_records.load(Ordering::Relaxed)
                };

                let key_extractions_failed = AtomicU64::new(0);
                let cfg = dedupe_cfg_from_options(&plan.etl.opts);

                let runs_dir = tmp_dir.join("runs");
                let runs = build_runs_sorted_with_key_stats(
                    &input,
                    &runs_dir,
                    key,
                    &cfg,
                    Some(&key_extractions_failed),
                )?;
                let failed_after_build = key_extractions_failed.load(Ordering::Relaxed);
                if plan.etl.opts.strict_key && failed_after_build > 0 {
                    let summary = DedupeKeySummary {
                        matched_records: matched_total,
                        unique_keys: 0,
                        key_extractions_failed: failed_after_build,
                    };
                    if matched_total > 0 && runs.is_empty() {
                        warn_dedupe_zero_keys(key, matched_total);
                    }
                    warn_dedupe_key_drops(key, &summary);
                    anyhow::bail!(
                        "--strict-key: {} of {} matching record(s) did not contain extractable key {}; aborting dedupe output",
                        failed_after_build,
                        matched_total,
                        dedupe_key_label(key),
                    );
                }

                let mut unique_count = 0_u64;
                merge_runs_sorted_with_key_stats(
                    &runs,
                    out_path,
                    key,
                    &cfg,
                    |current_key, _group, w| {
                        w.write_all(current_key.as_bytes())?;
                        w.write_all(b"\n")?;
                        unique_count += 1;
                        Ok(())
                    },
                    Some(&key_extractions_failed),
                )?;

                let key_extractions_failed = key_extractions_failed.load(Ordering::Relaxed);
                let summary = DedupeKeySummary {
                    matched_records: matched_total,
                    unique_keys: unique_count,
                    key_extractions_failed,
                };
                if matched_total > 0 && unique_count == 0 {
                    warn_dedupe_zero_keys(key, matched_total);
                }
                warn_dedupe_key_drops(key, &summary);

                let manifest = scan_manifest_input(
                    manifest_start,
                    "scan.dedupe_keys_to_lines",
                    "text-lines",
                    &plan.etl,
                    &plan.query,
                    &files,
                    plan.limit,
                    manifest_counts(&[
                        ("matched_records", summary.matched_records),
                        ("unique_keys", summary.unique_keys),
                        ("key_extractions_failed", summary.key_extractions_failed),
                    ]),
                    None,
                    None,
                    serde_json::json!({ "dedupe_key": dedupe_key_label(key) }),
                );
                maybe_write_run_manifest(
                    plan.etl.opts.emit_manifest,
                    manifest,
                    ManifestDestination::File(out_path.to_path_buf()),
                )?;

                Ok(summary)
            })();

            let _ = crate::util::remove_dir_all_with_short_backoff(&tmp_dir);
            result
        })
    }
}

// -------- Original operations (back-compat) --------
