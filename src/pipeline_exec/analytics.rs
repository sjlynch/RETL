// `ScanPlan` analytics outputs: month histograms, author counts, and the
// first-seen index TSV. These three live together because they share the
// same `scan_records` / checkpoint replay shape and emit small summary
// outputs rather than full-record exports.

impl ScanPlan {
    /// Count matched records per calendar month.
    ///
    /// **Run-manifest exemption (deliberate).** Unlike its analytics siblings
    /// [`ScanPlan::author_counts_to_tsv`] and
    /// [`ScanPlan::build_first_seen_index_to_tsv`], this method does **not**
    /// build a `RunManifestStart` or call `maybe_write_run_manifest`. It
    /// returns an in-memory `BTreeMap` and has no output file, so there is no
    /// `ManifestDestination::File` to anchor a sidecar manifest (and hence no
    /// `partial_read` snapshot) to. Emitting provenance is therefore the
    /// caller's responsibility:
    ///
    /// - The `retl count --mode month` handler writes a CLI scan manifest
    ///   (including the partial-read snapshot) when `--out <file>` is given,
    ///   and always prints the partial-read report to stderr.
    /// - A resumed run (`--resume`) routes through `materialize_scan_checkpoint`,
    ///   which emits a loud end-of-run `tracing::warn!` summarizing any month
    ///   skipped by `--allow-partial` — so a `--resume --allow-partial` count
    ///   that silently dropped a month is still visible to a watcher.
    pub fn count_by_month(self) -> Result<BTreeMap<YearMonth, u64>> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        let parallelism = plan.etl.opts.parallelism;
        with_thread_pool(parallelism, || {
            let total = Mutex::new(BTreeMap::<YearMonth, u64>::new());
            if plan.etl.opts.resume {
                let checkpoint = materialize_scan_checkpoint(
                    &plan.etl,
                    &plan.query,
                    /*show_progress=*/ true,
                    plan.limit,
                )?;
                for_each_checkpoint_record(
                    &checkpoint.parts,
                    plan.etl.opts.read_buffer_bytes,
                    |min, _line| {
                        if let Some(ts) = min.created_utc {
                            let ym = ym_from_epoch(ts);
                            *total.lock().unwrap().entry(ym).or_insert(0) += 1;
                        }
                        Ok(())
                    },
                )?;
            } else {
                scan_records(
                    &plan.etl,
                    &plan.query,
                    /*show_progress=*/ true,
                    plan.limit,
                    |min, _kind, _line| {
                        if let Some(ts) = min.created_utc {
                            let ym = ym_from_epoch(ts);
                            *total.lock().unwrap().entry(ym).or_insert(0) += 1;
                        }
                        Ok(())
                    },
                )?;
            }
            Ok(total.into_inner().unwrap())
        })
    }

    pub fn author_counts_to_tsv(self, out_path: &Path) -> Result<()> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        let parallelism = plan.etl.opts.parallelism;
        with_thread_pool(parallelism, || {
            let manifest_start = RunManifestStart::now();
            let files = plan_pipeline_files(&plan.etl, Some(&plan.query))?;
            let work_dir = plan.etl.ensure_work_dir()?;
            let kv =
                ShardedKVWriter::create(&work_dir, "author_counts", plan.etl.opts.shard_count)?;
            let scratch_root = kv.scratch_root().to_path_buf();

            let result = (|| -> Result<()> {
                let matched_records = AtomicU64::new(0);
                if plan.etl.opts.resume {
                    let checkpoint = materialize_scan_checkpoint(
                        &plan.etl,
                        &plan.query,
                        /*show_progress=*/ true,
                        plan.limit,
                    )?;
                    for_each_checkpoint_record(
                        &checkpoint.parts,
                        plan.etl.opts.read_buffer_bytes,
                        |min, _line| {
                            if let Some(a) = min.author.as_deref() {
                                let a = a.trim();
                                if a.is_empty() {
                                    return Ok(());
                                }
                                matched_records.fetch_add(1, Ordering::Relaxed);
                                kv.write_kv(a, 1)?;
                            }
                            Ok(())
                        },
                    )?;
                } else {
                    scan_records(
                        &plan.etl,
                        &plan.query,
                        /*show_progress=*/ true,
                        plan.limit,
                        |min, _kind, _line| {
                            if let Some(a) = min.author.as_deref() {
                                let a = a.trim();
                                if a.is_empty() {
                                    return Ok(());
                                }
                                matched_records.fetch_add(1, Ordering::Relaxed);
                                kv.write_kv(a, 1)?;
                            }
                            Ok(())
                        },
                    )?;
                }

                let (shards, _scratch_root) = kv.reduce_sum_with_scratch("author_counts")?;
                concat_tsvs(&shards, out_path, plan.etl.opts.write_buffer_bytes)?;
                let output_rows = count_text_lines(out_path)?;
                let manifest = scan_manifest_input(
                    manifest_start,
                    "scan.author_counts_to_tsv",
                    "tsv",
                    &plan.etl,
                    &plan.query,
                    &files,
                    plan.limit,
                    manifest_counts(&[
                        ("matched_records", matched_records.load(Ordering::Relaxed)),
                        ("output_rows", output_rows),
                    ]),
                    None,
                    None,
                    serde_json::json!({}),
                );
                maybe_write_run_manifest(
                    plan.etl.opts.emit_manifest,
                    manifest,
                    ManifestDestination::File(out_path.to_path_buf()),
                )?;
                Ok(())
            })();
            cleanup_scratch_dir(&scratch_root, "author_counts");
            result
        })
    }

    pub fn build_first_seen_index_to_tsv(self, out_path: &Path) -> Result<()> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        let parallelism = plan.etl.opts.parallelism;
        with_thread_pool(parallelism, || {
            let manifest_start = RunManifestStart::now();
            let files = plan_pipeline_files(&plan.etl, Some(&plan.query))?;
            let work_dir = plan.etl.ensure_work_dir()?;
            let kv = ShardedKVWriter::create(&work_dir, "first_seen", plan.etl.opts.shard_count)?;
            let scratch_root = kv.scratch_root().to_path_buf();

            let result = (|| -> Result<()> {
                let matched_records = AtomicU64::new(0);
                if plan.etl.opts.resume {
                    let checkpoint = materialize_scan_checkpoint(
                        &plan.etl,
                        &plan.query,
                        /*show_progress=*/ true,
                        plan.limit,
                    )?;
                    for_each_checkpoint_record(
                        &checkpoint.parts,
                        plan.etl.opts.read_buffer_bytes,
                        |min, _line| {
                            if let (Some(a), Some(ts)) = (min.author.as_deref(), min.created_utc) {
                                let a = a.trim();
                                if a.is_empty() {
                                    return Ok(());
                                }
                                matched_records.fetch_add(1, Ordering::Relaxed);
                                kv.write_kv(a, ts)?;
                            }
                            Ok(())
                        },
                    )?;
                } else {
                    scan_records(
                        &plan.etl,
                        &plan.query,
                        /*show_progress=*/ true,
                        plan.limit,
                        |min, _kind, _line| {
                            if let (Some(a), Some(ts)) = (min.author.as_deref(), min.created_utc) {
                                let a = a.trim();
                                if a.is_empty() {
                                    return Ok(());
                                }
                                matched_records.fetch_add(1, Ordering::Relaxed);
                                kv.write_kv(a, ts)?;
                            }
                            Ok(())
                        },
                    )?;
                }

                let (shards, _scratch_root) = kv.reduce_min_with_scratch("first_seen")?;
                concat_tsvs(&shards, out_path, plan.etl.opts.write_buffer_bytes)?;
                let output_rows = count_text_lines(out_path)?;
                let manifest = scan_manifest_input(
                    manifest_start,
                    "scan.build_first_seen_index_to_tsv",
                    "tsv",
                    &plan.etl,
                    &plan.query,
                    &files,
                    plan.limit,
                    manifest_counts(&[
                        ("matched_records", matched_records.load(Ordering::Relaxed)),
                        ("output_rows", output_rows),
                    ]),
                    None,
                    None,
                    serde_json::json!({}),
                );
                maybe_write_run_manifest(
                    plan.etl.opts.emit_manifest,
                    manifest,
                    ManifestDestination::File(out_path.to_path_buf()),
                )?;
                Ok(())
            })();
            cleanup_scratch_dir(&scratch_root, "first_seen");
            result
        })
    }
}
