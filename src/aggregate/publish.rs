
impl RedditETL {
    /// Build per-file aggregation shards in parallel, then merge into `final_out`.
    /// - Always rebuilds shards in a fresh per-run directory under `shards_dir`
    ///   (aggregate has no resume behavior).
    /// - `pretty == true` field-indents the final JSON output (shards are compact).
    /// - Mid-file JSONL read errors are strict by default: the partial input is
    ///   reported and not merged. Use
    ///   [`Self::aggregate_jsonls_parallel_collect_with_policy`] with
    ///   [`AggregatePartialReadPolicy::MergePartial`] to explicitly opt into
    ///   historical tolerant merging.
    ///
    /// Returns an [`AggregateBuildReport`] naming clean, partial, and fatal
    /// inputs. If every non-empty input fails to produce a merged shard, no
    /// final output is published and an error is returned. When
    /// [`ETLOptions::aggregate_strict`](crate::ETLOptions::aggregate_strict) is
    /// set (via [`RedditETL::aggregate_strict`]), *any* fatal input fails the
    /// whole run before publishing — a mistyped path in a multi-file batch can
    /// no longer slip through as a partial success.
    pub fn aggregate_jsonls_parallel<A: Aggregator>(
        &self,
        inputs: Vec<PathBuf>,
        shards_dir: &Path,
        final_out: &Path,
        pretty: bool,
    ) -> Result<AggregateBuildReport> {
        let manifest_start = RunManifestStart::now();
        let input_count = inputs.len();
        let manifest_inputs = inputs.clone();
        let (total, report) = self.aggregate_jsonls_parallel_collect::<A>(inputs, shards_dir)?;
        if input_count > 0 && report.merged_shards == 0 && report.problem_count() > 0 {
            anyhow::bail!(
                "aggregate failed: {} of {} input(s) failed or were partial; 0 shard(s) merged",
                report.problem_count(),
                input_count
            );
        }
        // Strict mode: any fatal input fails the whole run before publishing,
        // so a mistyped input path in a 12-file batch can't silently produce a
        // successful 11/12 result. Each fatal input was already named via
        // `tracing::warn!` during shard build.
        if self.opts.aggregate_strict && report.fatal_count() > 0 {
            anyhow::bail!(
                "aggregate strict mode: {} of {} input(s) failed during shard build; no output published (clear --strict / aggregate_strict to merge the surviving shards)",
                report.fatal_count(),
                input_count
            );
        }

        // Atomically publish the final output through `<out-parent>/_staging`
        // so an interrupted run never leaves a half-written `final_out` in
        // place and concurrent runs never share a fixed temp path.
        write_at_path_atomic(final_out, AGGREGATE_WRITE_BUF_BYTES, |w| {
            if pretty {
                serde_json::to_writer_pretty(w, &total)?;
            } else {
                serde_json::to_writer(w, &total)?;
            }
            Ok(())
        })
        .with_context(|| format!("publishing aggregate output {}", final_out.display()))?;

        let mut counts = BTreeMap::new();
        counts.insert("input_files".to_string(), input_count as u64);
        counts.insert("ok_inputs".to_string(), report.ok_inputs.len() as u64);
        counts.insert("partial_inputs".to_string(), report.partial_count() as u64);
        counts.insert("fatal_inputs".to_string(), report.fatal_count() as u64);
        counts.insert("merged_shards".to_string(), report.merged_shards as u64);
        counts.insert("records_ingested".to_string(), report.records_ingested);
        let mut manifest = RunManifestInput::new("aggregate_jsonls_parallel");
        manifest.start = manifest_start;
        manifest.api_operation = Some("RedditETL::aggregate_jsonls_parallel".to_string());
        manifest.query = serde_json::Value::Null;
        manifest.options = serde_json::json!({
            "pretty": pretty,
            "partial_read_policy": "strict",
            "aggregate_strict": self.opts.aggregate_strict,
            "shards_dir": crate::run_manifest::path_to_stable_string(shards_dir),
            "parallelism": self.opts.parallelism,
            "progress": self.opts.progress,
            "emit_manifest": self.opts.emit_manifest,
        });
        manifest.inputs = file_identities(&manifest_inputs);
        manifest.output_format = "json".to_string();
        manifest.counts = counts;
        manifest.upstream_manifests = discover_upstream_manifests_from_inputs(&manifest_inputs);
        maybe_write_run_manifest(
            self.opts.emit_manifest,
            manifest,
            ManifestDestination::File(final_out.to_path_buf()),
        )?;

        Ok(report)
    }

    /// Build per-file aggregation shards in a fresh per-run directory under
    /// `shards_dir`, merge them, and return the merged aggregate state to the
    /// caller instead of publishing JSON.
    ///
    /// Uses [`AggregatePartialReadPolicy::Strict`], so partial reads are
    /// reported but not merged.
    pub fn aggregate_jsonls_parallel_collect<A: Aggregator>(
        &self,
        inputs: Vec<PathBuf>,
        shards_dir: &Path,
    ) -> Result<(A, AggregateBuildReport)> {
        self.aggregate_jsonls_parallel_collect_with::<A, _>(inputs, shards_dir, A::default)
    }

    /// Like [`Self::aggregate_jsonls_parallel_collect`], but uses `make_agg` to
    /// construct per-input shard states. This lets callers provide runtime
    /// configuration while still using the [`Aggregator`] merge contract.
    ///
    /// Uses [`AggregatePartialReadPolicy::Strict`], so partial reads are
    /// reported but not merged.
    pub fn aggregate_jsonls_parallel_collect_with<A, F>(
        &self,
        inputs: Vec<PathBuf>,
        shards_dir: &Path,
        make_agg: F,
    ) -> Result<(A, AggregateBuildReport)>
    where
        A: Aggregator,
        F: Fn() -> A + Send + Sync,
    {
        self.aggregate_jsonls_parallel_collect_with_policy(
            inputs,
            shards_dir,
            make_agg,
            AggregatePartialReadPolicy::Strict,
        )
    }

    /// Like [`Self::aggregate_jsonls_parallel_collect_with`], but lets library
    /// callers opt into merging partial-read shards.
    pub fn aggregate_jsonls_parallel_collect_with_policy<A, F>(
        &self,
        inputs: Vec<PathBuf>,
        shards_dir: &Path,
        make_agg: F,
        partial_policy: AggregatePartialReadPolicy,
    ) -> Result<(A, AggregateBuildReport)>
    where
        A: Aggregator,
        F: Fn() -> A + Send + Sync,
    {
        crate::util::create_dir_all_with_default_backoff(shards_dir)
            .with_context(|| format!("creating shards_dir {}", shards_dir.display()))?;
        let run_token = aggregate_run_token();
        let run_dir = aggregate_run_dir(shards_dir, &run_token);
        crate::util::create_dir_all_with_default_backoff(&run_dir)
            .with_context(|| format!("creating aggregate run directory {}", run_dir.display()))?;
        let staging_dir = ensure_staging_dir(&run_dir)?;
        let shard_paths = shard_names_for_inputs(&run_dir, &run_token, &inputs);

        // The per-run shard directory is pure scratch: every shard is folded
        // into `total`, after which it serves no purpose. Aggregate
        // historically accreted one `run_*` directory of shard JSON per
        // `retl aggregate` run (hundreds of MB over a 12-year spool).
        //
        // A `ScratchGuard` reclaims it on *every* exit path from the block
        // below — a normal return, a `?`-propagated error inside
        // `with_thread_pool`, and (critically) a panic unwinding out of
        // `merge_aggregator_shards_parallel`, `load_shard`, or a non-
        // associative user `Aggregator::merge` impl. The old
        // `if outcome.is_ok()` check missed the panic case: the panic unwound
        // straight past it and `run_*` scratch silently accreted.
        //
        // Only an explicit `Err` outcome disarms the guard, preserving the
        // documented post-mortem policy — a failed merge leaves its shards
        // behind for inspection. A panic is a different failure class and is
        // *always* cleaned up.
        let mut run_scratch = crate::util::ScratchGuard::new(run_dir);

        let outcome = with_thread_pool(self.opts.parallelism, || {
            let BuildOutcome { shards, report } = build_aggregate_shards_with::<A, F>(
                &inputs,
                &shard_paths,
                &staging_dir,
                self.opts.progress,
                &make_agg,
                partial_policy,
            );

            let pb_merge = if self.opts.progress {
                Some(make_count_progress(
                    shards.len() as u64,
                    "Aggregate: merge shards",
                ))
            } else {
                None
            };
            let total: A = merge_aggregator_shards_parallel(&shards, pb_merge.as_ref())?;
            if let Some(pb) = pb_merge {
                pb.finish_with_message("Aggregate: merge done");
            }

            Ok((total, report))
        });

        // A run that merged shards but ingested zero records is otherwise
        // presented as a successful aggregation with no signal. That is almost
        // always a wrong input path or an empty spool directory, so warn once
        // here — covering every caller (publish and collect, library and CLI).
        if let Ok((_, report)) = &outcome {
            if report.ingested_zero_records() {
                tracing::warn!(
                    merged_shards = report.merged_shards,
                    "aggregate merged {} shard(s) but ingested 0 record(s); the result is an empty aggregation — verify the inputs point at non-empty JSONL",
                    report.merged_shards
                );
            }
        }

        // Success and panic both let `run_scratch` clean up on drop. Only a
        // returned `Err` is left for post-mortem inspection.
        if outcome.is_err() {
            run_scratch.disarm();
        }

        outcome
    }
}
