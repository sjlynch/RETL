
impl RedditETL {
    pub fn resolve_parent_maps(
        &self,
        ids: &ParentIds,
        cache_dir: &Path,
        resume: bool,
    ) -> Result<ParentMaps> {
        // Surface a deferred ConfigBuildError (e.g. a backwards date range from
        // `with_date_range`) before planning, so the parents resolver fails
        // fast with the "invalid date range" message instead of the confusing
        // "planned zero corpus files" context below.
        self.opts.check_config()?;
        with_thread_pool(self.opts.parallelism, || {
            let comments_out = cache_dir.join("comments");
            let submissions_out = cache_dir.join("submissions");
            crate::util::create_dir_all_with_default_backoff(&comments_out).with_context(|| {
                format!(
                    "create comments parent cache dir {}",
                    comments_out.display()
                )
            })?;
            crate::util::create_dir_all_with_default_backoff(&submissions_out).with_context(|| {
                format!(
                    "create submissions parent cache dir {}",
                    submissions_out.display()
                )
            })?;

            // Sweep crash leftovers under `<cache>/_staging/`: shard writers
            // stage there now (see `build_id_shard_index`), and any file owned
            // by a no-longer-running PID is a partial flush from a prior
            // crashed run that should not be promoted.
            sweep_stale_inprogress(&comments_out, true)?;
            sweep_stale_inprogress(&submissions_out, true)?;

            let discovered =
                discover_all_checked(&self.opts.comments_dir, &self.opts.submissions_dir)?;
            let files = plan_files_checked(
                &discovered,
                &self.opts.comments_dir,
                &self.opts.submissions_dir,
                crate::config::Sources::Both,
                self.opts.start,
                self.opts.end,
            )
            .with_context(|| {
                format!(
                    "resolve_parent_maps planned zero corpus files for resolver range {:?}..={:?}",
                    self.opts.start, self.opts.end
                )
            })?;
            // Report corpus holes inside the resolution window. A child whose
            // `parent_id` lives in a missing month resolves as `unresolved`;
            // without this the only signal is the generic unresolved-rate
            // warning, whose "use a larger --window-months" remedy is
            // misleading when the real cause is a gap inside the window.
            warn_resolver_window_gaps(&discovered, self.opts.start, self.opts.end);

            let parent_ids_fp = parent_ids_fingerprint(ids)?;
            let resolution_range = attach_resolution_range(self.opts.start, self.opts.end);
            let payload_spec = self.opts.parent_payload_spec.clone();

            let total_bytes = total_compressed_size(&files);
            let pb = if self.opts.progress {
                Some(make_progress_bar_labeled(
                    total_bytes,
                    self.opts.progress_label.as_deref(),
                ))
            } else {
                None
            };

            let (comment_shards, submission_shards) = build_id_shard_index(
                &files,
                ids,
                &parent_ids_fp,
                &resolution_range,
                &payload_spec,
                &comments_out,
                &submissions_out,
                resume,
                self.opts.read_buffer_bytes,
                self.opts.write_buffer_bytes,
                self.opts.file_concurrency,
                pb.as_ref(),
            )?;

            if let Some(pb) = pb {
                let final_msg = if let Some(l) = self.opts.progress_label.as_deref() {
                    format!("{l} done")
                } else {
                    "done".to_string()
                };
                pb.finish_with_message(final_msg);
            }

            // `build_id_shard_index` only ever adds shards. Prune any cache
            // JSON for a month outside the current `files` set so a narrowed
            // re-run against an existing `--cache` does not leave (or resolve
            // against) shards the user dropped from the window.
            prune_stale_resolver_shards(&comments_out, "RC", &comment_shards)?;
            prune_stale_resolver_shards(&submissions_out, "RS", &submission_shards)?;

            // Much stricter: only eager-load when plenty of RAM is free.
            let eager_ok = available_memory_fraction() > 0.50;

            let mut comments_map: HashMap<String, String> = HashMap::new();
            let mut submissions_map: HashMap<String, (String, String)> = HashMap::new();

            if eager_ok && payload_spec.is_legacy_default() {
                // Gate the eager load to exactly the shards recorded in the
                // shard index — the in-window months only. Globbing the cache
                // dir instead would merge stale out-of-window `.json` files
                // left by a prior wider run into the eager map, silently
                // resolving against months the user excluded.
                let sorted_shard_paths = |shards: &HashMap<YearMonth, PathBuf>| -> Vec<PathBuf> {
                    let mut v: Vec<PathBuf> = shards.values().cloned().collect();
                    v.sort();
                    v
                };

                // The eager load can `break` mid-way when memory runs low.
                // Correctness is preserved — `load_shard_value` falls through
                // to per-shard reads for whatever was skipped — but *which*
                // months end up eager is then nondeterministic, so emit a
                // warn naming shards loaded vs. skipped. Without it, a user
                // debugging why attach is slow on one run and fast on the
                // next gets no signal.
                let comment_shard_paths = sorted_shard_paths(&comment_shards);
                let comment_shard_total = comment_shard_paths.len();
                for (idx, p) in comment_shard_paths.iter().enumerate() {
                    let f = crate::util::open_with_default_backoff(p)?;
                    let r = BufReader::new(f);
                    let m: HashMap<String, String> = serde_json::from_reader(r)?;
                    for (k, v) in m {
                        comments_map.insert(k, v);
                    }
                    if is_low_memory(0.10) {
                        let loaded = idx + 1;
                        tracing::warn!(
                            cache = "comments",
                            shards_loaded = loaded as u64,
                            shards_skipped = (comment_shard_total - loaded) as u64,
                            shards_total = comment_shard_total as u64,
                            "parent resolver eager comment-map load truncated under memory pressure; skipped shards fall back to per-shard reads at attach time"
                        );
                        break;
                    }
                }
                let submission_shard_paths = sorted_shard_paths(&submission_shards);
                let submission_shard_total = submission_shard_paths.len();
                for (idx, p) in submission_shard_paths.iter().enumerate() {
                    let f = crate::util::open_with_default_backoff(p)?;
                    let r = BufReader::new(f);
                    let m: HashMap<String, (String, String)> = serde_json::from_reader(r)?;
                    for (k, v) in m {
                        submissions_map.insert(k, v);
                    }
                    if is_low_memory(0.10) {
                        let loaded = idx + 1;
                        tracing::warn!(
                            cache = "submissions",
                            shards_loaded = loaded as u64,
                            shards_skipped = (submission_shard_total - loaded) as u64,
                            shards_total = submission_shard_total as u64,
                            "parent resolver eager submission-map load truncated under memory pressure; skipped shards fall back to per-shard reads at attach time"
                        );
                        break;
                    }
                }
            }

            Ok(ParentMaps {
                comments: comments_map,
                submissions: submissions_map,
                comment_shards: Some(comment_shards),
                submission_shards: Some(submission_shards),
                payload_spec,
            })
        })
    }
}
