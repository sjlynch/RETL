
impl RedditETL {
    pub fn resolve_parent_maps(
        &self,
        ids: &ParentIds,
        cache_dir: &Path,
        resume: bool,
    ) -> Result<ParentMaps> {
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

            // Much stricter: only eager-load when plenty of RAM is free.
            let eager_ok = available_memory_fraction() > 0.50;

            let mut comments_map: HashMap<String, String> = HashMap::new();
            let mut submissions_map: HashMap<String, (String, String)> = HashMap::new();

            if eager_ok && payload_spec.is_legacy_default() {
                let load = |dir: &Path| -> Vec<PathBuf> {
                    let mut v: Vec<PathBuf> = crate::util::read_dir_with_default_backoff(dir)
                        .map_err(|e| {
                            tracing::warn!(dir=%dir.display(), error=%e, "failed to eager-list parent cache shards");
                            e
                        })
                        .ok()
                        .into_iter()
                        .flat_map(|entries| entries.into_iter().map(|e| e.path()))
                        .filter(|p| {
                            p.file_name()
                                .and_then(|name| name.to_str())
                                .map(|name| {
                                    name.ends_with(".json")
                                        && !name.ends_with(RESOLVER_SIDECAR_SUFFIX)
                                        && !name.ends_with(INPROGRESS_EXT)
                                })
                                .unwrap_or(false)
                        })
                        .collect();
                    v.sort();
                    v
                };

                for p in load(&comments_out) {
                    let f = crate::util::open_with_default_backoff(&p)?;
                    let r = BufReader::new(f);
                    let m: HashMap<String, String> = serde_json::from_reader(r)?;
                    for (k, v) in m {
                        comments_map.insert(k, v);
                    }
                    if is_low_memory(0.10) {
                        break;
                    }
                }
                for p in load(&submissions_out) {
                    let f = crate::util::open_with_default_backoff(&p)?;
                    let r = BufReader::new(f);
                    let m: HashMap<String, (String, String)> = serde_json::from_reader(r)?;
                    for (k, v) in m {
                        submissions_map.insert(k, v);
                    }
                    if is_low_memory(0.10) {
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
