
impl RedditETL {
    pub fn collect_parent_ids_from_jsonls<I>(&self, jsonl_paths: I) -> Result<ParentIds>
    where
        I: IntoIterator<Item = PathBuf>,
    {
        let paths: Vec<PathBuf> = jsonl_paths.into_iter().collect();
        if paths.is_empty() {
            return Ok(ParentIds::new());
        }

        with_thread_pool(self.opts.parallelism, || {
            let work_dir = self.ensure_work_dir()?;
            let scratch_root = IdScratchRoot::create(&work_dir)?;

            // Fewer shards reduces disk thrash later; we keep a wider FIFO in memory.
            let shard_count = MAX_SHARDS;
            let t1_writer = IdShardWriter::create(scratch_root.clone(), "t1", shard_count)?;
            let t3_writer = IdShardWriter::create(scratch_root, "t3", shard_count)?;

            let total_bytes: u64 = paths
                .iter()
                .map(|p| fs::metadata(p).map(|m| m.len()).unwrap_or(0))
                .sum();
            let pb = if self.opts.progress {
                Some(make_progress_bar_labeled(
                    total_bytes,
                    self.opts.progress_label.as_deref(),
                ))
            } else {
                None
            };

            let read_buf = self.opts.read_buffer_bytes;

            let parent_ref_count = AtomicUsize::new(0);

            paths.par_iter().try_for_each(|p| -> Result<()> {
                let f = crate::util::open_with_default_backoff(p)
                    .with_context(|| format!("open parent-id spool input {}", p.display()))?;
                let mut r = BufReader::with_capacity(read_buf, f);
                let mut buf = String::with_capacity(64 * 1024);
                let mut line_number = 0u64;
                loop {
                    let n = read_line_capped(&mut r, &mut buf, DEFAULT_MAX_LINE_BYTES, p)
                        .with_context(|| {
                            format!(
                                "read parent-id spool input {} near line {}",
                                p.display(),
                                line_number + 1
                            )
                        })?;
                    if n == 0 {
                        break;
                    }
                    line_number += 1;
                    if buf.is_empty() {
                        if let Some(pb) = &pb {
                            pb.inc(n as u64);
                        }
                        continue;
                    }

                    let v: Value = serde_json::from_str(&buf)
                        .map_err(|e| malformed_json_error(p, line_number, e))?;
                    if let Some(parent_id) = v.get("parent_id").and_then(|x| x.as_str()) {
                        parent_ref_count.fetch_add(1, Ordering::Relaxed);
                        if let Some(rest) = parent_id.strip_prefix("t1_") {
                            t1_writer.write(rest)?;
                        } else if let Some(rest) = parent_id.strip_prefix("t3_") {
                            t3_writer.write(rest)?;
                        }
                    }
                    if let Some(link_id) = v.get("link_id").and_then(|x| x.as_str()) {
                        parent_ref_count.fetch_add(1, Ordering::Relaxed);
                        if let Some(rest) = link_id.strip_prefix("t3_") {
                            t3_writer.write(rest)?;
                        }
                    }

                    if let Some(pb) = &pb {
                        pb.inc(n as u64);
                    }
                }
                Ok(())
            })?;

            let t1_shards = t1_writer.dedup()?;
            let t3_shards = t3_writer.dedup()?;

            if let Some(pb) = pb {
                let final_msg = if let Some(l) = self.opts.progress_label.as_deref() {
                    format!("{l} done")
                } else {
                    "done".to_string()
                };
                pb.finish_with_message(final_msg);
            }

            let parent_refs = parent_ref_count.load(Ordering::Relaxed);
            if parent_refs == 0 {
                tracing::warn!(
                    input_files = paths.len(),
                    "collect_parent_ids_from_jsonls found zero parent_id/link_id fields across the spool; parents pipeline requires parent_id and link_id to survive any --whitelist/.whitelist_fields"
                );
            }

            Ok(ParentIds::from_shards(t1_shards, t3_shards))
        })
    }
}
