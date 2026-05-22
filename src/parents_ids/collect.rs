
/// Scale parent-id shard fan-out to the total input size.
///
/// `IdShardWriter::create` eagerly creates one `<kind>_ids_NNNN.tmp` scratch
/// file per shard, and `IdShardWriter::dedup` later spawns one rayon task per
/// shard. Pinning the count to `MAX_SHARDS` makes a tiny spool create hundreds
/// of mostly-empty files (×2 for the `t1`/`t3` writers) and schedule hundreds
/// of trivial dedup tasks — on a slow or networked `work_dir` that allocation
/// dominates runtime. One shard per 32 MiB of input, clamped to
/// `[1, MAX_SHARDS]`, keeps large corpora at full fan-out while a small input
/// gets only a handful of shards.
fn parent_id_shard_count(total_bytes: u64) -> usize {
    const BYTES_PER_SHARD: u64 = 32 * 1024 * 1024;
    let scaled = total_bytes / BYTES_PER_SHARD + 1;
    (scaled as usize).clamp(1, MAX_SHARDS)
}

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

            let total_bytes: u64 = paths
                .iter()
                .map(|p| fs::metadata(p).map(|m| m.len()).unwrap_or(0))
                .sum();

            // Size shard fan-out to the input rather than always opening
            // `2 * MAX_SHARDS` scratch files — see `parent_id_shard_count`.
            let shard_count = parent_id_shard_count(total_bytes);
            let t1_writer = IdShardWriter::create(scratch_root.clone(), "t1", shard_count)?;
            let t3_writer = IdShardWriter::create(scratch_root, "t3", shard_count)?;

            let pb = if self.opts.progress {
                Some(make_progress_bar_labeled(
                    total_bytes,
                    self.opts.progress_label.as_deref(),
                ))
            } else {
                None
            };

            let read_buf = self.opts.read_buffer_bytes;

            // Counts *records* that carry at least one parent reference, not
            // raw field occurrences: a comment almost always has both
            // `parent_id` and `link_id`, so the old per-field tally was
            // ~2×records and meaningless to report.
            let records_with_ref = AtomicUsize::new(0);
            // Records whose `parent_id`/`link_id` field was present but in a
            // form RETL cannot use (bare/numeric/non-`t1_`/`t3_`). These
            // contribute zero IDs, so a spool dominated by them yields an
            // empty `ParentIds` even though `records_with_ref > 0` — the
            // generic "zero fields" warning would then be wrong, so they are
            // tracked separately for the distinct warning emitted below.
            let records_with_unprefixed_ref = AtomicUsize::new(0);

            // Route through `for_each_file_limited` so `--file-concurrency`
            // bounds in-flight readers on this stage exactly as it does on the
            // resolve and attach stages; an unbounded `par_iter` here would
            // silently ignore the knob the run manifest records as applied.
            crate::concurrency::for_each_file_limited(&paths, self.opts.file_concurrency, |p| -> Result<()> {
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
                    let mut record_has_ref = false;
                    let mut record_has_usable_id = false;
                    if let Some(parent_id) = v.get("parent_id").and_then(|x| x.as_str()) {
                        record_has_ref = true;
                        if let Some(rest) = parent_id.strip_prefix("t1_") {
                            t1_writer.write(rest)?;
                            record_has_usable_id = true;
                        } else if let Some(rest) = parent_id.strip_prefix("t3_") {
                            t3_writer.write(rest)?;
                            record_has_usable_id = true;
                        }
                    }
                    if let Some(link_id) = v.get("link_id").and_then(|x| x.as_str()) {
                        record_has_ref = true;
                        if let Some(rest) = link_id.strip_prefix("t3_") {
                            t3_writer.write(rest)?;
                            record_has_usable_id = true;
                        }
                    }
                    if record_has_ref {
                        records_with_ref.fetch_add(1, Ordering::Relaxed);
                        if !record_has_usable_id {
                            records_with_unprefixed_ref.fetch_add(1, Ordering::Relaxed);
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

            let records_with_ref = records_with_ref.load(Ordering::Relaxed);
            let records_with_unprefixed_ref = records_with_unprefixed_ref.load(Ordering::Relaxed);
            if records_with_ref == 0 {
                tracing::warn!(
                    input_files = paths.len(),
                    "collect_parent_ids_from_jsonls found zero parent_id/link_id fields across the spool; parents pipeline requires parent_id and link_id to survive any --whitelist/.whitelist_fields"
                );
            } else if records_with_unprefixed_ref * 2 >= records_with_ref {
                // Most (or all) records carried a `parent_id`/`link_id` but in
                // a non-Reddit form, so few or zero IDs were collected. This
                // is a distinct failure from the "zero fields" case above and
                // from a --whitelist that stripped the fields: the fields ARE
                // present, just not Reddit fullnames. Naming it here keeps the
                // caller's `bail_empty_parent_ids` (--whitelist diagnosis)
                // from being the only — and wrong — signal the user sees when
                // feeding their own non-Reddit JSONL.
                let rate = records_with_unprefixed_ref as f64 / records_with_ref as f64;
                tracing::warn!(
                    input_files = paths.len(),
                    records_with_parent_ref = records_with_ref,
                    records_with_unprefixed_ref,
                    unprefixed_ref_rate = %format!("{:.1}%", rate * 100.0),
                    "collect_parent_ids_from_jsonls: parent_id/link_id present but not t1_/t3_-prefixed — RETL expects Reddit fullnames (e.g. t1_abc123 for a comment, t3_xyz789 for a submission); a non-Reddit JSONL with bare/numeric IDs collects no parent references"
                );
            } else {
                tracing::info!(
                    input_files = paths.len(),
                    records_with_parent_ref = records_with_ref,
                    "collect_parent_ids_from_jsonls: records carrying a parent_id/link_id reference"
                );
            }

            Ok(ParentIds::from_shards(t1_shards, t3_shards))
        })
    }
}

#[cfg(test)]
mod collect_tests {
    use super::parent_id_shard_count;
    use crate::config::MAX_SHARDS;

    const MIB: u64 = 1024 * 1024;

    #[test]
    fn parent_id_shard_count_scales_with_input_size() {
        // A tiny input gets a single shard — not `2 * MAX_SHARDS` eagerly
        // created scratch files (one set per `t1`/`t3` writer).
        assert_eq!(parent_id_shard_count(0), 1);
        assert_eq!(parent_id_shard_count(1024), 1);
        assert_eq!(parent_id_shard_count(31 * MIB), 1);
        // One additional shard per 32 MiB of input.
        assert_eq!(parent_id_shard_count(32 * MIB), 2);
        assert_eq!(parent_id_shard_count(200 * MIB), 7);
        // A large corpus still saturates at the hard `MAX_SHARDS` cap.
        assert_eq!(parent_id_shard_count(64 * 1024 * MIB), MAX_SHARDS);
        assert!(parent_id_shard_count(u64::MAX) <= MAX_SHARDS);
    }
}
