
impl RedditETL {
    /// Write resolved parent payloads for direct `t1_...` / `t3_...` IDs as
    /// JSONL. Each emitted object is the same payload shape used under
    /// attached records' `parent` key: selected/full parent fields plus RETL's
    /// `kind` (`comment`/`submission`) and bare `id` metadata. Unresolved IDs
    /// are counted but omitted from the output.
    pub fn write_resolved_parent_payloads_jsonl<I, S>(
        &self,
        prefixed_ids: I,
        out: &Path,
        parents: &ParentMaps,
    ) -> Result<ParentAttachStats>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let parent_dir = output_parent(out);
        crate::util::create_dir_all_with_default_backoff(parent_dir)
            .with_context(|| format!("create direct parent output dir {}", parent_dir.display()))?;
        sweep_stale_inprogress(parent_dir, true)?;

        let empty_parent_payloads: HashMap<String, ParentPayload> = HashMap::new();
        let ctx = AttachFileCtx {
            legacy_payload: parents.payload_spec.is_legacy_default(),
            parents_c_eager: &parents.comments,
            parents_s_eager: &parents.submissions,
            empty_parent_payloads: &empty_parent_payloads,
            comment_shards: parents.comment_shards.as_ref(),
            submission_shards: parents.submission_shards.as_ref(),
        };
        let mut caches = AttachWorkerCaches::new();

        write_at_path_atomic(
            out,
            self.opts.write_buffer_bytes,
            |w| -> Result<ParentAttachStats> {
                let mut stats = ParentAttachStats::default();
                for raw_id in prefixed_ids {
                    let raw_id = raw_id.as_ref().trim();
                    if !raw_id.starts_with("t1_") && !raw_id.starts_with("t3_") {
                        anyhow::bail!(
                            "direct parent ID is not prefixed with t1_ or t3_: {raw_id}"
                        );
                    }
                    let payload = resolve_parent_into_value(raw_id, None, &ctx, &mut caches)?;

                    if let Some(payload) = payload {
                        serde_json::to_writer(&mut *w, &payload)?;
                        w.write_all(b"\n")?;
                        stats.resolved += 1;
                    } else {
                        stats.unresolved += 1;
                    }
                }
                Ok(stats)
            },
        )
    }
}
