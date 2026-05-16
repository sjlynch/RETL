
/// Shared scan+filter+emit loop for [`ScanPlan`] methods.
///
/// Encapsulates file discovery/planning, optional progress-bar setup, the
/// `for_each_file_limited` per-file fan-out, and the line-level
/// `parse_minimal` → `matches_minimal` → `within_bounds` →
/// (optional) `matches_full` filtering ladder. The caller owns whatever
/// accumulator the matched records feed into and supplies it through
/// interior mutability inside `on_record`.
///
/// Callers are responsible for entering `with_thread_pool` themselves when
/// they want bounded parallelism that *also* covers post-scan reductions like
/// `dedup` / `reduce_sum` (which use rayon `par_iter` internally). Methods
/// that historically don't enter a scoped pool — e.g. `count_by_month`,
/// `build_first_seen_index_to_tsv` — call this helper directly so the
/// per-file fan-out runs on the global rayon pool, matching the prior shape.
fn scan_records<F>(
    etl: &RedditETL,
    query: &QuerySpec,
    show_progress: bool,
    limit: Option<u64>,
    on_record: F,
) -> Result<()>
where
    F: Sync + Send + Fn(&MinimalRecord, FileKind, &str) -> Result<()>,
{
    let targets = resolve_target_subs_from(&etl.opts.subreddit, &query.subreddits);
    let targets_ref = targets.as_ref();
    let bounds = bounds_tuple(etl.opts.start, etl.opts.end);
    let read_buf = etl.opts.read_buffer_bytes;
    let record_limit = record_limit_from(limit);
    if record_limit.as_ref().is_some_and(|limit| limit.is_zero()) {
        return Ok(());
    }

    let files = plan_pipeline_files(etl, Some(query))?;
    warn_if_unfiltered_undated_query(etl, query, &files);

    let pb = if show_progress && etl.opts.progress {
        let total_bytes = total_compressed_size(&files);
        Some(make_progress_bar_labeled(
            total_bytes,
            etl.opts.progress_label.as_deref(),
        ))
    } else {
        None
    };

    let fanout = crate::concurrency::for_each_file_limited(
        &files,
        etl.opts.file_concurrency,
        |job| -> Result<()> {
            if record_limit
                .as_ref()
                .is_some_and(|limit| limit.is_exhausted())
            {
                return Ok(());
            }
            let kind = job.kind;
            let mut line_number: u64 = 0;
            let line_cb = |line: &str| -> Result<()> {
                line_number += 1;
                let min = match parse_minimal(line) {
                    Ok(min) => min,
                    Err(_) => match serde_json::from_str::<serde_json::Value>(line) {
                        Ok(_) => return Ok(()),
                        Err(e) => return Err(malformed_json_error(&job.path, line_number, e)),
                    },
                };
                if !matches_minimal(&min, targets_ref, query, kind) {
                    return Ok(());
                }
                if !within_bounds(&min, bounds) {
                    return Ok(());
                }
                if query.requires_full_parse() {
                    let val: serde_json::Value = serde_json::from_str(line)
                        .map_err(|e| malformed_json_error(&job.path, line_number, e))?;
                    if !matches_full(&val, kind, query) {
                        return Ok(());
                    }
                }
                claim_record_or_stop(record_limit.as_deref())?;
                on_record(&min, kind, line)?;
                Ok(())
            };
            let partial_read_policy = if etl.opts.allow_partial {
                PartialReadPolicy::AllowPartial
            } else {
                PartialReadPolicy::Strict
            };
            let mut progress_cb = pb.as_ref().map(|pb| move |delta| pb.inc(delta));
            let mut skip_cb = |path: &Path, err: &anyhow::Error| {
                etl.opts.partial_read_reporter.record(path, err);
            };
            for_each_line_with_opts_status(
                &job.path,
                LineStreamOpts {
                    read_buf_bytes: Some(read_buf),
                    progress: progress_cb.as_mut().map(|cb| cb as &mut dyn FnMut(u64)),
                    on_skip: etl
                        .opts
                        .allow_partial
                        .then_some(&mut skip_cb as &mut dyn FnMut(&Path, &anyhow::Error)),
                    partial_read_policy,
                    ..Default::default()
                },
                line_cb,
            )?;
            Ok(())
        },
    );
    match fanout {
        Ok(()) => {}
        Err(e) if is_record_limit_reached(&e) => {}
        Err(e) => return Err(e),
    }

    if let Some(pb) = pb {
        pb.finish_with_message("done");
    }
    Ok(())
}
