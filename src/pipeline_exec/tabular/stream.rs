// Per-corpus-file scan-to-row streaming for tabular (CSV/TSV) extracts.
// Included into `pipeline_exec` by `mod.rs`; no public module boundary.

#[allow(clippy::too_many_arguments)]
fn stream_tabular_job<W: Write + ?Sized>(
    job: &FileJob,
    writer: &mut W,
    targets: Option<&Vec<String>>,
    query: &QuerySpec,
    fields: &[String],
    selectors: &[TabularFieldSelector],
    format: TabularFormat,
    pb: Option<ProgressBar>,
    bounds: Option<DateBounds>,
    read_buf_bytes: usize,
    whitelist_tracker: Option<&WhitelistMatchTracker>,
    allow_partial: bool,
    partial_reporter: Option<&crate::config::PartialReadReporter>,
    record_limit: Option<&RecordLimit>,
) -> Result<StreamJobResult> {
    let mut written = 0_u64;
    let mut line_number = 0_u64;
    let mut on_line = |line: &str| -> Result<()> {
        line_number += 1;
        let min = match parse_minimal(line) {
            Ok(min) => min,
            Err(_) => match serde_json::from_str::<Value>(line) {
                Ok(_) => return Ok(()),
                Err(e) => return Err(malformed_json_error(&job.path, line_number, e)),
            },
        };
        if !matches_minimal(&min, targets, query, job.kind) || !within_bounds(&min, bounds) {
            return Ok(());
        }
        // Parse the full JSON `Value` at most once per surviving record.
        // When the query needs a full parse, keep that `Value` and reuse it
        // for `tabular_cells_from_value` below instead of re-deserializing
        // the same line; otherwise parse lazily just before building cells.
        let prevalidated: Option<Value> = if query.requires_full_parse() {
            let val: Value = serde_json::from_str(line)
                .map_err(|e| malformed_json_error(&job.path, line_number, e))?;
            if !matches_full(&val, job.kind, query) {
                return Ok(());
            }
            Some(val)
        } else {
            None
        };
        claim_record_or_stop(record_limit)?;
        let val: Value = match prevalidated {
            Some(val) => val,
            None => serde_json::from_str(line)
                .map_err(|e| malformed_json_error(&job.path, line_number, e))?,
        };
        let (cells, matched_indices) = tabular_cells_from_value(&val, selectors)?;
        write_tabular_row(writer, fields, &cells, format).with_context(|| {
            format!(
                "writing {} row for {} line {}",
                format.label(),
                job.path.display(),
                line_number
            )
        })?;
        written += 1;
        if let Some(tracker) = whitelist_tracker {
            tracker.observe(crate::streaming::WhitelistEmission {
                matched_fields: &matched_indices,
                used_slow_path: false,
            })?;
        }
        Ok(())
    };

    let partial_read_policy = if allow_partial {
        PartialReadPolicy::AllowPartial
    } else {
        PartialReadPolicy::Strict
    };
    let mut progress_cb = pb.map(|pb| move |delta| pb.inc(delta));
    let mut skip_cb = |path: &Path, err: &anyhow::Error| {
        if let Some(reporter) = partial_reporter {
            reporter.record(path, err);
        }
    };
    let stream_result = for_each_line_with_opts_status(
        &job.path,
        LineStreamOpts {
            read_buf_bytes: Some(read_buf_bytes),
            progress: progress_cb.as_mut().map(|cb| cb as &mut dyn FnMut(u64)),
            on_skip: allow_partial.then_some(&mut skip_cb as &mut dyn FnMut(&Path, &anyhow::Error)),
            partial_read_policy,
            ..Default::default()
        },
        |s| on_line(s),
    );
    let complete = match stream_result {
        Ok(complete) => complete,
        Err(e) if is_record_limit_reached(&e) => true,
        Err(e) => return Err(e),
    };
    Ok(StreamJobResult { written, complete })
}
