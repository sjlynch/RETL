
fn write_raw_line<W: Write + ?Sized>(writer: &mut W, line: &str, written: &mut u64) -> Result<()> {
    write_and_count(writer, line.as_bytes(), written)?;
    Ok(())
}

fn write_with_timestamps<W: Write + ?Sized>(
    writer: &mut W,
    line: &str,
    timestamp_buf: &mut String,
    written: &mut u64,
) -> Result<()> {
    rewrite_human_timestamps_bytes(line, timestamp_buf);
    write_and_count(writer, timestamp_buf.as_bytes(), written)?;
    Ok(())
}

fn write_with_whitelist<W: Write + ?Sized>(
    writer: &mut W,
    line: &str,
    fields: &[String],
    tokenizer: &WhitelistTokenizer,
    tokenizer_buf: &mut String,
    matched_indices: &mut Vec<usize>,
    human_timestamps: bool,
    written: &mut u64,
    path: &std::path::Path,
    line_number: u64,
) -> Result<bool> {
    // Preferred path: the streaming tokenizer copies raw value bytes verbatim
    // and never builds a `serde_json::Value`. If it rejects a structurally
    // surprising line, fall back to the slow Value path so correctness on odd
    // records is preserved.
    let tok_result = if human_timestamps {
        // Fused single-pass: project whitelisted keys AND rewrite the three
        // timestamp keys' integer values to RFC3339 in one walk over the raw
        // line bytes. Replaces the older tokenize_into →
        // rewrite_human_timestamps_bytes chain.
        tokenizer.tokenize_and_rewrite_timestamps_into_with_matches(
            line,
            tokenizer_buf,
            matched_indices,
        )
    } else {
        tokenizer.tokenize_into_with_matches(line, tokenizer_buf, matched_indices)
    };

    if tok_result.is_ok() {
        write_and_count(writer, tokenizer_buf.as_bytes(), written)?;
        return Ok(false);
    }

    write_via_value(
        writer,
        line,
        Some(fields),
        Some(matched_indices),
        human_timestamps,
        written,
        path,
        line_number,
    )?;
    Ok(true)
}

fn write_via_value<W: Write + ?Sized>(
    writer: &mut W,
    line: &str,
    whitelist: Option<&[String]>,
    mut matched_indices: Option<&mut Vec<usize>>,
    human_timestamps: bool,
    written: &mut u64,
    path: &std::path::Path,
    line_number: u64,
) -> Result<()> {
    if let Some(indices) = matched_indices.as_mut() {
        indices.clear();
    }
    let val: Value =
        serde_json::from_str(line).map_err(|e| malformed_json_error(path, line_number, e))?;
    let mut out_val = if let Some(fields) = whitelist {
        let mut obj = Map::new();
        if let Some(map) = val.as_object() {
            for (idx, k) in fields.iter().enumerate() {
                if let Some(v) = map.get(k) {
                    obj.insert(k.clone(), v.clone());
                    if let Some(indices) = matched_indices.as_mut() {
                        indices.push(idx);
                    }
                }
            }
        }
        Value::Object(obj)
    } else {
        val
    };

    if human_timestamps {
        apply_human_timestamps(&mut out_val);
    }

    serde_json::to_writer(&mut *writer, &out_val)?;
    writer.write_all(b"\n")?;
    *written += 1;
    Ok(())
}

#[doc(hidden)]
pub fn project_whitelist_line_for_tests(
    line: &str,
    fields: &[String],
    path: &std::path::Path,
    line_number: u64,
) -> Result<String> {
    let tokenizer = WhitelistTokenizer::new(fields.iter().map(|s| s.as_str()));
    let mut tokenizer_buf = String::new();
    let mut matched_indices = Vec::new();
    let mut out = Vec::new();
    let mut written = 0_u64;
    let _used_slow_path = write_with_whitelist(
        &mut out,
        line,
        fields,
        &tokenizer,
        &mut tokenizer_buf,
        &mut matched_indices,
        false,
        &mut written,
        path,
        line_number,
    )?;
    Ok(String::from_utf8(out)?.trim_end_matches('\n').to_string())
}

#[derive(Clone, Copy)]
enum StreamWritePath<'a> {
    Raw,
    Timestamps,
    Whitelist {
        fields: &'a [String],
        tokenizer: &'a WhitelistTokenizer,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamJobResult {
    pub written: u64,
    /// False when the zstd decoder reported corruption after delivering zero
    /// or more lines. Callers that publish resumable outputs must not commit
    /// such files as complete.
    pub complete: bool,
}

#[allow(dead_code)]
pub fn stream_job<W: Write + ?Sized>(
    job: &FileJob,
    writer: &mut W,
    targets: Option<&Vec<String>>,
    query: &QuerySpec,
    whitelist: &Option<Vec<String>>,
    pb: Option<ProgressBar>,
    bounds: Option<DateBounds>,
    read_buf_bytes: usize,
    human_timestamps: bool,
    whitelist_tracker: Option<&WhitelistMatchTracker>,
) -> Result<StreamJobResult> {
    stream_job_with_partial_policy(
        job,
        writer,
        targets,
        query,
        whitelist,
        pb,
        bounds,
        read_buf_bytes,
        human_timestamps,
        whitelist_tracker,
        false,
        None,
        None,
    )
}

pub(crate) fn stream_job_with_partial_policy<W: Write + ?Sized>(
    job: &FileJob,
    writer: &mut W,
    targets: Option<&Vec<String>>,
    query: &QuerySpec,
    whitelist: &Option<Vec<String>>,
    pb: Option<ProgressBar>,
    bounds: Option<DateBounds>,
    read_buf_bytes: usize,
    human_timestamps: bool,
    whitelist_tracker: Option<&WhitelistMatchTracker>,
    allow_partial: bool,
    partial_reporter: Option<&crate::config::PartialReadReporter>,
    record_limit: Option<&RecordLimit>,
) -> Result<StreamJobResult> {
    let mut written: u64 = 0;
    let mut ts_buf = String::new();
    let mut tok_buf = String::new();
    let mut matched_indices = Vec::new();

    // Build the streaming tokenizer once per file so the small key-set is
    // hashed exactly once and the buffers above are reused across every line.
    let tokenizer: Option<WhitelistTokenizer> = whitelist
        .as_ref()
        .map(|fields| WhitelistTokenizer::new(fields.iter().map(|s| s.as_str())));

    let write_path = match whitelist.as_deref() {
        None if human_timestamps => StreamWritePath::Timestamps,
        None => StreamWritePath::Raw,
        Some(fields) => StreamWritePath::Whitelist {
            fields,
            tokenizer: tokenizer
                .as_ref()
                .expect("whitelist tokenizer is built when fields are present"),
        },
    };

    let mut line_number: u64 = 0;
    let mut on_line = |line: &str| -> Result<()> {
        line_number += 1;
        let min = match parse_minimal(line) {
            Ok(min) => min,
            Err(_) => match serde_json::from_str::<Value>(line) {
                Ok(_) => return Ok(()),
                Err(e) => return Err(malformed_json_error(&job.path, line_number, e)),
            },
        };
        if !matches_minimal(&min, targets, query, job.kind) {
            return Ok(());
        }
        if !within_bounds(&min, bounds) {
            return Ok(());
        }
        if query.requires_full_parse() {
            let val: Value = serde_json::from_str(line)
                .map_err(|e| malformed_json_error(&job.path, line_number, e))?;
            if !matches_full(&val, job.kind, query) {
                return Ok(());
            }
        }

        claim_record_or_stop(record_limit)?;

        match write_path {
            StreamWritePath::Raw => write_raw_line(writer, line, &mut written),
            StreamWritePath::Timestamps => {
                write_with_timestamps(writer, line, &mut ts_buf, &mut written)
            }
            StreamWritePath::Whitelist { fields, tokenizer } => {
                let used_slow_path = write_with_whitelist(
                    writer,
                    line,
                    fields,
                    tokenizer,
                    &mut tok_buf,
                    &mut matched_indices,
                    human_timestamps,
                    &mut written,
                    &job.path,
                    line_number,
                )?;
                if let Some(tracker) = whitelist_tracker {
                    tracker.observe(WhitelistEmission {
                        matched_fields: &matched_indices,
                        used_slow_path,
                    })?;
                }
                Ok(())
            }
        }
    };

    let partial_read_policy = if allow_partial {
        PartialReadPolicy::AllowPartial
    } else {
        PartialReadPolicy::Strict
    };
    let mut progress_cb = pb.map(|pb| move |delta| pb.inc(delta));
    let mut skip_cb = |path: &std::path::Path, err: &anyhow::Error| {
        if let Some(reporter) = partial_reporter {
            reporter.record(path, err);
        }
    };
    let stream_result = for_each_line_with_opts_status(
        &job.path,
        LineStreamOpts {
            read_buf_bytes: Some(read_buf_bytes),
            progress: progress_cb.as_mut().map(|cb| cb as &mut dyn FnMut(u64)),
            on_skip: allow_partial
                .then_some(&mut skip_cb as &mut dyn FnMut(&std::path::Path, &anyhow::Error)),
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
