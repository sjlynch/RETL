
/// Process a single monthly file and optionally tolerate zstd decode errors.
/// In strict mode (the default policy for corpus scans) decode errors are
/// returned. In `allow_partial` mode the file is logged, reported through
/// `on_skip`, and skipped.
pub fn process_file_for_usernames_with_skip(
    job: &FileJob,
    read_buf_bytes: usize,
    subreddit: &str,
    shard_writer: &ShardedWriter,
    pb: Option<ProgressBar>,
    allow_partial: bool,
    partial_reporter: Option<&crate::config::PartialReadReporter>,
    mut on_skip: impl FnMut(&std::path::Path, &anyhow::Error),
) -> Result<()> {
    let mut line_number: u64 = 0;
    let mut handle_line = |line: &str| -> Result<()> {
        line_number += 1;
        let min = match parse_minimal(line) {
            Ok(min) => min,
            Err(_) => match serde_json::from_str::<Value>(line) {
                Ok(_) => return Ok(()),
                Err(e) => return Err(malformed_json_error(&job.path, line_number, e)),
            },
        };
        if !matches_subreddit_basic(&min, subreddit) {
            return Ok(());
        }
        if let Some(author) = min.author.as_deref() {
            let a = author.trim();
            if a.is_empty() || a == "[deleted]" || a == "[removed]" {
                return Ok(());
            }
            shard_writer.write(a)?;
        }
        Ok(())
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
        on_skip(path, err);
    };
    for_each_line_with_opts_status(
        &job.path,
        LineStreamOpts {
            read_buf_bytes: Some(read_buf_bytes),
            progress: progress_cb.as_mut().map(|cb| cb as &mut dyn FnMut(u64)),
            on_skip: allow_partial
                .then_some(&mut skip_cb as &mut dyn FnMut(&std::path::Path, &anyhow::Error)),
            partial_read_policy,
            ..Default::default()
        },
        |s| handle_line(s),
    )?;
    Ok(())
}
