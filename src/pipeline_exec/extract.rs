
fn export_part_path(tmp_dir: &Path, key: &str) -> PathBuf {
    tmp_dir.join(format!(".part_{}.jsonl", key))
}

fn validate_jsonl_part(path: &Path) -> Result<MonthEntry> {
    let file = crate::util::open_with_default_backoff(path)
        .with_context(|| format!("opening extract resume part {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut buf = String::new();
    let mut lines = 0_u64;
    loop {
        let n = crate::ndjson::read_line_capped(
            &mut reader,
            &mut buf,
            crate::ndjson::DEFAULT_MAX_LINE_BYTES,
            path,
        )?;
        if n == 0 {
            break;
        }
        if buf.is_empty() {
            continue;
        }
        let _: serde_json::Value = serde_json::from_str(&buf)
            .with_context(|| format!("validating JSONL part {}", path.display()))?;
        lines += 1;
    }
    let size = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    Ok(MonthEntry {
        size,
        lines,
        sha256: None,
    })
}

impl ScanPlan {
    pub fn extract_to_jsonl(self, out_path: &Path) -> Result<()> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        let targets = resolve_target_subs_from(&plan.etl.opts.subreddit, &plan.query.subreddits);
        extract_common(
            &plan.etl,
            &plan.query,
            targets.as_ref(),
            "extract_jsonl_q_tmp",
            out_path,
            Finalize::Jsonl,
            "jsonl",
            "scan.extract_to_jsonl",
            plan.limit,
        )
    }

    pub fn extract_to_json(self, out_path: &Path, pretty: bool) -> Result<()> {
        let plan = self.build()?;
        log_pseudo_user_filter(&plan.query);
        let targets = resolve_target_subs_from(&plan.etl.opts.subreddit, &plan.query.subreddits);
        extract_common(
            &plan.etl,
            &plan.query,
            targets.as_ref(),
            "extract_json_q_tmp",
            out_path,
            Finalize::JsonArray { pretty },
            "json",
            "scan.extract_to_json",
            plan.limit,
        )
    }

    /// Write a single Apache Parquet file at `out_path`. Internally extracts
    /// to a temporary JSONL file (via [`Self::extract_to_jsonl`] — same
    /// resume/whitelist/progress semantics) and then converts that JSONL to
    /// Parquet in a second pass, publishing the `.parquet` atomically and
    /// discarding the intermediate.
    ///
    /// Requires the `parquet` cargo feature; without it the conversion step
    /// returns a build-flag error from [`crate::parquet_writer::write_parquet_atomic_if`].
    pub fn extract_to_parquet(self, out_path: &Path) -> Result<()> {
        let plan = self.clone_for_parquet_intermediate();
        let intermediate = parquet_intermediate_path(out_path)?;
        // Run extract → stitched JSONL at the intermediate path.
        self.extract_to_jsonl(&intermediate)?;
        let result = convert_jsonl_to_parquet(
            &intermediate,
            out_path,
            plan.opts.parquet_row_group_size,
            &plan.opts.parquet_compression,
            plan.opts.write_buffer_bytes,
            plan.opts.read_buffer_bytes,
        );
        // Always try to remove the intermediate JSONL — leaving it would
        // double on-disk footprint for every parquet export.
        if let Err(e) = crate::util::remove_with_short_backoff(&intermediate) {
            tracing::warn!(path=%intermediate.display(), error=%e, "failed to remove parquet intermediate JSONL");
        }
        result
    }

    /// Snapshot a tiny view of the [`RedditETL`] config the parquet two-pass
    /// path needs to read after [`Self::extract_to_jsonl`] consumes `self`.
    /// Only the parquet/io fields are copied — everything else has already
    /// been baked into the `ScanPlan`.
    fn clone_for_parquet_intermediate(&self) -> ParquetExtractSnapshot {
        ParquetExtractSnapshot {
            opts: ParquetExtractOpts {
                parquet_row_group_size: self.etl.opts.parquet_row_group_size,
                parquet_compression: self.etl.opts.parquet_compression.clone(),
                write_buffer_bytes: self.etl.opts.write_buffer_bytes,
                read_buffer_bytes: self.etl.opts.read_buffer_bytes,
            },
        }
    }
}

struct ParquetExtractOpts {
    parquet_row_group_size: usize,
    parquet_compression: String,
    write_buffer_bytes: usize,
    read_buffer_bytes: usize,
}

struct ParquetExtractSnapshot {
    opts: ParquetExtractOpts,
}

/// Derive the intermediate JSONL path for a parquet two-pass export. We sit
/// the intermediate next to the final destination so both files share a
/// filesystem (atomic rename works) and so a crash that leaves the
/// intermediate behind is easy for an operator to spot and reclaim.
fn parquet_intermediate_path(out_path: &Path) -> Result<PathBuf> {
    let parent = out_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let file_name = out_path
        .file_name()
        .ok_or_else(|| anyhow!("parquet output path has no file name: {}", out_path.display()))?;
    let mut name = std::ffi::OsString::from(".");
    name.push(file_name);
    name.push(format!(".retl-{}.jsonl-intermediate", std::process::id()));
    Ok(parent.join(name))
}

/// Stream a JSONL file through the parquet writer at `dest`. The JSONL input
/// is opened with the configured read buffer; the parquet writer stages and
/// atomically publishes onto `dest`.
fn convert_jsonl_to_parquet(
    jsonl_input: &Path,
    dest: &Path,
    row_group_size: usize,
    compression: &str,
    write_buf_bytes: usize,
    read_buf_bytes: usize,
) -> Result<()> {
    let parent = dest
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let staging = crate::atomic_write::ensure_staging_dir(&parent)?;
    let rows = crate::parquet_writer::write_parquet_atomic(
        &staging,
        dest,
        row_group_size,
        compression,
        write_buf_bytes,
        |w| {
            let mut emitted = 0_u64;
            crate::ndjson::for_each_jsonl_line_cfg(jsonl_input, read_buf_bytes, |line| {
                if line.is_empty() {
                    return Ok(());
                }
                w.write_all(line.as_bytes())?;
                w.write_all(b"\n")?;
                emitted += 1;
                Ok(())
            })?;
            Ok(emitted)
        },
    )?;
    tracing::info!(
        rows = rows,
        dest = %dest.display(),
        "extract_to_parquet: published {} row(s)",
        rows,
    );
    Ok(())
}

fn count_text_lines(path: &Path) -> Result<u64> {
    let file = crate::util::open_with_default_backoff(path)
        .with_context(|| format!("opening output to count lines {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut buf = String::new();
    let mut lines = 0_u64;
    loop {
        let n = crate::ndjson::read_line_capped(
            &mut reader,
            &mut buf,
            crate::ndjson::DEFAULT_MAX_LINE_BYTES,
            path,
        )?;
        if n == 0 {
            break;
        }
        if !buf.is_empty() {
            lines += 1;
        }
    }
    Ok(lines)
}
