
/// Run an operation that writes to a file path, then stream the resulting
/// file to stdout and remove it. Used to honor `--out -` for APIs that only
/// know how to write to a `Path`.
pub(crate) fn stream_path_output_to_stdout(
    work_dir: &Path,
    temp_prefix: &str,
    file_stem: &str,
    write_output: impl FnOnce(&Path) -> Result<()>,
) -> Result<()> {
    let lib_tmp = work_dir.join("lib_tmp");
    retl::create_dir_all_with_default_backoff(&lib_tmp)
        .with_context(|| format!("creating work_dir {}", lib_tmp.display()))?;
    let tmp_path = lib_tmp.join(format!(
        "retl_{}_{}_{}",
        temp_prefix,
        std::process::id(),
        file_stem
    ));
    let _ = retl::remove_with_short_backoff(&tmp_path);

    let result = write_output(&tmp_path);

    if let Err(e) = result {
        let _ = retl::remove_with_short_backoff(&tmp_path);
        return Err(e);
    }

    let copy_result = (|| -> Result<()> {
        let mut f = retl::open_with_default_backoff(&tmp_path)
            .with_context(|| format!("opening {temp_prefix} tempfile {}", tmp_path.display()))?;
        let stdout = io::stdout();
        let mut w = stdout.lock();
        io::copy(&mut f, &mut w)
            .with_context(|| format!("streaming {temp_prefix} output to stdout"))?;
        w.flush()?;
        Ok(())
    })();

    let _ = retl::remove_with_short_backoff(&tmp_path);
    copy_result
}

/// Run an extraction that writes to a file path, then stream the resulting
/// file to stdout and remove it.
pub(crate) fn stream_extract_to_stdout(
    work_dir: &Path,
    file_stem: &str,
    extract: impl FnOnce(&Path) -> Result<()>,
) -> Result<()> {
    stream_path_output_to_stdout(work_dir, "export", file_stem, extract)
}

/// Discover spool parts in `dir`, parsing `part_RC_YYYY-MM.jsonl` and
/// `part_RS_YYYY-MM.jsonl` filenames. Returns `(sorted_paths, min, max)`.
pub(crate) fn discover_spool_parts(dir: &Path) -> Result<(Vec<PathBuf>, YearMonth, YearMonth)> {
    let entries = retl::read_dir_with_default_backoff(dir)
        .with_context(|| format!("reading spool dir {}", dir.display()))?;
    let mut parts: Vec<(YearMonth, PathBuf)> = Vec::new();
    for e in entries {
        let name = e.file_name().to_string_lossy().into_owned();
        let stem = name
            .strip_prefix("part_RC_")
            .or_else(|| name.strip_prefix("part_RS_"))
            .and_then(|s| s.strip_suffix(".jsonl"));
        if let Some(stem) = stem {
            if let Ok(ym) = stem.parse::<YearMonth>() {
                parts.push((ym, e.path()));
            }
        }
    }
    if parts.is_empty() {
        anyhow::bail!(
            "no part_RC_YYYY-MM.jsonl or part_RS_YYYY-MM.jsonl files found in {}",
            dir.display()
        );
    }
    parts.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    let min_ym = parts.iter().map(|(ym, _)| *ym).min().unwrap();
    let max_ym = parts.iter().map(|(ym, _)| *ym).max().unwrap();
    Ok((parts.into_iter().map(|(_, p)| p).collect(), min_ym, max_ym))
}
