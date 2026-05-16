
fn export_part_key(job: &FileJob) -> String {
    let prefix = match job.kind {
        FileKind::Comment => "RC",
        FileKind::Submission => "RS",
    };
    crate::progress_manifest::month_key(prefix, job.ym)
}

fn partitioned_resume_operation(format: ExportFormat) -> &'static str {
    match format {
        ExportFormat::Jsonl => "partitioned-jsonl",
        ExportFormat::Zst => "partitioned-zst",
    }
}

fn partitioned_ext(format: ExportFormat) -> &'static str {
    match format {
        ExportFormat::Jsonl => "jsonl",
        ExportFormat::Zst => "zst",
    }
}

fn partitioned_output_path(out_base_dir: &Path, job: &FileJob, format: ExportFormat) -> PathBuf {
    let (dir, prefix) = match job.kind {
        FileKind::Comment => ("comments", "RC"),
        FileKind::Submission => ("submissions", "RS"),
    };
    out_base_dir
        .join(dir)
        .join(format!("{}_{}.{}", prefix, job.ym, partitioned_ext(format)))
}

fn partitioned_output_path_for_key(
    out_base_dir: &Path,
    key: &str,
    format: ExportFormat,
) -> Option<PathBuf> {
    let dir = if key.starts_with("RC_") {
        "comments"
    } else if key.starts_with("RS_") {
        "submissions"
    } else {
        return None;
    };
    Some(
        out_base_dir
            .join(dir)
            .join(format!("{}.{}", key, partitioned_ext(format))),
    )
}

fn partitioned_key_from_output_name(
    name: &str,
    key_prefix: &str,
    format: ExportFormat,
) -> Option<String> {
    let prefix = format!("{key_prefix}_");
    let suffix = format!(".{}", partitioned_ext(format));
    let ym = name.strip_prefix(&prefix)?.strip_suffix(&suffix)?;
    ym.parse::<YearMonth>().ok()?;
    Some(format!("{key_prefix}_{ym}"))
}

fn clear_partitioned_resume_outputs(out_base_dir: &Path, format: ExportFormat) -> Result<()> {
    remove_matching_files(&out_base_dir.join("comments"), |name| {
        partitioned_key_from_output_name(name, "RC", format).is_some()
    })?;
    remove_matching_files(&out_base_dir.join("submissions"), |name| {
        partitioned_key_from_output_name(name, "RS", format).is_some()
    })?;
    Ok(())
}

fn prune_partitioned_outputs_except(
    out_base_dir: &Path,
    format: ExportFormat,
    keep_keys: &HashSet<String>,
) -> Result<()> {
    remove_matching_files(&out_base_dir.join("comments"), |name| {
        match partitioned_key_from_output_name(name, "RC", format) {
            Some(key) => !keep_keys.contains(&key),
            None => false,
        }
    })?;
    remove_matching_files(&out_base_dir.join("submissions"), |name| {
        match partitioned_key_from_output_name(name, "RS", format) {
            Some(key) => !keep_keys.contains(&key),
            None => false,
        }
    })?;
    Ok(())
}

fn validate_partitioned_resume_output(
    path: &Path,
    format: ExportFormat,
    expected: &MonthEntry,
) -> Result<()> {
    if expected.lines == 0 && !path.exists() {
        return Ok(());
    }
    let meta = fs::metadata(path)
        .with_context(|| format!("stat partitioned output {}", path.display()))?;
    if meta.len() != expected.size {
        anyhow::bail!(
            "partitioned output size mismatch for {}: expected {}, got {}",
            path.display(),
            expected.size,
            meta.len()
        );
    }
    match format {
        ExportFormat::Jsonl => {
            let actual = validate_jsonl_part(path)?;
            if actual.lines != expected.lines {
                anyhow::bail!(
                    "partitioned JSONL line-count mismatch for {}: expected {}, got {}",
                    path.display(),
                    expected.lines,
                    actual.lines
                );
            }
        }
        ExportFormat::Zst => {
            crate::zstd_jsonl::validate_zst_full(path)
                .with_context(|| format!("validating partitioned zst {}", path.display()))?;
        }
    }
    Ok(())
}

fn load_and_validate_partitioned_manifest(
    out_base_dir: &Path,
    fingerprint: &str,
    format: ExportFormat,
    planned_keys: &HashSet<String>,
) -> Result<(HashMap<String, MonthEntry>, HashSet<String>)> {
    let manifest = crate::progress_manifest::load(out_base_dir);
    if manifest.fingerprint.as_deref() != Some(fingerprint) && !manifest.months.is_empty() {
        tracing::warn!(
            path=%crate::progress_manifest::manifest_path(out_base_dir).display(),
            stored=?manifest.fingerprint,
            current=%fingerprint,
            "partitioned resume manifest fingerprint does not match current query/config; discarding partitioned outputs"
        );
        clear_partitioned_resume_outputs(out_base_dir, format)?;
        return Ok((HashMap::new(), HashSet::new()));
    }

    let mut keep: HashMap<String, MonthEntry> = HashMap::new();
    for (key, entry) in manifest.months {
        if !planned_keys.contains(&key) {
            tracing::info!(key=%key, "dropping partitioned progress entry outside current plan");
            continue;
        }
        let Some(path) = partitioned_output_path_for_key(out_base_dir, &key, format) else {
            tracing::info!(key=%key, "dropping unrecognized partitioned progress entry");
            continue;
        };
        match validate_partitioned_resume_output(&path, format, &entry) {
            Ok(()) => {
                keep.insert(key, entry);
            }
            Err(e) => {
                tracing::info!(key=%key, path=%path.display(), error=%e, "dropping stale partitioned progress entry; month will be re-run");
            }
        }
    }
    let completed_keys: HashSet<String> = keep.keys().cloned().collect();
    Ok((keep, completed_keys))
}
