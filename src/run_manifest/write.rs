
pub fn write_run_manifest(
    input: RunManifestInput,
    destination: ManifestDestination,
) -> Result<PathBuf> {
    let (output_path, output_kind, manifest_path) = match destination {
        ManifestDestination::File(path) => {
            let manifest_path = manifest_path_for_file(&path);
            (path, "file".to_string(), manifest_path)
        }
        ManifestDestination::Directory(path) => {
            let manifest_path = manifest_path_for_directory(&path);
            (path, "directory".to_string(), manifest_path)
        }
    };

    let finished_at = now_rfc3339();
    let output = OutputSnapshot {
        path: path_to_stable_string(&output_path),
        kind: output_kind,
        format: input.output_format,
        manifest_path: path_to_stable_string(&manifest_path),
    };
    let resume = ResumeSnapshot {
        enabled: input.resume_enabled,
        checkpoint_fingerprint: input.checkpoint_fingerprint,
        checkpoint_path: input
            .checkpoint_path
            .as_ref()
            .map(|path| path_to_stable_string(path)),
    };

    let fingerprint = manifest_fingerprint(
        &input.operation,
        input.command.as_deref(),
        input.api_operation.as_deref(),
        &input.query,
        &input.options,
        &input.corpus,
        &input.inputs,
        &output,
        &input.counts,
        &resume,
    )?;

    let mut warnings = input.warnings;
    if input.partial_read.skipped_file_count > 0 {
        warnings.push(format!(
            "allow_partial skipped {} input file(s); see partial_read.skipped_files",
            input.partial_read.skipped_file_count
        ));
    }

    let manifest = RunManifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        generated_by: GeneratedBy {
            name: "retl",
            version: env!("CARGO_PKG_VERSION"),
            git_hash: git_hash(),
        },
        manifest_fingerprint: fingerprint,
        operation: input.operation,
        command: input.command,
        api_operation: input.api_operation,
        started_at: input.start.started_at,
        finished_at,
        duration_ms: input.start.instant.elapsed().as_millis(),
        query: input.query,
        options: input.options,
        corpus: input.corpus,
        inputs: input.inputs,
        output,
        counts: input.counts,
        partial_read: input.partial_read,
        resume,
        warnings,
        upstream_manifests: input.upstream_manifests,
    };

    let parent = manifest_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let staging_dir = ensure_staging_dir(parent)
        .with_context(|| format!("creating manifest staging dir under {}", parent.display()))?;
    write_jsonl_atomic(
        &staging_dir,
        &manifest_path,
        MANIFEST_WRITE_BUF_BYTES,
        |w| {
            serde_json::to_writer_pretty(&mut *w, &manifest)?;
            w.write_all(b"\n")?;
            Ok(())
        },
    )
    .with_context(|| format!("writing run manifest {}", manifest_path.display()))?;

    Ok(manifest_path)
}

pub(crate) fn maybe_write_run_manifest(
    emit: bool,
    input: RunManifestInput,
    destination: ManifestDestination,
) -> Result<Option<PathBuf>> {
    if !emit {
        return Ok(None);
    }
    write_run_manifest(input, destination).map(Some)
}

pub(crate) fn corpus_snapshot_from_etl(etl: &ETLOptions, files: &[FileJob]) -> CorpusSnapshot {
    CorpusSnapshot {
        base_dir: Some(path_to_stable_string(&etl.base_dir)),
        comments_dir: Some(path_to_stable_string(&etl.comments_dir)),
        submissions_dir: Some(path_to_stable_string(&etl.submissions_dir)),
        sources: Some(sources_label(etl.sources).to_string()),
        selected_files: selected_file_identities(files),
    }
}

pub(crate) fn selected_file_identities(files: &[FileJob]) -> Vec<FileIdentity> {
    let mut out: Vec<FileIdentity> = files
        .iter()
        .map(|job| {
            let kind = match job.kind {
                FileKind::Comment => "comment",
                FileKind::Submission => "submission",
            };
            file_identity_with_context(&job.path, Some(kind.to_string()), Some(job.ym.to_string()))
        })
        .collect();
    out.sort_by(|a, b| {
        a.kind
            .cmp(&b.kind)
            .then_with(|| a.month.cmp(&b.month))
            .then_with(|| a.path.cmp(&b.path))
    });
    out
}
