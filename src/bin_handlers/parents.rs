
/// Default `--window-months` slack used by spool mode when the flag is
/// omitted. Direct-ID mode rejects the flag instead of defaulting it.
const DEFAULT_WINDOW_MONTHS: u32 = 3;

pub(crate) fn run_parents(args: ParentsArgs) -> Result<()> {
    let has_spool = args.spool.is_some();
    let has_direct_ids = !args.ids_file.is_empty() || !args.parent_id.is_empty();

    match (has_spool, has_direct_ids) {
        (true, true) => anyhow::bail!(
            "choose exactly one parents input mode: --spool <DIR> or --ids-file/--parent-id"
        ),
        (false, false) => {
            anyhow::bail!("parents requires an input mode: --spool <DIR> or --ids-file/--parent-id")
        }
        (true, false) => run_parents_spool(args),
        (false, true) => run_parents_direct(args),
    }
}

fn run_parents_spool(args: ParentsArgs) -> Result<()> {
    if args.start.is_some() || args.end.is_some() {
        anyhow::bail!("--start/--end are only used with direct parent IDs; spool mode uses --window-months around the discovered spool range");
    }
    if args.id_kind.is_some() {
        anyhow::bail!("--id-kind is only used with --ids-file/--parent-id direct-ID mode");
    }

    let window_months = args.window_months.unwrap_or(DEFAULT_WINDOW_MONTHS);
    let manifest_start = RunManifestStart::now();
    let spool = args
        .spool
        .as_ref()
        .expect("spool mode already validated --spool is present");
    let (spool_parts, min_ym, max_ym) = discover_spool_parts(spool)?;
    let manifest_spool_parts = spool_parts.clone();

    let mut wstart = min_ym;
    let mut wend = max_ym;
    for _ in 0..window_months {
        if let Some(p) = wstart.prev() {
            wstart = p;
        }
        if let Some(n) = wend.next() {
            wend = n;
        }
    }

    retl::create_dir_all_with_default_backoff(&args.out)
        .with_context(|| format!("creating output dir {}", args.out.display()))?;
    let lib_tmp = ensure_parent_work_dirs(&args)?;
    let parent_payload_spec = parent_payload_spec_from_args(&args);

    let ids = build_parent_etl(&args, &lib_tmp, &parent_payload_spec, None, None, None)
        .collect_parent_ids_from_jsonls(spool_parts.clone())?;
    if ids.is_empty() {
        return bail_empty_parent_ids(&spool_parts);
    }

    let parents = build_parent_etl(
        &args,
        &lib_tmp,
        &parent_payload_spec,
        Some(Sources::Both),
        Some(wstart),
        Some(wend),
    )
    .resolve_parent_maps(&ids, &args.cache, args.resume)?;
    let (attached, stats) = build_parent_etl(
        &args,
        &lib_tmp,
        &parent_payload_spec,
        None,
        Some(wstart),
        Some(wend),
    )
    .attach_parents_jsonls_parallel_with_stats(
        spool_parts,
        &args.out,
        &parents,
        args.resume,
    )?;

    if !args.no_manifest {
        let mut warnings = Vec::new();
        if stats.total() > 0 && stats.unresolved_rate() > 0.05 {
            warnings.push(format!(
                "more than 5% of parent lookups were unresolved ({:.2}%)",
                stats.unresolved_rate() * 100.0
            ));
        }
        let mut manifest = RunManifestInput::new("cli.parents");
        manifest.start = manifest_start;
        manifest.command = Some("retl parents".to_string());
        manifest.options = serde_json::json!({
            "spool": path_to_stable_string(spool),
            "cache": path_to_stable_string(&args.cache),
            "out": path_to_stable_string(&args.out),
            "data_dir": path_to_stable_string(&args.data_dir),
            "work_dir": path_to_stable_string(&args.work_dir),
            "resume": args.resume,
            "window_months": window_months,
            "resolved_range": { "start": wstart.to_string(), "end": wend.to_string() },
            "parent_payload": {
                "full_record": parent_payload_spec.is_full_record(),
                "fields": parent_payload_spec.fields(),
            },
            "parallelism": args.parallelism,
            "file_concurrency": args.file_concurrency,
            "inflight_bytes": args.inflight_bytes,
            "inflight_groups": args.inflight_groups,
            "progress": !args.no_progress,
            "emit_manifest": !args.no_manifest,
        });
        manifest.inputs = file_identities(&manifest_spool_parts);
        manifest.output_format = "jsonl-directory".to_string();
        manifest.counts = counts_map(&[
            ("input_files", manifest_spool_parts.len() as u64),
            ("attached_files", attached.len() as u64),
            ("parents_resolved", stats.resolved),
            ("parents_unresolved", stats.unresolved),
        ]);
        manifest.warnings = warnings;
        manifest.upstream_manifests = vec![upstream_manifest_for_directory(spool)];
        write_run_manifest(manifest, ManifestDestination::Directory(args.out.clone()))?;
    }

    if stats.total() > 0 && stats.unresolved_rate() > 0.05 {
        tracing::warn!(
            resolved = stats.resolved,
            unresolved = stats.unresolved,
            unresolved_rate = stats.unresolved_rate(),
            window_months,
            "more than 5% of parent lookups were unresolved; consider a larger --window-months"
        );
    }

    eprintln!(
        "Attached parents to {} file(s) in {} (resolved over {}..={}; parents resolved={}, unresolved={})",
        attached.len(),
        args.out.display(),
        wstart,
        wend,
        stats.resolved,
        stats.unresolved
    );
    Ok(())
}

fn run_parents_direct(args: ParentsArgs) -> Result<()> {
    // `--window-months` only shapes spool mode's discovered-range slack.
    // Direct-ID mode scopes its scan with `--start`/`--end` (or scans all
    // discovered months), so the flag would be silently ignored here. Reject
    // it, mirroring how `run_parents_spool` rejects `--start`/`--end`/`--id-kind`.
    if args.window_months.is_some() {
        anyhow::bail!(
            "--window-months only applies to --spool mode; direct parent-ID resolution scopes its scan with --start/--end (or scans all discovered months when omitted)"
        );
    }
    if let (Some(start), Some(end)) = (args.start, args.end) {
        if start > end {
            anyhow::bail!("invalid date range: start {start} is after end {end}");
        }
    }

    let (ids, ordered_prefixed) = collect_direct_parent_ids(&args)?;
    if ids.is_empty() || ordered_prefixed.is_empty() {
        anyhow::bail!("direct parent-ID mode received no usable IDs");
    }

    let lib_tmp = ensure_parent_work_dirs(&args)?;
    let parent_payload_spec = parent_payload_spec_from_args(&args);
    let resolver = build_parent_etl(
        &args,
        &lib_tmp,
        &parent_payload_spec,
        Some(Sources::Both),
        args.start,
        args.end,
    );
    let parents = resolver.resolve_parent_maps(&ids, &args.cache, args.resume)?;
    let stats =
        resolver.write_resolved_parent_payloads_jsonl(ordered_prefixed, &args.out, &parents)?;

    if stats.total() > 0 && stats.unresolved_rate() > 0.05 {
        tracing::warn!(
            resolved = stats.resolved,
            unresolved = stats.unresolved,
            unresolved_rate = stats.unresolved_rate(),
            "more than 5% of direct parent IDs were unresolved; consider --start/--end that cover the parent records or omit the range to scan all discovered months"
        );
    }

    eprintln!(
        "Resolved direct parent IDs to {} (cache {}, range {}; parents resolved={}, unresolved={})",
        args.out.display(),
        args.cache.display(),
        direct_resolution_range_label(args.start, args.end),
        stats.resolved,
        stats.unresolved
    );
    Ok(())
}
