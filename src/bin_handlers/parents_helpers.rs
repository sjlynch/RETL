
fn bail_empty_parent_ids(spool_parts: &[PathBuf]) -> Result<()> {
    let observed = match first_spool_record_keys(spool_parts)? {
        Some((path, keys)) => format!(
            "first record in {} contained keys: [{}]",
            path.display(),
            keys.join(",")
        ),
        None => "the discovered spool parts contained no JSON records".to_string(),
    };

    anyhow::bail!(
        "parents pipeline found no usable parent references in the spool: no `t1_`/`t3_` `parent_id` or `link_id` values were collected. This usually means the spool was produced with --whitelist/.whitelist_fields that omitted `parent_id` and `link_id`; {observed}. Re-run export/spool with --whitelist including body,parent_id,link_id (body is optional for matching but preserves the child comment text)."
    );
}

fn parent_payload_spec_from_args(args: &ParentsArgs) -> ParentPayloadSpec {
    if args.parent_full {
        if !args.parent_fields.is_empty() {
            tracing::warn!(
                parent_fields = ?args.parent_fields,
                "--parent-full overrides --parent-fields; the supplied --parent-fields list is ignored and the full parent record is attached"
            );
        }
        ParentPayloadSpec::full_record()
    } else if args.parent_fields.is_empty() {
        ParentPayloadSpec::default()
    } else {
        ParentPayloadSpec::from_fields(&args.parent_fields)
    }
}

fn ensure_parent_work_dirs(args: &ParentsArgs) -> Result<PathBuf> {
    retl::create_dir_all_with_default_backoff(&args.cache)
        .with_context(|| format!("creating cache dir {}", args.cache.display()))?;
    retl::create_dir_all_with_default_backoff(&args.work_dir)
        .with_context(|| format!("creating work_dir {}", args.work_dir.display()))?;
    let lib_tmp = args.work_dir.join("lib_tmp");
    retl::create_dir_all_with_default_backoff(&lib_tmp)
        .with_context(|| format!("creating work_dir {}", lib_tmp.display()))?;
    Ok(lib_tmp)
}

fn build_parent_etl(
    args: &ParentsArgs,
    lib_tmp: &Path,
    parent_payload_spec: &ParentPayloadSpec,
    sources: Option<Sources>,
    start: Option<YearMonth>,
    end: Option<YearMonth>,
) -> RedditETL {
    let mut etl = RedditETL::new()
        .base_dir(&args.data_dir)
        .work_dir(lib_tmp)
        .parent_payload_spec(parent_payload_spec.clone())
        .progress(!args.no_progress)
        .run_manifest(!args.no_manifest);
    if let Some(s) = sources {
        etl = etl.sources(s);
    }
    if start.is_some() || end.is_some() {
        etl = etl.date_range(start, end);
    }
    if let Some(p) = args.parallelism {
        etl = etl.parallelism(p);
    }
    if let Some(fc) = args.file_concurrency {
        etl = etl.file_concurrency(fc);
    }
    if let Some(b) = args.inflight_bytes {
        etl = etl.inflight_bytes(b);
    }
    if let Some(g) = args.inflight_groups {
        etl = etl.inflight_groups(g);
    }
    etl
}

fn validate_parent_bare_id(raw: &str, source: &str) -> Result<String> {
    let id = raw.trim();
    if id.is_empty() {
        anyhow::bail!("empty parent ID in {source}");
    }
    if id.chars().any(char::is_whitespace) {
        anyhow::bail!("parent ID in {source} contains whitespace: {id:?}");
    }
    Ok(id.to_string())
}

fn unsupported_fullname_prefix(id: &str) -> Option<String> {
    let (prefix, _) = id.split_once('_')?;
    let bytes = prefix.as_bytes();
    if bytes.len() == 2 && bytes[0] == b't' && bytes[1].is_ascii_digit() {
        Some(format!("{prefix}_"))
    } else {
        None
    }
}

fn direct_parent_kind_prefix(kind: ParentIdKindArg) -> &'static str {
    match kind {
        ParentIdKindArg::Comment => "t1_",
        ParentIdKindArg::Submission => "t3_",
    }
}

fn direct_parent_kind_label(kind: ParentIdKindArg) -> &'static str {
    match kind {
        ParentIdKindArg::Comment => "comment",
        ParentIdKindArg::Submission => "submission",
    }
}

/// Reject a `t1_`/`t3_`-prefixed direct ID whose prefix contradicts an
/// explicit `--id-kind`.
///
/// Without this check `retl parents --parent-id t1_abc --id-kind submission`
/// silently honors the `t1_` prefix and discards `--id-kind` — a silent
/// winner between two contradictory user inputs. `--id-kind` only ever
/// disambiguates *bare* IDs, so a prefixed ID plus a disagreeing `--id-kind`
/// is always a mistake worth surfacing.
fn check_prefixed_id_kind_agreement(
    id: &str,
    prefix_kind: ParentIdKindArg,
    bare_kind: Option<ParentIdKindArg>,
    source: &str,
) -> Result<()> {
    if let Some(requested) = bare_kind {
        if requested != prefix_kind {
            anyhow::bail!(
                "parent ID `{id}` in {source} is `{}`-prefixed (RETL reads it as a {}), but \
                 --id-kind {} was given; the fullname prefix and --id-kind disagree — drop \
                 --id-kind, or pass the ID without its `{}` prefix",
                direct_parent_kind_prefix(prefix_kind),
                direct_parent_kind_label(prefix_kind),
                direct_parent_kind_label(requested),
                direct_parent_kind_prefix(prefix_kind),
            );
        }
    }
    Ok(())
}

fn normalize_direct_parent_id(
    raw: &str,
    bare_kind: Option<ParentIdKindArg>,
    source: &str,
) -> Result<(ParentIdKindArg, String, String)> {
    let id = raw.trim();
    if id.is_empty() {
        anyhow::bail!("empty parent ID in {source}");
    }

    let (kind, bare) = if let Some(rest) = id.strip_prefix("t1_") {
        check_prefixed_id_kind_agreement(id, ParentIdKindArg::Comment, bare_kind, source)?;
        (
            ParentIdKindArg::Comment,
            validate_parent_bare_id(rest, source)?,
        )
    } else if let Some(rest) = id.strip_prefix("t3_") {
        check_prefixed_id_kind_agreement(id, ParentIdKindArg::Submission, bare_kind, source)?;
        (
            ParentIdKindArg::Submission,
            validate_parent_bare_id(rest, source)?,
        )
    } else {
        if let Some(prefix) = unsupported_fullname_prefix(id) {
            anyhow::bail!(
                "unsupported parent ID prefix `{prefix}` in {source}; expected `t1_`/`t3_` or a bare ID with --id-kind"
            );
        }
        let kind = bare_kind.ok_or_else(|| {
            anyhow::anyhow!(
                "bare parent ID `{id}` in {source} requires --id-kind comment or --id-kind submission"
            )
        })?;
        (kind, validate_parent_bare_id(id, source)?)
    };

    let prefixed = format!("{}{}", direct_parent_kind_prefix(kind), bare);
    Ok((kind, bare, prefixed))
}

fn add_direct_parent_id(
    raw: &str,
    bare_kind: Option<ParentIdKindArg>,
    source: &str,
    ids: &mut ParentIds,
    ordered_prefixed: &mut Vec<String>,
    seen: &mut HashSet<String>,
) -> Result<()> {
    let (kind, bare, prefixed) = normalize_direct_parent_id(raw, bare_kind, source)?;
    let already_seen = seen.contains(&prefixed);
    let inserted = match kind {
        ParentIdKindArg::Comment => ids.insert_t1(&bare),
        ParentIdKindArg::Submission => ids.insert_t3(&bare),
    };
    if !inserted && !already_seen {
        anyhow::bail!(
            "invalid {} parent ID `{}` in {source}",
            direct_parent_kind_label(kind),
            bare
        );
    }
    if !already_seen {
        seen.insert(prefixed.clone());
        ordered_prefixed.push(prefixed);
    }
    Ok(())
}

fn collect_direct_parent_ids(args: &ParentsArgs) -> Result<(ParentIds, Vec<String>)> {
    let mut ids = ParentIds::new();
    let mut ordered_prefixed = Vec::new();
    let mut seen = HashSet::new();

    for path in &args.ids_file {
        let f = retl::open_with_default_backoff(path)
            .with_context(|| format!("opening parent IDs file {}", path.display()))?;
        let mut r = BufReader::new(f);
        let mut buf = String::new();
        let mut line_no = 0_u64;
        loop {
            let n = read_line_capped(&mut r, &mut buf, DEFAULT_MAX_LINE_BYTES, path)
                .with_context(|| format!("reading parent IDs file {}", path.display()))?;
            if n == 0 {
                break;
            }
            line_no += 1;
            let trimmed = buf.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let source = format!("{} line {}", path.display(), line_no);
            add_direct_parent_id(
                trimmed,
                args.id_kind,
                &source,
                &mut ids,
                &mut ordered_prefixed,
                &mut seen,
            )?;
        }
    }

    for (idx, raw) in args.parent_id.iter().enumerate() {
        let source = format!("--parent-id #{}", idx + 1);
        add_direct_parent_id(
            raw,
            args.id_kind,
            &source,
            &mut ids,
            &mut ordered_prefixed,
            &mut seen,
        )?;
    }

    Ok((ids, ordered_prefixed))
}

fn direct_resolution_range_label(start: Option<YearMonth>, end: Option<YearMonth>) -> String {
    match (start, end) {
        (Some(s), Some(e)) => format!("{s}..={e}"),
        (Some(s), None) => format!("{s}..=latest discovered month"),
        (None, Some(e)) => format!("earliest discovered month..={e}"),
        (None, None) => "all discovered months".to_string(),
    }
}
