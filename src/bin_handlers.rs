//! Subcommand handlers for the `retl` binary. Each `run_*` function
//! corresponds 1:1 to a `Command` variant and is invoked from `main.rs`.

use anyhow::{Context, Result};
use retl::{replace_file_atomic_backoff, IntegrityMode, KeyExtractor, RedditETL, Sources, YearMonth};
use std::fs;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::bin_args::{
    AggregateArgs, CountArgs, CountMode, DedupeArgs, ExportArgs, ExportFmt, FirstSeenArgs,
    IntegrityArgs, IntegrityModeArg, ParentsArgs, ScanArgs,
};
use crate::bin_helpers::{
    build_etl, discover_spool_parts, plan, stream_extract_to_stdout, GroupBySpec, GroupMetricAgg,
    MetricSpec, RecCount,
};

pub(crate) fn run_scan(args: ScanArgs) -> Result<()> {
    let etl = build_etl(&args.common)?;
    let scan = plan!(etl, args.common);

    match args.out {
        Some(path) => {
            let f = fs::File::create(&path)
                .with_context(|| format!("creating output file {}", path.display()))?;
            let mut w = BufWriter::new(f);
            scan.for_each_username(|u| {
                let _ = writeln!(w, "{u}");
            })?;
            w.flush()?;
        }
        None => {
            let stdout = io::stdout();
            let mut w = BufWriter::new(stdout.lock());
            scan.for_each_username(|u| {
                let _ = writeln!(w, "{u}");
            })?;
            w.flush()?;
        }
    }
    Ok(())
}

pub(crate) fn run_dedupe(args: DedupeArgs) -> Result<()> {
    let key = parse_dedupe_key(&args.key)?;
    let etl = build_etl(&args.common)?;
    let work_dir = args.common.work_dir.clone();
    let scan = plan!(etl, args.common);

    if args.out == Path::new("-") {
        let tmp_path = stdout_dedupe_path(&work_dir);
        let _ = fs::remove_file(&tmp_path);
        let result = scan.dedupe_keys_to_lines(&key, &tmp_path);
        if let Err(e) = result {
            let _ = fs::remove_file(&tmp_path);
            return Err(e);
        }
        let copy_result = (|| -> Result<()> {
            let mut f = fs::File::open(&tmp_path)
                .with_context(|| format!("opening dedupe tempfile {}", tmp_path.display()))?;
            let stdout = io::stdout();
            let mut w = stdout.lock();
            io::copy(&mut f, &mut w).context("streaming dedupe output to stdout")?;
            w.flush()?;
            Ok(())
        })();
        let _ = fs::remove_file(&tmp_path);
        copy_result?;
    } else {
        scan.dedupe_keys_to_lines(&key, &args.out)?;
    }
    Ok(())
}

fn parse_dedupe_key(spec: &str) -> Result<KeyExtractor> {
    let trimmed = spec.trim();
    match trimmed.to_ascii_lowercase().as_str() {
        "author" => Ok(KeyExtractor::author_lowercase_fast()),
        "subreddit" => Ok(KeyExtractor::subreddit_lowercase_fast()),
        _ => {
            let ptr = trimmed.strip_prefix("json:").ok_or_else(|| {
                anyhow::anyhow!("unsupported --key {trimmed:?}; expected author, subreddit, or json:/pointer")
            })?;
            if !ptr.starts_with('/') {
                anyhow::bail!("JSON pointer keys must start with '/': use --key json:/field");
            }
            Ok(KeyExtractor::json_pointer(ptr.to_string()))
        }
    }
}

fn stdout_dedupe_path(work_dir: &Path) -> PathBuf {
    work_dir
        .join("lib_tmp")
        .join(format!("retl_dedupe_stdout_{}.txt", std::process::id()))
}

pub(crate) fn run_export(args: ExportArgs) -> Result<()> {
    let mut etl = build_etl(&args.common)?;
    if let Some(level) = args.zst_level {
        etl = etl.zst_level(level);
    }
    if args.resume {
        etl = etl.resume(true);
    }
    let work_dir = args.common.work_dir.clone();
    let scan = plan!(etl, args.common);
    let to_stdout = args.out == Path::new("-");

    match args.format {
        ExportFmt::Jsonl => {
            if to_stdout {
                stream_extract_to_stdout(&work_dir, "stdout.jsonl", |p| scan.extract_to_jsonl(p))?;
            } else {
                scan.extract_to_jsonl(&args.out)?;
            }
        }
        ExportFmt::Json => {
            let pretty = args.pretty;
            if to_stdout {
                stream_extract_to_stdout(&work_dir, "stdout.json", |p| {
                    scan.extract_to_json(p, pretty)
                })?;
            } else {
                scan.extract_to_json(&args.out, pretty)?;
            }
        }
        ExportFmt::Spool => {
            if to_stdout {
                anyhow::bail!("--out - is not valid for --format spool (it expects a directory)");
            }
            fs::create_dir_all(&args.out)
                .with_context(|| format!("creating spool dir {}", args.out.display()))?;
            let (parts, n) = scan.extract_spool_monthly(&args.out)?;
            eprintln!("Spooled {} records across {} part files", n, parts.len());
        }
    }
    Ok(())
}

pub(crate) fn run_count(args: CountArgs) -> Result<()> {
    let etl = build_etl(&args.common)?;
    let scan = plan!(etl, args.common);

    match args.mode {
        CountMode::Month => {
            let counts = scan.count_by_month()?;
            let to_stdout = args.out.as_deref().map_or(true, |p| p == Path::new("-"));
            if to_stdout {
                let stdout = io::stdout();
                let mut w = stdout.lock();
                for (ym, n) in &counts {
                    writeln!(w, "{ym}\t{n}")?;
                }
                w.flush()?;
            } else {
                let path = args.out.unwrap();
                let f = fs::File::create(&path)
                    .with_context(|| format!("creating output file {}", path.display()))?;
                let mut w = BufWriter::new(f);
                for (ym, n) in &counts {
                    writeln!(w, "{ym}\t{n}")?;
                }
                w.flush()?;
            }
        }
        CountMode::Author => {
            let out = args.out.ok_or_else(|| {
                anyhow::anyhow!("--out is required for `count --mode author` (TSV destination)")
            })?;
            scan.author_counts_to_tsv(&out)?;
        }
    }
    Ok(())
}

pub(crate) fn run_integrity(args: IntegrityArgs) -> Result<()> {
    let etl = build_etl(&args.common)?;
    let mode = match args.mode {
        IntegrityModeArg::Quick => IntegrityMode::Quick {
            sample_bytes: args.sample_bytes,
        },
        IntegrityModeArg::Full => IntegrityMode::Full,
    };
    let bad = etl.check_corpus_integrity(mode)?;
    if bad.is_empty() {
        eprintln!("OK: no corruption detected.");
    } else {
        eprintln!("FAILED: {} file(s) failed integrity check:", bad.len());
        for (p, e) in &bad {
            println!("{}\t{}", p.display(), e);
        }
        std::process::exit(2);
    }
    Ok(())
}

pub(crate) fn run_aggregate(args: AggregateArgs) -> Result<()> {
    let etl = build_etl(&args.common)?;
    let shards_dir = args.shards_dir.unwrap_or_else(|| {
        args.out
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("agg_shards")
    });
    fs::create_dir_all(&shards_dir)
        .with_context(|| format!("creating shards_dir {}", shards_dir.display()))?;

    if let Some(by) = args.by {
        let group_by = GroupBySpec::parse(&by)?;
        let metric = MetricSpec::parse(args.metric.as_deref())?;
        let top = args.top;
        let (agg, built, errors) = etl
            .aggregate_jsonls_parallel_collect_with::<GroupMetricAgg, _>(
                args.inputs,
                &shards_dir,
                || GroupMetricAgg::new(group_by.clone(), metric.clone()),
            )?;
        write_grouped_tsv(&args.out, agg.rows(top))?;
        eprintln!(
            "Aggregated {} shard(s) to TSV; {} input(s) failed during shard build",
            built, errors
        );
    } else {
        if args.metric.is_some() {
            anyhow::bail!("--metric requires --by");
        }
        if args.top.is_some() {
            anyhow::bail!("--top requires --by");
        }
        let (built, errors) = etl.aggregate_jsonls_parallel::<RecCount>(
            args.inputs,
            &shards_dir,
            &args.out,
            true,
            args.pretty,
        )?;
        eprintln!(
            "Aggregated {} shard(s); {} input(s) failed during shard build",
            built, errors
        );
    }
    Ok(())
}

fn write_grouped_tsv(out: &Path, rows: Vec<(String, String)>) -> Result<()> {
    let tmp = out.with_extension("tsv.inprogress");
    {
        let f = fs::File::create(&tmp)
            .with_context(|| format!("creating output tempfile {}", tmp.display()))?;
        let mut w = BufWriter::new(f);
        for (key, value) in rows {
            writeln!(w, "{key}\t{value}")?;
        }
        w.flush()?;
    }
    replace_file_atomic_backoff(&tmp, out)
        .with_context(|| format!("publishing output file {}", out.display()))?;
    Ok(())
}

pub(crate) fn run_first_seen(args: FirstSeenArgs) -> Result<()> {
    let etl = build_etl(&args.common)?;
    let scan = plan!(etl, args.common);
    scan.build_first_seen_index_to_tsv(&args.out)?;
    Ok(())
}

pub(crate) fn run_parents(args: ParentsArgs) -> Result<()> {
    let (spool_parts, min_ym, max_ym) = discover_spool_parts(&args.spool)?;

    let mut wstart = min_ym;
    let mut wend = max_ym;
    for _ in 0..args.window_months {
        if let Some(p) = wstart.prev() {
            wstart = p;
        }
        if let Some(n) = wend.next() {
            wend = n;
        }
    }

    fs::create_dir_all(&args.cache)
        .with_context(|| format!("creating cache dir {}", args.cache.display()))?;
    fs::create_dir_all(&args.out)
        .with_context(|| format!("creating output dir {}", args.out.display()))?;
    fs::create_dir_all(&args.work_dir)
        .with_context(|| format!("creating work_dir {}", args.work_dir.display()))?;
    let lib_tmp = args.work_dir.join("lib_tmp");
    fs::create_dir_all(&lib_tmp)
        .with_context(|| format!("creating work_dir {}", lib_tmp.display()))?;

    let build = |sources: Option<Sources>, range: Option<(YearMonth, YearMonth)>| -> RedditETL {
        let mut etl = RedditETL::new()
            .base_dir(&args.data_dir)
            .work_dir(&lib_tmp)
            .progress(!args.no_progress);
        if let Some(s) = sources {
            etl = etl.sources(s);
        }
        if let Some((s, e)) = range {
            etl = etl.date_range(Some(s), Some(e));
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
        etl
    };

    let ids = build(None, None).collect_parent_ids_from_jsonls(spool_parts.clone())?;
    let parents = build(Some(Sources::Both), Some((wstart, wend))).resolve_parent_maps(
        &ids,
        &args.cache,
        args.resume,
    )?;
    let attached = build(None, None).attach_parents_jsonls_parallel(
        spool_parts,
        &args.out,
        &parents,
        args.resume,
    )?;

    eprintln!(
        "Attached parents to {} file(s) in {} (resolved over {}..={})",
        attached.len(),
        args.out.display(),
        wstart,
        wend
    );
    Ok(())
}
