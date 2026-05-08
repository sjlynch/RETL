//! `retl` — command-line interface to the RETL toolkit.
//!
//! Subcommands map onto existing builder methods on `retl::RedditETL` /
//! `retl::ScanPlan`. The original demo binary lives at
//! `examples/quickstart.rs` (`cargo run --example quickstart`).

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use retl::{
    Aggregator, IntegrityMode, RedditETL, Sources, YearMonth,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
#[command(
    name = "retl",
    version,
    about = "Reddit ETL toolkit — scan, export, count, validate, and aggregate Reddit RC/RS .zst dumps.",
    long_about = None,
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Scan and emit unique usernames matching the query selection.
    Scan(ScanArgs),
    /// Export filtered records as JSONL, a JSON array, or per-month spool files.
    Export(ExportArgs),
    /// Count records by month, or write per-author counts to TSV.
    Count(CountArgs),
    /// Validate `.zst` monthly files (quick sample or full decode).
    Integrity(IntegrityArgs),
    /// Aggregate JSONL inputs into a single JSON file (built-in record-count).
    Aggregate(AggregateArgs),
    /// Resolve and attach parent comments/submissions onto a spool directory.
    Parents(ParentsArgs),
    /// Build a per-author "first-seen" timestamp index TSV.
    #[command(name = "first-seen")]
    FirstSeen(FirstSeenArgs),
}

// -----------------------------------------------------------------------------
// Common flags shared by all subcommands.
// -----------------------------------------------------------------------------

#[derive(Args, Debug, Clone)]
struct CommonOpts {
    /// Path to corpus base dir (containing `comments/` and `submissions/`).
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// Scratch directory for sharded writers and stitched intermediates.
    #[arg(long, default_value = "./etl_work")]
    work_dir: PathBuf,

    /// Inclusive start month (YYYY-MM).
    #[arg(long, value_name = "YYYY-MM")]
    start: Option<YearMonth>,

    /// Inclusive end month (YYYY-MM).
    #[arg(long, value_name = "YYYY-MM")]
    end: Option<YearMonth>,

    /// Number of Rayon worker threads (defaults to the global pool).
    #[arg(long)]
    parallelism: Option<usize>,

    /// Number of monthly files processed concurrently.
    #[arg(long)]
    file_concurrency: Option<usize>,

    /// Disable progress bars.
    #[arg(long)]
    no_progress: bool,

    /// Whitelist of fields to keep on export. Comma-separated, repeatable.
    #[arg(long, value_delimiter = ',')]
    whitelist: Vec<String>,

    /// Convert `created_utc` to RFC3339 strings on export.
    #[arg(long)]
    human_timestamps: bool,

    /// Source selection: rc (comments), rs (submissions), or both.
    #[arg(long, value_enum, default_value_t = SourceArg::Both)]
    source: SourceArg,

    /// Subreddit name (repeat for multiple). If none given, all subreddits match.
    #[arg(long = "subreddit", short = 's')]
    subreddits: Vec<String>,

    /// Inflight bytes budget for bucketing/dedupe producer/consumer pairs.
    /// 0 disables the explicit cap and falls back to memory-fraction sampling.
    #[arg(long)]
    inflight_bytes: Option<usize>,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum SourceArg {
    Rc,
    Rs,
    Both,
}

impl From<SourceArg> for Sources {
    fn from(s: SourceArg) -> Self {
        match s {
            SourceArg::Rc => Sources::Comments,
            SourceArg::Rs => Sources::Submissions,
            SourceArg::Both => Sources::Both,
        }
    }
}

// -----------------------------------------------------------------------------
// Subcommand argument structs.
// -----------------------------------------------------------------------------

#[derive(Args, Debug)]
struct ScanArgs {
    #[command(flatten)]
    common: CommonOpts,
    /// Output file for usernames (default: stdout).
    #[arg(long, short)]
    out: Option<PathBuf>,
}

#[derive(Args, Debug)]
struct ExportArgs {
    #[command(flatten)]
    common: CommonOpts,
    /// Output format.
    #[arg(long, value_enum, default_value_t = ExportFmt::Jsonl)]
    format: ExportFmt,
    /// Output destination — file for `jsonl`/`json` (use `-` for stdout),
    /// directory for `spool`.
    #[arg(long, short)]
    out: PathBuf,
    /// Pretty-print the JSON array (only with `--format json`).
    #[arg(long)]
    pretty: bool,
    /// zstd compression level for `.zst` outputs. Clamped to 1..=22 by the library.
    #[arg(long)]
    zst_level: Option<i32>,
    /// Resume a prior `--format spool` run by skipping months already published
    /// (requires the same `--out` directory and a populated `_progress.json`).
    #[arg(long)]
    resume: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum ExportFmt {
    /// Single stitched `.jsonl` file (one record per line).
    Jsonl,
    /// Single `.json` file containing a JSON array of records.
    Json,
    /// Per-source per-month `part_RC_YYYY-MM.jsonl` / `part_RS_YYYY-MM.jsonl`.
    Spool,
}

#[derive(Args, Debug)]
struct CountArgs {
    #[command(flatten)]
    common: CommonOpts,
    /// Count mode: per month (`month`) or per author (`author`, writes TSV).
    #[arg(long, value_enum, default_value_t = CountMode::Month)]
    mode: CountMode,
    /// Output file (default stdout for `month`, required for `author`).
    /// Pass `-` to stream to stdout when `--mode month`.
    #[arg(long, short)]
    out: Option<PathBuf>,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum CountMode {
    Month,
    Author,
}

#[derive(Args, Debug)]
struct IntegrityArgs {
    #[command(flatten)]
    common: CommonOpts,
    /// Validation mode.
    #[arg(long, value_enum, default_value_t = IntegrityModeArg::Quick)]
    mode: IntegrityModeArg,
    /// Bytes (decompressed) to sample per file in quick mode.
    #[arg(long, default_value_t = 64 * 1024)]
    sample_bytes: u64,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
enum IntegrityModeArg {
    Quick,
    Full,
}

#[derive(Args, Debug)]
struct AggregateArgs {
    #[command(flatten)]
    common: CommonOpts,
    /// JSONL input files to aggregate.
    #[arg(required = true, num_args = 1..)]
    inputs: Vec<PathBuf>,
    /// Output JSON file for the aggregated result.
    #[arg(long, short)]
    out: PathBuf,
    /// Directory used for per-input aggregate shards (default: alongside `--out`).
    #[arg(long)]
    shards_dir: Option<PathBuf>,
    /// Pretty-print the final JSON.
    #[arg(long)]
    pretty: bool,
}

#[derive(Args, Debug)]
struct ParentsArgs {
    /// Spool directory containing `part_RC_YYYY-MM.jsonl` /
    /// `part_RS_YYYY-MM.jsonl` files produced by `retl export --format spool`.
    #[arg(long)]
    spool: PathBuf,
    /// Cache directory for resolved parent shards.
    #[arg(long)]
    cache: PathBuf,
    /// Output directory for spool files with `parent` payloads attached.
    #[arg(long, short)]
    out: PathBuf,
    /// Reuse cache shards and skip already-attached output files.
    #[arg(long)]
    resume: bool,
    /// Months of slack added on each side of the spool's date range when
    /// scanning the corpus to resolve parent payloads.
    #[arg(long, default_value_t = 1)]
    window_months: u32,
    /// Path to corpus base dir (containing `comments/` and `submissions/`).
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,
    /// Scratch directory for sharded writers and stitched intermediates.
    #[arg(long, default_value = "./etl_work")]
    work_dir: PathBuf,
    /// Number of Rayon worker threads (defaults to the global pool).
    #[arg(long)]
    parallelism: Option<usize>,
    /// Number of monthly files processed concurrently.
    #[arg(long)]
    file_concurrency: Option<usize>,
    /// Disable progress bars.
    #[arg(long)]
    no_progress: bool,
    /// Inflight bytes budget for bucketing/dedupe producer/consumer pairs.
    #[arg(long)]
    inflight_bytes: Option<usize>,
}

#[derive(Args, Debug)]
struct FirstSeenArgs {
    #[command(flatten)]
    common: CommonOpts,
    /// Output TSV file: `<author>\t<earliest_created_utc>` per line.
    #[arg(long, short)]
    out: PathBuf,
}

// -----------------------------------------------------------------------------
// Aggregator used by the `aggregate` subcommand.
// -----------------------------------------------------------------------------

/// Built-in aggregator: counts records across the supplied JSONL inputs.
#[derive(Default, Serialize, Deserialize)]
struct RecCount {
    count: u64,
}

impl Aggregator for RecCount {
    fn ingest(&mut self, _record: &Value) {
        self.count += 1;
    }
    fn merge(&mut self, other: Self) {
        self.count += other.count;
    }
}

// -----------------------------------------------------------------------------
// Builder helpers.
// -----------------------------------------------------------------------------

fn ensure_dirs(common: &CommonOpts) -> Result<PathBuf> {
    fs::create_dir_all(&common.work_dir)
        .with_context(|| format!("creating work_dir {}", common.work_dir.display()))?;
    let lib_tmp = common.work_dir.join("lib_tmp");
    fs::create_dir_all(&lib_tmp)
        .with_context(|| format!("creating work_dir {}", lib_tmp.display()))?;
    Ok(lib_tmp)
}

fn build_etl(common: &CommonOpts) -> Result<RedditETL> {
    let lib_tmp = ensure_dirs(common)?;
    let mut etl = RedditETL::new()
        .base_dir(&common.data_dir)
        .work_dir(&lib_tmp)
        .progress(!common.no_progress)
        .sources(Sources::from(common.source))
        .date_range(common.start, common.end);

    if let Some(p) = common.parallelism {
        etl = etl.parallelism(p);
    }
    if let Some(fc) = common.file_concurrency {
        etl = etl.file_concurrency(fc);
    }
    if !common.whitelist.is_empty() {
        etl = etl.whitelist_fields(common.whitelist.iter().cloned());
    }
    if common.human_timestamps {
        etl = etl.timestamps_human_readable(true);
    }
    if let Some(b) = common.inflight_bytes {
        etl = etl.inflight_bytes(b);
    }
    Ok(etl)
}

/// Build a `ScanPlan` from `etl` with the (possibly empty) subreddit selection
/// applied. Inlined as a macro because `ScanPlan` is not re-exported from the
/// crate root and we want to avoid widening the public API just for the CLI.
macro_rules! plan {
    ($etl:expr, $subs:expr) => {{
        let scan = $etl.scan();
        if $subs.is_empty() {
            scan
        } else {
            scan.subreddits($subs.iter().map(String::as_str))
        }
    }};
}

// -----------------------------------------------------------------------------
// Subcommand handlers.
// -----------------------------------------------------------------------------

fn run_scan(args: ScanArgs) -> Result<()> {
    let etl = build_etl(&args.common)?;
    let scan = plan!(etl, args.common.subreddits);

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

fn run_export(args: ExportArgs) -> Result<()> {
    let mut etl = build_etl(&args.common)?;
    if let Some(level) = args.zst_level {
        etl = etl.zst_level(level);
    }
    if args.resume {
        etl = etl.resume(true);
    }
    let work_dir = args.common.work_dir.clone();
    let scan = plan!(etl, args.common.subreddits);
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
                stream_extract_to_stdout(&work_dir, "stdout.json", |p| scan.extract_to_json(p, pretty))?;
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

/// Run an extraction that writes to a file path, then stream the resulting
/// file to stdout and remove it. Used to honor `--out -` for
/// `extract_to_jsonl` / `extract_to_json`, which only know how to write to a
/// `Path`.
fn stream_extract_to_stdout(
    work_dir: &Path,
    file_stem: &str,
    extract: impl FnOnce(&Path) -> Result<()>,
) -> Result<()> {
    let lib_tmp = work_dir.join("lib_tmp");
    fs::create_dir_all(&lib_tmp)
        .with_context(|| format!("creating work_dir {}", lib_tmp.display()))?;
    let tmp_path = lib_tmp.join(format!("retl_export_{}_{}", std::process::id(), file_stem));
    let _ = fs::remove_file(&tmp_path);

    let result = extract(&tmp_path);

    if let Err(e) = result {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }

    let copy_result = (|| -> Result<()> {
        let mut f = fs::File::open(&tmp_path)
            .with_context(|| format!("opening export tempfile {}", tmp_path.display()))?;
        let stdout = io::stdout();
        let mut w = stdout.lock();
        io::copy(&mut f, &mut w).context("streaming export to stdout")?;
        w.flush()?;
        Ok(())
    })();

    let _ = fs::remove_file(&tmp_path);
    copy_result
}

fn run_count(args: CountArgs) -> Result<()> {
    let etl = build_etl(&args.common)?;
    let scan = plan!(etl, args.common.subreddits);

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

fn run_integrity(args: IntegrityArgs) -> Result<()> {
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

fn run_aggregate(args: AggregateArgs) -> Result<()> {
    let etl = build_etl(&args.common)?;
    let shards_dir = args.shards_dir.unwrap_or_else(|| {
        args.out
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("agg_shards")
    });
    fs::create_dir_all(&shards_dir)
        .with_context(|| format!("creating shards_dir {}", shards_dir.display()))?;

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
    Ok(())
}

fn run_first_seen(args: FirstSeenArgs) -> Result<()> {
    let etl = build_etl(&args.common)?;
    let scan = plan!(etl, args.common.subreddits);
    scan.build_first_seen_index_to_tsv(&args.out)?;
    Ok(())
}

/// Discover spool parts in `dir`, parsing `part_RC_YYYY-MM.jsonl` and
/// `part_RS_YYYY-MM.jsonl` filenames. Returns `(sorted_paths, min, max)`.
fn discover_spool_parts(dir: &Path) -> Result<(Vec<PathBuf>, YearMonth, YearMonth)> {
    let entries = fs::read_dir(dir)
        .with_context(|| format!("reading spool dir {}", dir.display()))?;
    let mut parts: Vec<(YearMonth, PathBuf)> = Vec::new();
    for e in entries {
        let e = e?;
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
    parts.sort_by(|a, b| a.1.cmp(&b.1));
    let min_ym = parts.iter().map(|(ym, _)| *ym).min().unwrap();
    let max_ym = parts.iter().map(|(ym, _)| *ym).max().unwrap();
    Ok((parts.into_iter().map(|(_, p)| p).collect(), min_ym, max_ym))
}

fn run_parents(args: ParentsArgs) -> Result<()> {
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
    let parents = build(Some(Sources::Both), Some((wstart, wend)))
        .resolve_parent_maps(&ids, &args.cache, args.resume)?;
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

// -----------------------------------------------------------------------------
// Entry point.
// -----------------------------------------------------------------------------

fn main() -> Result<()> {
    retl::init_tracing_for_binary();
    let cli = Cli::parse();
    match cli.command {
        Command::Scan(a) => run_scan(a),
        Command::Export(a) => run_export(a),
        Command::Count(a) => run_count(a),
        Command::Integrity(a) => run_integrity(a),
        Command::Aggregate(a) => run_aggregate(a),
        Command::Parents(a) => run_parents(a),
        Command::FirstSeen(a) => run_first_seen(a),
    }
}
