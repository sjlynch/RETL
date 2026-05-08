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
use std::path::PathBuf;

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
    /// Output destination — file for `jsonl`/`json`, directory for `spool`.
    #[arg(long, short)]
    out: PathBuf,
    /// Pretty-print the JSON array (only with `--format json`).
    #[arg(long)]
    pretty: bool,
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
    let etl = build_etl(&args.common)?;
    let scan = plan!(etl, args.common.subreddits);

    match args.format {
        ExportFmt::Jsonl => scan.extract_to_jsonl(&args.out)?,
        ExportFmt::Json => scan.extract_to_json(&args.out, args.pretty)?,
        ExportFmt::Spool => {
            fs::create_dir_all(&args.out)
                .with_context(|| format!("creating spool dir {}", args.out.display()))?;
            let (parts, n) = scan.extract_spool_monthly(&args.out)?;
            eprintln!("Spooled {} records across {} part files", n, parts.len());
        }
    }
    Ok(())
}

fn run_count(args: CountArgs) -> Result<()> {
    let etl = build_etl(&args.common)?;
    let scan = plan!(etl, args.common.subreddits);

    match args.mode {
        CountMode::Month => {
            let counts = scan.count_by_month()?;
            match args.out {
                Some(path) => {
                    let f = fs::File::create(&path)
                        .with_context(|| format!("creating output file {}", path.display()))?;
                    let mut w = BufWriter::new(f);
                    for (ym, n) in &counts {
                        writeln!(w, "{ym}\t{n}")?;
                    }
                    w.flush()?;
                }
                None => {
                    let stdout = io::stdout();
                    let mut w = stdout.lock();
                    for (ym, n) in &counts {
                        writeln!(w, "{ym}\t{n}")?;
                    }
                    w.flush()?;
                }
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
    }
}
