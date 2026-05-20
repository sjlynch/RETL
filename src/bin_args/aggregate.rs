use clap::Args;
use std::path::PathBuf;

use super::MonitorOpts;

#[derive(Args, Debug, Clone)]
pub(crate) struct AggregateRuntimeOpts {
    /// Number of Rayon worker threads (defaults to the global pool; clamped to RETL's safe cap).
    #[arg(long)]
    pub(crate) parallelism: Option<usize>,

    /// Disable progress bars.
    #[arg(long)]
    pub(crate) no_progress: bool,

    /// Do not write aggregate provenance manifest sidecars.
    #[arg(long)]
    pub(crate) no_manifest: bool,
}

#[derive(Args, Debug)]
pub(crate) struct AggregateArgs {
    #[command(flatten)]
    pub(crate) runtime: AggregateRuntimeOpts,
    /// Observability / monitoring flags (opt-in, off by default).
    /// `aggregate` reduces materialized JSONL inputs and is one of the
    /// most memory-hungry stages, so `--max-rss-mb`/`--status-file` and
    /// the rest of the watchdog surface apply here.
    #[command(flatten)]
    pub(crate) monitor: MonitorOpts,
    /// Spool directory containing `part_RC_YYYY-MM.jsonl` /
    /// `part_RS_YYYY-MM.jsonl` files to aggregate.
    #[arg(long)]
    pub(crate) spool: Option<PathBuf>,
    /// JSONL input files to aggregate. Omit when using `--spool`.
    #[arg(num_args = 1.., value_name = "INPUTS")]
    pub(crate) inputs: Vec<PathBuf>,
    /// Output path. Without `--by`, writes JSON record-count state; with `--by`, writes TSV.
    #[arg(long, short)]
    pub(crate) out: PathBuf,
    /// Directory used for per-run aggregate shard namespaces (default:
    /// alongside `--out`).
    #[arg(long)]
    pub(crate) shards_dir: Option<PathBuf>,
    /// Field-indent the final JSON (only used when `--by` is omitted).
    #[arg(long)]
    pub(crate) pretty: bool,
    /// Built-in group key: `subreddit`, `month`, `author`, or `json:/pointer`.
    #[arg(long = "by")]
    pub(crate) by: Option<String>,
    /// Metric for grouped aggregation: `count` (default), `sum:/pointer`,
    /// `avg:/pointer`, `min:/pointer`, or `max:/pointer`.
    #[arg(long = "metric")]
    pub(crate) metric: Option<String>,
    /// Keep only the top N groups by metric value (ties sort by key).
    #[arg(long)]
    pub(crate) top: Option<usize>,
    /// Allow exponent notation for inexact floating grouped metrics. This is a
    /// display-style toggle, not a precision toggle: exact integer results stay
    /// exact decimal, and default decimal output no longer rounds to six places.
    #[arg(long)]
    pub(crate) scientific: bool,
    /// Hidden compatibility trap: aggregate reduces materialized inputs and is
    /// intentionally not resumable.
    #[arg(long, hide = true)]
    pub(crate) resume: bool,
}
