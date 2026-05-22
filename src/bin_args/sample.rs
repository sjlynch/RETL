use clap::Args;
use std::path::PathBuf;

use super::{CommonOpts, ExportFmt, QueryOpts};

#[derive(Args, Debug)]
pub(crate) struct SampleArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    #[command(flatten)]
    pub(crate) query: QueryOpts,
    /// Output format for the sample.
    #[arg(long, value_enum, default_value_t = ExportFmt::Jsonl)]
    pub(crate) format: ExportFmt,
    /// Output destination (default stdout). Directories are required for spool/zst/partitioned-jsonl.
    #[arg(long, short, default_value = "-")]
    pub(crate) out: PathBuf,
    /// Maximum matching records to emit. With file_concurrency >1, already-running workers may overshoot slightly.
    #[arg(long, visible_alias = "head", default_value_t = 10)]
    pub(crate) limit: u64,
    /// Field-indent the JSON array (only with `--format json`; rejected for
    /// every other format because it would have no effect).
    #[arg(long)]
    pub(crate) pretty: bool,
    /// Whitelist of top-level fields to keep. Required for csv/tsv.
    #[arg(long, value_delimiter = ',')]
    pub(crate) whitelist: Vec<String>,
    /// Error if a `--whitelist` field never matches. The verdict is post-hoc
    /// (decided after streaming); a strict failure publishes no output file.
    #[arg(long)]
    pub(crate) strict_whitelist: bool,
    /// Convert `created_utc`, `retrieved_on`, and `edited` to RFC3339 strings
    /// on JSON-family exports (not csv/tsv).
    #[arg(long)]
    pub(crate) human_timestamps: bool,
}
