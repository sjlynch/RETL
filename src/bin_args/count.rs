use clap::{Args, ValueEnum};
use std::path::PathBuf;

use super::{CommonOpts, QueryOpts};

#[derive(Args, Debug)]
pub(crate) struct CountArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    #[command(flatten)]
    pub(crate) query: QueryOpts,
    /// Count mode. Both write TSV (`<key><TAB><count>\n`, one row per key, sorted).
    /// `month` keys are `YYYY-MM`; `author` keys are lowercased author names. There is
    /// no JSON/CSV mode — pipe through `retl convert` or post-process if you need one.
    #[arg(long, value_enum, default_value_t = CountMode::Month)]
    pub(crate) mode: CountMode,
    /// Output file (default stdout for `month`, required for `author`).
    /// Pass `-` to stream either mode to stdout.
    #[arg(long, short)]
    pub(crate) out: Option<PathBuf>,
    /// Stop after approximately N matching records have been scanned/counted.
    /// With file_concurrency >1, already-running workers may emit a bounded over-shoot.
    #[arg(long, visible_alias = "head")]
    pub(crate) limit: Option<u64>,
    /// Resume by reusing per-source per-month matched-record checkpoints under
    /// `--work-dir` when the query/config/corpus fingerprint still matches.
    #[arg(long)]
    pub(crate) resume: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum CountMode {
    Month,
    Author,
}
