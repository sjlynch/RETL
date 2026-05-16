use clap::{Args, ValueEnum};
use std::path::PathBuf;

use super::{CommonOpts, QueryOpts};

#[derive(Args, Debug)]
pub(crate) struct CountArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    #[command(flatten)]
    pub(crate) query: QueryOpts,
    /// Count mode: per month (`month`) or per author (`author`, writes TSV).
    #[arg(long, value_enum, default_value_t = CountMode::Month)]
    pub(crate) mode: CountMode,
    /// Output file (default stdout for `month`, required for `author`).
    /// Pass `-` to stream either mode to stdout.
    #[arg(long, short)]
    pub(crate) out: Option<PathBuf>,
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
