use clap::Args;
use std::path::PathBuf;

use super::{CommonOpts, QueryOpts};

#[derive(Args, Debug)]
pub(crate) struct ScanArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    #[command(flatten)]
    pub(crate) query: QueryOpts,
    /// Output file for usernames (default: stdout; `-` also means stdout).
    #[arg(long, short)]
    pub(crate) out: Option<PathBuf>,
    /// Stop after approximately N matching records have been scanned/emitted.
    /// With file_concurrency >1, already-running workers may emit a bounded over-shoot.
    #[arg(long, visible_alias = "head")]
    pub(crate) limit: Option<u64>,
    /// Resume by reusing per-source per-month matched-record checkpoints under
    /// `--work-dir` when the query/config/corpus fingerprint still matches.
    #[arg(long)]
    pub(crate) resume: bool,
}
