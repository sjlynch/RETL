use clap::Args;
use std::path::PathBuf;

use super::{CommonOpts, QueryOpts};

#[derive(Args, Debug)]
pub(crate) struct FirstSeenArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    #[command(flatten)]
    pub(crate) query: QueryOpts,
    /// Output TSV file: `<author>\t<earliest_created_utc>` per line.
    #[arg(long, short)]
    pub(crate) out: PathBuf,
    /// Resume by reusing per-source per-month matched-record checkpoints under
    /// `--work-dir` when the query/config/corpus fingerprint still matches.
    #[arg(long)]
    pub(crate) resume: bool,
}
