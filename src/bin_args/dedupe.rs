use clap::Args;
use std::path::PathBuf;

use super::{CommonOpts, QueryOpts};

#[derive(Args, Debug)]
pub(crate) struct DedupeArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    #[command(flatten)]
    pub(crate) query: QueryOpts,
    /// Key to deduplicate: `author`, `subreddit`, or `json:/pointer`.
    /// JSON pointers should reference a scalar; strings, numbers, and bools are emitted as text.
    #[arg(long, value_name = "KEY")]
    pub(crate) key: String,
    /// Output text file, one unique key per line. Use `-` for stdout.
    #[arg(long, short)]
    pub(crate) out: PathBuf,
    /// Stop after approximately N matching records have been scanned.
    /// With file_concurrency >1, already-running workers may emit a bounded over-shoot.
    #[arg(long, visible_alias = "head")]
    pub(crate) limit: Option<u64>,
    /// Per-flush byte budget for bucketing/dedupe (`per_flush_cap = N / 2`).
    /// 0 disables the explicit cap and falls back to memory-fraction sampling.
    /// NOTE: this is NOT the worst-case peak — see `--inflight-groups`.
    #[arg(long)]
    pub(crate) inflight_bytes: Option<usize>,
    /// Bucketing channel depth (default 8). Worst-case bucketing peak is
    /// `(1 + inflight_groups) * inflight_bytes / 2`, NOT `inflight_bytes`.
    /// Set to 1 to match the declared `inflight_bytes` budget exactly.
    #[arg(long)]
    pub(crate) inflight_groups: Option<usize>,
    /// Error if any matched record does not contain the requested dedupe key.
    #[arg(long)]
    pub(crate) strict_key: bool,
    /// Resume by reusing per-source per-month matched-record checkpoints under
    /// `--work-dir` when the query/config/corpus fingerprint still matches.
    #[arg(long)]
    pub(crate) resume: bool,
}
