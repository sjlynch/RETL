use clap::{Args, ValueEnum};
use std::path::PathBuf;

use super::{CommonOpts, QueryOpts};

#[derive(Args, Debug)]
pub(crate) struct ExportArgs {
    #[command(flatten)]
    pub(crate) common: CommonOpts,
    #[command(flatten)]
    pub(crate) query: QueryOpts,
    /// Output format.
    #[arg(long, value_enum, default_value_t = ExportFmt::Jsonl)]
    pub(crate) format: ExportFmt,
    /// Output destination — file for `jsonl`/`json` (use `-` for stdout),
    /// directory for `spool`/`zst`/`partitioned-jsonl`.
    #[arg(long, short)]
    pub(crate) out: PathBuf,
    /// Field-indent the JSON array (only with `--format json`).
    #[arg(long)]
    pub(crate) pretty: bool,
    /// zstd compression level for `.zst` outputs. Clamped to 1..=22 by the library.
    #[arg(long)]
    pub(crate) zst_level: Option<i32>,
    /// Whitelist of top-level fields to keep on export. Comma-separated, repeatable.
    /// Required for csv/tsv; missing fields render as empty cells.
    /// Comments use `body`/`parent_id`/`link_id`; submissions use `title`/`selftext`/`domain`.
    #[arg(long, value_delimiter = ',')]
    pub(crate) whitelist: Vec<String>,
    /// Error if `--whitelist` matches zero fields in the first sampled records.
    #[arg(long)]
    pub(crate) strict_whitelist: bool,
    /// Convert `created_utc` to RFC3339 strings on JSON-family exports (not csv/tsv).
    #[arg(long)]
    pub(crate) human_timestamps: bool,
    /// Stop after approximately N records have been emitted.
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
    /// Resume a prior JSON-family export with the same query/config/corpus (not csv/tsv).
    /// `jsonl`/`json` reuse per-month `.part_*.jsonl` files and `_progress.json`
    /// in a namespaced scratch dir under `--work-dir`; `spool`, `zst`, and
    /// `partitioned-jsonl` use `_progress.json` under `--out`. Changing filters,
    /// corpus paths, sources, date range, whitelist, timestamp formatting, or
    /// (for ZST) zst level invalidates the checkpoint and rebuilds the parts.
    #[arg(long)]
    pub(crate) resume: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug)]
pub(crate) enum ExportFmt {
    /// Single stitched `.jsonl` file (one record per line).
    Jsonl,
    /// Single `.json` file containing a JSON array of records.
    Json,
    /// Single RFC4180-style CSV file (requires --whitelist).
    Csv,
    /// Single tab-separated file (requires --whitelist; tabs and line breaks in values are rejected).
    Tsv,
    /// Per-source per-month `part_RC_YYYY-MM.jsonl` / `part_RS_YYYY-MM.jsonl`.
    Spool,
    /// Corpus-style partitioned `.zst` files under `comments/` and `submissions/`.
    Zst,
    /// Corpus-style partitioned `.jsonl` files under `comments/` and `submissions/`.
    PartitionedJsonl,
}
