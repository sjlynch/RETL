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
    /// Output destination — file for `jsonl`/`json` (default stdout; `-` also
    /// means stdout), directory for `spool`/`zst`/`partitioned-jsonl` (those
    /// formats require an explicit `--out <DIR>`).
    #[arg(long, short, default_value = "-")]
    pub(crate) out: PathBuf,
    /// Field-indent the JSON array (only with `--format json`; rejected for
    /// every other format because it would have no effect).
    #[arg(long)]
    pub(crate) pretty: bool,
    /// zstd compression level for `.zst` outputs. Clamped to 1..=22 by the
    /// library. Only valid with `--format zst`; rejected for other formats
    /// because they produce no `.zst` output.
    #[arg(long)]
    pub(crate) zst_level: Option<i32>,
    /// Rows per Parquet row group (only with `--format parquet`/
    /// `partitioned-parquet`; rejected for other formats). Default: 131072.
    #[arg(long)]
    pub(crate) parquet_row_group_size: Option<usize>,
    /// Parquet compression codec (only with `--format parquet`/
    /// `partitioned-parquet`; rejected for other formats). Accepts
    /// `uncompressed`, `snappy`, `gzip[:LEVEL]`, `brotli[:LEVEL]`, `lz4`,
    /// `lz4_raw`, `zstd[:LEVEL]`. Default: `zstd:3`.
    #[arg(long)]
    pub(crate) parquet_compression: Option<String>,
    /// Whitelist of top-level fields to keep on export. Comma-separated, repeatable.
    /// Required for csv/tsv; missing fields render as empty cells.
    /// Comments use `body`/`parent_id`/`link_id`; submissions use `title`/`selftext`/`domain`.
    #[arg(long, value_delimiter = ',')]
    pub(crate) whitelist: Vec<String>,
    /// Error if a `--whitelist` field never matches. The verdict is post-hoc
    /// (decided after streaming); on failure the run's output files and any
    /// `_progress.json` are discarded so a resumed run re-streams every month
    /// instead of skipping them.
    #[arg(long)]
    pub(crate) strict_whitelist: bool,
    /// Convert `created_utc`, `retrieved_on`, and `edited` to RFC3339 strings
    /// on JSON-family exports (not csv/tsv).
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
    /// corpus paths, sources, date range, whitelist, `--limit`, timestamp
    /// formatting, or (for ZST) zst level invalidates the checkpoint and
    /// rebuilds the parts.
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
    /// Single Apache Parquet file. Schema covers `MinimalRecord` fields plus a
    /// `payload` STRING column carrying the raw JSON line for downstream
    /// readers that need fields outside the minimal schema. Requires the
    /// `parquet` cargo feature.
    Parquet,
    /// Corpus-style partitioned `.parquet` files under `comments/` and
    /// `submissions/`, one file per month. Requires the `parquet` cargo
    /// feature.
    PartitionedParquet,
}
