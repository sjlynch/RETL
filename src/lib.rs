//! # `retl` — streaming ETL toolkit for the Reddit RC/RS monthly corpus
//!
//! `retl` scans large `.zst`-compressed JSONL drops (`RC_YYYY-MM.zst`,
//! `RS_YYYY-MM.zst`), filters them with a query DSL, and produces analysis-
//! ready exports — JSONL/JSON-array, partitioned JSONL/ZST, per-month spool,
//! per-author shards, parent-attached records, integrity reports.
//!
//! The library is built around a single-pass, memory-aware pipeline with
//! explicit backpressure and Windows-friendly atomic writes. Most user-facing
//! work flows through [`RedditETL`], a builder configured via [`ETLOptions`]
//! and a [`QuerySpec`].
//!
//! ## Pipeline order
//!
//! Public surface, listed in roughly the order data flows through it:
//!
//! 1. **Configure**
//!    - [`ETLOptions`] / [`Sources`] — base dirs, date range, parallelism,
//!      `file_concurrency` (default `1` to bound peak RAM on giant zstd
//!      windows), `inflight_bytes` (default 256 MiB; cap on producer→consumer
//!      backpressure), `resume`, IO buffer sizes, `zst_level`.
//!    - [`YearMonth`] / `iter_year_months` — inclusive month range cursors.
//!    - [`QuerySpec`] — subreddit / author / regex / keyword / domain / score
//!      filters; exposes `requires_full_parse()` to choose between the
//!      [`MinimalRecord`] fast-path and a full `serde_json::Value` parse.
//!
//! 2. **Discover & plan**
//!    - `discover_all` / `plan_files` (doc(hidden)) walk the corpus and emit
//!      [`FileJob`]s honoring the configured sources and date bounds.
//!    - [`for_each_file_limited`](crate::concurrency::for_each_file_limited)
//!      drives the per-file fan-out under a scoped Rayon pool.
//!
//! 3. **Decode & filter (hot loop)**
//!    - [`MinimalRecord`] + [`parse_minimal`] — line-level fast-path schema.
//!    - [`matches_minimal`] / `within_bounds` / `bounds_tuple` — predicate
//!      evaluation against the minimal record.
//!    - The slow path (whitelist/full parse) is taken only when
//!      `QuerySpec::requires_full_parse()` returns true; otherwise the
//!      minimal struct alone decides keep/drop.
//!    - [`for_each_line_cfg`] / [`quick_validate_zst`] / [`validate_zst_full`]
//!      — zstd readers configured with `window_log_max(31)` so frames written
//!      at the spec's max window size decode without "Frame requires too much
//!      memory."
//!
//! 4. **Emit**
//!    - [`stream_job`](crate::streaming::stream_job) drives the per-file
//!      scan + write; [`apply_human_timestamps`] rewrites unix epochs to
//!      RFC3339 when enabled.
//!    - [`ShardedWriter`] (per-month spool) and [`ShardedKVWriter`]
//!      (per-author shards) handle on-disk fan-out.
//!    - [`PartitionWriters`] writes RC/RS partitions as JSONL or ZST.
//!    - [`NdjsonReader`] / [`NdjsonWriter`] are the line-buffered helpers
//!      used by stitch and dedupe.
//!
//! 5. **Reduce**
//!    - [`bucketize_shard`] / [`process_bucket_streaming`] / [`partition_stage1`]
//!      perform the bucketing stage that feeds dedupe.
//!    - [`build_runs_sorted`] / [`merge_runs_sorted`] (driven by
//!      [`DedupeCfg`] + [`KeyExtractor`]) produce sorted, de-duplicated runs.
//!      `KeyExtractor::key_from_line` uses [`MinimalRecord`] for the common
//!      `author`/`subreddit` keys; pointer/custom keys fall back to a full
//!      `serde_json::Value` parse.
//!    - [`Aggregator`] computes per-author / per-month rollups.
//!    - [`ParentIds`] / [`ParentMaps`] resolve parent content for the
//!      parents-pipeline.
//!
//! 6. **Publish atomically**
//!    - Every output is written to `<dir>/_staging/<file>.inprogress`, then
//!      promoted to its final path via
//!      [`replace_file_atomic_backoff`]. Library code never writes to a final
//!      path directly. See [`crate::atomic_write`] for the staging contract.
//!    - The optional resume manifest at `<out_dir>/_progress.json` (see
//!      [`progress_manifest`](crate::progress_manifest)) records each
//!      committed month so a crashed run can skip what already landed.
//!
//! 7. **Verify**
//!    - [`IntegrityMode::Quick`] / [`IntegrityMode::Full`] +
//!      `RedditETL::check_corpus_integrity` validate `.zst` files.
//!    - [`quick_validate_zst`] / [`validate_zst_full`] are the underlying
//!      decoders, also re-exported for direct use.
//!
//! ## Cross-cutting helpers
//!
//! - [`open_with_backoff`] / [`create_with_backoff`] / [`remove_with_backoff`]
//!   / [`replace_file_atomic_backoff`] — Windows-friendly retry/backoff over
//!   transient sharing/AV errors.
//! - [`with_thread_pool`] — scoped Rayon pool (preferred over
//!   `build_global`).
//! - [`init_tracing_for_binary`] — *binary-only* tracing init; library code
//!   must not call it.
//! - [`set_global_multiprogress`] / [`make_count_progress`] /
//!   [`make_progress_bar_labeled`] / [`ProgressScope`] — indicatif glue.
//! - [`available_memory_fraction`] / [`is_low_memory`] — adaptive throttling
//!   knobs for binaries.
//!
//! See `CLAUDE.md` at the repository root for invariants (atomic-write
//! contract, backpressure model, taskboard rules) and bench/fuzz commands.

mod config;
mod date;
mod paths;
mod zstd_jsonl;
mod shard_common;
mod shard;
mod username_stream;
mod query;
mod kv_shard;

mod filters;
mod progress;
mod stitch;
mod concurrency;
mod streaming;
mod util;
mod mem;
mod atomic_write;
mod progress_manifest;
mod pipeline;
mod pipeline_exec;

mod parents;
mod parents_ids;
mod aggregate;
mod integrity;
mod partition;

mod bucketing;
mod json_utils;
mod json_whitelist;
mod ndjson;
mod key_extractor;
mod dedupe;

pub use crate::config::{ETLOptions, Sources};
pub use crate::date::YearMonth;
pub use crate::pipeline::RedditETL;
pub use crate::pipeline_exec::ExportFormat;
pub use crate::shard::UsernameStream;
pub use crate::query::QuerySpec;

pub use crate::parents::{ParentIds, ParentMaps};
pub use crate::aggregate::Aggregator;

#[doc(hidden)]
pub use crate::aggregate::{merge_aggregator_shards_parallel, merge_aggregator_shards_serial};

// Expose multiprogress and progress helpers.
pub use crate::progress::{set_global_multiprogress, make_count_progress, make_progress_bar_labeled, ProgressScope};

// Expose memory helpers for adaptive throttling from the binary.
pub use crate::mem::{available_memory_fraction, is_low_memory, maybe_throttle_low_memory};

// Test-only injection point so integration tests can drive the cooperative
// throttles in dedupe/bucketing/zstd_jsonl. Strictly gated; production
// builds (no `test-utils`) don't see this symbol.
#[cfg(any(test, feature = "test-utils"))]
#[doc(hidden)]
pub use crate::mem::set_available_memory_fraction_for_tests;

// Expose integrity checker mode, and (optionally) direct zstd validators.
pub use crate::integrity::IntegrityMode;
pub use crate::zstd_jsonl::{quick_validate_zst, validate_zst_full};

//export partition writers (lambda-capable)
pub use crate::partition::PartitionWriters;

//export robust file ops from util so binaries can import from crate root.
pub use crate::util::{open_with_backoff, create_with_backoff, remove_with_backoff, replace_file_atomic_backoff};

// Scoped rayon pool + opt-in tracing init for binaries.
pub use crate::util::{init_tracing_for_binary, with_thread_pool};

//export bucketing & json utils to application code
pub use crate::bucketing::{BucketingCfg, partition_stage1, bucketize_shards, process_bucket_streaming};
pub use crate::json_utils::{author_lower, subreddit_lower, is_comment_record};

// export NDJSON helpers
pub use crate::ndjson::{NdjsonReader, NdjsonWriter};

// export streaming whitelist tokenizer for tests/benches
#[doc(hidden)]
pub use crate::json_whitelist::{TokenizerError, WhitelistTokenizer};

// export KeyExtractor abstraction
pub use crate::key_extractor::KeyExtractor;

// export dedupe engine
pub use crate::dedupe::{DedupeCfg, build_runs_sorted, merge_runs_sorted};
pub use crate::mem::AdaptiveMemCfg;

// Test-only re-exports of internals so behavioral tests can drive them directly.
// Behavior is unchanged; these are additive exports used by tests/*.rs.
#[doc(hidden)]
pub use crate::shard::ShardedWriter;
#[doc(hidden)]
pub use crate::kv_shard::ShardedKVWriter;
#[doc(hidden)]
pub use crate::concurrency::for_each_file_limited;
#[doc(hidden)]
pub use crate::paths::{plan_files, discover_all, FileKind, FileJob, Discovered};
#[doc(hidden)]
pub use crate::date::iter_year_months;
#[doc(hidden)]
pub use crate::filters::{bounds_tuple, within_bounds};
#[doc(hidden)]
pub use crate::zstd_jsonl::{MinimalRecord, parse_minimal};

// Bench-only re-exports of hot inner-loop functions. Used by `benches/inner_loops.rs`
// (criterion harness) to defend ahash/byte-rewrite perf changes against regressions.
// Also exposes human-timestamp rewriters so fuzz targets and behavioral tests can
// exercise them directly. Keep `#[doc(hidden)]` to avoid signalling these as part
// of the supported public API.
#[doc(hidden)]
pub use crate::filters::matches_minimal;
#[doc(hidden)]
pub use crate::streaming::{apply_human_timestamps, rewrite_human_timestamps_bytes};
#[doc(hidden)]
pub use crate::zstd_jsonl::for_each_line_cfg;
