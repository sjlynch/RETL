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
//!    - [`ETLOptions`] / [`Sources`] — base dirs, date range,
//!      `file_concurrency` (default `1`, capped by [`MAX_FILE_CONCURRENCY`] to
//!      bound peak RAM on giant zstd windows), `shard_count` (capped by
//!      [`MAX_SHARDS`]), `parallelism` (capped by [`max_parallelism_limit`]),
//!      `inflight_bytes` (default 256 MiB; cap on producer→consumer
//!      backpressure), `inflight_groups` (bucketing channel depth),
//!      `adaptive_mem` thresholds, `resume`, `allow_partial`, IO buffer
//!      sizes, `zst_level`.
//!    - [`YearMonth`] / `iter_year_months` — inclusive month range cursors.
//!    - [`ScanPlan`] / [`QuerySpec`] — the query builder returned by
//!      [`RedditETL::scan`], plus subreddit / author / regex / keyword / domain
//!      / score / JSON-pointer predicate filters. `ScanPlan::build` returns
//!      [`QueryBuildError`] for contradictory, empty, or blank-normalized query
//!      settings before any corpus file is scanned. `.contains_url(true)` is a
//!      positive URL-presence filter; `.contains_url(false)` clears it and is
//!      equivalent to omitting the filter (there is no negative URL filter yet).
//!      `QuerySpec` exposes
//!      `requires_full_parse()` to choose between the [`MinimalRecord`] fast-path
//!      and a full `serde_json::Value` parse.
//!
//! 2. **Discover & plan**
//!    - `discover_sources_checked` / `plan_files_checked` (doc(hidden)) walk
//!      the corpus and emit [`FileJob`]s honoring the configured sources and
//!      date bounds while surfacing directory/filename diagnostics.
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
//!      memory." Corpus scans are strict by default; opt into
//!      `allow_partial` only when lossy skipped-file reporting is acceptable.
//!
//! 4. **Emit**
//!    - [`stream_job`](crate::streaming::stream_job) drives the per-file
//!      scan + write; [`apply_human_timestamps`] rewrites unix epochs to
//!      RFC3339 when enabled.
//!    - [`ShardedWriter`] (per-month spool) and [`ShardedKVWriter`]
//!      (per-author shards) handle on-disk fan-out.
//!    - `ScanPlan::export_partitioned` (called after `RedditETL::scan`)
//!      writes RC/RS partitions as JSONL or ZST.
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
//!    - Every output is written to a unique
//!      `<dir>/_staging/<file>.retl-<pid>-<nonce>.inprogress`, then
//!      promoted to its final path via
//!      [`replace_file_atomic_backoff`]. Library code never writes to a final
//!      path directly. See [`crate::atomic_write`] for the staging contract.
//!    - The optional resume manifest at `<out_dir>/_progress.json` (see
//!      [`progress_manifest`](crate::progress_manifest)) records each
//!      committed month so a crashed run can skip what already landed; months
//!      skipped by `allow_partial` are intentionally left uncommitted.
//!
//! 7. **Verify**
//!    - [`IntegrityMode::Quick`] (positive prefix sample) /
//!      [`IntegrityMode::Full`] + `RedditETL::check_corpus_integrity` validate
//!      `.zst` files and return a materialized failure list.
//!    - [`RedditETL::check_corpus_integrity_with_failure_sink`] streams each
//!      failure to a caller-provided callback during long runs.
//!    - [`quick_validate_zst`] / [`validate_zst_full`] are the underlying
//!      decoders, also re-exported for direct use.
//!
//! ## Cross-cutting helpers
//!
//! - [`open_with_backoff`] / [`create_with_backoff`] /
//!   [`create_dir_all_with_backoff`] / [`read_dir_with_backoff`] /
//!   [`remove_with_backoff`] / [`replace_file_atomic_backoff`] —
//!   Windows-friendly retry/backoff over transient sharing/AV errors.
//! - [`with_thread_pool`] — scoped Rayon pool (preferred over
//!   `build_global`).
//! - [`init_tracing_for_binary`] — *binary-only* tracing init; library code
//!   must not call it.
//! - [`set_global_multiprogress`] / [`make_count_progress`] /
//!   [`make_progress_bar_labeled`] / [`ProgressScope`] — indicatif glue.
//! - [`PartitionWriters`] — standalone user-keyed NDJSON fan-out helper that
//!   writes `<stem>_part_NNNN.ndjson`; it is not the RC/RS JSONL/ZST
//!   partitioned export path.
//! - [`available_memory_fraction`] / [`is_low_memory`] — adaptive throttling
//!   knobs for binaries.
//!
//! See `CLAUDE.md` at the repository root for invariants (atomic-write
//! contract, backpressure model, taskboard rules) and bench/fuzz commands.

mod config;
mod date;
mod kv_shard;
mod paths;
mod query;
mod shard;
mod shard_common;
mod username_stream;
mod zstd_jsonl;

mod atomic_write;
mod concurrency;
mod filters;
mod mem;
mod pipeline;
mod pipeline_exec;
mod progress;
mod progress_manifest;
mod run_manifest;
mod stitch;
mod streaming;
mod util;

mod aggregate;
mod integrity;
mod parents;
mod parents_ids;
mod partition;

mod bucketing;
mod dedupe;
mod json_utils;
mod json_whitelist;
mod key_extractor;
mod ndjson;

pub use crate::config::{
    max_parallelism_limit, ConfigBuildError, ETLOptions, PartialReadReport, PartialReadReporter,
    SkippedFile, Sources, MAX_FILE_CONCURRENCY, MAX_RAYON_THREADS, MAX_SHARDS,
};
pub use crate::date::YearMonth;
pub use crate::pipeline::{RedditETL, ScanPlan};
pub use crate::pipeline_exec::{DedupeKeySummary, ExportFormat, TabularExportOptions};
pub use crate::query::{JsonPointerPredicate, NumericComparison, QueryBuildError, QuerySpec};
pub use crate::run_manifest::{
    discover_upstream_manifests_from_inputs, file_identities, file_identity,
    manifest_path_for_directory, manifest_path_for_file, path_to_stable_string,
    upstream_manifest_for_directory, write_run_manifest, CorpusSnapshot, FileIdentity, GeneratedBy,
    ManifestDestination, OutputSnapshot, ResumeSnapshot, RunManifest, RunManifestInput,
    RunManifestStart, UpstreamManifest, DIR_MANIFEST_NAME, FILE_MANIFEST_SUFFIX,
};
pub use crate::shard::UsernameStream;

pub use crate::aggregate::{
    AggregateBuildReport, AggregateInputIssue, AggregatePartialReadPolicy, Aggregator,
};
pub use crate::parents::{
    ParentAttachStats, ParentIds, ParentMaps, ParentPayload, ParentPayloadSpec,
};

#[doc(hidden)]
pub use crate::aggregate::{merge_aggregator_shards_parallel, merge_aggregator_shards_serial};

// Expose multiprogress and progress helpers.
pub use crate::progress::{
    make_count_progress, make_progress_bar_labeled, set_global_multiprogress,
    total_compressed_size, ProgressScope,
};

// Expose memory helpers for adaptive throttling from the binary.
pub use crate::mem::{
    available_memory_fraction, is_low_memory, maybe_throttle_low_memory, sysinfo_ok,
    FALLBACK_FRAC_ENV,
};

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
pub use crate::partition::{PartitionWriters, MAX_PARTITIONS};

//export robust file ops from util so binaries can import from crate root.
pub use crate::util::{
    create_dir_all_with_backoff, create_new_with_backoff, create_with_backoff, open_with_backoff,
    read_dir_with_backoff, remove_with_backoff, replace_file_atomic_backoff,
};

// Scoped rayon pool + opt-in tracing init for binaries.
pub use crate::util::{init_tracing_for_binary, with_thread_pool};

//export bucketing & json utils to application code
pub use crate::bucketing::{
    bucketize_shards, partition_stage1, process_bucket_streaming, BucketingCfg,
};
pub use crate::json_utils::{author_lower, is_comment_record, subreddit_lower};

// export NDJSON helpers
pub use crate::ndjson::{read_line_capped, NdjsonReader, NdjsonWriter, DEFAULT_MAX_LINE_BYTES};

// export streaming whitelist tokenizer for tests/benches
#[doc(hidden)]
pub use crate::json_whitelist::{TokenizerError, WhitelistTokenizer};

// export KeyExtractor abstraction
pub use crate::key_extractor::KeyExtractor;

// export dedupe engine
pub use crate::dedupe::{build_runs_sorted, merge_runs_sorted, DedupeCfg};
pub use crate::mem::AdaptiveMemCfg;

// Test-only re-exports of internals so behavioral tests can drive them directly.
// Behavior is unchanged; these are additive exports used by tests/*.rs.
#[doc(hidden)]
pub use crate::concurrency::for_each_file_limited;
#[doc(hidden)]
pub use crate::date::iter_year_months;
#[doc(hidden)]
pub use crate::filters::{bounds_tuple, resolve_target_subs_from, within_bounds, DateBounds};
#[doc(hidden)]
pub use crate::kv_shard::ShardedKVWriter;
#[doc(hidden)]
pub use crate::paths::{
    discover_all, discover_all_checked, discover_sources_checked, format_year_month_ranges,
    log_missing_month_warnings, missing_month_diagnostics, plan_files, plan_files_checked,
    Discovered, FileJob, FileKind, MissingMonthDiagnostic, PlanningError, SourceStatus,
};
#[doc(hidden)]
pub use crate::shard::ShardedWriter;
#[doc(hidden)]
pub use crate::zstd_jsonl::{parse_minimal, MinimalRecord};

// Bench-only re-exports of hot inner-loop functions. Used by `benches/inner_loops.rs`
// (criterion harness) to defend ahash/byte-rewrite perf changes against regressions.
// Also exposes human-timestamp rewriters so fuzz targets and behavioral tests can
// exercise them directly. Keep `#[doc(hidden)]` to avoid signalling these as part
// of the supported public API.
#[doc(hidden)]
pub use crate::filters::matches_minimal;
#[doc(hidden)]
pub use crate::streaming::{
    apply_human_timestamps, project_whitelist_line_for_tests, rewrite_human_timestamps_bytes,
};
#[doc(hidden)]
pub use crate::zstd_jsonl::for_each_line_cfg;
