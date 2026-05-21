# CLAUDE.md — orientation for `retl`

`retl` is a streaming ETL toolkit for Reddit's monthly RC (comments) and RS
(submissions) `.zst` JSONL corpora. It is library-first (`src/lib.rs`); the
`retl` binary in `src/main.rs` is a thin clap wrapper. `examples/quickstart.rs`
demonstrates the library API.

## Canonical pipeline

`extract` → `spool` → `parents` → `aggregate`

1. **extract** — discover monthly files, decode the zstd frames as JSONL,
   filter against a `QuerySpec`, write results.
2. **spool** — `extract_spool_monthly` writes one file per month to a unique
   `<out_dir>/_staging/<file>.retl-<pid>-<nonce>.inprogress`, atomically
   promotes it on completion, and (when `resume = true`) records
   `_progress.json`.
3. **parents** — `ParentIds` collects referenced parent IDs, then
   `ParentMaps` resolves and stitches parent payloads back onto records.
4. **aggregate** — `bucketize_shards` → `build_runs_sorted` →
   `merge_runs_sorted` → `Aggregator` produces sorted, de-duplicated rollups.

`RedditETL` (in `src/pipeline.rs`) is the builder that configures everything;
`ETLOptions` holds the knobs.

## Minimal-vs-full parse pattern (the hot loop)

The line-level fast path is `zstd_jsonl::MinimalRecord` (a serde struct with
just the fields we filter on: `subreddit`, `author`, `created_utc`, `score`,
`id`, plus optional `selftext`/`body`/`title`/`parent_id`/`domain`).
`parse_minimal(line)` returns it; `filters::matches_minimal` decides
keep/drop using only that struct.

A full `serde_json::Value` parse is taken **only** when
`QuerySpec::requires_full_parse()` is true (custom `JsonPointer` keys,
arbitrary value-level predicates, etc.). Same idea in `KeyExtractor`:
`key_from_line` uses `parse_minimal` for `author`/`subreddit` keys and falls
back to `serde_json::Value` for pointer/custom keys.

When you add a new filter or key extractor, prefer extending `MinimalRecord`
(serde ignores extras, so the cost is trivial) over routing through full
`Value` parsing.

## Atomic-write invariants

- Every published file is staged at a unique
  `<out_dir>/_staging/<name>.retl-<pid>-<nonce>.inprogress`, then promoted to
  its final path with `util::replace_file_atomic_backoff(tmp, dest)`.
- Library code **never** writes to a final path directly. If you add a new
  output, route through `atomic_write::write_jsonl_atomic` /
  `write_zst_atomic_if` (or replicate their unique staging+rename dance) so a
  crashed run never leaves a partial file readers can mistake for complete
  output. Staging sweeps must not delete live files from another PID.
- `replace_file_atomic_backoff` relies on Windows
  `MoveFileExW(MOVEFILE_REPLACE_EXISTING)` for an atomic swap — readers see
  either the old or new content, never `NotFound`. Do **not** pre-remove the
  destination.
- All open/create/rename/remove go through `*_with_backoff` helpers that
  retry the transient Windows error codes (5, 32, 33, 225, 433, 1006, 1117,
  1224, 21) — sharing/AV/USB hiccups. Use `create_new_with_backoff` for
  staged files so a temp-path collision fails instead of truncating another
  writer. Don't add raw `fs::*` calls in hot publish paths.

## zstd decoding

All decoders set `decoder.window_log_max(31)`. Reddit dumps in later years
were written with the spec's max window (~2 GiB), and the zstd default
rejects them with "Frame requires too much memory." If you instantiate a
decoder, do the same.

`for_each_line_cfg` (and its `_with_skip` / `_with_progress` variants) is
the canonical reader; prefer it over rolling your own `Decoder`.

## Backpressure invariants

The bucketing/dedupe pipeline is producer→consumer with a bounded crossbeam
channel between them. The channel capacity differs by stage:

- **Dedupe** (`src/dedupe.rs::build_runs_sorted`): channel capacity is hard-
  coded to **1**. Worst-case peak ≈ `2 * per_flush_cap = inflight_bytes`.
- **Bucketing** (`src/bucketing/micro.rs::process_bucket_streaming`): channel
  capacity is `inflight_groups` (default 8). Worst-case peak ≈
  `(1 + inflight_groups) * (inflight_bytes / 2)`. With the defaults this is
  ≈ 1.125 GiB, **not** 256 MiB. The two knobs are **not** independent — use
  `ETLOptions::with_inflight_budget(bytes)` (or the `RedditETL` builder's
  `.inflight_budget(bytes)` forwarder) to set both together so the declared
  budget matches the actual peak. `BucketingCfg::from(&ETLOptions)` emits a
  one-shot `tracing::warn!` when a pair exceeds roughly 2× `inflight_bytes`;
  the dedupe stage does **not** emit this warning (its peak is always
  `inflight_bytes`, independent of `inflight_groups`).
- `per_flush_cap = inflight_bytes / 2` — the producer flushes a map once
  it reaches that byte budget.
- `inflight_bytes` defaults to **256 MiB** and is the per-flush byte lever.
  Lower it for tight environments; do **not** unbound it.
- `file_concurrency` defaults to **1** because each in-flight zstd frame
  can hold a multi-GiB decompression window. Bump it only when you've
  measured RAM headroom.

If you add another stage, give it explicit bounded channels — never
`unbounded()` between stages on the hot path.

## Resume manifest (`_progress.json`)

When `ETLOptions.resume = true`, `extract_spool_monthly` reads/writes
`<out_dir>/_progress.json`. Each entry is keyed `RC_YYYY-MM` /
`RS_YYYY-MM` and stores `{ size, lines, sha256? }`. On re-entry the recorded
`size` is checked against the on-disk file; mismatches drop the entry and
re-run that month. Format/version live in `src/progress_manifest.rs`.

## Tracing

`init_tracing_for_binary` (in `util.rs`) is **binary-only**. The library
must not initialize tracing — the binary owns the subscriber. The legacy
helper is still exported, but `src/main.rs` now installs tracing through
`retl::install_monitor` (see below) so the optional monitoring layer can
attach. `examples/quickstart.rs` still calls `init_tracing_for_binary`
directly for the no-monitoring case.

## Query authoring (for humans and LLMs)

The CLI's per-flag `--help` text covers individual filters, but composition
patterns and the non-obvious semantic landmines (missing-field handling on
`--json` predicates, URL-detection scope, subreddit normalization,
case-insensitivity rules) live in `docs/query_cookbook.md`. Stack-3-filters
worked examples cover the common shapes:

- Multi-axis author+subreddit+date+score+URL filters.
- AND/NOT keyword stacking with distinct-author dedupe.
- JSON-pointer predicates with safe missing-field handling
  (`exists:/path` before any other operator).
- `--source rc` vs `rs` vs `both` performance/semantics trade-offs.

`docs/default_bots.md` enumerates the curated bot list that
`--exclude-common-bots` applies, plus `ETL_EXCLUDE_AUTHORS` /
`ETL_EXCLUDE_AUTHORS_FILE` semantics (both additive; file errors fatal).

When adding a new filter flag, update `docs/query_cookbook.md` with at
least one worked example showing it composed with two other filters,
not just in isolation.

## Monitoring / observability

`retl` ships an opt-in machine-readable monitoring surface for LLM-driven
or scripted watchers. Tracing logs are still text on stderr by default —
nothing changes for a normal run. Flags are wired into `CommonOpts`:

- `--events <path>` — append-truncate NDJSON event stream
  (`retl.v1` schema; one JSON object per line; lifecycle + tracing +
  watchdog + status events).
- `--status-file <path>` — atomically rewritten JSON snapshot
  (`retl.status.v1`; current/peak RSS, throughput, throttle state,
  watchdog config).
- `--max-rss-mb` / `--max-runtime-sec` / `--stop-file` — watchdog kill
  switches (hard exit; safe because the library's atomic-write contract
  never leaves torn final outputs).
- `--log-format text|json` — stderr fmt layer format (`RETL_LOG_FORMAT`
  env wins).
- `--heartbeat-sec N` — periodic `kind=status` mirror events.

Implementation lives in `src/obs/` (see `src/obs/CLAUDE.md`). Schema
reference + watcher quickstart: `docs/monitoring.md`. Adding new
monitored facts: just emit `tracing::info!(field=value, ...)` — the
`EventLayer` forwards it to the stream as `kind=tracing` with no other
plumbing required.

## Build, test, bench, fuzz

```sh
cargo build --release
cargo test                              # ~95 s warm (default features)
cargo test --features test-utils        # ~60 s warm (recommended daily)
cargo doc --no-deps --open

# Bench: full fidelity for baselines.
cargo bench --bench inner_loops
cargo bench --bench inner_loops -- --save-baseline <name>
cargo bench --bench inner_loops -- --baseline <name>

# Bench: fast iteration loop (~5x faster, lower precision).
RETL_BENCH_QUICK=1 cargo bench --bench inner_loops -- \
    --quick --sample-size 10 --warm-up-time 1 --measurement-time 2 --noplot

# Property-test depth knobs (default is small for fast iteration).
RETL_PROPTEST_CASES=128 cargo test --test properties
RETL_PROPTEST_CASES=256 cargo test --test json_whitelist_proptest

# Wide-N stress + fallback budget exhaustion verification.
cargo test -- --ignored

# Fuzz targets live in fuzz/fuzz_targets (cargo-fuzz workspace excluded
# from the main workspace via Cargo.toml `exclude = ["fuzz"]`).
cd fuzz && cargo +nightly fuzz run fuzz_parse_minimal
cd fuzz && cargo +nightly fuzz run fuzz_rewrite_timestamps
```

### `test-utils` feature

Recommended for daily test iteration. Enables a small set of test-only
injection points the test suite uses to skip multi-second production retry
budgets when a test deliberately drives a non-recoverable failure (e.g.
`spool_publish_failure_is_fatal_and_does_not_commit_manifest` blocks the
publish destination with a directory, which under the production backoff
budget waits ~14 s on `ERROR_ACCESS_DENIED` retries). Without the feature
the same tests still run and pass; they just take ~35 % longer overall.

The feature also gates `retl::set_available_memory_fraction_for_tests`
(memory-pressure injection) and `retl::cap_backoff_budget_for_test` (retry
budget cap). Both are `#[doc(hidden)]` and intended only for the test
harness — production builds (`--no-default-features` or just no
`--features test-utils`) compile them out entirely.

## Files worth knowing

- `src/lib.rs` — crate-level rustdoc + public re-exports (read this first).
- `src/pipeline.rs` — `RedditETL` builder, end-to-end orchestration.
- `src/query.rs` — `QuerySpec`, `ScanPlan`, `normalize_str` (moved from `pipeline.rs`).
- `src/streaming.rs` — `stream_job` (per-file scan/write hot loop).
- `src/zstd_jsonl.rs` — `MinimalRecord`, `parse_minimal`,
  `for_each_line_cfg`, `window_log_max(31)`.
- `src/filters/` — split predicate helpers: minimal fast-path filters, full JSON-pointer predicates, text/URL matching, subreddit targets, and date bounds.
- `src/dedupe/` — bounded-channel backpressure, sorted-run dedupe.
- `src/bucketing/` — disk shard routing, seed policy, and adaptive micro-bucket backpressure.
- `src/key_extractor.rs` — `KeyExtractor::key_from_line`: `MinimalRecord` fast path for author/subreddit keys, `Value` slow path for pointer/custom keys.
- `src/atomic_write/` — staging-name contract, stale sweeps, and atomic publish helpers (see `src/atomic_write/CLAUDE.md`).
- `src/progress_manifest.rs` — resume sidecar.
- `src/util/` — split by concern (see `src/util/CLAUDE.md`):
  `backoff/` for retry policy, `*_with_backoff` I/O wrappers, and
  `replace_file_atomic_backoff`,
  `exclusions.rs` for `default_bot_authors` / `try_merge_extra_exclusions`,
  `scratch.rs` for `unique_scratch_dir`, `thread_pool.rs` for `with_thread_pool`,
  `tracing.rs` for `init_tracing_for_binary`. `mod.rs` re-exports everything so
  `crate::util::*` import paths are unchanged.
- `src/bin_args/` — clap `Cli`, `Command`, and the per-subcommand `*Args`
  structs for the binary (one file per subcommand; see `src/bin_args/CLAUDE.md`).
- `src/bin_helpers/` — shared CLI helpers: `build_etl`, `ensure_dirs`, `plan!`
  macro, `discover_spool_parts` (see `src/bin_helpers/CLAUDE.md`).
- `src/bin_handlers/` — per-subcommand `run_*` handlers, one file per
  subcommand (see `src/bin_handlers/CLAUDE.md`).
- `src/paths.rs` — `discover_all`, `plan_files`, `FileKind`, `FileJob`.
- `src/date.rs` — `YearMonth` type and year-month string parsing.
- `src/mem.rs` — `available_memory_fraction`, `is_low_memory`, `smoothstep_memory_fraction`, `maybe_throttle_low_memory`.
- `src/concurrency.rs` — semaphore-bounded rayon job-stealing helper.
- `src/progress.rs` — `make_progress_bar_labeled`, `make_count_progress`, `ProgressScope`.
- `src/shard_common.rs` — `seeded_state`/`shard_index` shared by `ShardedWriter`, `ShardedKVWriter`, `IdShardWriter`.
- `src/kv_shard.rs` — `ShardedKVWriter`: key-value sharded writer used by the parents pipeline.
- `src/stitch.rs` — `stitch_tmp_parts`, `stitch_tmp_parts_to_json_array`, `concat_tsvs`.
- `src/integrity.rs` — file-level integrity runner (`quick_validate_zst`, `validate_zst_full`).
- `src/partition.rs` — `PartitionWriters`: per-partition concurrent output for `export_partitioned`.
- `src/ndjson.rs` — `NdjsonReader`/`NdjsonWriter`: plain JSONL I/O without zstd.
- `src/json_utils.rs` — small JSON utility helpers.
