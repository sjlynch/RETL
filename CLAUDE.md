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
4. **aggregate** — `bucketize_shard` → `build_runs_sorted` →
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
  `write_zst_atomic` (or replicate their unique staging+rename dance) so a
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
- **Bucketing** (`src/bucketing.rs::process_bucket_streaming`): channel
  capacity is `inflight_groups` (default 8). Worst-case peak ≈
  `(1 + inflight_groups) * (inflight_bytes / 2)`. With the defaults this is
  ≈ 1.125 GiB, **not** 256 MiB. The two knobs are **not** independent — use
  `RedditETL::inflight_budget(bytes)` to set both together so the declared
  budget matches the actual peak. `BucketingCfg::from(&ETLOptions)` emits a
  one-shot `tracing::warn!` when a pair exceeds roughly 2× `inflight_bytes`.
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
must not initialize tracing — the binary owns the subscriber. `main.rs` and
`examples/quickstart.rs` call it once at startup; nothing else should.

## Taskboard rules (Lattice)

When `$LATTICE_API_URL` is set, "the board" means the Lattice taskboard, not
Claude Code's task list:

- Create/update tasks via `POST $LATTICE_API_URL/api/tasks/batch`. Do not
  invent endpoints; check `/api/tasks/...` for what's supported.
- Lattice routes parallel tasks into separate worktrees; **multiple tasks
  editing the same lines of the same file conflict at merge time**. When
  splitting work, partition by file (or by clearly disjoint regions).
- A task's worktree gets a `LATTICE_TASK.md` with its instructions. Read
  that first; resume in place if a prior session committed partial work.
- Lattice merges via `git merge`, so always commit before ending a session.
- `PATCH /api/tasks/<id>` with `{"description": "..."}` to summarize what
  actually changed once the task lands in "Ready to Merge".

## Build, test, bench, fuzz

```sh
cargo build --release
cargo test
cargo doc --no-deps --open

cargo bench --bench inner_loops      # criterion harness, defends ahash /
                                     # byte-rewrite hot loops vs regressions
cargo bench --bench inner_loops -- --save-baseline <name>
cargo bench --bench inner_loops -- --baseline <name>

# Fuzz targets live in fuzz/fuzz_targets (cargo-fuzz workspace excluded
# from the main workspace via Cargo.toml `exclude = ["fuzz"]`).
cd fuzz && cargo +nightly fuzz run fuzz_parse_minimal
cd fuzz && cargo +nightly fuzz run fuzz_rewrite_timestamps
```

## Files worth knowing

- `src/lib.rs` — crate-level rustdoc + public re-exports (read this first).
- `src/pipeline.rs` — `RedditETL` builder, end-to-end orchestration.
- `src/query.rs` — `QuerySpec`, `ScanPlan`, `normalize_str` (moved from `pipeline.rs`).
- `src/streaming.rs` — `stream_job` (per-file scan/write hot loop).
- `src/zstd_jsonl.rs` — `MinimalRecord`, `parse_minimal`,
  `for_each_line_cfg`, `window_log_max(31)`.
- `src/filters.rs` — `matches_minimal` / `matches_full` / `within_bounds`.
- `src/dedupe.rs` — bounded-channel backpressure, sorted-run dedupe.
- `src/key_extractor.rs` — `KeyExtractor::key_from_line`: `MinimalRecord` fast path for author/subreddit keys, `Value` slow path for pointer/custom keys.
- `src/atomic_write.rs` — staging + atomic publish helpers.
- `src/progress_manifest.rs` — resume sidecar.
- `src/util.rs` — `*_with_backoff` I/O retries, `with_thread_pool`,
  `init_tracing_for_binary`.
- `src/bin_args.rs` — clap `Cli`, `Command`, and `*Args` structs for the binary.
- `src/bin_helpers.rs` — shared CLI helpers: `build_etl`, `ensure_dirs`, `plan!` macro, `discover_spool_parts`.
- `src/bin_handlers.rs` — per-subcommand `run_*` handlers.
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
