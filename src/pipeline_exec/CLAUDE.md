# `src/pipeline_exec/` orientation

See the root `CLAUDE.md` for atomic-write, zstd, resume, and backpressure invariants.

- Each `ScanPlan` public execution method now lives in the sibling file that owns its private helpers — `extract_spool_monthly` in `spool.rs`, `extract_to_jsonl`/`extract_to_json` in `extract.rs`, `extract_to_csv`/`extract_to_tsv` in `tabular/extract.rs`, `dedupe_keys_to_lines{,_with_stats}` in `dedupe_keys.rs`, `export_partitioned` in `partitioned.rs`, `count_by_month`/`author_counts_to_tsv`/`build_first_seen_index_to_tsv` in `analytics.rs`, and the `RedditETL::usernames` shim plus `ScanPlan::usernames`/`for_each_username`/`try_for_each_username` in `usernames.rs`. Keep public method signatures stable.
- Tabular orchestration is split by concern and spliced into the flat module by ordered `include!`s: `tabular/parts.rs` discovers/stitches temp parts, `tabular/stream.rs` scans one corpus file into rows, `tabular/extract.rs` owns CSV/TSV extract entry points, and `tabular/convert.rs` converts existing JSONL/spool files. The pure field-selector parser and row writer stay in `tabular_format/selector.rs` and `tabular_format/writer.rs`.
- `scan.rs` / `scan_records.rs` contain file planning and the per-file scan callback path used by analytics/checkpoint callers.
- `checkpoint.rs` owns resumable scan checkpoints under `work_dir/scan_checkpoints/`; replay and fresh scan arms must invoke the same per-record callback shape.
- `spool.rs`, `extract.rs` / `extract_common.rs`, `partitioned.rs`, `tabular/`, and `dedupe_keys.rs` also house the corresponding private helper functions; the per-output `impl ScanPlan { ... }` block sits beside them.
- `fingerprint.rs` builds resume fingerprints from operation namespace (`scan`, `spool`, `extract`, `partition_jsonl`, `partition_zst`), query/options, limit, and planned corpus file identities.
- Final outputs must be staged/published via `atomic_write::write_*_atomic` or equivalent unique `_staging/*.inprogress` + `replace_file_atomic_backoff`; never create final paths directly.
- Manifest/fingerprint bytes are part of resume compatibility. Do not reorder serialized fields or change namespace strings without a migration.
- `pipeline_exec` unit tests live under `tests/` by scenario: checkpoint reuse, resume fingerprint sensitivity, and manifest-commit failure durability share fixtures from `tests/fixtures.rs`.
