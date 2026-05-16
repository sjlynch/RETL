# `src/pipeline_exec/` orientation

See the root `CLAUDE.md` for atomic-write, zstd, resume, and backpressure invariants.

- `entry_points.rs` holds the `RedditETL::usernames` shim and `ScanPlan` public execution methods; keep public method signatures stable.
- `scan.rs` / `scan_records.rs` contain file planning and the per-file scan callback path used by analytics/checkpoint callers.
- `checkpoint.rs` owns resumable scan checkpoints under `work_dir/scan_checkpoints/`; replay and fresh scan arms must invoke the same per-record callback shape.
- `spool.rs`, `extract.rs` / `extract_common.rs`, `partitioned.rs`, `tabular.rs`, and `dedupe_keys.rs` implement the corresponding `ScanPlan` outputs.
- `fingerprint.rs` builds resume fingerprints from operation namespace (`scan`, `spool`, `extract`, `partition_jsonl`, `partition_zst`), query/options, limit, and planned corpus file identities.
- Final outputs must be staged/published via `atomic_write::write_*_atomic` or equivalent unique `_staging/*.inprogress` + `replace_file_atomic_backoff`; never create final paths directly.
- Manifest/fingerprint bytes are part of resume compatibility. Do not reorder serialized fields or change namespace strings without a migration.
