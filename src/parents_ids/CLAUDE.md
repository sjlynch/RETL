# `src/parents_ids/` orientation

- Internal parent-ID shard scratch code; public parent APIs live in `src/parents/`.
- `scratch.rs` allocates and owns unique run roots under `work_dir/parent_ids`; drop cleanup is best-effort short backoff.
- `writer.rs`/`shards.rs` keep deterministic shard naming (`t1_ids_*.tmp`, `t3_ids_*.tmp`) and use `shard_common` for seeded routing/writer creation. `IdShardWriter::dedup` wraps its raw `<kind>_ids_shards/` and `<kind>_ids_dedup/` directories in a `crate::util::ScratchGuard`: a `dedup_one` failure or rayon-worker panic removes both eagerly rather than waiting on the `IdScratchRoot::drop` fallback. On success the guard is disarmed and only the raw shard scratch is removed — `<kind>_ids_dedup/` becomes the live output.
- `dedupe.rs` reduces shard files into sorted/deduped ID files without changing line format.
- `worker_cache.rs` and `shared_cache.rs` back resolver/attach lookups; preserve cache-cap and eviction behavior.
- `collect.rs` is `RedditETL::collect_parent_ids_from_jsonls` orchestration.
