# `src/parents_ids/` orientation

- Internal parent-ID shard scratch code; public parent APIs live in `src/parents/`.
- `scratch.rs` allocates and owns unique run roots under `work_dir/parent_ids`; drop cleanup is best-effort short backoff.
- `writer.rs`/`shards.rs` keep deterministic shard naming (`t1_ids_*.tmp`, `t3_ids_*.tmp`) and use `shard_common` for seeded routing/writer creation.
- `dedupe.rs` reduces shard files into deduped (unordered) ID files without changing line format — `dedup_one` collects IDs into an `AHashSet`, so output order is unspecified.
- `worker_cache.rs` and `shared_cache.rs` back resolver/attach lookups; preserve cache-cap and eviction behavior.
- `collect.rs` is `RedditETL::collect_parent_ids_from_jsonls` orchestration.
