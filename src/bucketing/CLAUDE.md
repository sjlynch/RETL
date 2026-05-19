# `src/bucketing/` orientation

Bucketing is split by pipeline stage:

- `cfg.rs` — `BucketingCfg` and `ETLOptions` conversion.
- `hash.rs` — Stage 1, Stage 2, and micro-bucket `RandomState` seeds/helpers. The three seed sets must remain distinct.
- `routing.rs` — disk routing: `partition_stage1` and `bucketize_shards` via the shared shard router.
- `micro.rs` — adaptive in-memory producer/consumer micro-bucketing and `process_bucket_streaming`.

Backpressure guardrails in `micro.rs` are behavioral invariants: keep `per_flush_cap = inflight_bytes / 2` with the 1 MiB floor, channel capacity `cfg.inflight_groups.max(1)`, and surface consumer errors before producer errors.
