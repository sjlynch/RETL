# `src/dedupe/` orientation

Sorted-run dedupe engine: ingest one NDJSON file, group lines by a
`KeyExtractor` key, emit a single sorted, de-duplicated output. Used by the
parents/aggregate stages of the canonical pipeline.

- `cfg.rs` — `DedupeCfg` (memory + buffer + `inflight_bytes` knobs) and the
  `BUILD_RUNS_CHANNEL_CAP` / `BYTES_PER_MB` crate-internal constants. `From<&ETLOptions>`
  bridge so the pipeline builder can hand a single config through.
- `runs.rs` — phase 1: `build_runs_sorted` streams the input through a
  producer (line reader + key extractor) feeding a writer thread that emits
  one `run_<n>.ndjson` per flush, with keys in sorted order within each run.
- `merge.rs` — phase 2: `merge_runs_sorted` k-way merges the run files
  through a `BinaryHeap`, applies the reducer per key, and publishes the
  final output atomically. Variant `_with_key_stats` exposes a key-
  extraction-failure counter for the parents pipeline. The input `run_*`
  files are scratch: a `crate::util::ScratchGuard` removes them on **every**
  exit path — success, a propagated merge error, and a panic out of the
  caller's `merge_same_key` — so a re-run with a reused `runs_dir` cannot
  pick up stale runs. There is no post-mortem case here; the guard is never
  disarmed. (Contrast `aggregate`, which keeps its shard scratch on `Err`.)
- `mod.rs` — module wiring + `note_key_extraction_failed` shared helper.

## Backpressure contract

`build_runs_sorted` is the load-bearing piece for the crate-wide
`inflight_bytes` budget. The producer/consumer split uses a **bounded**
crossbeam channel with capacity `BUILD_RUNS_CHANNEL_CAP = 1`. Combined with
`per_flush_cap = cfg.inflight_bytes / 2`, the worst-case peak in-memory
footprint is:

```
peak ≈ (1 + BUILD_RUNS_CHANNEL_CAP) * per_flush_cap
     = 2 * per_flush_cap
     = inflight_bytes
```

(one map being filled by the producer + one map awaiting disk write in the
channel).

`BUILD_RUNS_CHANNEL_CAP` is **not a tunable**. Raising it raises the peak
proportionally and breaks the declared `inflight_bytes` budget — see the
top-level `CLAUDE.md` "Backpressure invariants" section for how this
interacts with the bucketing stage's separate `inflight_groups` channel.
The constant carries its own doc comment in `cfg.rs` repeating this rule
so anyone tempted to bump it sees the invariant inline.
