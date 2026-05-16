# `src/aggregate/` orientation

- Flow: `publish.rs` entry points plan inputs -> `build.rs` writes one aggregate shard per input -> `merge.rs` folds shards -> final JSON/manifest publish.
- `Aggregator::merge` must be associative. Parallel merge uses tree reduction over adjacent shards; non-associative states produce nondeterministic results.
- `paths.rs` owns run tokens, per-run scratch dirs, and shard names. Preserve filename patterns and per-run isolation.
- Partial-read policy is part of API behavior: strict drops partial shards; merge-partial folds them but reports the issue.
- Output JSON pretty/compact bytes and manifest counts/options are stable for CLI tests.
- Final aggregate and manifest writes go through atomic staging helpers; do not create final output paths directly.
