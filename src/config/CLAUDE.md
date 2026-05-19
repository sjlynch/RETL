# `src/config/` orientation

- Public `ETLOptions`, `Sources`, limits, and partial-read reporter types are re-exported from crate root; keep paths stable.
- `limits.rs` owns hard caps/clamps for shard/thread/file-concurrency knobs.
- `sources.rs` defines corpus source toggles.
- `partial_read.rs` records tolerated zstd decode skips for later manifest/report emission.
- `options/types.rs` owns the public `ETLOptions` fields and docs.
- `options/defaults.rs` owns user-visible defaults; keep values reviewable.
- `options/builders_core.rs` and `options/builders_output.rs` own builder-style setters.
- `options/inflight.rs` owns inflight-budget setters, peak math, and warning policy.
- Validate date ranges and resource knobs without changing existing `ConfigBuildError` wording.
