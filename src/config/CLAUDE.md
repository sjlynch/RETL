# `src/config/` orientation

- Public `ETLOptions`, `Sources`, limits, and partial-read reporter types are re-exported from crate root; keep paths stable.
- `limits.rs` owns hard caps/clamps and inflight-budget warning math.
- `sources.rs` defines corpus source toggles.
- `partial_read.rs` records tolerated zstd decode skips for later manifest/report emission.
- `options.rs` owns defaults and builder-style setters. Defaults are user-visible CLI/library behavior.
- Validate date ranges and resource knobs without changing existing `ConfigBuildError` wording.
