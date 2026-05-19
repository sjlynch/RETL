# `src/pipeline/` orientation

- Public builder shell only; execution methods live in `src/pipeline_exec/`.
- `etl_builder.rs` owns `RedditETL::new`, forwarding setters, work-dir creation, and `scan()`.
- `scan_builder/` owns `ScanPlan` query-builder methods plus `build()` validation/fingerprint-sensitive query construction. It is an include bundle (not a submodule): `types.rs` defines `ScanPlan` and author-regex input adapters; setters are split by filter family (`subreddit_author.rs`, `ids.rs`, `text_url_domain.rs`, `timestamps_scores.rs`, `json.rs`, `options.rs`); `helpers.rs` holds shared normalization/warning helpers; `build.rs` performs final validation/compilation.
- Author-regex input conversion lives in `scan_builder/types.rs`; malformed raw patterns must surface as `QueryBuildError` during `build()`.
- Keep public paths stable (`retl::RedditETL`, `retl::ScanPlan`) and avoid initializing tracing from library code.
