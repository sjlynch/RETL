# `src/pipeline/` orientation

- Public builder shell only; execution methods live in `src/pipeline_exec/`.
- `etl_builder.rs` owns `RedditETL::new`, forwarding setters, work-dir creation, and `scan()`.
- `scan_builder.rs` owns `ScanPlan` query-builder methods plus `build()` validation/fingerprint-sensitive query construction.
- Author-regex input conversion lives next to `ScanPlan`; malformed raw patterns must surface as `QueryBuildError` during `build()`.
- Keep public paths stable (`retl::RedditETL`, `retl::ScanPlan`) and avoid initializing tracing from library code.
