//! Generic parallel aggregation support over JSONL inputs with progress.
//! Implement `Aggregator` for your aggregation state and call `aggregate_jsonls_parallel`.

include!("types.rs");
include!("paths.rs");
include!("merge.rs");
include!("build.rs");
include!("publish.rs");
