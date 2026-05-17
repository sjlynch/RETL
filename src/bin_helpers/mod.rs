//! Shared helpers for the `retl` binary's subcommand handlers:
//! the `RecCount` aggregator, ETL builder glue, the `plan!` macro, and a
//! couple of CLI-only path/I/O helpers.

include!("aggregate/mod.rs");
include!("build.rs");
include!("query_predicates.rs");
include!("etl.rs");
include!("plan.rs");
include!("io.rs");
