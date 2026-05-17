# `src/bin_helpers/` orientation

- CLI-only helpers; library public API belongs under `src/lib.rs` and friends.
- `aggregate/` contains built-in `RecCount`/grouped metric aggregators and exact numeric formatting/sorting semantics, split by concern: `spec.rs` (CLI-parsing wrappers), `numeric.rs` (exact-arithmetic core), `format.rs` (pure number-to-string formatting, no dependency on the other aggregate submodules), `group.rs` (grouped `Aggregator` impl). Files are textually concatenated into `bin_helpers/mod.rs` via `include!`.
- `query_predicates.rs` parses CLI JSON-pointer predicates into `JsonPointerPredicate`.
- `build.rs`/`etl.rs` translate common CLI flags into `RedditETL` options and emit partial-read reports.
- `plan.rs` contains the `plan!` macro used by handlers.
- `io.rs` streams file outputs to stdout and discovers spool parts.
- Preserve CLI wording and numeric output bytes; integration tests depend on them.
