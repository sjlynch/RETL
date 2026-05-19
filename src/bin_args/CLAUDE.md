# `src/bin_args/` orientation

Clap argument structs for the `retl` binary. Binary-only — these types must not leak into `src/lib.rs`. Handler bodies live in `src/bin_handlers/` (parallel layout: one file per subcommand).

- `mod.rs` owns the top-level `Cli` + `Command` enum and re-exports the per-subcommand `*Args` structs.
- `tests/` holds the in-process clap help-surface and parse-validation unit tests.
- One file per subcommand: `aggregate.rs`, `convert.rs`, `corpus.rs`, `count.rs`, `dedupe.rs`, `describe.rs`, `export.rs`, `first_seen.rs`, `integrity.rs`, `parents.rs`, `quickstart.rs`, `sample.rs`, `scan.rs`, `schema.rs`. Each derives `clap::Args` and flattens `CommonOpts`/`QueryOpts` where applicable.
- `common.rs` defines the shared `CommonOpts` (paths, date range, parallelism, source, partial-read flag, etc.), `QueryOpts` (filters, regex, timestamps, JSON predicates), and the `SourceArg` clap enum that converts to `retl::Sources`.
- `parsers.rs` holds custom clap value parsers: `parse_timestamp_bound` (epoch / RFC3339 / `YYYY-MM-DD`) and `parse_positive_sample_bytes`. Wording of their error strings is asserted by CLI tests.
- Builders deliberately accept large values (e.g. `usize::MAX` for `--parallelism`) — clamping happens downstream in `bin_helpers::build_etl`.
