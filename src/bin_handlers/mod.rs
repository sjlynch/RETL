//! Subcommand handlers for the `retl` binary. Each `run_*` function
//! corresponds 1:1 to a `Command` variant and is invoked from `main.rs`.

include!("common.rs");
include!("cli_manifest.rs");
include!("corpus/mod.rs");
include!("corpus/plan.rs");
include!("corpus/manifest.rs");
include!("describe.rs");
include!("quickstart.rs");
include!("schema.rs");
include!("scan.rs");
include!("dedupe.rs");
include!("export.rs");
include!("sample.rs");
include!("convert.rs");
include!("count.rs");
include!("integrity.rs");
include!("aggregate.rs");
include!("first_seen.rs");
include!("parents_helpers.rs");
include!("parents.rs");
include!("tests.rs");
