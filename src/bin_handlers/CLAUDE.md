# `src/bin_handlers/` orientation

- One file per CLI subcommand handler; CLI argument structs/enums live in `src/bin_args.rs`.
- `corpus.rs`: manifest-backed corpus plan/manifest commands. `describe.rs`: corpus availability description.
- `quickstart.rs`: tiny built-in demo corpus. `schema.rs`: sampled schema discovery.
- `scan.rs`, `dedupe.rs`, `export.rs`, `sample.rs`, `convert.rs`, `count.rs`, `integrity.rs`: thin orchestration around library APIs.
- `aggregate.rs`: CLI aggregate dispatch/manifest emission. `first_seen.rs`: first-seen index export.
- `parents.rs`/`parents_helpers.rs`: parent-ID direct and spool workflows.
- Handlers do not initialize tracing; `src/main.rs` calls `retl::init_tracing_for_binary()` once.
- Start `RunManifestStart::now()` at the top of handlers that emit manifests so wall time covers the full operation.
- Atomic CLI text writes route through `retl::write_text_atomic` (a thin wrapper over the library's `write_at_path_atomic` so staged leftovers carry the `.retl-<pid>-<nonce>` marker and are sweepable); do not write final output paths directly.
