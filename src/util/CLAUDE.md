# `src/util/` orientation

Crate-internal utility submodules. Submodules are private; `mod.rs` re-exports every
public/`pub(crate)` item so existing callers keep importing from `crate::util::*`.

- `backoff.rs` — Windows-friendly `*_with_backoff` / `*_with_default_backoff` /
  `*_with_short_backoff` retry helpers, `replace_file_atomic_backoff`,
  retriable-error classifier, `DEFAULT_BACKOFF_*` constants, plus the
  `cfg(test)` failure-injection scaffolding (`TestIoOp`, `TestIoFailureGuard`,
  `inject_retriable_io_errors_for_*`) used by tests in other modules.
- `exclusions.rs` — `default_bot_authors` and `try_merge_extra_exclusions`
  (consumes `ETL_EXCLUDE_AUTHORS` / `ETL_EXCLUDE_AUTHORS_FILE`).
- `scratch.rs` — `unique_scratch_dir` for per-PID scratch directory naming.
- `thread_pool.rs` — `with_thread_pool` scoped Rayon helper.
- `tracing.rs` — `init_tracing_for_binary` (binary-only).
- `mod.rs` — grab-bag bits with no clearer home (`output_parent`,
  `smoothstep_memory_fraction`, FNV-1a hashing, `system_time_parts`) plus
  re-exports.

Do not add raw `fs::*` calls; route through the `backoff` helpers so transient
Windows error codes (5, 21, 32, 33, 225, 433, 1006, 1117, 1224) keep retrying.
