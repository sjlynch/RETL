# `src/util/` orientation

Crate-internal utility submodules. Submodules are private; `mod.rs` re-exports every
public/`pub(crate)` item so existing callers keep importing from `crate::util::*`.

- `backoff/` — Windows-friendly backoff stack split by concern:
  - `mod.rs` re-exports the stable `crate::util::*` surface.
  - `retry.rs` owns retry budgets, Win32 error classification, `with_backoff`,
    and the test-utils retry-budget cap.
  - `fs_ops.rs` owns the `open_*`, `create_*`, `read_dir_*`, and `remove_*`
    filesystem wrappers.
  - `replace.rs` owns `rename_with_backoff`, `copy_with_backoff`, and
    `replace_file_atomic_backoff`.
  - `testing.rs` owns the `cfg(test)` retriable-I/O failure-injection
    scaffolding (`TestIoOp`, `TestIoFailureGuard`,
    `inject_retriable_io_errors_for_*`) used by tests in other modules.
- `exclusions.rs` — `default_bot_authors` and `try_merge_extra_exclusions`
  (consumes `ETL_EXCLUDE_AUTHORS` / `ETL_EXCLUDE_AUTHORS_FILE`).
- `scratch.rs` — `unique_scratch_dir` for per-PID scratch directory naming,
  and `ScratchGuard`: an RAII guard that removes scratch paths on drop
  (covering success, `Err`, and panic-unwind) unless `disarm()`ed for a
  deliberate post-mortem.
- `thread_pool.rs` — `with_thread_pool` scoped Rayon helper.
- `tracing.rs` — `init_tracing_for_binary` (binary-only).
- `mod.rs` — grab-bag bits with no clearer home (`output_parent`,
  `smoothstep_memory_fraction`, FNV-1a hashing, `system_time_parts`) plus
  re-exports.

Do not add raw `fs::*` calls; route through the `backoff` helpers so transient
Windows error codes (5, 21, 32, 33, 225, 433, 1006, 1117, 1224) keep retrying.
