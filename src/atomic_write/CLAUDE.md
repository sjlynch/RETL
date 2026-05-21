# `src/atomic_write/`

This module owns RETL's atomic-write contract.

- `naming.rs` is the staged filename contract. Do not change
  `<file>.retl-<pid>-<nonce>.inprogress` without a migration plan for stale
  sweeps and crash recovery.
- `sweep.rs` deletes RETL-owned leftovers whose PID is no longer live, plus
  leftovers older than `LIVE_PID_RECYCLE_GRACE` (6 h) whose embedded PID has
  been recycled by an unrelated live process. Legacy/unowned `.inprogress`
  files, and recent files owned by a live PID, must be left in place. The
  age gate reads the nanosecond timestamp embedded by `naming.rs` (mtime
  fallback for legacy suffixes) so a recycled PID can't leak `_staging`
  forever.
- `writer.rs` contains the staging + create-new + flush/drop +
  `replace_file_atomic_backoff` publish path. Preserve zstd
  `include_checksum(true)` and explicit `finish()`.
