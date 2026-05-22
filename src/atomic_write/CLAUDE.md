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
  forever. `sweep_stale_inprogress` reclaims two artifact kinds with the same
  rules: `.inprogress` files under `_staging`, and `.atomic-replace-tmp`
  copy+rename fallback siblings that `replace_file_atomic_backoff` orphans
  next to a published output (in `out_root` itself). `sweep_stale_atomic_replace_tmp`
  applies the `.atomic-replace-tmp` half to a nested output directory (e.g.
  a partitioned export's `comments/` dir) the `out_root` scan can't reach.
- `writer.rs` contains the staging + create-new + flush/drop +
  `replace_file_atomic_backoff` publish path. Preserve zstd
  `include_checksum(true)` and explicit `finish()`.
