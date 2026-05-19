# `src/atomic_write/`

This module owns RETL's atomic-write contract.

- `naming.rs` is the staged filename contract. Do not change
  `<file>.retl-<pid>-<nonce>.inprogress` without a migration plan for stale
  sweeps and crash recovery.
- `sweep.rs` only deletes RETL-owned leftovers whose PID is no longer live;
  live or legacy/unowned `.inprogress` files must be left in place.
- `writer.rs` contains the staging + create-new + flush/drop +
  `replace_file_atomic_backoff` publish path. Preserve zstd
  `include_checksum(true)` and explicit `finish()`.
