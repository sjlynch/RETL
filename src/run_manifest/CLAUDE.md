# `src/run_manifest/` orientation

- User-facing run manifests are provenance sidecars, separate from resumable `_progress.json`.
- `types.rs` owns schema structs/constants; schema version, field names, pretty JSON, and trailing newline are stable surfaces.
- `paths.rs` and `path_stability.rs` define sidecar naming and stable path rendering.
- `identity.rs`/`identity_tail.rs` snapshot file existence, length, and mtime; missing files are represented, not ignored.
- `fingerprint.rs` hashes the canonical manifest payload as `fnv1a64:{:016x}` via shared util helpers. Do not reorder inputs.
- `upstream.rs` discovers producer manifests next to inputs/directories.
- `write.rs`/`write_tail.rs` publish through `atomic_write`; never write manifest final paths directly.
