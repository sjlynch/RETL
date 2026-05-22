# `src/corpus_manifest/` orientation

A corpus manifest is the upstream catalog of `RC_YYYY-MM.zst` / `RS_YYYY-MM.zst` files (per-source defaults, expected compressed bytes, optional SHA-256) consumed by `retl corpus plan` / `corpus manifest` and the optional checks on `describe` / `integrity`.

- `types.rs` defines `CorpusManifest`, `CorpusSourceManifest`, `CorpusManifestFile`, `CorpusSource`, `CorpusAvailability`, `CorpusLocalStatus`, `CorpusPlanItem`, and `CorpusManifestError`. Schema version is pinned to `SUPPORTED_MANIFEST_VERSION`.
- `load.rs` parses JSON (`from_json_str` / `from_reader`), validates the schema version, the per-source `first_month <= last_month` ordering (`InvalidSourceRange`), and `unavailable` ranges, and exposes the bundled `builtin()` / `builtin_json()` (the `manifests/reddit_corpus_manifest.v1.json` blob shipped with the crate).
- `local.rs::inspect_local_file` returns `CorpusLocalStatus` (Missing / Inaccessible / Present + size match + optional SHA-256). SHA-256 is computed only when `verify_checksums` is true and an expected value exists; the hash read is routed through `open_with_default_backoff`. A hash *failure* on a present, readable file does not collapse to `Inaccessible` — the file stays `Present` with `sha256_matches: None` and the error surfaced in `sha256_error` (a warning, not a `has_local_problem`).
- `plan.rs` iterates the requested inclusive month range and emits one `CorpusPlanItem` per source/month, resolving file name, URL/torrent templates, and per-month overrides; `availability_note` decides Available vs Unavailable from `first_month`/`last_month`/`unavailable` ranges/per-file flags.
- This module only inspects on-disk files; it never writes corpus output, so the root `CLAUDE.md` atomic-write invariants do not apply here.
