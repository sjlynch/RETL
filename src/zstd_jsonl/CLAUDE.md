# `src/zstd_jsonl/` orientation

- `minimal.rs` is the hot minimal-parse path. Prefer adding cheap optional fields to `MinimalRecord` over forcing full `serde_json::Value` parses.
- `line_stream.rs` owns all zstd JSONL readers and compatibility wrappers.
- Every decoder must set `window_log_max(31)` for large Reddit frames.
- Strict mode propagates decode/callback errors; allow-partial mode reports incomplete status and must not commit resume progress.
- Progress deltas are bounded by compressed file metadata length.
- `errors.rs` centralizes malformed JSON and zstd decode error constructors; preserve wording used by tests.
