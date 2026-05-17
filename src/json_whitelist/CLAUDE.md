# `src/json_whitelist/` orientation

Streaming JSON tokenizer used by `extract --whitelist <field>` to project a subset of top-level fields directly from raw bytes, without ever building a `serde_json::Value`. Reddit records are flat top-level objects, so a top-level walk suffices.

- `tokenizer.rs` owns `WhitelistTokenizer`: built once with the requested field set, then reused per line. Walks bytes once, copies `key:raw_value` slices verbatim for whitelisted keys, skips others. `tokenize_into` / `tokenize_into_with_matches` are the plain copy paths; `tokenize_and_rewrite_timestamps_into[_with_matches]` fuse the projection with the human-timestamp byte rewrite in a single pass.
- `skip.rs` provides the cheap helpers: `skip_ws`, `scan_string_body` (returns close-index + has-escape), and `skip_value` (brace/bracket/string depth walker) used to advance past non-whitelisted values without parsing them.
- `timestamps.rs` defines `TIMESTAMP_KEYS = [created_utc, retrieved_on, edited]` and `emit_timestamp_rewritten_value`; non-integer values and out-of-range integers fall through to verbatim emission. Mirrors `streaming::rewrite_human_timestamps_bytes`.
- On any structural surprise the tokenizer returns `TokenizerError::Malformed` and the caller in `streaming/job.rs::write_via_value` falls back to the full `serde_json::Value` slow path — that is where to land when debugging "why did my line not project?".
