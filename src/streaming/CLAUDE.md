# `src/streaming/` orientation

- `job.rs` drives `stream_job` and selects `StreamWritePath::{Raw, Timestamps, Whitelist}`.
- Raw path writes matching input lines unchanged. Timestamp path rewrites `created_utc` bytes to RFC3339 when human timestamps are enabled.
- Whitelist path uses `WhitelistTokenizer` fast projection when possible and falls back to `serde_json::Value` for timestamp rewrites/full projection.
- `limit.rs` owns cooperative record limits; `RecordLimitReached` is used as a private sentinel, not a user-facing error.
- `whitelist_tracker.rs` tracks per-field matches across the whole job. Strict whitelist finalization errors when requested fields never match. The verdict is post-hoc (only known once every record is seen), so resumable callers roll back published outputs + `_progress.json` on a strict failure — see `pipeline_exec::finalize_whitelist_strict`.
- `timestamps.rs::rewrite_human_timestamps_bytes` is a hot byte-rewrite path; benchmark before changing it.
- `usernames.rs` is the legacy per-file username collector used by the deprecated `RedditETL::usernames()` path.
