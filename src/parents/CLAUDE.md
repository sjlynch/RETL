# `src/parents/` orientation

See the root `CLAUDE.md` for atomic-write and zstd-reader invariants.

- `ids.rs` defines `ParentIds`/`ParentMaps` data surfaces and prefix normalization for `t1_` comments and `t3_` submissions.
- `payload.rs` owns `ParentPayloadSpec`: legacy payloads use `LEGACY_PARENT_PAYLOAD_FORMAT_VERSION`; structured payloads use `STRUCTURED_PARENT_PAYLOAD_FORMAT_VERSION`.
- Resolver outputs must rebuild when payload format/version, payload fields, source file identities, or ID-set fingerprints change.
- `resolver.rs`/`resolver_tail.rs` build parent map shards from source corpora; `attach.rs` stitches resolved payloads onto consuming JSONL records.
- `fingerprint_types.rs`/`fingerprint.rs` define sidecar schemas and digest helpers. Fingerprint field names/order are compatibility surfaces.
- Worker shard caches in attach are FIFO, not LRU: hits do not bump recency. This keeps eviction deterministic and cheap under parallel workers.
- Unordered map/set digests use the stable `(sum, xor, count)` convention; do not iterate-order hash `HashMap`/`AHashSet` directly.
- Parent attach/resolver final files and sidecars must publish through atomic staging helpers.
