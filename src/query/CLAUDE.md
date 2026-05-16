# `src/query/` orientation

See the root `CLAUDE.md` for the minimal-vs-full parse contract.

- `spec.rs` defines `QuerySpec` and builder methods. `QuerySpec::requires_full_parse()` is the switch into full `serde_json::Value` filtering.
- `record_ids.rs` normalizes bare and fullname Reddit IDs; preserve `t1_`/`t3_` semantics and file parsing errors.
- `predicates.rs` owns JSON-pointer predicate validation and scalar/numeric comparison behavior.
- `timestamps.rs` maps exact/unix timestamp bounds to month planning helpers.
- `normalize.rs` lowercases/sorts filters and builds keyword automatons.
- Keyword `AhoCorasick` caches are `OnceLock<Arc<AhoCorasick>>`: built lazily and shared by `Clone` via `clone_keyword_cache`.
- Builder validation order is normalize -> validate -> build automata. Keep `QueryBuildError` messages stable; CLI tests assert wording.
