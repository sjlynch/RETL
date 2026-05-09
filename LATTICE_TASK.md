# [Perf] Drop serde_json::Value in bucketing + parallelize stage-2 + add backpressure regression test

src/bucketing.rs has three perf hot spots that share a fix, plus a missing regression test that's bundled here because it edits the same functions:

PERF
1. `partition_stage1` (src/bucketing.rs:~98) does `serde_json::from_str::<Value>(&line)` *just to call* `extract_key(&Value)`. For Reddit records that's a full DOM parse (~1-3 KiB of allocations per line) thrown away after one field lookup.
2. `bucketize_shard` (src/bucketing.rs:~121) is sequential AND does the same per-line Value parse. Stage 1 was parallelized in T8; Stage 2 wasn't. Parallelize across multiple input shards via rayon par_iter.
3. `process_bucket_streaming` (src/bucketing.rs:~281) does the same Value parse per line, plus uses `r.lines()` which allocates a fresh String per line. The dedupe path already proved a `KeyExtractor::key_from_line(&str)` abstraction works on raw bytes — see src/key_extractor.rs and src/dedupe.rs.

Refactor:
  - Change all three stages from `extract_key: Fn(&Value) -> Option<String>` to     `key: &KeyExtractor` (or a `Fn(&str) -> Option<String>` over raw lines),     matching what dedupe.rs already does.
  - Replace `for line in r.lines()` with `read_line(&mut buf)` against a     reusable String buffer (mirror src/zstd_jsonl.rs:for_each_line_attempt).
  - Parallelize bucketize_shard via rayon::par_iter.
  - Append a 'bucketing' bench group to benches/inner_loops.rs.

TEST (folded in here to avoid editing the same functions in two PRs)
Add tests/backpressure_bucketing.rs that proves the bounded crossbeam channel in process_bucket_streaming actually blocks the producer when the consumer is slow. Strategy:
  - synthesize ~50 MiB of NDJSON in a tempdir bucket
  - call process_bucket_streaming with cfg.inflight_bytes = 4 MiB and     cfg.inflight_groups = 2
  - in on_group, sleep ~50ms before returning
  - peak resident memory (or buffered_bytes via a shared atomic exposed through     a test-only callback hook) must stay under a documented cap.
If a metrics hook is needed in src/bucketing.rs to make this observable, gate it behind `#[cfg(any(test, feature="test-utils"))]` so production carries no overhead.

MERGE-CONFLICT NOTES (read before starting)
  - benches/inner_loops.rs is also appended to by [Perf] tokenizer-fuse task.     Whichever lands first wins — the second PR should append its bench group     BELOW (not interleaved with) the first.
  - src/lib.rs may need a new pub use for KeyExtractor changes; append at the     bottom of the existing pub use list — do not edit the top-of-file rustdoc     block (owned by the [Docs/LLM] task).

Acceptance:
  - cargo bench --bench inner_loops shows >=2x speedup on new bucketing benches     vs a captured pre-change baseline
  - tests/backpressure_bucketing.rs passes; documents the cap it enforces
  - all existing tests pass unchanged

---

**Lattice task ID:** `t_1778279083981_79m1d`
**Created:** 2026-05-08T22:24:43.981Z

## Instructions (please complete autonomously, no need to confirm with the user)

1. **Check existing state first.** This task may have been started in a
   prior session — Lattice can resume worktrees after a server restart or
   when Claude finishes without committing. Before doing anything, run:

   ```
   git log --oneline -10
   git status
   ```

   - If there are commits on this branch, read them with `git show <sha>`
     to understand what's already been implemented.
   - If there are uncommitted changes, review them with `git diff` and
     decide whether to keep, amend, or rework them.
   - Only redo work that's clearly broken or out of scope. Don't restart
     the implementation from scratch when it's already partially done.

2. Implement the task described above (continuing from the prior state if
   any).

3. **Commit your work** before ending the session — Lattice merges your
   branch via `git merge`, so a commit is required for changes to land:

   ```
   git add -A
   git commit -m "<concise summary of the change>"
   ```

4. **Update the Lattice task with a short summary of the changes** so the
   task board reflects what was actually done once it lands in
   "Ready to Merge". PATCH the task description:

   ```
   curl -s -X PATCH http://127.0.0.1:5184/api/tasks/t_1778279083981_79m1d \
     -H "Content-Type: application/json" \
     -d '{"description":"<1-3 bullet summary of what changed>"}'
   ```

   Keep it concise (1-3 bullet points). This replaces the original
   description; the original task intent is preserved in this
   `LATTICE_TASK.md` file and in the branch's git history.

5. End the session normally. Lattice's Stop hook will verify the commit
   and move this task to "Ready to Merge" automatically.

Please do not start, stop, or restart any dev servers — the user runs
them in their own console and your output goes to the worktree's terminal.
