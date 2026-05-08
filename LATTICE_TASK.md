# [T12 tests] Behavioral coverage backfill (multi-month, concurrency, env vars, edge cases, module unit tests)

Combines former tasks: P1 direct unit tests for bucketing/dedupe/kv_shard/ndjson, P1 multi-month / missing-month coverage, P1 concurrency tests for ShardedWriter and for_each_file_limited, P1 extract_to_json (array form) with pretty=true, P1 ETL_EXCLUDE_AUTHORS env-var integration, P1 UsernameStream error handling test.

New test files:
  1. tests/unit_modules.rs — focused tests for bucketing::partition_stage1 / process_bucket_streaming, dedupe::build_runs_sorted / merge_runs_sorted, kv_shard::ShardedKVWriter, ndjson::NdjsonReader/Writer. Drive bucketing with ~10K records to exercise the adaptive flush threshold (today's tiny test corpus skips that branch).
  2. tests/multi_month.rs — build 2005-12 and 2006-02 (skipping 2006-01); assert paths::plan_files silently skips the missing month; iter_year_months crosses the year boundary correctly; bounds_tuple semantics in within_bounds.
  3. tests/concurrency.rs — stress ShardedWriter with many threads writing concurrently (use std::thread::scope with N=16); assert dedup output matches the expected unique set. Also test for_each_file_limited (#T7) at meaningful concurrency.
  4. tests/extract_json_array.rs — cover ScanPlan::extract_to_json with pretty=false and pretty=true; assert the output parses as a valid JSON array via serde_json::from_str::<Vec<Value>>.
  5. tests/exclude_bots_env.rs — use `serial_test` (or temp-env crate) to set ETL_EXCLUDE_AUTHORS / ETL_EXCLUDE_AUTHORS_FILE; assert exclude_common_bots() picks them up. README.md documents this; today nothing tests it.
  6. tests/username_stream_errors.rs — mock a Read that returns Err repeatedly; assert UsernameStream terminates rather than spinning forever (after #T4 lands).

Shared helpers at the BOTTOM of tests/common/mod.rs (append-only — coexist with #T11): `make_corpus_multi_month(months: &[YearMonth])`, `make_corpus_n_records(n: usize)`.

Touches: 6 NEW tests/*.rs files; tests/common/mod.rs (append-only); Cargo.toml dev-dependencies (serial_test or temp-env).

---

**Lattice task ID:** `t_1778255685153_bdtx6`
**Created:** 2026-05-08T15:54:45.153Z

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
   curl -s -X PATCH http://127.0.0.1:5184/api/tasks/t_1778255685153_bdtx6 \
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
