# [T1 pipeline.rs] Atomic writers + RAII zstd encoder + checksum + sweep + zst level + small API hygiene

Single PR for all pipeline.rs work. Combines former tasks: P0 atomic-write export_partitioned, P0 atomic-write extract_spool_monthly, P0 RAII/AutoFinish ZstdEncoder, P0 enable encoder-side checksum, P0 sweep stale *.inprogress, P0 lower default zst level from 19, P2 remove unused _resume param, P2 remove duplicate RedditETL::extract_to_jsonl, P2 fix progress-flag-changes-filter-logic in build_first_seen_index_to_tsv, P2 stop calling rayon::build_global / init_tracing_once from this file.

Concrete fixes (all in src/pipeline.rs unless noted):
  1. Lines 466-486 (export_partitioned, both Jsonl and Zst): write to *.inprogress, finish encoder + flush + replace_file_atomic_backoff to final. Use `crate::atomic_write::write_zst_atomic` from #T10. This is almost certainly the source of unreadable .zst files.
  2. Lines 285-322 (extract_spool_monthly): same pattern with .jsonl.inprogress in a staging subdir.
  3. Line 478: replace `ZstdEncoder::new(file, 19)?` + manual `enc.finish()?` with the helper from #T10. Set include_checksum(true) on the encoder so the trailing XXH64 lets validate_zst_full actually catch silent bit corruption.
  4. Add ETLOptions::with_zst_level(i32) builder to src/config.rs; default 7 (configurable). Replace the literal 19 at the call site.
  5. On entry to export_partitioned and extract_spool_monthly, sweep <out_dir>/_staging for leftover *.inprogress and either delete or warn-and-list (default delete).
  6. Line 254: drop the `_resume: bool` parameter from extract_spool_monthly entirely; update README/examples/tests if any pass it.
  7. Lines 101-107: remove RedditETL::extract_to_jsonl; route everyone through ScanPlan::extract_to_jsonl.
  8. Line 408: drop the `progress &&` guard on `requires_full_parse()` in build_first_seen_index_to_tsv. The progress flag must not change filter semantics.
  9. Remove every `init_tracing_once()` and `rayon::ThreadPoolBuilder::new().num_threads(n).build_global().ok()` call inside this file. For per-operation parallelism, use `crate::util::with_thread_pool` from #T10.

Touches: src/pipeline.rs (primary), src/config.rs (one builder method).

---

**Lattice task ID:** `t_1778255685150_62v4p`
**Created:** 2026-05-08T15:54:45.150Z

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
   curl -s -X PATCH http://127.0.0.1:5184/api/tasks/t_1778255685150_62v4p \
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
