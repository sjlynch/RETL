# Add fail-fast / metrics variant of for_each_line_* on decode error

src/zstd_jsonl.rs:78-101 (for_each_line, for_each_line_cfg) catch decode errors via warn_decode_skip (line 44) and return Ok(()). Same pattern in for_each_line_with_progress_cfg (line 158) and ..._no_throttle (line 179). Good for batch resilience; bad for callers that need to know 'this file was unreadable' — they currently get Ok indistinguishable from success.

Add a counterpart that reports the outcome. Two options:

1. for_each_line_cfg_strict(...) -> Result<u64, FileError> that does NOT swallow decode errors. Callers that want resilience keep using for_each_line_cfg.
2. Add an optional on_skip: impl FnMut(&Path, &anyhow::Error) parameter (or callback-bearing *_with_skip variant) so pipelines can record per-file skip metrics, alert, or fail.

Recommend option 2: it preserves the swallow-by-default ergonomics that the rest of the codebase relies on (e.g., process_file_for_usernames at src/streaming.rs:219) while making the skip observable. Wire one caller (e.g., the integrity stage in src/integrity.rs) through the new variant to validate the API. Update the unit test at src/zstd_jsonl.rs:300-348 to also cover the strict/observed-skip path.

---

**Lattice task ID:** `t_1778271766330_8jtcq`
**Created:** 2026-05-08T20:22:46.330Z

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
   curl -s -X PATCH http://127.0.0.1:5184/api/tasks/t_1778271766330_8jtcq \
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
