# Surface UsernameStream file-open errors instead of swallowing them

src/shard.rs:208 calls self.open_next().ok()? inside Iterator::next. A permission-denied / I/O error opening file N is silently converted to None, which the caller cannot distinguish from a clean end-of-stream. Read errors mid-file are already handled (lines 220–236 log and advance), but open failures are not.

Minimum fix: when open_next() returns Err(e) for a file that exists, emit a tracing::warn! (path + error) before advancing, matching the style at line 229. Then advance current_idx past the bad file and try the next one rather than terminating the iterator.

Better: add a try_next(&mut self) -> Option<Result<String>> (or a fail_fast: bool constructor flag) so callers who must know about open failures can react. Keep the existing Iterator<Item = String> impl as a lossy convenience layer over the new API.

Update src/shard.rs tests around lines 258–316 to cover the open-error case (e.g., point a UsernameStream at a non-existent file path and a real one, assert the warning + that the real file's data is still yielded).

---

**Lattice task ID:** `t_1778271766330_3l2vt`
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
   curl -s -X PATCH http://127.0.0.1:5184/api/tasks/t_1778271766330_3l2vt \
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
