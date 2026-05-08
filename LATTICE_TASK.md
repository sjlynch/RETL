# Make replace_file_atomic_backoff actually atomic on Windows

src/util.rs:242 performs remove_with_backoff(dest) THEN rename_with_backoff(tmp, dest) (lines 245–248). Between those two operations any concurrent reader sees no file at the destination — a regression from the 'atomic' name in the function and a real source of flaky test/output behavior on Windows.

On modern Rust, std::fs::rename on Windows already uses MoveFileExW(MOVEFILE_REPLACE_EXISTING) underneath, which atomically swaps the destination if it exists. So the 'if dest.exists() { remove_with_backoff(...) }' block (line 245) is unnecessary and harmful — drop it and let rename_with_backoff overwrite.

Steps:
1. Remove the pre-rename remove call at src/util.rs:245-247.
2. Verify that on the copy+remove fallback path (line 250), copy can still overwrite. fs::copy on Windows opens the dest with CREATE_ALWAYS, so it should — confirm with the test below.
3. Add a regression test in tests/ that races a reader against a writer doing many replace_file_atomic_backoff calls and asserts the reader never observes a 'missing file' error.
4. If portability is a concern, consider pulling in windows-sys and calling MoveFileExW directly so we don't depend on stdlib internals — but only if the regression test is flaky after step 1.

---

**Lattice task ID:** `t_1778271766330_5il0k`
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
   curl -s -X PATCH http://127.0.0.1:5184/api/tasks/t_1778271766330_5il0k \
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
