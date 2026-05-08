# [Docs] Resolve merge-conflict markers in readme.md and reconcile drifted sections

readme.md still contains literal git conflict markers and TWO duplicated sections from an unresolved merge:
  - line 418: <<<<<<< HEAD
  - line 471: =======
  - line 598: >>>>>>> 707a4869...
The HEAD side has the older 'Performance & Tuning' / 'Environment Aids' / 'License' / 'Project Goals' block; the merged side has 'Performance and tuning' / 'Benchmarks' / 'Environment variables' / 'Fuzzing'. Both halves are valid — the goal is to MERGE them into a single, non-duplicated set of sections that reflects the current state:
  - keep the new Benchmarks + Fuzzing sections (criterion + cargo-fuzz are     actually shipped)
  - keep one Performance/Tuning section (prefer the merged-side wording)
  - keep one Environment Variables section (prefer the merged-side wording,     but preserve `.exclude_common_bots()` mention)
  - end the file with the License + Project Goals from the HEAD side
Also: the README still tells users to `cargo run --release` against a hand-written `main()` driver, but the repo now ships a clap CLI (src/main.rs). Add a 'Quick CLI start' example BEFORE the library quick-start so newcomers find it first.

Acceptance:
  - `grep -E '<<<<<<<|=======|>>>>>>>' readme.md` returns nothing
  - readme.md has exactly ONE section per topic
  - `retl --help` and `retl export --help` smoke through copy-paste

---

**Lattice task ID:** `t_1778279083980_fc6t4`
**Created:** 2026-05-08T22:24:43.980Z

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
   curl -s -X PATCH http://127.0.0.1:5184/api/tasks/t_1778279083980_fc6t4 \
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
