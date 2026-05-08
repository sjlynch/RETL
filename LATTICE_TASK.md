# [T6 filters.rs + query.rs] Eliminate per-line to_lowercase + aho_corasick keywords

Combines: P3 eliminate per-line to_lowercase in matches_minimal, P3 aho_corasick for keywords_any.

Concrete fixes:
  1. src/filters.rs:19, 28: replace `min.subreddit.as_deref().map(|s| s.to_lowercase())` and `let a_low = a.to_lowercase()` with `eq_ignore_ascii_case` byte-compare against pre-lowered targets (already lowercased in QuerySpec::normalize, src/query.rs:23). For a typically tiny `targets: Vec<String>` (<10), linear scan with eq_ignore_ascii_case beats binary_search-with-allocation. ASCII fast path; fall back to char-by-char only if any byte > 0x7F.
  2. src/filters.rs:62-76 (keywords_any): build an `aho_corasick::AhoCorasick` ONCE per QuerySpec (in normalize() or lazily on first use, stored on a new `compiled_keywords: OnceCell<AhoCorasick>` field) with `ascii_case_insensitive=true, match_kind=LeftmostFirst`. Run `ac.is_match(&text)` over each text field's raw bytes. Drops the per-line `String::new() + to_lowercase + 3x contains` allocation entirely.
  3. src/filters.rs:69-76 (URL detection): drop the lowercase pass; search for the case-insensitive byte sequence `http` directly (https is a superset). Or fold this into the same aho_corasick automaton with `http://` and `https://` as patterns.

Touches: src/filters.rs, src/query.rs, Cargo.toml (add aho-corasick).

---

**Lattice task ID:** `t_1778255685152_z04a7`
**Created:** 2026-05-08T15:54:45.152Z

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
   curl -s -X PATCH http://127.0.0.1:5184/api/tasks/t_1778255685152_z04a7 \
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
