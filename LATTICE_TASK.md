# [T2 parents.rs] Atomic cache writes + error surfacing + ParentMaps cleanup + shared shard cache + LRU fix

Single PR for all parents.rs work. Combines: P0 atomic parent cache shards, P2 remove silent error swallowing in parents.rs, P2 replace ParentMaps.comment_index huge map, P3 shared parent shard cache instead of per-worker, P3 replace O(n) LRU update.

Concrete fixes (all src/parents.rs):
  1. Lines 389-412: write parent shard JSONs via tmp + replace_file_atomic_backoff. On resume, validate JSON parses or treat as missing.
  2. Lines 202-235: stop dropping I/O / parse errors from `paths.par_iter().for_each(|p| { let _ = (...)(); })`. Surface via tracing::warn! and return an error count to caller.
  3. Lines 342-387: same cleanup for the `let _ = for_each_line_with_progress_cfg_no_throttle(...)` site.
  4. Lines 166-171: replace `comment_index: Option<HashMap<String, PathBuf>>` (one entry PER parent id — tens of millions of entries on a real corpus) with a `HashMap<(YearMonth, FileKind), PathBuf>` index plus a helper that derives which shard owns a given id from its FileKind + own-month metadata. ~10^6x memory reduction at scale. Same for submission_index.
  5. Lines 303-337 (ensure_contains): replace per-rayon-worker `idset_t1_cache`/`idset_t3_cache` with shared `Arc<DashMap<PathBuf, Arc<AHashSet<String>>>>` (or `Arc<RwLock<HashMap<...>>>`). With many workers, the same shard file currently gets opened+parsed N times; this collapses to once.
  6. Lines 520-523, 547-550: replace O(n) `c_order.iter().position(|x| x == p)` + remove + push_back with indexmap::IndexMap (shift_remove) for true O(1) LRU, OR drop the LRU-on-hit update entirely and accept FIFO (the existing `_order` deque already does correct FIFO eviction without touching order on hit).
  7. Drop init_tracing_once() and rayon::build_global() calls inside parents.rs; use crate::util::with_thread_pool where parallelism is needed.

Touches: src/parents.rs, possibly Cargo.toml (dashmap or indexmap dep).

---

**Lattice task ID:** `t_1778255685150_4xf0v`
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
   curl -s -X PATCH http://127.0.0.1:5184/api/tasks/t_1778255685150_4xf0v \
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
