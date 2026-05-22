use anyhow::{Context, Result};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::naming::{parse_staged_suffix, StagedSuffix};
use super::{INPROGRESS_EXT, STAGING_DIR_NAME};
use crate::util::ATOMIC_REPLACE_TMP_EXT;

/// A staged file owned by a *currently-live* PID is normally left in place —
/// that PID may be a concurrent RETL run still writing the file. But PIDs are
/// recycled aggressively by both Windows and Linux: a crashed run's leftover
/// `.inprogress` can match an unrelated live process indefinitely, so without
/// an age gate the leftover is never reclaimed and `_staging` grows unbounded
/// across runs.
///
/// Age-gate the liveness check: once a staged file is older than this grace
/// window, a live-PID match is overwhelmingly likely to be a recycled PID
/// rather than a still-running writer, so the leftover is swept anyway. The
/// window is far longer than any single staged file stays open in practice
/// (each staged file is one output file — e.g. one month of spool output),
/// so this never races a genuine live writer.
const LIVE_PID_RECYCLE_GRACE: Duration = Duration::from_secs(6 * 60 * 60);

/// Best-effort wall-clock age of a staged file.
///
/// Prefers the nanosecond timestamp embedded in the staged filename; falls
/// back to the filesystem mtime for legacy suffixes that carry no timestamp.
/// Returns `None` when neither source is usable (e.g. the embedded timestamp
/// or mtime sits in the future because of clock skew) — callers treat an
/// unknown age conservatively and keep the file.
fn staged_age(path: &Path, created_nanos: Option<u128>) -> Option<Duration> {
    if let Some(nanos) = created_nanos {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_nanos();
        let elapsed = now.checked_sub(nanos)?;
        return Some(Duration::from_nanos(
            u64::try_from(elapsed).unwrap_or(u64::MAX),
        ));
    }
    std::fs::metadata(path).ok()?.modified().ok()?.elapsed().ok()
}

/// Return the subset of `candidates` whose process is still alive.
///
/// Does a **single** `sysinfo` process-table refresh for the whole PID set.
/// A sweep can inspect many staged files in one pass (and runs near the top
/// of the minute), so refreshing per-entry made the sweep O(n) in full
/// process-table scans.
fn live_pids(candidates: &HashSet<u32>) -> HashSet<u32> {
    let self_pid = std::process::id();
    let mut live = HashSet::new();
    // This process is always live; never query or sweep our own staged files.
    if candidates.contains(&self_pid) {
        live.insert(self_pid);
    }
    let to_query: Vec<sysinfo::Pid> = candidates
        .iter()
        .filter(|&&pid| pid != self_pid)
        .map(|&pid| sysinfo::Pid::from_u32(pid))
        .collect();
    if to_query.is_empty() {
        return live;
    }
    let mut system = sysinfo::System::new();
    system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&to_query), true);
    for &pid in candidates {
        if pid != self_pid && system.process(sysinfo::Pid::from_u32(pid)).is_some() {
            live.insert(pid);
        }
    }
    live
}

/// Reclaim stale RETL-owned staged artifacts ending in `ext` from `dir`.
///
/// Shared core of [`sweep_stale_inprogress`] and
/// [`sweep_stale_atomic_replace_tmp`]: collect every `*<ext>` entry in `dir`,
/// resolve process liveness for the whole owner-PID set in one batched
/// `sysinfo` refresh, then remove leftovers whose owner PID is no longer live
/// — plus those older than [`LIVE_PID_RECYCLE_GRACE`] whose embedded PID has
/// been recycled by an unrelated live process.
///
/// `delete` chooses behavior:
///   - `true`  → remove the reclaimable leftovers, returning how many were
///     removed
///   - `false` → warn and list, leave files in place (forensic mode)
///
/// Legacy/unowned names (no parseable `STAGE_MARKER` suffix) are never
/// deleted. Staged files owned by a live PID are kept while they are recent
/// enough that the live PID is plausibly the original writer — this avoids a
/// concurrent run deleting another run's active artifact.
fn reclaim_staged_in(dir: &Path, ext: &str, delete: bool) -> Result<usize> {
    if !dir.exists() {
        return Ok(0);
    }

    // Collect every matching entry first so the process-liveness check can be
    // resolved with one batched `sysinfo` refresh below.
    let mut staged: Vec<PathBuf> = Vec::new();
    for entry in crate::util::read_dir_with_default_backoff(dir)
        .with_context(|| format!("read_dir {}", dir.display()))?
    {
        let path = entry.path();
        let matches = path
            .file_name()
            .and_then(|s| s.to_str())
            .map(|s| s.ends_with(ext))
            .unwrap_or(false);
        if matches {
            staged.push(path);
        }
    }

    if !delete {
        for path in &staged {
            tracing::warn!(path=%path.display(), ext, "leftover staged artifact (sweep disabled)");
        }
        return Ok(staged.len());
    }

    // Pair each staged file with its parsed RETL suffix, then resolve liveness
    // for the whole PID set in one refresh instead of one per file.
    let owners: Vec<(PathBuf, Option<StagedSuffix>)> = staged
        .into_iter()
        .map(|path| {
            let suffix = parse_staged_suffix(&path, ext);
            (path, suffix)
        })
        .collect();
    let candidate_pids: HashSet<u32> = owners
        .iter()
        .filter_map(|(_, suffix)| suffix.as_ref().map(|s| s.pid))
        .collect();
    let live = live_pids(&candidate_pids);
    let self_pid = std::process::id();

    let mut count = 0usize;
    for (path, suffix) in owners {
        let Some(StagedSuffix {
            pid: owner_pid,
            created_nanos,
        }) = suffix
        else {
            tracing::warn!(
                path=%path.display(),
                "leaving legacy/unowned staged artifact in place; not safe to sweep"
            );
            continue;
        };

        if live.contains(&owner_pid) {
            // Owned by a currently-live PID. Keep it while it is recent enough
            // that the live PID is plausibly the original writer. Our own
            // staged files are kept regardless of age — a long-running process
            // legitimately holds an old, still-live staged file. An old file
            // matching some *other* live PID means that PID was recycled, so
            // the original writer is long gone and the leftover is swept.
            let age = staged_age(&path, created_nanos);
            let recycled_pid = owner_pid != self_pid
                && age.is_some_and(|age| age >= LIVE_PID_RECYCLE_GRACE);
            if !recycled_pid {
                tracing::warn!(
                    path=%path.display(),
                    owner_pid,
                    "leaving live staged artifact in place"
                );
                continue;
            }
            tracing::warn!(
                path=%path.display(),
                owner_pid,
                age_secs = age.map(|a| a.as_secs()).unwrap_or_default(),
                "sweeping stale staged artifact whose embedded PID was recycled by an unrelated live process"
            );
        }

        match crate::util::remove_with_short_backoff(&path) {
            Ok(_) => {
                tracing::info!(path=%path.display(), owner_pid, "swept stale staged artifact");
                count += 1;
            }
            Err(e) => {
                tracing::warn!(path=%path.display(), owner_pid, error=%e, "failed to sweep staged artifact");
            }
        }
    }
    Ok(count)
}

/// Sweep stale RETL-owned leftovers under an output root.
///
/// Reclaims two kinds of artifact, both attributed to their owner PID via the
/// `.retl-<pid>-<nonce>-<nanos>` suffix:
///
///   - `*.inprogress` staged files in `<out_root>/_staging` — the normal
///     atomic-write staging path.
///   - `*.atomic-replace-tmp` files directly in `out_root` — the copy+rename
///     fallback siblings `replace_file_atomic_backoff` writes next to a
///     published output. A crash mid-fallback (or a failed best-effort
///     cleanup) orphans one of these beside real outputs, where the `_staging`
///     sweep never sees it; reclaiming them here keeps a flaky host from
///     accreting `*.atomic-replace-tmp` files forever.
///
/// `delete` chooses behavior:
///   - `true`  → remove RETL-owned leftovers whose PID is no longer live, plus
///     leftovers older than [`LIVE_PID_RECYCLE_GRACE`] whose embedded PID has
///     been recycled by an unrelated live process
///   - `false` → warn and list, leave files in place (forensic mode)
///
/// Legacy fixed-name files are never deleted. Staged files owned by a live PID
/// are kept while they are recent enough that the live PID is plausibly the
/// original writer — this avoids a concurrent run deleting another run's
/// active output. Once a leftover outlives the grace window a live-PID match
/// is treated as a recycled PID and it is swept; see [`LIVE_PID_RECYCLE_GRACE`].
///
/// Returns the total number of leftovers removed.
pub fn sweep_stale_inprogress(out_root: &Path, delete: bool) -> Result<usize> {
    let staging = out_root.join(STAGING_DIR_NAME);
    let inprogress = reclaim_staged_in(&staging, INPROGRESS_EXT, delete)?;
    // The copy+rename fallback stages its private sibling next to the
    // *published* file, not under `_staging`, so a crash mid-fallback orphans
    // an `.atomic-replace-tmp` file in `out_root` itself — reclaim those too.
    let fallback = reclaim_staged_in(out_root, ATOMIC_REPLACE_TMP_EXT, delete)?;
    Ok(inprogress + fallback)
}

/// Sweep orphaned `*.atomic-replace-tmp` copy+rename fallback files directly
/// in `dir`.
///
/// [`sweep_stale_inprogress`] already reclaims them from an output *root*, but
/// some pipelines publish into nested subdirectories (e.g. partitioned export
/// writes `comments/RC_*.jsonl` / `submissions/RS_*.jsonl`). Call this on each
/// such directory so a fallback file orphaned beside those nested outputs is
/// reclaimed too, using the same PID-liveness + age-gate rules.
///
/// Returns the number of leftovers removed.
pub fn sweep_stale_atomic_replace_tmp(dir: &Path, delete: bool) -> Result<usize> {
    reclaim_staged_in(dir, ATOMIC_REPLACE_TMP_EXT, delete)
}
