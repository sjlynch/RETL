use anyhow::{Context, Result};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::naming::{parse_inprogress_suffix, StagedSuffix};
use super::{INPROGRESS_EXT, STAGING_DIR_NAME};

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
/// `sweep_stale_inprogress` can inspect many `.inprogress` files in one pass
/// (and runs near the top of the minute), so refreshing per-entry made the
/// sweep O(n) in full process-table scans.
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

/// Sweep stale `*.inprogress` files in `<out_root>/_staging`.
///
/// `delete` chooses behavior:
///   - `true`  → remove RETL-owned leftovers whose PID is no longer live, plus
///     leftovers older than [`LIVE_PID_RECYCLE_GRACE`] whose embedded PID has
///     been recycled by an unrelated live process
///   - `false` → warn and list, leave files in place (forensic mode)
///
/// Legacy fixed-name `*.inprogress` files are never deleted. Staged files
/// owned by a live PID are kept while they are recent enough that the live PID
/// is plausibly the original writer — this avoids a concurrent run deleting
/// another run's active staged output. Once a staged file outlives the grace
/// window a live-PID match is treated as a recycled PID and the leftover is
/// swept; see [`LIVE_PID_RECYCLE_GRACE`].
pub fn sweep_stale_inprogress(out_root: &Path, delete: bool) -> Result<usize> {
    let dir = out_root.join(STAGING_DIR_NAME);
    if !dir.exists() {
        return Ok(0);
    }

    // Collect every `.inprogress` entry first so the process-liveness check
    // can be resolved with one batched `sysinfo` refresh below.
    let mut inprogress: Vec<PathBuf> = Vec::new();
    for entry in crate::util::read_dir_with_default_backoff(&dir)
        .with_context(|| format!("read_dir {}", dir.display()))?
    {
        let path = entry.path();
        let is_inprogress = path
            .file_name()
            .and_then(|s| s.to_str())
            .map(|s| s.ends_with(INPROGRESS_EXT))
            .unwrap_or(false);
        if is_inprogress {
            inprogress.push(path);
        }
    }

    if !delete {
        for path in &inprogress {
            tracing::warn!(path=%path.display(), "leftover .inprogress (sweep disabled)");
        }
        return Ok(inprogress.len());
    }

    // Pair each staged file with its parsed RETL suffix, then resolve liveness
    // for the whole PID set in one refresh instead of one per file.
    let owners: Vec<(PathBuf, Option<StagedSuffix>)> = inprogress
        .into_iter()
        .map(|path| {
            let suffix = parse_inprogress_suffix(&path);
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
                "leaving legacy/unowned .inprogress in place; not safe to sweep"
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
                    "leaving live .inprogress in place"
                );
                continue;
            }
            tracing::warn!(
                path=%path.display(),
                owner_pid,
                age_secs = age.map(|a| a.as_secs()).unwrap_or_default(),
                "sweeping stale .inprogress whose embedded PID was recycled by an unrelated live process"
            );
        }

        match crate::util::remove_with_short_backoff(&path) {
            Ok(_) => {
                tracing::info!(path=%path.display(), owner_pid, "swept stale .inprogress");
                count += 1;
            }
            Err(e) => {
                tracing::warn!(path=%path.display(), owner_pid, error=%e, "failed to sweep .inprogress");
            }
        }
    }
    Ok(count)
}
