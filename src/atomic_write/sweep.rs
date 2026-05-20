use anyhow::{Context, Result};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use super::naming::owner_pid_from_inprogress;
use super::{INPROGRESS_EXT, STAGING_DIR_NAME};

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
///   - `true`  → remove only RETL-owned leftovers whose PID is no longer live
///   - `false` → warn and list, leave files in place (forensic mode)
///
/// Legacy fixed-name `*.inprogress` files and staged files owned by a live PID
/// are not deleted. This avoids a second concurrent run deleting the first
/// run's active staged output.
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

    // Pair each staged file with its owning PID, then resolve liveness for
    // the whole PID set in one refresh instead of one per file.
    let owners: Vec<(PathBuf, Option<u32>)> = inprogress
        .into_iter()
        .map(|path| {
            let owner_pid = owner_pid_from_inprogress(&path);
            (path, owner_pid)
        })
        .collect();
    let candidate_pids: HashSet<u32> = owners.iter().filter_map(|(_, pid)| *pid).collect();
    let live = live_pids(&candidate_pids);

    let mut count = 0usize;
    for (path, owner_pid) in owners {
        let Some(owner_pid) = owner_pid else {
            tracing::warn!(
                path=%path.display(),
                "leaving legacy/unowned .inprogress in place; not safe to sweep"
            );
            continue;
        };

        if live.contains(&owner_pid) {
            tracing::warn!(
                path=%path.display(),
                owner_pid,
                "leaving live .inprogress in place"
            );
            continue;
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
