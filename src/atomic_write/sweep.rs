use anyhow::{Context, Result};
use std::path::Path;

use super::naming::owner_pid_from_inprogress;
use super::{INPROGRESS_EXT, STAGING_DIR_NAME};

fn process_is_running(pid: u32) -> bool {
    if pid == std::process::id() {
        return true;
    }
    let sys_pid = sysinfo::Pid::from_u32(pid);
    let mut system = sysinfo::System::new();
    system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[sys_pid]), true);
    system.process(sys_pid).is_some()
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
    let mut count = 0usize;
    for entry in crate::util::read_dir_with_default_backoff(&dir)
        .with_context(|| format!("read_dir {}", dir.display()))?
    {
        let path = entry.path();
        let is_inprogress = path
            .file_name()
            .and_then(|s| s.to_str())
            .map(|s| s.ends_with(INPROGRESS_EXT))
            .unwrap_or(false);
        if !is_inprogress {
            continue;
        }

        if !delete {
            tracing::warn!(path=%path.display(), "leftover .inprogress (sweep disabled)");
            count += 1;
            continue;
        }

        let Some(owner_pid) = owner_pid_from_inprogress(&path) else {
            tracing::warn!(
                path=%path.display(),
                "leaving legacy/unowned .inprogress in place; not safe to sweep"
            );
            continue;
        };

        if process_is_running(owner_pid) {
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
