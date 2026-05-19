use anyhow::Result;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Extension appended to staged filenames during atomic writes. The published
/// path is the final destination; a crashed run leaves a unique
/// `<dest>.retl-<pid>-<nonce>.inprogress` in the staging dir, never a partial
/// file at the published path.
pub(crate) const INPROGRESS_EXT: &str = ".inprogress";
pub(super) const STAGE_MARKER: &str = ".retl-";
static STAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// Return a unique staged path for `final_dest` under `staging_dir`.
///
/// The filename deliberately keeps the final basename as a prefix for operator
/// readability, then appends a RETL-owned PID/nonce/timestamp suffix before the
/// `.inprogress` extension.
pub(crate) fn unique_inprogress_path(staging_dir: &Path, final_dest: &Path) -> Result<PathBuf> {
    let file_name = final_dest
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("final_dest has no file name: {}", final_dest.display()))?;
    let counter = STAGE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut staged_name = file_name.to_os_string();
    staged_name.push(format!(
        "{STAGE_MARKER}{}-{counter}-{}{INPROGRESS_EXT}",
        std::process::id(),
        now_nanos()
    ));
    Ok(staging_dir.join(staged_name))
}

pub(super) fn owner_pid_from_inprogress(path: &Path) -> Option<u32> {
    let name = path.file_name()?.to_str()?;
    if !name.ends_with(INPROGRESS_EXT) {
        return None;
    }
    let start = name.rfind(STAGE_MARKER)? + STAGE_MARKER.len();
    let rest = &name[start..name.len() - INPROGRESS_EXT.len()];
    let pid = rest.split('-').next()?;
    pid.parse().ok()
}
