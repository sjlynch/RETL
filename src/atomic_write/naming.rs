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

/// RETL-owned suffix fields parsed out of a staged filename.
///
/// `unique_inprogress_path` always emits `<pid>-<nonce>-<nanos>`, but a sweep
/// may also encounter `.inprogress` files left by an older RETL build whose
/// suffix carried no trailing timestamp; `created_nanos` is `None` for those.
pub(super) struct StagedSuffix {
    /// PID of the process that staged the file.
    pub pid: u32,
    /// Wall-clock nanoseconds since the Unix epoch captured when the staged
    /// file was created. `None` for legacy two-field suffixes.
    pub created_nanos: Option<u128>,
}

/// Parse the RETL-owned suffix of a staged path whose name ends in `ext`.
///
/// Both staged `.inprogress` files and the `.atomic-replace-tmp` copy+rename
/// fallback siblings created by `replace_file_atomic_backoff` carry the same
/// `<base>.retl-<pid>-<nonce>-<nanos><ext>` shape — only the trailing
/// extension differs — so the sweep can attribute either kind to its owner
/// PID with one parser.
///
/// Returns `None` for legacy fixed-name files and any path that does not carry
/// the `STAGE_MARKER` suffix — those cannot be safely attributed to a RETL
/// process, so the sweep leaves them in place.
pub(super) fn parse_staged_suffix(path: &Path, ext: &str) -> Option<StagedSuffix> {
    let name = path.file_name()?.to_str()?;
    if !name.ends_with(ext) {
        return None;
    }
    let start = name.rfind(STAGE_MARKER)? + STAGE_MARKER.len();
    let rest = &name[start..name.len() - ext.len()];
    let parts: Vec<&str> = rest.split('-').collect();
    let pid: u32 = parts.first()?.parse().ok()?;
    // `unique_inprogress_path` emits `<pid>-<nonce>-<nanos>`; the trailing
    // field is the creation timestamp. A legacy `<pid>-<nonce>` suffix has no
    // third field, so `created_nanos` stays `None` and the sweep falls back to
    // the file's mtime to estimate its age.
    let created_nanos = if parts.len() >= 3 {
        parts.last().and_then(|field| field.parse().ok())
    } else {
        None
    };
    Some(StagedSuffix { pid, created_nanos })
}
