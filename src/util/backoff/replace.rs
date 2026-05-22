use anyhow::{Context, Result};
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::fs_ops::remove_with_backoff;
use super::retry::{with_backoff, DEFAULT_BACKOFF_DELAY_MS, DEFAULT_BACKOFF_TRIES};

/// Per-process nonce so two concurrent fallbacks never collide on a temp name.
static FALLBACK_NONCE: AtomicU64 = AtomicU64::new(0);

/// Extension on the copy+rename fallback's private sibling temp file.
///
/// The fallback in [`replace_file_atomic_backoff`] streams bytes into a
/// `<dest>.retl-<pid>-<nonce>-<nanos>.atomic-replace-tmp` sibling before
/// renaming it over `dest`. A crash between the copy and that rename — or a
/// failed best-effort cleanup in the non-atomic last-resort branch — can
/// orphan the sibling next to published outputs. The suffix deliberately
/// mirrors the `.inprogress` staged-file contract (`.retl-<pid>-<nonce>-
/// <nanos>`) so `atomic_write`'s stale sweep can age-gate and reclaim a
/// leftover whose owner PID is no longer live, instead of leaking it forever.
pub(crate) const ATOMIC_REPLACE_TMP_EXT: &str = ".atomic-replace-tmp";

/// Rename a file with retries/backoff for transient errors.
fn rename_with_backoff(src: &Path, dest: &Path, tries: usize, delay_ms: u64) -> Result<()> {
    with_backoff(tries, delay_ms, || fs::rename(src, dest))
        .with_context(|| format!("rename {} -> {}", src.display(), dest.display()))
}

/// Copy a file with retries/backoff for transient errors.
fn copy_with_backoff(src: &Path, dest: &Path, tries: usize, delay_ms: u64) -> Result<()> {
    with_backoff(tries, delay_ms, || fs::copy(src, dest))
        .map(|_| ())
        .with_context(|| format!("copy {} -> {}", src.display(), dest.display()))
}

/// Build a unique temp path that is a sibling of `dest`.
///
/// The copy+rename fallback streams bytes into this private path (no reader
/// knows it), then renames it over `dest`. Living in `dest`'s own directory
/// keeps that rename a same-volume `MoveFileExW` swap rather than a degrading
/// cross-volume copy. The PID + nonce + timestamp suffix keeps it unique
/// across processes and concurrent in-process calls — and lets the
/// `atomic_write` stale sweep reclaim it (see [`ATOMIC_REPLACE_TMP_EXT`]) if a
/// crash mid-fallback orphans it next to the published output.
fn unique_sibling_tmp(dest: &Path) -> Result<PathBuf> {
    let parent = match dest.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let base = dest
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("dest has no file name: {}", dest.display()))?;
    let nonce = FALLBACK_NONCE.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut name = OsString::from(base);
    name.push(format!(
        ".retl-{}-{nonce}-{nanos}{ATOMIC_REPLACE_TMP_EXT}",
        std::process::id()
    ));
    Ok(parent.join(name))
}

/// Atomically replace `dest` with `tmp` (Windows-friendly).
///
/// `std::fs::rename` on Windows is implemented via
/// `MoveFileExW(MOVEFILE_REPLACE_EXISTING)`, which atomically swaps the
/// destination if it exists — concurrent readers see either the old contents
/// or the new contents, never a missing file. We therefore do NOT pre-remove
/// `dest`: a remove-then-rename sequence opens a window where readers observe
/// `NotFound`, breaking the "atomic" contract this function advertises.
///
/// ## Fallback ordering
///
/// If the primary rename fails (e.g. a sharing violation on `dest` that does
/// not clear within the retry budget), we fall back **without** ever opening
/// `dest` for a streaming write:
///
/// 1. Copy `tmp` into a unique sibling temp path in `dest`'s own directory.
///    A reader cannot see this path, so a torn/zero-length read is impossible.
/// 2. `rename` that sibling over `dest` — still an atomic `MoveFileExW` swap,
///    so readers see either the old or the new contents.
/// 3. Remove the original `tmp` (the sibling copy, not `tmp`, became `dest`).
///
/// Only if that second rename *also* fails do we resort to an in-place
/// `fs::copy(tmp, dest)`. On Windows `fs::copy` opens `dest` with
/// `CREATE_ALWAYS`, truncating it before streaming bytes in — a concurrent
/// reader may briefly observe a zero-length or partially written `dest`. This
/// last-resort path is therefore **not** atomic, and emits a `tracing::warn!`
/// recording that atomicity was lost.
pub fn replace_file_atomic_backoff(tmp: &Path, dest: &Path) -> Result<()> {
    // The atomic publish step is the most critical I/O path in the toolkit;
    // it shares the crate-wide backoff budget so it can never silently
    // diverge from the other `*_with_backoff` call sites, and a future tune
    // of `DEFAULT_BACKOFF_TRIES` reaches it for free.
    let tries = DEFAULT_BACKOFF_TRIES;
    let delay_ms = DEFAULT_BACKOFF_DELAY_MS;

    // Primary path: a single atomic MoveFileExW(REPLACE_EXISTING).
    if rename_with_backoff(tmp, dest, tries, delay_ms).is_ok() {
        return Ok(());
    }

    // Fallback: copy into a private sibling temp, then rename THAT over dest.
    // The rename keeps the swap atomic; the copy never touches `dest`.
    let staged = unique_sibling_tmp(dest)?;
    copy_with_backoff(tmp, &staged, tries, delay_ms)?;
    match rename_with_backoff(&staged, dest, tries, delay_ms) {
        Ok(_) => {
            remove_with_backoff(tmp, tries, delay_ms)?;
            Ok(())
        }
        Err(staged_rename_err) => {
            // Last resort: even the sibling rename failed. Overwrite `dest`
            // in place with fs::copy (CREATE_ALWAYS) — this is NOT atomic, so
            // a concurrent reader may observe a torn `dest`. Warn loudly.
            tracing::warn!(
                tmp = %tmp.display(),
                dest = %dest.display(),
                error = %staged_rename_err,
                "replace_file_atomic_backoff: sibling rename failed; falling back to \
                 non-atomic in-place fs::copy over dest — concurrent readers may \
                 observe a zero-length or partially written destination"
            );
            // Drop the orphaned sibling copy first (best-effort): the in-place
            // copy below makes it dead weight either way. If this remove
            // fails, the `.atomic-replace-tmp` sibling is left behind — but it
            // carries the RETL-owned PID/nonce/timestamp suffix, so a later
            // run's `sweep_stale_inprogress` reclaims it once the owner PID is
            // no longer live (it is no longer leaked permanently).
            let _ = remove_with_backoff(&staged, tries, delay_ms);
            copy_with_backoff(tmp, dest, tries, delay_ms)?;
            remove_with_backoff(tmp, tries, delay_ms)?;
            Ok(())
        }
    }
}
