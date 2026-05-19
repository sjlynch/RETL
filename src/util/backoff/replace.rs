use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

use super::fs_ops::remove_with_backoff;
use super::retry::with_backoff;

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

/// Atomically replace `dest` with `tmp` (Windows-friendly).
///
/// `std::fs::rename` on Windows is implemented via
/// `MoveFileExW(MOVEFILE_REPLACE_EXISTING)`, which atomically swaps the
/// destination if it exists — concurrent readers see either the old contents
/// or the new contents, never a missing file. We therefore do NOT pre-remove
/// `dest`: a remove-then-rename sequence opens a window where readers observe
/// `NotFound`, breaking the "atomic" contract this function advertises.
///
/// If rename fails (e.g., due to a sharing violation that doesn't clear within
/// the retry budget), fall back to copy+remove. `fs::copy` opens the
/// destination with `CREATE_ALWAYS` on Windows, so it overwrites in place.
pub fn replace_file_atomic_backoff(tmp: &Path, dest: &Path) -> Result<()> {
    let tries = 20usize;
    let delay_ms = 50u64;
    match rename_with_backoff(tmp, dest, tries, delay_ms) {
        Ok(_) => Ok(()),
        Err(_) => {
            copy_with_backoff(tmp, dest, tries, delay_ms)?;
            remove_with_backoff(tmp, tries, delay_ms)?;
            Ok(())
        }
    }
}
