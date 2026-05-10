//! Atomic file writers for partitioned outputs.
//!
//! Pattern: write to `<staging_dir>/<filename>.inprogress`, finalize the writer
//! (flushing buffers and finishing the zstd frame), then atomically replace the
//! final destination with the staged file. A run that crashes mid-write leaves
//! a `.inprogress` artifact that the next run sweeps away — never a partial,
//! unreadable file at the published path.

use anyhow::{Context, Result};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use zstd::stream::write::Encoder as ZstdEncoder;

use crate::util::{create_with_backoff, remove_with_backoff, replace_file_atomic_backoff};

/// Directory used to stage `*.inprogress` files under an output root.
pub const STAGING_DIR_NAME: &str = "_staging";

/// Extension appended to staged filenames during atomic writes. The published
/// path is the final destination; a crashed run leaves `<dest>.inprogress` in
/// the staging dir, never a partial file at the published path.
pub(crate) const INPROGRESS_EXT: &str = ".inprogress";

/// Build the staging directory path for a given output root, creating it.
pub fn ensure_staging_dir(out_root: &Path) -> Result<PathBuf> {
    let dir = out_root.join(STAGING_DIR_NAME);
    fs::create_dir_all(&dir)
        .with_context(|| format!("create staging dir {}", dir.display()))?;
    Ok(dir)
}

/// Sweep stale `*.inprogress` files in `<out_root>/_staging`.
///
/// `delete` chooses behavior:
///   - `true`  → remove leftovers (default; recovery from a previous crash)
///   - `false` → warn and list, leave files in place (forensic mode)
pub fn sweep_stale_inprogress(out_root: &Path, delete: bool) -> Result<usize> {
    let dir = out_root.join(STAGING_DIR_NAME);
    if !dir.exists() {
        return Ok(0);
    }
    let mut count = 0usize;
    for entry in fs::read_dir(&dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(dir=%dir.display(), error=%e, "skipping unreadable staging entry");
                continue;
            }
        };
        let path = entry.path();
        let is_inprogress = path
            .file_name()
            .and_then(|s| s.to_str())
            .map(|s| s.ends_with(INPROGRESS_EXT))
            .unwrap_or(false);
        if !is_inprogress {
            continue;
        }
        if delete {
            match remove_with_backoff(&path, 8, 50) {
                Ok(_) => {
                    tracing::info!(path=%path.display(), "swept stale .inprogress");
                    count += 1;
                }
                Err(e) => {
                    tracing::warn!(path=%path.display(), error=%e, "failed to sweep .inprogress");
                }
            }
        } else {
            tracing::warn!(path=%path.display(), "leftover .inprogress (sweep disabled)");
            count += 1;
        }
    }
    Ok(count)
}

/// Stage a write under `<staging_dir>/<dest_filename>.inprogress`, run `body`
/// against a buffered writer, then atomically promote the staged file onto
/// `final_dest`. Caller is responsible for any encoder finalization (e.g.
/// closing a zstd frame) before returning from `body`; this helper only
/// flushes the underlying buffer.
///
/// On a `body` error the staged file is removed so we never leave a partial
/// `<dest>.inprogress` behind a successful return path.
fn stage_and_execute<T, F>(
    staging_dir: &Path,
    final_dest: &Path,
    write_buf_bytes: usize,
    body: F,
) -> Result<T>
where
    F: FnOnce(&mut BufWriter<File>) -> Result<T>,
{
    let file_name = final_dest
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("final_dest has no file name: {}", final_dest.display()))?;
    let mut staged = staging_dir.join(file_name);
    staged.as_mut_os_string().push(INPROGRESS_EXT);

    fs::create_dir_all(staging_dir)
        .with_context(|| format!("create staging dir {}", staging_dir.display()))?;

    let file = create_with_backoff(&staged, 16, 50)
        .with_context(|| format!("create staged {}", staged.display()))?;
    let mut writer = BufWriter::with_capacity(write_buf_bytes, file);

    let result = match body(&mut writer) {
        Ok(v) => v,
        Err(e) => {
            drop(writer);
            let _ = remove_with_backoff(&staged, 8, 50);
            return Err(e).with_context(|| format!("write staged {}", staged.display()));
        }
    };

    writer
        .flush()
        .with_context(|| format!("flush staged {}", staged.display()))?;
    drop(writer); // release file handle before atomic rename (Windows)

    if let Some(parent) = final_dest.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create dest parent {}", parent.display()))?;
    }
    replace_file_atomic_backoff(&staged, final_dest)?;
    Ok(result)
}

/// Atomically write a JSONL file: stage to `<staging_dir>/<filename>.inprogress`,
/// invoke `body` to fill the buffer, flush, then rename onto `final_dest`.
///
/// Returns whatever the body returns (typically a record count).
pub fn write_jsonl_atomic<T, F>(
    staging_dir: &Path,
    final_dest: &Path,
    write_buf_bytes: usize,
    body: F,
) -> Result<T>
where
    F: FnOnce(&mut dyn Write) -> Result<T>,
{
    stage_and_execute(staging_dir, final_dest, write_buf_bytes, |writer| {
        body(writer)
    })
}

/// Atomically write a `.zst` file with a checksum-protected zstd frame.
///
/// Stages to `<staging_dir>/<filename>.inprogress`, runs `body` against a
/// `ZstdEncoder` configured with `include_checksum(true)` so `validate_zst_full`
/// can detect silent corruption. The encoder is explicitly finished before the
/// atomic rename — this is the fix for unreadable `.zst` outputs where the
/// frame was never closed. On `body` error the encoder is dropped without
/// finishing and the staged file is cleaned up.
pub fn write_zst_atomic<T, F>(
    staging_dir: &Path,
    final_dest: &Path,
    level: i32,
    write_buf_bytes: usize,
    body: F,
) -> Result<T>
where
    F: FnOnce(&mut dyn Write) -> Result<T>,
{
    stage_and_execute(staging_dir, final_dest, write_buf_bytes, |writer| {
        // Build the encoder over a mutable reference to the staged BufWriter so
        // we can finish() it before stage_and_execute flushes/renames.
        let mut enc = ZstdEncoder::new(writer.by_ref(), level)
            .context("zstd encoder init")?;
        enc.include_checksum(true)
            .context("enable zstd content checksum")?;

        let result = body(&mut enc)?;

        enc.finish().context("finish zstd frame")?;
        Ok(result)
    })
}
