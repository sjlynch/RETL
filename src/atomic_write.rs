//! Atomic file writers for partitioned outputs.
//!
//! Pattern: write to a unique
//! `<staging_dir>/<filename>.retl-<pid>-<nonce>.inprogress`, finalize the
//! writer (flushing buffers and finishing the zstd frame), then
//! atomically replace the final destination with the staged file. A run that
//! crashes mid-write leaves a uniquely owned `.inprogress` artifact in the
//! staging directory — never a partial, unreadable file at the published path.
//!
//! Staged names include the writer process ID and a per-process nonce so
//! concurrent RETL processes sharing an output root cannot open/truncate each
//! other's scratch files. Stale sweeping only deletes staged files whose owner
//! PID is no longer running; live or legacy/unowned staged files are left in
//! place with a warning.

use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use zstd::stream::write::Encoder as ZstdEncoder;

use crate::util::{
    create_dir_all_with_backoff, create_new_with_backoff, read_dir_with_backoff,
    remove_with_backoff, replace_file_atomic_backoff,
};

/// Directory used to stage `*.inprogress` files under an output root.
pub const STAGING_DIR_NAME: &str = "_staging";

/// Extension appended to staged filenames during atomic writes. The published
/// path is the final destination; a crashed run leaves a unique
/// `<dest>.retl-<pid>-<nonce>.inprogress` in the staging dir, never a partial
/// file at the published path.
pub(crate) const INPROGRESS_EXT: &str = ".inprogress";
const STAGE_MARKER: &str = ".retl-";
static STAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Build the staging directory path for a given output root, creating it.
pub fn ensure_staging_dir(out_root: &Path) -> Result<PathBuf> {
    let dir = out_root.join(STAGING_DIR_NAME);
    create_dir_all_with_backoff(&dir, 16, 50)
        .with_context(|| format!("create staging dir {}", dir.display()))?;
    Ok(dir)
}

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

fn owner_pid_from_inprogress(path: &Path) -> Option<u32> {
    let name = path.file_name()?.to_str()?;
    if !name.ends_with(INPROGRESS_EXT) {
        return None;
    }
    let start = name.rfind(STAGE_MARKER)? + STAGE_MARKER.len();
    let rest = &name[start..name.len() - INPROGRESS_EXT.len()];
    let pid = rest.split('-').next()?;
    pid.parse().ok()
}

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
    for entry in read_dir_with_backoff(&dir, 16, 50)
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

        match remove_with_backoff(&path, 8, 50) {
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

/// Stage a write under a unique `<staging_dir>/<dest_filename>.*.inprogress`,
/// run `body` against a buffered writer, then atomically promote the staged
/// file onto `final_dest`. Caller is responsible for any encoder finalization
/// (e.g. closing a zstd frame) before returning from `body`; this helper only
/// flushes the underlying buffer.
///
/// On a `body` or flush/publish error the staged file is removed so we never
/// leave a partial `<dest>.*.inprogress` behind a successful return path.
fn stage_and_execute<T, F>(
    staging_dir: &Path,
    final_dest: &Path,
    write_buf_bytes: usize,
    body: F,
) -> Result<T>
where
    F: FnOnce(&mut BufWriter<File>) -> Result<T>,
{
    create_dir_all_with_backoff(staging_dir, 16, 50)
        .with_context(|| format!("create staging dir {}", staging_dir.display()))?;
    let staged = unique_inprogress_path(staging_dir, final_dest)?;

    let file = create_new_with_backoff(&staged, 16, 50)
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

    if let Err(e) = writer.flush() {
        drop(writer);
        let _ = remove_with_backoff(&staged, 8, 50);
        return Err(e).with_context(|| format!("flush staged {}", staged.display()));
    }
    drop(writer); // release file handle before atomic rename (Windows)

    if let Some(parent) = final_dest.parent() {
        create_dir_all_with_backoff(parent, 16, 50)
            .with_context(|| format!("create dest parent {}", parent.display()))?;
    }
    if let Err(e) = replace_file_atomic_backoff(&staged, final_dest) {
        let _ = remove_with_backoff(&staged, 8, 50);
        return Err(e).with_context(|| {
            format!(
                "publish staged {} -> {}",
                staged.display(),
                final_dest.display()
            )
        });
    }
    Ok(result)
}

/// Atomically write a JSONL/text file: stage to a unique
/// `<staging_dir>/<filename>.*.inprogress`, invoke `body` to fill the buffer,
/// flush, then rename onto `final_dest`.
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
/// Stages to a unique `<staging_dir>/<filename>.*.inprogress`, runs `body`
/// against a `ZstdEncoder` configured with `include_checksum(true)` so
/// `validate_zst_full` can detect silent corruption. The encoder is explicitly
/// finished before the atomic rename — this is the fix for unreadable `.zst`
/// outputs where the frame was never closed. On `body` error the encoder is
/// dropped without finishing and the staged file is cleaned up.
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
        let mut enc = ZstdEncoder::new(writer.by_ref(), level).context("zstd encoder init")?;
        enc.include_checksum(true)
            .context("enable zstd content checksum")?;

        let result = body(&mut enc)?;

        enc.finish().context("finish zstd frame")?;
        Ok(result)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::{Arc, Barrier};
    use std::thread;

    fn inprogress_entries(staging: &Path) -> Vec<PathBuf> {
        let mut paths: Vec<PathBuf> = fs::read_dir(staging)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .and_then(|s| s.to_str())
                    .map(|name| name.ends_with(INPROGRESS_EXT))
                    .unwrap_or(false)
            })
            .collect();
        paths.sort();
        paths
    }

    #[test]
    fn concurrent_writes_use_distinct_live_staging_paths() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let staging = ensure_staging_dir(root).unwrap();
        let final_dest = root.join("out.jsonl");

        let ready = Arc::new(Barrier::new(3));
        let release = Arc::new(Barrier::new(3));
        let mut handles = Vec::new();
        for body in ["one\n", "two\n"] {
            let ready = Arc::clone(&ready);
            let release = Arc::clone(&release);
            let staging = staging.clone();
            let final_dest = final_dest.clone();
            handles.push(thread::spawn(move || {
                write_jsonl_atomic(&staging, &final_dest, 1024, |w| {
                    ready.wait();
                    release.wait();
                    w.write_all(body.as_bytes())?;
                    Ok(())
                })
            }));
        }

        ready.wait();
        assert_eq!(
            inprogress_entries(&staging).len(),
            2,
            "concurrent writers must not share a fixed staged path"
        );
        assert_eq!(
            sweep_stale_inprogress(root, true).unwrap(),
            0,
            "sweeping must not delete live staged files"
        );
        assert_eq!(
            inprogress_entries(&staging).len(),
            2,
            "live staged files survived sweep"
        );

        release.wait();
        for handle in handles {
            handle.join().unwrap().unwrap();
        }

        let published = fs::read_to_string(&final_dest).unwrap();
        assert!(matches!(published.as_str(), "one\n" | "two\n"));
        assert!(
            inprogress_entries(&staging).is_empty(),
            "successful writes clean up staged files"
        );
    }
}
