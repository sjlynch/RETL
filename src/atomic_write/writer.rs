use anyhow::{Context, Result};
use serde::Serialize;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use zstd::stream::write::Encoder as ZstdEncoder;

use super::{unique_inprogress_path, STAGING_DIR_NAME};
use crate::util::replace_file_atomic_backoff;

#[cfg(test)]
use super::testing::notify_stage_path_for_tests;

/// Build the staging directory path for a given output root, creating it.
pub fn ensure_staging_dir(out_root: &Path) -> Result<PathBuf> {
    let dir = out_root.join(STAGING_DIR_NAME);
    crate::util::create_dir_all_with_default_backoff(&dir)
        .with_context(|| format!("create staging dir {}", dir.display()))?;
    Ok(dir)
}

/// Stage a write under a unique `<staging_dir>/<dest_filename>.*.inprogress`,
/// run `body` against a buffered writer, then — when `should_publish` accepts
/// the body's result — atomically promote the staged file onto `final_dest`.
/// Caller is responsible for any encoder finalization (e.g. closing a zstd
/// frame) before returning from `body`; this helper only flushes the
/// underlying buffer.
///
/// When `should_publish` returns `false` the staged file is discarded and the
/// atomic rename is skipped, so the suppressed output is never momentarily
/// visible at the published path. A failed discard only strands a PID-owned
/// `.inprogress` file in the staging dir (reclaimed by the stale sweep) —
/// never a stray file at `final_dest`.
///
/// On a `body` or flush/publish error the staged file is removed so we never
/// leave a partial `<dest>.*.inprogress` behind a successful return path.
fn stage_and_execute<T, F, P>(
    staging_dir: &Path,
    final_dest: &Path,
    write_buf_bytes: usize,
    should_publish: P,
    body: F,
) -> Result<T>
where
    F: FnOnce(&mut BufWriter<File>) -> Result<T>,
    P: FnOnce(&T) -> bool,
{
    crate::util::create_dir_all_with_default_backoff(staging_dir)
        .with_context(|| format!("create staging dir {}", staging_dir.display()))?;
    let staged = unique_inprogress_path(staging_dir, final_dest)?;

    let file = crate::util::create_new_with_default_backoff(&staged)
        .with_context(|| format!("create staged {}", staged.display()))?;
    #[cfg(test)]
    notify_stage_path_for_tests(&staged);
    let mut writer = BufWriter::with_capacity(write_buf_bytes, file);

    let result = match body(&mut writer) {
        Ok(v) => v,
        Err(e) => {
            drop(writer);
            let _ = crate::util::remove_with_short_backoff(&staged);
            return Err(e).with_context(|| format!("write staged {}", staged.display()));
        }
    };

    if let Err(e) = writer.flush() {
        drop(writer);
        let _ = crate::util::remove_with_short_backoff(&staged);
        return Err(e).with_context(|| format!("flush staged {}", staged.display()));
    }
    drop(writer); // release file handle before atomic rename (Windows)

    // Inspect the body's result before the atomic rename. A caller can decline
    // to publish (e.g. a zero-record partition); the staged file is discarded
    // in the staging dir and never reaches the published path.
    if !should_publish(&result) {
        let _ = crate::util::remove_with_short_backoff(&staged);
        return Ok(result);
    }

    if let Some(parent) = final_dest.parent() {
        if let Err(e) = crate::util::create_dir_all_with_default_backoff(parent) {
            let _ = crate::util::remove_with_short_backoff(&staged);
            return Err(e).with_context(|| format!("create dest parent {}", parent.display()));
        }
    }
    if let Err(e) = replace_file_atomic_backoff(&staged, final_dest) {
        let _ = crate::util::remove_with_short_backoff(&staged);
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
    write_jsonl_atomic_if(staging_dir, final_dest, write_buf_bytes, |_| true, body)
}

/// Like [`write_jsonl_atomic`], but publish the staged file onto `final_dest`
/// only when `should_publish` returns `true` for the body's result. When it
/// returns `false` the staged `*.inprogress` file is discarded and the atomic
/// rename is skipped, so an output the caller wants to suppress (e.g. a
/// zero-record partition) is never momentarily visible at the published path.
/// Returns the body's result either way.
pub fn write_jsonl_atomic_if<T, F, P>(
    staging_dir: &Path,
    final_dest: &Path,
    write_buf_bytes: usize,
    should_publish: P,
    body: F,
) -> Result<T>
where
    F: FnOnce(&mut dyn Write) -> Result<T>,
    P: FnOnce(&T) -> bool,
{
    stage_and_execute(
        staging_dir,
        final_dest,
        write_buf_bytes,
        should_publish,
        |writer| body(writer),
    )
}

/// Derive the staging directory from `final_dest.parent()` and atomically
/// publish `body`'s output at `final_dest`. Convenience wrapper around
/// `write_jsonl_atomic` for call sites that don't already carry a staging-dir
/// handle (use `write_jsonl_atomic` / `write_zst_atomic_if` directly when you
/// do, e.g. inside a per-shard loop that pre-derives one staging dir).
pub fn write_at_path_atomic<T, F>(final_dest: &Path, write_buf_bytes: usize, body: F) -> Result<T>
where
    F: FnOnce(&mut dyn Write) -> Result<T>,
{
    let parent = final_dest
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let staging_dir = ensure_staging_dir(parent)?;
    write_jsonl_atomic(&staging_dir, final_dest, write_buf_bytes, body)
}

/// Convenience wrapper for CLI-style atomic text writes that don't already
/// carry a staging-dir handle. Identical to `write_at_path_atomic`; named for
/// caller clarity at sites that emit plain text/JSON rather than newline-
/// delimited records.
pub fn write_text_atomic<T, F>(final_dest: &Path, write_buf_bytes: usize, body: F) -> Result<T>
where
    F: FnOnce(&mut dyn Write) -> Result<T>,
{
    write_at_path_atomic(final_dest, write_buf_bytes, body)
}

/// Atomically write a pretty JSON sidecar/text document followed by a trailing
/// newline. Stages through the same unique `*.retl-<pid>-<nonce>.inprogress`
/// path as [`write_jsonl_atomic`].
pub(crate) fn write_json_pretty_atomic<T>(
    staging_dir: &Path,
    final_dest: &Path,
    write_buf_bytes: usize,
    value: &T,
) -> Result<()>
where
    T: Serialize + ?Sized,
{
    write_jsonl_atomic(staging_dir, final_dest, write_buf_bytes, |w| {
        serde_json::to_writer_pretty(&mut *w, value)?;
        w.write_all(b"\n")?;
        Ok(())
    })
}

/// Atomically write a `.zst` file with a checksum-protected zstd frame,
/// publishing the staged file onto `final_dest` only when `should_publish`
/// returns `true` for the body's result.
///
/// Stages to a unique `<staging_dir>/<filename>.*.inprogress`, runs `body`
/// against a `ZstdEncoder` configured with `include_checksum(true)` so
/// `validate_zst_full` can detect silent corruption. The encoder is explicitly
/// finished before the atomic rename — this is the fix for unreadable `.zst`
/// outputs where the frame was never closed. On `body` error the encoder is
/// dropped without finishing and the staged file is cleaned up.
///
/// When `should_publish` returns `false` the staged `*.inprogress` file (a
/// closed, checksummed — but empty-payload — zstd frame) is discarded and the
/// atomic rename is skipped, so a zero-record partition never appears at the
/// published path. Pass `|_| true` to always publish. Returns the body's
/// result either way.
pub fn write_zst_atomic_if<T, F, P>(
    staging_dir: &Path,
    final_dest: &Path,
    level: i32,
    write_buf_bytes: usize,
    should_publish: P,
    body: F,
) -> Result<T>
where
    F: FnOnce(&mut dyn Write) -> Result<T>,
    P: FnOnce(&T) -> bool,
{
    stage_and_execute(
        staging_dir,
        final_dest,
        write_buf_bytes,
        should_publish,
        |writer| {
            // Build the encoder over a mutable reference to the staged
            // BufWriter so we can finish() it before stage_and_execute
            // flushes/renames.
            let mut enc =
                ZstdEncoder::new(writer.by_ref(), level).context("zstd encoder init")?;
            enc.include_checksum(true)
                .context("enable zstd content checksum")?;

            let result = body(&mut enc)?;

            enc.finish().context("finish zstd frame")?;
            Ok(result)
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn staged_file_count(staging: &Path) -> usize {
        fs::read_dir(staging)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .count()
    }

    #[test]
    fn write_jsonl_atomic_if_discards_staged_file_when_not_published() {
        let tmp = tempfile::tempdir().unwrap();
        let staging = ensure_staging_dir(tmp.path()).unwrap();
        let dest = tmp.path().join("out.jsonl");

        let written =
            write_jsonl_atomic_if(&staging, &dest, 64 * 1024, |&n: &u64| n > 0, |_w| Ok(0u64))
                .unwrap();

        assert_eq!(written, 0);
        assert!(
            !dest.exists(),
            "a declined write must never reach the published path",
        );
        assert_eq!(
            staged_file_count(&staging),
            0,
            "the discarded staged file must be cleaned out of the staging dir",
        );
    }

    #[test]
    fn write_jsonl_atomic_if_publishes_when_predicate_accepts() {
        let tmp = tempfile::tempdir().unwrap();
        let staging = ensure_staging_dir(tmp.path()).unwrap();
        let dest = tmp.path().join("out.jsonl");

        let written = write_jsonl_atomic_if(&staging, &dest, 64 * 1024, |&n: &u64| n > 0, |w| {
            w.write_all(b"one\n")?;
            Ok(1u64)
        })
        .unwrap();

        assert_eq!(written, 1);
        assert_eq!(fs::read_to_string(&dest).unwrap(), "one\n");
        assert_eq!(staged_file_count(&staging), 0);
    }

    #[test]
    fn write_zst_atomic_if_discards_staged_file_when_not_published() {
        let tmp = tempfile::tempdir().unwrap();
        let staging = ensure_staging_dir(tmp.path()).unwrap();
        let dest = tmp.path().join("out.zst");

        // The encoder still closes a real (empty-payload) frame on the staged
        // file; declining to publish must discard it rather than rename it on.
        let written =
            write_zst_atomic_if(&staging, &dest, 3, 64 * 1024, |&n: &u64| n > 0, |_w| Ok(0u64))
                .unwrap();

        assert_eq!(written, 0);
        assert!(
            !dest.exists(),
            "a declined zst write must never reach the published path",
        );
        assert_eq!(staged_file_count(&staging), 0);
    }
}
