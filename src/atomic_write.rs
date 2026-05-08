//! Atomic write helpers.
//!
//! Both helpers write to a sibling tmp file first, then rename into place via
//! [`crate::util::replace_file_atomic_backoff`]. If the user closure returns an
//! error or panics, the tmp file is removed and the destination is never
//! touched — readers therefore see either the previous file or the new one,
//! never a half-written intermediate.
//!
//! [`write_zst_atomic`] additionally wraps the tmp file in
//! [`zstd::stream::write::AutoFinishEncoder`], which finishes the zstd frame
//! on drop. That means a panic during write still produces a valid (truncated)
//! zstd stream in the tmp file before we delete it — there is no path by which
//! a half-written .zst frame can land at `path`.

use anyhow::{Context, Result};
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::util::{create_with_backoff, replace_file_atomic_backoff};

fn atomic_tmp_path(dest: &Path) -> PathBuf {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let parent = dest.parent().unwrap_or_else(|| Path::new("."));
    let file_name = dest
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("atomic");
    parent.join(format!(".{file_name}.tmp.{pid}.{nanos}"))
}

/// Write `path` atomically by streaming through `write_fn`.
///
/// The closure receives a buffered writer over a sibling tmp file. On success
/// the tmp file is renamed over `path`; on error or panic the tmp file is
/// removed and `path` is left untouched.
pub fn write_atomic<F>(path: &Path, write_fn: F) -> Result<()>
where
    F: FnOnce(&mut dyn Write) -> Result<()>,
{
    let tmp_path = atomic_tmp_path(path);

    let inner = (|| -> Result<()> {
        let file = create_with_backoff(&tmp_path, 16, 50)
            .with_context(|| format!("create tmp {}", tmp_path.display()))?;
        let mut buf = BufWriter::new(file);
        write_fn(&mut buf)?;
        buf.flush().with_context(|| format!("flush tmp {}", tmp_path.display()))?;
        Ok(())
    })();

    match inner {
        Ok(()) => replace_file_atomic_backoff(&tmp_path, path)
            .with_context(|| format!("replace {}", path.display())),
        Err(e) => {
            let _ = fs::remove_file(&tmp_path);
            Err(e)
        }
    }
}

/// Write a zstd-compressed file atomically.
///
/// The closure receives an `&mut dyn Write` that compresses through a
/// `zstd::stream::write::AutoFinishEncoder`. On success the tmp file is
/// renamed over `path`; on error or panic the tmp file is removed (after the
/// AutoFinishEncoder has finished the zstd frame on drop) and `path` is left
/// untouched.
pub fn write_zst_atomic<F>(
    path: &Path,
    level: i32,
    include_checksum: bool,
    write_fn: F,
) -> Result<()>
where
    F: FnOnce(&mut dyn Write) -> Result<()>,
{
    let tmp_path = atomic_tmp_path(path);

    let inner = (|| -> Result<()> {
        let file = create_with_backoff(&tmp_path, 16, 50)
            .with_context(|| format!("create tmp {}", tmp_path.display()))?;
        let buf = BufWriter::new(file);
        let mut enc = zstd::stream::write::Encoder::new(buf, level)
            .context("init zstd encoder")?;
        enc.include_checksum(include_checksum)
            .context("set zstd checksum flag")?;
        // AutoFinishEncoder writes the zstd frame trailer on drop, even if
        // the closure panics. The tmp file is therefore always a valid (if
        // truncated) zstd stream by the time we either rename or delete it.
        let mut auto = enc.auto_finish();
        write_fn(&mut auto)?;
        // Explicit drop: forces the trailer + flush before we rename.
        drop(auto);
        Ok(())
    })();

    match inner {
        Ok(()) => replace_file_atomic_backoff(&tmp_path, path)
            .with_context(|| format!("replace {}", path.display())),
        Err(e) => {
            let _ = fs::remove_file(&tmp_path);
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn write_atomic_writes_and_renames() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("out.txt");
        write_atomic(&dest, |w| {
            w.write_all(b"hello world")?;
            Ok(())
        })
        .unwrap();
        let mut s = String::new();
        std::fs::File::open(&dest).unwrap().read_to_string(&mut s).unwrap();
        assert_eq!(s, "hello world");
    }

    #[test]
    fn write_atomic_error_does_not_touch_dest() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("out.txt");
        std::fs::write(&dest, b"original").unwrap();

        let r: Result<()> = write_atomic(&dest, |_w| Err(anyhow::anyhow!("nope")));
        assert!(r.is_err());

        let mut s = String::new();
        std::fs::File::open(&dest).unwrap().read_to_string(&mut s).unwrap();
        assert_eq!(s, "original");

        // No leftover tmp files.
        let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains(".tmp."))
            .collect();
        assert!(leftovers.is_empty(), "tmp file leaked: {:?}", leftovers);
    }

    #[test]
    fn write_zst_atomic_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("out.zst");
        let payload = b"the quick brown fox jumps over the lazy dog\n".repeat(50);

        write_zst_atomic(&dest, 3, true, |w| {
            w.write_all(&payload)?;
            Ok(())
        })
        .unwrap();

        let f = std::fs::File::open(&dest).unwrap();
        let mut dec = zstd::stream::read::Decoder::new(f).unwrap();
        let mut got = Vec::new();
        dec.read_to_end(&mut got).unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn write_zst_atomic_error_does_not_touch_dest() {
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("out.zst");
        std::fs::write(&dest, b"prev").unwrap();

        let r: Result<()> = write_zst_atomic(&dest, 3, false, |w| {
            w.write_all(b"partial")?;
            Err(anyhow::anyhow!("boom"))
        });
        assert!(r.is_err());

        let mut s = Vec::new();
        std::fs::File::open(&dest).unwrap().read_to_end(&mut s).unwrap();
        assert_eq!(s, b"prev");
    }
}
