//! Output stitching helpers: merge per-file JSONL parts, build a single JSON array,
//! and concatenate TSV shards. Also provides a helper for temp part filenames.
//!
//! All stitched outputs route through `atomic_write::write_jsonl_atomic` (staging
//! `<dest>.inprogress` next to the destination, then atomic rename) so a crashed
//! run cannot leave a partial stitched file at the published path. The staging
//! directory is the destination's parent — same filesystem guarantees the rename
//! is atomic on Windows and POSIX.

use crate::atomic_write::write_jsonl_atomic;
use crate::paths::FileJob;
use anyhow::Result;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

/// Stage `<dest>.inprogress` in the destination's parent directory, run `body`,
/// then atomically replace `out_path`. Thin wrapper around `write_jsonl_atomic`
/// for the stitch call sites that don't carry an explicit staging dir.
fn write_atomic<F>(out_path: &Path, write_buf: usize, body: F) -> Result<()>
where
    F: FnOnce(&mut dyn Write) -> Result<()>,
{
    let parent = out_path.parent().unwrap_or_else(|| Path::new("."));
    write_jsonl_atomic(parent, out_path, write_buf, body)
}

pub fn stitch_tmp_parts(tmp_dir: &Path, out_path: &Path, write_buf: usize) -> Result<()> {
    let mut paths: Vec<_> = fs::read_dir(tmp_dir)?.filter_map(|e| e.ok().map(|e| e.path())).collect();
    paths.sort();
    write_atomic(out_path, write_buf, |out| {
        for p in paths {
            let mut r = BufReader::new(std::fs::File::open(&p)?);
            std::io::copy(&mut r, out)?;
        }
        Ok(())
    })
}

/// Stitch JSONL temp parts into a single JSON array at `out_path`.
///
/// In `pretty=true` mode each emitted record is written on its own line,
/// separated by `,\n`, with `[` and `]` on their own lines. Records are
/// passed through verbatim — they were emitted moments earlier as compact
/// JSON objects by `stream_job`, so a `serde_json::from_str` +
/// `to_writer_pretty` round-trip would only re-serialize the same data
/// (the bulk of pretty-mode wall time on large extracts) without changing
/// what callers actually consume. "Pretty" here therefore means
/// "array elements on separate lines", not "indented field-by-field".
///
/// IO errors from corrupt or truncated temp parts are surfaced via `?`;
/// the previous `r.lines().flatten()` form silently swallowed them and
/// produced a truncated array.
pub fn stitch_tmp_parts_to_json_array(tmp_dir: &Path, out_path: &Path, pretty: bool, write_buf: usize) -> Result<()> {
    let mut paths: Vec<_> = fs::read_dir(tmp_dir)?.filter_map(|e| e.ok().map(|e| e.path())).collect();
    paths.sort();

    write_atomic(out_path, write_buf, |out| {
        let mut first = true;

        if pretty {
            out.write_all(b"[\n")?;
        } else {
            out.write_all(b"[")?;
        }

        let mut buf = String::with_capacity(16 * 1024);
        for p in paths {
            let mut r = BufReader::new(std::fs::File::open(&p)?);
            loop {
                buf.clear();
                let n = r.read_line(&mut buf)?;
                if n == 0 { break; }
                // strip trailing \n and optional \r
                if buf.ends_with('\n') {
                    let _ = buf.pop();
                    if buf.ends_with('\r') { let _ = buf.pop(); }
                }
                if buf.is_empty() { continue; }
                if !first {
                    out.write_all(if pretty { b",\n" } else { b"," })?;
                }
                first = false;
                out.write_all(buf.as_bytes())?;
            }
        }

        if pretty {
            out.write_all(b"\n]")?;
        } else {
            out.write_all(b"]")?;
        }
        Ok(())
    })
}

pub fn concat_tsvs(parts: &Vec<PathBuf>, out_path: &Path, write_buf: usize) -> Result<()> {
    let mut paths = parts.clone();
    paths.sort();
    write_atomic(out_path, write_buf, |out| {
        for p in paths {
            let mut r = BufReader::new(std::fs::File::open(&p)?);
            std::io::copy(&mut r, out)?;
        }
        Ok(())
    })
}

pub fn tmp_name_for_job(job: &FileJob) -> String {
    let kind = match job.kind { crate::paths::FileKind::Comment => "RC", crate::paths::FileKind::Submission => "RS" };
    format!("{kind}_{}.jsonl.part", job.ym)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A temp part containing invalid UTF-8 must cause stitching to return
    /// `Err` instead of silently truncating the JSON array. Regression test
    /// for the prior `r.lines().flatten()` form which dropped `io::Error`
    /// values on the floor.
    #[test]
    fn corrupt_temp_part_surfaces_io_error() {
        let dir = tempfile::tempdir().unwrap();
        let tmp_dir = dir.path().join("parts");
        fs::create_dir_all(&tmp_dir).unwrap();

        // First part: well-formed compact JSON object on one line.
        {
            let mut f = fs::File::create(tmp_dir.join("RC_2024-01.jsonl.part")).unwrap();
            f.write_all(b"{\"id\":\"r1\"}\n").unwrap();
        }
        // Second part: invalid UTF-8 mid-line — `BufRead::read_line` will
        // return `io::Error(InvalidData)` when it tries to decode this.
        {
            let mut f = fs::File::create(tmp_dir.join("RC_2024-02.jsonl.part")).unwrap();
            f.write_all(b"{\"id\":\"r2\",\"x\":\"").unwrap();
            f.write_all(&[0xFF, 0xFE, 0xFD]).unwrap(); // invalid UTF-8 sequence
            f.write_all(b"\"}\n").unwrap();
        }

        let out = dir.path().join("out.json");
        let res = stitch_tmp_parts_to_json_array(&tmp_dir, &out, false, 64 * 1024);
        assert!(
            res.is_err(),
            "stitching a corrupt temp part must surface an Err, got Ok with file at {}",
            out.display()
        );
        // And — because the write is atomic — no partial stitched file
        // should exist at the published path.
        assert!(
            !out.exists(),
            "atomic stitch must not publish a partial output on error"
        );
    }

    /// Healthy stitch happy-path: two parts, two records, valid JSON array.
    #[test]
    fn stitches_compact_array() {
        let dir = tempfile::tempdir().unwrap();
        let tmp_dir = dir.path().join("parts");
        fs::create_dir_all(&tmp_dir).unwrap();

        {
            let mut f = fs::File::create(tmp_dir.join("RC_2024-01.jsonl.part")).unwrap();
            f.write_all(b"{\"id\":\"r1\"}\n").unwrap();
        }
        {
            let mut f = fs::File::create(tmp_dir.join("RC_2024-02.jsonl.part")).unwrap();
            f.write_all(b"{\"id\":\"r2\"}\n").unwrap();
        }

        let out = dir.path().join("out.json");
        stitch_tmp_parts_to_json_array(&tmp_dir, &out, false, 64 * 1024).unwrap();
        let got = fs::read_to_string(&out).unwrap();
        assert_eq!(got, "[{\"id\":\"r1\"},{\"id\":\"r2\"}]");
        // Round-trip through serde to confirm it parses as an array of two objects.
        let v: serde_json::Value = serde_json::from_str(&got).unwrap();
        assert_eq!(v.as_array().map(|a| a.len()), Some(2));
    }

    /// Pretty mode: array elements on separate lines, no DOM round-trip.
    #[test]
    fn stitches_pretty_array_one_per_line() {
        let dir = tempfile::tempdir().unwrap();
        let tmp_dir = dir.path().join("parts");
        fs::create_dir_all(&tmp_dir).unwrap();

        {
            let mut f = fs::File::create(tmp_dir.join("RC_2024-01.jsonl.part")).unwrap();
            f.write_all(b"{\"id\":\"r1\"}\n").unwrap();
            f.write_all(b"{\"id\":\"r2\"}\n").unwrap();
        }

        let out = dir.path().join("out.json");
        stitch_tmp_parts_to_json_array(&tmp_dir, &out, true, 64 * 1024).unwrap();
        let got = fs::read_to_string(&out).unwrap();
        assert_eq!(got, "[\n{\"id\":\"r1\"},\n{\"id\":\"r2\"}\n]");
        let v: serde_json::Value = serde_json::from_str(&got).unwrap();
        assert_eq!(v.as_array().map(|a| a.len()), Some(2));
    }
}
