//! Output stitching helpers: merge per-file JSONL parts, build a single JSON array,
//! and concatenate TSV shards. Also provides a helper for temp part filenames.
//!
//! All stitched outputs route through `atomic_write::write_jsonl_atomic` (staging
//! under `<dest-parent>/_staging`, then atomic rename) so a crashed run cannot
//! leave a partial stitched file at the published path. The staging directory
//! shares the destination's filesystem so the rename is atomic on Windows and
//! POSIX.

use crate::atomic_write::{ensure_staging_dir, write_jsonl_atomic};
use anyhow::Result;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

const STITCH_BUF_BYTES: usize = 16 * 1024;

/// Stage a unique `.inprogress` file in `<dest-parent>/_staging`, run `body`,
/// then atomically replace `out_path`. Thin wrapper around `write_jsonl_atomic`
/// for the stitch call sites that don't carry an explicit staging dir.
fn write_atomic<F>(out_path: &Path, write_buf: usize, body: F) -> Result<()>
where
    F: FnOnce(&mut dyn Write) -> Result<()>,
{
    let parent = out_path.parent().unwrap_or_else(|| Path::new("."));
    let staging_dir = ensure_staging_dir(parent)?;
    write_jsonl_atomic(&staging_dir, out_path, write_buf, body)
}

fn list_tmp_parts(dir: &Path) -> Result<Vec<PathBuf>> {
    jsonl_part_paths(dir)
}

pub fn stitch_tmp_parts(tmp_dir: &Path, out_path: &Path, write_buf: usize) -> Result<()> {
    let parts = list_tmp_parts(tmp_dir)?;
    write_atomic(out_path, write_buf, |out| {
        for path in &parts {
            let mut r = BufReader::new(std::fs::File::open(path)?);
            std::io::copy(&mut r, out)?;
        }
        Ok(())
    })
}

/// Stitch JSONL temp parts into a single JSON array at `out_path`.
///
/// In `pretty=true` mode each emitted record is parsed and re-serialized with
/// `serde_json` field indentation, matching `retl aggregate --pretty` and the
/// CLI help text. This costs more CPU than the compact stitch path, so leave
/// `pretty=false` for large machine-consumed exports.
///
/// IO errors from corrupt or truncated temp parts are surfaced via `?`;
/// the previous `r.lines().flatten()` form silently swallowed them and
/// produced a truncated array.
pub fn stitch_tmp_parts_to_json_array(
    tmp_dir: &Path,
    out_path: &Path,
    pretty: bool,
    write_buf: usize,
) -> Result<()> {
    if pretty {
        return stitch_tmp_parts_to_json_array_pretty(tmp_dir, out_path, write_buf);
    }

    let parts = list_tmp_parts(tmp_dir)?;
    write_atomic(out_path, write_buf, |out| {
        let mut first = true;

        out.write_all(b"[")?;

        let mut buf = String::with_capacity(STITCH_BUF_BYTES);
        for path in &parts {
            let mut r = BufReader::new(std::fs::File::open(path)?);
            loop {
                buf.clear();
                let n = r.read_line(&mut buf)?;
                if n == 0 {
                    break;
                }
                // strip trailing \n and optional \r
                if buf.ends_with('\n') {
                    let _ = buf.pop();
                    if buf.ends_with('\r') {
                        let _ = buf.pop();
                    }
                }
                if buf.is_empty() {
                    continue;
                }
                if !first {
                    out.write_all(b",")?;
                }
                first = false;
                out.write_all(buf.as_bytes())?;
            }
        }

        out.write_all(b"]")?;
        Ok(())
    })
}

fn stitch_tmp_parts_to_json_array_pretty(
    tmp_dir: &Path,
    out_path: &Path,
    write_buf: usize,
) -> Result<()> {
    let parts = list_tmp_parts(tmp_dir)?;
    write_atomic(out_path, write_buf, |out| {
        let mut first = true;

        out.write_all(b"[\n")?;

        let mut buf = String::with_capacity(STITCH_BUF_BYTES);
        for path in &parts {
            let mut r = BufReader::new(std::fs::File::open(path)?);
            loop {
                buf.clear();
                let n = r.read_line(&mut buf)?;
                if n == 0 {
                    break;
                }
                // strip trailing \n and optional \r
                if buf.ends_with('\n') {
                    let _ = buf.pop();
                    if buf.ends_with('\r') {
                        let _ = buf.pop();
                    }
                }
                if buf.is_empty() {
                    continue;
                }
                if !first {
                    out.write_all(b",\n")?;
                }
                first = false;

                let value: serde_json::Value = serde_json::from_str(&buf)?;
                let pretty = serde_json::to_string_pretty(&value)?;
                for (idx, line) in pretty.lines().enumerate() {
                    if idx > 0 {
                        out.write_all(b"\n")?;
                    }
                    out.write_all(b"  ")?;
                    out.write_all(line.as_bytes())?;
                }
            }
        }

        out.write_all(b"\n]")?;
        Ok(())
    })
}

fn jsonl_part_paths(tmp_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(tmp_dir)? {
        let path = entry?.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if name.ends_with(".jsonl.part") || (name.starts_with(".part_") && name.ends_with(".jsonl"))
        {
            paths.push(path);
        }
    }
    Ok(paths)
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

    /// Pretty mode: field-indented array elements, matching CLI --pretty docs.
    #[test]
    fn stitches_pretty_array_field_indented() {
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
        assert_eq!(
            got,
            "[\n  {\n    \"id\": \"r1\"\n  },\n  {\n    \"id\": \"r2\"\n  }\n]"
        );
        let v: serde_json::Value = serde_json::from_str(&got).unwrap();
        assert_eq!(v.as_array().map(|a| a.len()), Some(2));
    }
}
