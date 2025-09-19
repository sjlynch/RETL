//! Output stitching helpers: merge per-file JSONL parts, build a single JSON array,
//! and concatenate TSV shards. Also provides a helper for temp part filenames.

use crate::paths::FileJob;
use anyhow::Result;
use serde_json::Value;
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

pub fn stitch_tmp_parts(tmp_dir: &Path, out_path: &Path, write_buf: usize) -> Result<()> {
    let mut paths: Vec<_> = fs::read_dir(tmp_dir)?.filter_map(|e| e.ok().map(|e| e.path())).collect();
    paths.sort();
    let mut out = BufWriter::with_capacity(write_buf, std::fs::File::create(out_path)?);
    for p in paths {
        let mut r = BufReader::new(std::fs::File::open(&p)?);
        std::io::copy(&mut r, &mut out)?;
    }
    out.flush()?;
    Ok(())
}

pub fn stitch_tmp_parts_to_json_array(tmp_dir: &Path, out_path: &Path, pretty: bool, write_buf: usize) -> Result<()> {
    let mut paths: Vec<_> = fs::read_dir(tmp_dir)?.filter_map(|e| e.ok().map(|e| e.path())).collect();
    paths.sort();

    let mut out = BufWriter::with_capacity(write_buf, std::fs::File::create(out_path)?);
    let mut first = true;

    if pretty {
        out.write_all(b"[\n")?;
    } else {
        out.write_all(b"[")?;
    }

    for p in paths {
        let r = BufReader::new(std::fs::File::open(&p)?);
        for line in r.lines().flatten() {
            if line.is_empty() { continue; }
            if !first {
                out.write_all(if pretty { b",\n" } else { b"," })?;
            }
            first = false;

            if pretty {
                let val: Value = serde_json::from_str(&line)?;
                serde_json::to_writer_pretty(&mut out, &val)?;
            } else {
                out.write_all(line.as_bytes())?;
            }
        }
    }

    if pretty {
        out.write_all(b"\n]")?;
    } else {
        out.write_all(b"]")?;
    }
    out.flush()?;
    Ok(())
}

pub fn concat_tsvs(parts: &Vec<PathBuf>, out_path: &Path, write_buf: usize) -> Result<()> {
    let mut paths = parts.clone();
    paths.sort();
    let mut out = BufWriter::with_capacity(write_buf, std::fs::File::create(out_path)?);
    for p in paths {
        let mut r = BufReader::new(std::fs::File::open(&p)?);
        std::io::copy(&mut r, &mut out)?;
    }
    out.flush()?;
    Ok(())
}

pub fn tmp_name_for_job(job: &FileJob) -> String {
    let kind = match job.kind { crate::paths::FileKind::Comment => "RC", crate::paths::FileKind::Submission => "RS" };
    format!("{kind}_{}.jsonl.part", job.ym)
}
