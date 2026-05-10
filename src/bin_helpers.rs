//! Shared helpers for the `retl` binary's subcommand handlers:
//! the `RecCount` aggregator, ETL builder glue, the `plan!` macro, and a
//! couple of CLI-only path/I/O helpers.

use anyhow::{Context, Result};
use retl::{Aggregator, RedditETL, Sources, YearMonth};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::bin_args::CommonOpts;

// -----------------------------------------------------------------------------
// Aggregator used by the `aggregate` subcommand.
// -----------------------------------------------------------------------------

/// Built-in aggregator: counts records across the supplied JSONL inputs.
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct RecCount {
    pub(crate) count: u64,
}

impl Aggregator for RecCount {
    fn ingest(&mut self, _record: &Value) {
        self.count += 1;
    }
    fn merge(&mut self, other: Self) {
        self.count += other.count;
    }
}

// -----------------------------------------------------------------------------
// Builder helpers.
// -----------------------------------------------------------------------------

pub(crate) fn ensure_dirs(common: &CommonOpts) -> Result<PathBuf> {
    fs::create_dir_all(&common.work_dir)
        .with_context(|| format!("creating work_dir {}", common.work_dir.display()))?;
    let lib_tmp = common.work_dir.join("lib_tmp");
    fs::create_dir_all(&lib_tmp)
        .with_context(|| format!("creating work_dir {}", lib_tmp.display()))?;
    Ok(lib_tmp)
}

pub(crate) fn build_etl(common: &CommonOpts) -> Result<RedditETL> {
    let lib_tmp = ensure_dirs(common)?;
    let mut etl = RedditETL::new()
        .base_dir(&common.data_dir)
        .work_dir(&lib_tmp)
        .progress(!common.no_progress)
        .sources(Sources::from(common.source))
        .date_range(common.start, common.end);

    if let Some(p) = common.parallelism {
        etl = etl.parallelism(p);
    }
    if let Some(fc) = common.file_concurrency {
        etl = etl.file_concurrency(fc);
    }
    if !common.whitelist.is_empty() {
        etl = etl.whitelist_fields(common.whitelist.iter().cloned());
    }
    if common.human_timestamps {
        etl = etl.timestamps_human_readable(true);
    }
    if let Some(b) = common.inflight_bytes {
        etl = etl.inflight_bytes(b);
    }
    Ok(etl)
}

/// Build a `ScanPlan` from `etl` with the (possibly empty) subreddit selection
/// applied. Inlined as a macro because `ScanPlan` is not re-exported from the
/// crate root and we want to avoid widening the public API just for the CLI.
macro_rules! plan {
    ($etl:expr, $subs:expr) => {{
        let scan = $etl.scan();
        if $subs.is_empty() {
            scan
        } else {
            scan.subreddits($subs.iter().map(String::as_str))
        }
    }};
}
pub(crate) use plan;

// -----------------------------------------------------------------------------
// CLI-only path / I/O helpers.
// -----------------------------------------------------------------------------

/// Run an extraction that writes to a file path, then stream the resulting
/// file to stdout and remove it. Used to honor `--out -` for
/// `extract_to_jsonl` / `extract_to_json`, which only know how to write to a
/// `Path`.
pub(crate) fn stream_extract_to_stdout(
    work_dir: &Path,
    file_stem: &str,
    extract: impl FnOnce(&Path) -> Result<()>,
) -> Result<()> {
    let lib_tmp = work_dir.join("lib_tmp");
    fs::create_dir_all(&lib_tmp)
        .with_context(|| format!("creating work_dir {}", lib_tmp.display()))?;
    let tmp_path = lib_tmp.join(format!("retl_export_{}_{}", std::process::id(), file_stem));
    let _ = fs::remove_file(&tmp_path);

    let result = extract(&tmp_path);

    if let Err(e) = result {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }

    let copy_result = (|| -> Result<()> {
        let mut f = fs::File::open(&tmp_path)
            .with_context(|| format!("opening export tempfile {}", tmp_path.display()))?;
        let stdout = io::stdout();
        let mut w = stdout.lock();
        io::copy(&mut f, &mut w).context("streaming export to stdout")?;
        w.flush()?;
        Ok(())
    })();

    let _ = fs::remove_file(&tmp_path);
    copy_result
}

/// Discover spool parts in `dir`, parsing `part_RC_YYYY-MM.jsonl` and
/// `part_RS_YYYY-MM.jsonl` filenames. Returns `(sorted_paths, min, max)`.
pub(crate) fn discover_spool_parts(dir: &Path) -> Result<(Vec<PathBuf>, YearMonth, YearMonth)> {
    let entries = fs::read_dir(dir)
        .with_context(|| format!("reading spool dir {}", dir.display()))?;
    let mut parts: Vec<(YearMonth, PathBuf)> = Vec::new();
    for e in entries {
        let e = e?;
        let name = e.file_name().to_string_lossy().into_owned();
        let stem = name
            .strip_prefix("part_RC_")
            .or_else(|| name.strip_prefix("part_RS_"))
            .and_then(|s| s.strip_suffix(".jsonl"));
        if let Some(stem) = stem {
            if let Ok(ym) = stem.parse::<YearMonth>() {
                parts.push((ym, e.path()));
            }
        }
    }
    if parts.is_empty() {
        anyhow::bail!(
            "no part_RC_YYYY-MM.jsonl or part_RS_YYYY-MM.jsonl files found in {}",
            dir.display()
        );
    }
    parts.sort_by(|a, b| a.1.cmp(&b.1));
    let min_ym = parts.iter().map(|(ym, _)| *ym).min().unwrap();
    let max_ym = parts.iter().map(|(ym, _)| *ym).max().unwrap();
    Ok((parts.into_iter().map(|(_, p)| p).collect(), min_ym, max_ym))
}
