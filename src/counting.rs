//! Small reducers used by analytics: count-by-month over matches, with a merge helper.

use crate::date::YearMonth;
use crate::filters::{matches_full, matches_minimal, ym_from_epoch};
use crate::paths::FileJob;
use crate::query::QuerySpec;
use crate::zstd_jsonl::{for_each_line_cfg, parse_minimal};
use anyhow::Result;
use serde_json::Value;
use std::collections::BTreeMap;

pub fn count_for_job(
    job: &FileJob,
    targets: Option<&Vec<String>>,
    query: &QuerySpec,
    bounds: Option<(YearMonth, YearMonth)>,
    read_buf_bytes: usize,
) -> Result<BTreeMap<YearMonth, u64>> {
    let mut m = BTreeMap::<YearMonth, u64>::new();
    for_each_line_cfg(&job.path, read_buf_bytes, |line| {
        if let Ok(min) = parse_minimal(line) {
            if !matches_minimal(&min, targets, query) { return Ok(()); }
            if let Some(ts) = min.created_utc {
                let ym = ym_from_epoch(ts);
                if let Some((lo, hi)) = bounds {
                    if ym < lo || ym > hi { return Ok(()); }
                }
                if query.requires_full_parse() {
                    let val: Value = serde_json::from_str(line)?;
                    if !matches_full(&val, job.kind, query) { return Ok(()); }
                }
                *m.entry(ym).or_insert(0) += 1;
            }
        }
        Ok(())
    })?;
    Ok(m)
}

pub fn merge_counts(total: &mut BTreeMap<YearMonth, u64>, part: BTreeMap<YearMonth, u64>) {
    for (k, v) in part {
        *total.entry(k).or_insert(0) += v;
    }
}
