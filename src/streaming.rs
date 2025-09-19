//! Streaming primitives: the one-pass record filter/writer (`stream_job`) with progress,
//! and the usernames collector for a single monthly file.

use crate::filters::{matches_minimal, matches_subreddit_basic, within_bounds};
use crate::paths::FileJob;
use crate::query::QuerySpec;
use crate::shard::ShardedWriter;
use crate::zstd_jsonl::{for_each_line_cfg, for_each_line_with_progress_cfg, parse_minimal};
use anyhow::Result;
use indicatif::ProgressBar;
use serde_json::{Map, Value};
use std::io::Write;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

fn apply_human_timestamp_in_place(map: &mut Map<String, Value>, key: &str) {
    if let Some(v) = map.get_mut(key) {
        if let Some(n) = v.as_i64() {
            if let Ok(dt) = OffsetDateTime::from_unix_timestamp(n) {
                if let Ok(s) = dt.format(&Rfc3339) {
                    *v = Value::String(s);
                }
            }
        }
    }
}

fn apply_human_timestamps(val: &mut Value) {
    if let Some(obj) = val.as_object_mut() {
        // Convert common timestamp fields if they are numeric.
        apply_human_timestamp_in_place(obj, "created_utc");
        apply_human_timestamp_in_place(obj, "retrieved_on");
        // "edited" can be bool or number. Only convert numeric forms.
        apply_human_timestamp_in_place(obj, "edited");
    }
}

pub fn stream_job<W: Write>(
    job: &FileJob,
    writer: &mut W,
    targets: Option<&Vec<String>>,
    query: &QuerySpec,
    whitelist: &Option<Vec<String>>,
    pb: Option<ProgressBar>,
    bounds: Option<(crate::date::YearMonth, crate::date::YearMonth)>,
    read_buf_bytes: usize,
    human_timestamps: bool,
) -> Result<u64> {
    let mut written: u64 = 0;

    let mut on_line = |line: &str| -> Result<()> {
        if let Ok(min) = parse_minimal(line) {
            if !matches_minimal(&min, targets, query) { return Ok(()); }
            if !within_bounds(&min, bounds) { return Ok(()); }

            // Fast-path: raw line when no transforms needed.
            if whitelist.is_none() && !human_timestamps {
                writer.write_all(line.as_bytes())?;
                writer.write_all(b"\n")?;
                written += 1;
                return Ok(());
            }

            // Parse full value for whitelisting and/or timestamp conversion.
            let val: Value = serde_json::from_str(line)?;
            let mut out_val = if let Some(fields) = whitelist {
                let mut obj = Map::new();
                if let Some(map) = val.as_object() {
                    for k in fields {
                        if let Some(v) = map.get(k) { obj.insert(k.clone(), v.clone()); }
                    }
                }
                Value::Object(obj)
            } else {
                val
            };

            if human_timestamps {
                apply_human_timestamps(&mut out_val);
            }

            serde_json::to_writer(&mut *writer, &out_val)?;
            writer.write_all(b"\n")?;
            written += 1;
        }
        Ok(())
    };

    if let Some(pb) = pb {
        for_each_line_with_progress_cfg(&job.path, read_buf_bytes, |delta| pb.inc(delta), |s| on_line(s))?;
    } else {
        for_each_line_cfg(&job.path, read_buf_bytes, |s| on_line(s))?;
    }

    Ok(written)
}

pub fn process_file_for_usernames(
    job: &FileJob,
    read_buf_bytes: usize,
    subreddit: &str,
    shard_writer: &ShardedWriter,
    pb: Option<ProgressBar>,
) -> Result<()> {
    let handle_line = |line: &str| -> Result<()> {
        let min = match parse_minimal(line) { Ok(m) => m, Err(_) => return Ok(()) };
        if !matches_subreddit_basic(&min, subreddit) { return Ok(()); }
        if let Some(author) = min.author.as_deref() {
            let a = author.trim();
            if a.is_empty() || a == "[deleted]" || a == "[removed]" { return Ok(()); }
            shard_writer.write(a)?;
        }
        Ok(())
    };

    if let Some(pb) = pb {
        for_each_line_with_progress_cfg(&job.path, read_buf_bytes, |delta| pb.inc(delta), |s| handle_line(s))?;
    } else {
        for_each_line_cfg(&job.path, read_buf_bytes, |s| handle_line(s))?;
    }
    Ok(())
}
