//! Regression test for `extract_to_jsonl` / `extract_to_json` output order
//! stability. Per the prior `[Bug] extract_to_jsonl / extract_to_json output is
//! filesystem-readdir-order dependent (not byte-stable across runs)` task, the
//! stitch step must sort per-month temp parts before concatenation so that:
//!
//! - The same query on the same corpus produces byte-identical output across
//!   runs (e.g. for content-addressed downstream caches and snapshot tests).
//! - Resume runs that only rebuild a subset of months still emit records in
//!   the same total order.
//! - Output is independent of filesystem readdir order (NTFS alphabetical
//!   vs ext4 hash-based vs APFS variable).
//!
//! This integration test feeds a three-month corpus with month-tagged records
//! and asserts that the stitched JSONL and JSON-array outputs interleave
//! months in `(year-month, source-kind)` order.

#[path = "common/mod.rs"]
mod common;

use common::write_zst_lines;
use retl::{RedditETL, Sources, YearMonth};
use serde_json::{json, Value};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

/// Build a small three-month corpus where each record's `id` is tagged with
/// its month and source kind, e.g. `RC_2020-02:00`. Months are deliberately
/// created in a non-sorted order so that filesystems whose `read_dir` reflects
/// creation order (or hash-order) would return parts out of sequence.
fn make_three_month_corpus() -> PathBuf {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.keep();

    // Insertion order is INTENTIONALLY reversed/scrambled relative to the
    // sorted month order, so the stitch path's sort is what produces the
    // assertion-expected output.
    let months = [
        ("2020-03", 1_583_020_800_i64),
        ("2020-01", 1_577_836_800_i64),
        ("2020-02", 1_580_515_200_i64),
    ];

    for (label, ts0) in months.iter().copied() {
        let mut rc_lines = Vec::with_capacity(3);
        let mut rs_lines = Vec::with_capacity(3);
        for i in 0..3 {
            rc_lines.push(
                json!({
                    "author": "alice",
                    "body": format!("comment {label} {i}"),
                    "created_utc": ts0 + i as i64,
                    "id": format!("RC_{label}:{i:02}"),
                    "parent_id": "t3_root",
                    "score": 1,
                    "subreddit": "programming"
                })
                .to_string(),
            );
            rs_lines.push(
                json!({
                    "author": "alice",
                    "created_utc": ts0 + 10_000 + i as i64,
                    "domain": "example.com",
                    "id": format!("RS_{label}:{i:02}"),
                    "score": 1,
                    "subreddit": "programming",
                    "title": format!("submission {label} {i}"),
                    "url": "http://example.com/"
                })
                .to_string(),
            );
        }
        write_zst_lines(
            &base.join("comments").join(format!("RC_{label}.zst")),
            &rc_lines,
        );
        write_zst_lines(
            &base.join("submissions").join(format!("RS_{label}.zst")),
            &rs_lines,
        );
    }

    base
}

/// `extract_to_jsonl` output order must reflect a stable sort over the
/// per-month parts, not whatever order `read_dir` returns them in.
#[test]
fn extract_to_jsonl_is_byte_stable_across_runs() {
    let base = make_three_month_corpus();

    let run = |out: &PathBuf, parallelism: usize| {
        RedditETL::new()
            .base_dir(&base)
            .sources(Sources::Both)
            .date_range(
                Some(YearMonth::new(2020, 1)),
                Some(YearMonth::new(2020, 3)),
            )
            .parallelism(parallelism)
            .file_concurrency(parallelism)
            .progress(false)
            .scan()
            .subreddit("programming")
            .extract_to_jsonl(out)
            .unwrap();
    };

    let out_a = base.join("a.jsonl");
    let out_b = base.join("b.jsonl");
    run(&out_a, 1);
    run(&out_b, 4);

    // Across-run byte stability.
    let bytes_a = fs::read(&out_a).unwrap();
    let bytes_b = fs::read(&out_b).unwrap();
    assert_eq!(
        bytes_a, bytes_b,
        "extract_to_jsonl output must be byte-identical across parallelism levels"
    );

    // Order: RC months come before RS months (PathBuf sort: "RC_..." < "RS_..."),
    // and within each kind months are ascending.
    let ids: Vec<String> = BufReader::new(fs::File::open(&out_a).unwrap())
        .lines()
        .map(|l| l.unwrap())
        .filter(|s| !s.is_empty())
        .map(|s| serde_json::from_str::<Value>(&s).unwrap())
        .map(|v| v["id"].as_str().unwrap().to_string())
        .collect();

    // First per-month record from each part — proves part-level ordering.
    let firsts: Vec<&str> = ids.iter().map(|s| s.as_str()).step_by(3).collect();
    assert_eq!(
        firsts,
        vec![
            "RC_2020-01:00",
            "RC_2020-02:00",
            "RC_2020-03:00",
            "RS_2020-01:00",
            "RS_2020-02:00",
            "RS_2020-03:00",
        ],
        "stitched parts must be sorted by full PathBuf — RC months ascending, \
         then RS months ascending"
    );
}

/// Same invariant for `extract_to_json` (JSON-array output). Pretty mode also
/// needs to be deterministic since it's commonly diffed.
#[test]
fn extract_to_json_array_is_byte_stable_across_runs() {
    let base = make_three_month_corpus();

    let run = |out: &PathBuf, pretty: bool, parallelism: usize| {
        RedditETL::new()
            .base_dir(&base)
            .sources(Sources::Both)
            .date_range(
                Some(YearMonth::new(2020, 1)),
                Some(YearMonth::new(2020, 3)),
            )
            .parallelism(parallelism)
            .file_concurrency(parallelism)
            .progress(false)
            .scan()
            .subreddit("programming")
            .extract_to_json(out, pretty)
            .unwrap();
    };

    for pretty in [false, true] {
        let out_a = base.join(if pretty { "a_pretty.json" } else { "a.json" });
        let out_b = base.join(if pretty { "b_pretty.json" } else { "b.json" });
        run(&out_a, pretty, 1);
        run(&out_b, pretty, 4);

        let bytes_a = fs::read(&out_a).unwrap();
        let bytes_b = fs::read(&out_b).unwrap();
        assert_eq!(
            bytes_a, bytes_b,
            "extract_to_json (pretty={pretty}) must be byte-identical across runs"
        );

        let arr: Vec<Value> = serde_json::from_slice(&bytes_a).unwrap();
        let firsts: Vec<&str> = arr
            .iter()
            .map(|v| v["id"].as_str().unwrap())
            .step_by(3)
            .collect();
        assert_eq!(
            firsts,
            vec![
                "RC_2020-01:00",
                "RC_2020-02:00",
                "RC_2020-03:00",
                "RS_2020-01:00",
                "RS_2020-02:00",
                "RS_2020-03:00",
            ],
            "extract_to_json (pretty={pretty}) array elements must follow \
             PathBuf sort order"
        );
    }
}
