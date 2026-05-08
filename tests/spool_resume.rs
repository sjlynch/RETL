//! Resumable `extract_spool_monthly`: a second run reads the sidecar
//! `_progress.json` next to the spool outputs and skips months that the prior
//! run already published.

#[path = "common/mod.rs"]
mod common;

use common::*;
use serde_json::Value;
use std::fs;
use std::path::Path;

use retl::{RedditETL, Sources, YearMonth};

fn spool_run(base: &Path, out_dir: &Path, resume: bool) -> u64 {
    let (_parts, n) = RedditETL::new()
        .base_dir(base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 2)))
        .progress(false)
        .resume(resume)
        .scan()
        .subreddit("programming")
        .extract_spool_monthly(out_dir)
        .unwrap();
    n
}

#[test]
fn resume_skips_already_completed_months_on_second_run() {
    // Two months so we can observe per-month manifest entries.
    let base = make_corpus_multi_month(&[
        YearMonth::new(2006, 1),
        YearMonth::new(2006, 2),
    ]);
    let out_dir = base.join("spool");

    // First run: publishes RC_2006-01, RC_2006-02, RS_2006-01, RS_2006-02 and
    // writes _progress.json with one entry per month.
    let first = spool_run(&base, &out_dir, true);
    assert!(first > 0, "first spool run should have written some records");

    let manifest_path = out_dir.join("_progress.json");
    assert!(manifest_path.exists(), "manifest must exist after first run");
    let manifest: Value = serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    assert_eq!(manifest["version"], 1);
    let months = manifest["months"].as_object().unwrap();
    for k in ["RC_2006-01", "RC_2006-02", "RS_2006-01", "RS_2006-02"] {
        let e = months.get(k).unwrap_or_else(|| panic!("manifest missing key {k}"));
        assert!(e["size"].as_u64().unwrap() > 0, "size must be recorded for {k}");
        assert!(e["lines"].as_u64().unwrap() > 0, "lines must be recorded for {k}");
    }

    // Capture mtimes to detect rewrites on the second run.
    let outputs = [
        "part_RC_2006-01.jsonl",
        "part_RC_2006-02.jsonl",
        "part_RS_2006-01.jsonl",
        "part_RS_2006-02.jsonl",
    ];
    let before_mtimes: Vec<_> = outputs
        .iter()
        .map(|n| fs::metadata(out_dir.join(n)).unwrap().modified().unwrap())
        .collect();

    // Sleep just long enough that any rewrite would produce a measurably
    // different mtime on platforms with low-resolution timestamps.
    std::thread::sleep(std::time::Duration::from_millis(50));

    // Second run with resume=true: every month is already complete, so nothing
    // should be re-written. Returned record count must therefore be zero.
    let second = spool_run(&base, &out_dir, true);
    assert_eq!(
        second, 0,
        "resume run should not re-process any months"
    );

    let after_mtimes: Vec<_> = outputs
        .iter()
        .map(|n| fs::metadata(out_dir.join(n)).unwrap().modified().unwrap())
        .collect();
    for (i, (before, after)) in before_mtimes.iter().zip(after_mtimes.iter()).enumerate() {
        assert_eq!(
            before, after,
            "resume must not rewrite {} (mtime changed)",
            outputs[i]
        );
    }
}

#[test]
fn resume_drops_stale_entry_when_output_size_changes() {
    let base = make_corpus_multi_month(&[
        YearMonth::new(2006, 1),
        YearMonth::new(2006, 2),
    ]);
    let out_dir = base.join("spool_stale");

    // First run primes the manifest.
    let _ = spool_run(&base, &out_dir, true);

    // Tamper with one output: append a byte so size no longer matches the
    // recorded value. The next resume run must invalidate the entry and
    // re-publish that month from scratch (overwriting the tampered file).
    let tampered = out_dir.join("part_RC_2006-01.jsonl");
    let original_size = fs::metadata(&tampered).unwrap().len();
    {
        use std::io::Write;
        let mut f = fs::OpenOptions::new().append(true).open(&tampered).unwrap();
        f.write_all(b"\n").unwrap();
    }
    let bumped_size = fs::metadata(&tampered).unwrap().len();
    assert!(bumped_size > original_size);

    let second = spool_run(&base, &out_dir, true);
    assert!(
        second > 0,
        "tampered month must be re-run, producing some records"
    );

    // After the rerun the size must match the original (clean) output again.
    let restored_size = fs::metadata(&tampered).unwrap().len();
    assert_eq!(
        restored_size, original_size,
        "tampered month must be rewritten from scratch"
    );

    // Manifest must also reflect the restored size.
    let manifest: Value =
        serde_json::from_slice(&fs::read(out_dir.join("_progress.json")).unwrap()).unwrap();
    assert_eq!(
        manifest["months"]["RC_2006-01"]["size"].as_u64().unwrap(),
        restored_size
    );
}

#[test]
fn resume_disabled_does_not_create_manifest() {
    // Default (resume=false) must preserve the prior behavior: no sidecar
    // manifest is written, and a second run re-processes every month.
    let base = make_corpus_multi_month(&[YearMonth::new(2006, 1)]);
    let out_dir = base.join("spool_no_resume");

    let first = spool_run(&base, &out_dir, false);
    assert!(first > 0);
    assert!(
        !out_dir.join("_progress.json").exists(),
        "no manifest should be created when resume is off"
    );

    let second = spool_run(&base, &out_dir, false);
    assert_eq!(
        second, first,
        "without resume, the second run must do the same work as the first"
    );
}
