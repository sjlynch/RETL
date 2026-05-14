//! Resumable `extract_spool_monthly`: a second run reads the sidecar
//! `_progress.json` next to the spool outputs and skips months that the prior
//! run already published.

#[path = "common/mod.rs"]
mod common;

use common::*;
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};

use retl::{RedditETL, Sources, YearMonth};

fn make_two_subreddit_comment_corpus() -> PathBuf {
    let base = tempfile::tempdir().unwrap().keep();
    let rc_path = base.join("comments").join("RC_2006-01.zst");
    let lines = vec![
        json!({
            "id":"p1", "author":"alice", "subreddit":"programming",
            "created_utc":1136073600_i64, "score":1, "body":"programming"
        })
        .to_string(),
        json!({
            "id":"r1", "author":"ferris", "subreddit":"rust",
            "created_utc":1136073601_i64, "score":1, "body":"rust"
        })
        .to_string(),
    ];
    write_zst_lines(&rc_path, &lines);
    fs::create_dir_all(base.join("submissions")).unwrap();
    base
}

fn write_marker_comment_corpus(base: &Path, marker: &str) {
    let rc_path = base.join("comments").join("RC_2006-01.zst");
    let lines = vec![json!({
        "id": marker,
        "author":"alice",
        "subreddit":"programming",
        "created_utc":1136073600_i64,
        "score":1,
        "body": marker,
    })
    .to_string()];
    write_zst_lines(&rc_path, &lines);
    fs::create_dir_all(base.join("submissions")).unwrap();
}

fn make_over18_submission_corpus() -> PathBuf {
    let base = tempfile::tempdir().unwrap().keep();
    let rs_path = base.join("submissions").join("RS_2006-01.zst");
    let lines = vec![
        json!({
            "id":"safe", "author":"alice", "subreddit":"programming",
            "created_utc":1136073600_i64, "score":1, "domain":"example.com",
            "title":"safe", "selftext":"", "over_18": false, "is_self": true,
            "num_comments": 2
        })
        .to_string(),
        json!({
            "id":"nsfw", "author":"alice", "subreddit":"programming",
            "created_utc":1136073601_i64, "score":1, "domain":"example.com",
            "title":"nsfw", "selftext":"", "over_18": true, "is_self": true,
            "num_comments": 3
        })
        .to_string(),
    ];
    write_zst_lines(&rs_path, &lines);
    fs::create_dir_all(base.join("comments")).unwrap();
    base
}

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
    let base = make_corpus_multi_month(&[YearMonth::new(2006, 1), YearMonth::new(2006, 2)]);
    let out_dir = base.join("spool");

    // First run: publishes RC_2006-01, RC_2006-02, RS_2006-01, RS_2006-02 and
    // writes _progress.json with one entry per month.
    let first = spool_run(&base, &out_dir, true);
    assert!(
        first > 0,
        "first spool run should have written some records"
    );

    let manifest_path = out_dir.join("_progress.json");
    assert!(
        manifest_path.exists(),
        "manifest must exist after first run"
    );
    let manifest: Value = serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    assert_eq!(manifest["version"], 1);
    let months = manifest["months"].as_object().unwrap();
    for k in ["RC_2006-01", "RC_2006-02", "RS_2006-01", "RS_2006-02"] {
        let e = months
            .get(k)
            .unwrap_or_else(|| panic!("manifest missing key {k}"));
        assert!(
            e["size"].as_u64().unwrap() > 0,
            "size must be recorded for {k}"
        );
        assert!(
            e["lines"].as_u64().unwrap() > 0,
            "lines must be recorded for {k}"
        );
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
    assert_eq!(second, 0, "resume run should not re-process any months");

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
    let base = make_corpus_multi_month(&[YearMonth::new(2006, 1), YearMonth::new(2006, 2)]);
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
fn resume_fingerprint_change_rebuilds_spool_parts() {
    let base = make_two_subreddit_comment_corpus();
    let out_dir = base.join("spool_fingerprint");

    let first = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .resume(true)
        .scan()
        .subreddit("programming")
        .extract_spool_monthly(&out_dir)
        .unwrap()
        .1;
    assert_eq!(first, 1);
    let part = out_dir.join("part_RC_2006-01.jsonl");
    assert!(fs::read_to_string(&part).unwrap().contains("programming"));

    let second = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .resume(true)
        .scan()
        .subreddit("rust")
        .extract_spool_monthly(&out_dir)
        .unwrap()
        .1;
    assert_eq!(
        second, 1,
        "changed query must reprocess instead of resume-skipping"
    );
    let rewritten = fs::read_to_string(&part).unwrap();
    assert!(
        rewritten.contains("rust"),
        "expected rebuilt rust part, got {rewritten}"
    );
    assert!(
        !rewritten.contains("programming"),
        "stale programming record was reused"
    );
}

#[test]
fn resume_fingerprint_changes_when_json_pointer_predicates_change() {
    let base = make_over18_submission_corpus();
    let out_dir = base.join("spool_json_fingerprint");

    let first = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Submissions)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .resume(true)
        .scan()
        .json_eq("/over_18", false)
        .extract_spool_monthly(&out_dir)
        .unwrap()
        .1;
    assert_eq!(first, 1);
    let part = out_dir.join("part_RS_2006-01.jsonl");
    assert!(fs::read_to_string(&part).unwrap().contains("safe"));

    let second = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Submissions)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .resume(true)
        .scan()
        .json_eq("/over_18", true)
        .extract_spool_monthly(&out_dir)
        .unwrap()
        .1;
    assert_eq!(second, 1, "changed JSON predicate must reprocess the month");
    let rewritten = fs::read_to_string(&part).unwrap();
    assert!(
        rewritten.contains("nsfw"),
        "expected rebuilt nsfw part, got {rewritten}"
    );
    assert!(
        !rewritten.contains("safe"),
        "stale JSON-filtered part was reused"
    );
}

#[test]
fn resume_fingerprint_includes_corpus_paths() {
    let root = tempfile::tempdir().unwrap().keep();
    let corpus_a = root.join("corpus_a");
    let corpus_b = root.join("corpus_b");
    write_marker_comment_corpus(&corpus_a, "from_corpus_a");
    write_marker_comment_corpus(&corpus_b, "from_corpus_b");
    let out_dir = root.join("spool_corpus_fingerprint");

    let first = RedditETL::new()
        .base_dir(&corpus_a)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .resume(true)
        .scan()
        .subreddit("programming")
        .extract_spool_monthly(&out_dir)
        .unwrap()
        .1;
    assert_eq!(first, 1);
    let part = out_dir.join("part_RC_2006-01.jsonl");
    assert!(fs::read_to_string(&part).unwrap().contains("from_corpus_a"));

    let second = RedditETL::new()
        .base_dir(&corpus_b)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .resume(true)
        .scan()
        .subreddit("programming")
        .extract_spool_monthly(&out_dir)
        .unwrap()
        .1;
    assert_eq!(second, 1, "corpus path change must reprocess the month");
    let rewritten = fs::read_to_string(&part).unwrap();
    assert!(
        rewritten.contains("from_corpus_b"),
        "expected corpus B part, got {rewritten}"
    );
    assert!(
        !rewritten.contains("from_corpus_a"),
        "stale corpus A spool part was reused"
    );
}

#[test]
fn corrupt_zstd_month_is_not_recorded_complete_and_resume_retries_after_repair() {
    let base = tempfile::tempdir().unwrap().keep();
    let rc_path = base.join("comments").join("RC_2006-01.zst");
    make_truncated_zst(&rc_path, 500, 32);
    fs::create_dir_all(base.join("submissions")).unwrap();
    let out_dir = base.join("spool_corrupt");

    let first = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .resume(true)
        .allow_partial(true)
        .scan()
        .subreddit("programming")
        .extract_spool_monthly(&out_dir)
        .unwrap()
        .1;
    assert_eq!(first, 0, "corrupt month must not be counted as complete");
    let manifest: Value =
        serde_json::from_slice(&fs::read(out_dir.join("_progress.json")).unwrap()).unwrap();
    assert!(manifest["months"]
        .as_object()
        .unwrap()
        .get("RC_2006-01")
        .is_none());

    let repaired = vec![json!({
        "id":"ok", "author":"alice", "subreddit":"programming",
        "created_utc":1136073600_i64, "score":1, "body":"repaired"
    })
    .to_string()];
    write_zst_lines(&rc_path, &repaired);

    let second = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .resume(true)
        .scan()
        .subreddit("programming")
        .extract_spool_monthly(&out_dir)
        .unwrap()
        .1;
    assert_eq!(second, 1, "resume must retry the previously corrupt month");
    let manifest: Value =
        serde_json::from_slice(&fs::read(out_dir.join("_progress.json")).unwrap()).unwrap();
    assert!(manifest["months"]
        .as_object()
        .unwrap()
        .get("RC_2006-01")
        .is_some());
}

#[test]
fn concurrent_manifest_commits_record_all_months() {
    let months: Vec<_> = (1..=12).map(|m| YearMonth::new(2006, m)).collect();
    let base = make_corpus_multi_month(&months);
    let out_dir = base.join("spool_concurrent_manifest");

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(
            Some(YearMonth::new(2006, 1)),
            Some(YearMonth::new(2006, 12)),
        )
        .progress(false)
        .file_concurrency(8)
        .resume(true)
        .scan()
        .subreddit("programming")
        .extract_spool_monthly(&out_dir)
        .unwrap();

    let manifest: Value =
        serde_json::from_slice(&fs::read(out_dir.join("_progress.json")).unwrap()).unwrap();
    let months_obj = manifest["months"].as_object().unwrap();
    assert_eq!(months_obj.len(), 24, "all RC/RS months must be committed");
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
