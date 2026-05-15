#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{RedditETL, Sources, YearMonth};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

fn find_extract_resume_dir(work_dir: &Path) -> PathBuf {
    let mut dirs: Vec<PathBuf> = fs::read_dir(work_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.is_dir()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(|name| name.starts_with("extract_jsonl_q_tmp_"))
                    .unwrap_or(false)
                && path.join("_progress.json").exists()
        })
        .collect();
    dirs.sort();
    assert_eq!(
        dirs.len(),
        1,
        "expected one extract resume dir, got {dirs:?}"
    );
    dirs.pop().unwrap()
}

fn write_marker_corpus(base: &Path, marker: &str) {
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

fn write_dual_subreddit_corpus(base: &Path, months: &[YearMonth], per_subreddit: usize) {
    for ym in months {
        let label = format!("{:04}-{:02}", ym.year, ym.month);
        let mut lines = Vec::with_capacity(per_subreddit * 2);
        for i in 0..per_subreddit {
            lines.push(
                json!({
                    "id": format!("p_{label}_{i}"),
                    "author": format!("programmer_{i}"),
                    "subreddit":"programming",
                    "created_utc":1136073600_i64 + i as i64,
                    "score":1,
                    "body": format!("programming payload {label} {i}"),
                })
                .to_string(),
            );
            lines.push(
                json!({
                    "id": format!("r_{label}_{i}"),
                    "author": format!("rustacean_{i}"),
                    "subreddit":"rust",
                    "created_utc":1136073600_i64 + i as i64,
                    "score":1,
                    "body": format!("rust payload {label} {i}"),
                })
                .to_string(),
            );
        }
        write_zst_lines(
            &base.join("comments").join(format!("RC_{label}.zst")),
            &lines,
        );
    }
    fs::create_dir_all(base.join("submissions")).unwrap();
}

fn has_jsonl_part(dir: &Path) -> bool {
    let Ok(entries) = fs::read_dir(dir) else {
        return false;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if has_jsonl_part(&path) {
                return true;
            }
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.starts_with(".part_") && name.ends_with(".jsonl"))
            .unwrap_or(false)
        {
            return true;
        }
    }
    false
}

fn wait_for_part_before_output(work_dir: &Path, out_path: &Path, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if out_path.exists() {
            return false;
        }
        if has_jsonl_part(work_dir) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    false
}

#[test]
fn jsonl_resume_fingerprint_change_rebuilds_tmp_parts() {
    let base = tempfile::tempdir().unwrap().keep();
    let work_dir = base.join("work_fp");
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

    let out = base.join("out.jsonl");
    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .resume(true)
        .scan()
        .subreddit("programming")
        .extract_to_jsonl(&out)
        .unwrap();
    assert!(fs::read_to_string(&out).unwrap().contains("programming"));

    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .resume(true)
        .scan()
        .subreddit("rust")
        .extract_to_jsonl(&out)
        .unwrap();

    let second = fs::read_to_string(&out).unwrap();
    assert!(
        second.contains("rust"),
        "changed query must rebuild tmp part, got {second}"
    );
    assert!(
        !second.contains("programming"),
        "stale programming part was stitched"
    );
}

#[test]
fn jsonl_resume_rebuilds_when_corpus_file_identity_changes() {
    let months = [YearMonth::new(2006, 1), YearMonth::new(2006, 2)];
    let base = make_corpus_multi_month(&months);
    let work_dir = base.join("work");
    let out1 = base.join("first.jsonl");

    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .sources(Sources::Both)
        .date_range(Some(months[0]), Some(months[1]))
        .progress(false)
        .resume(true)
        .scan()
        .subreddit("programming")
        .extract_to_jsonl(&out1)
        .unwrap();

    let first = fs::read_to_string(&out1).unwrap();
    assert_eq!(read_jsonl_values(&out1).len(), 8);

    let tmp_dir = find_extract_resume_dir(&work_dir);
    assert!(tmp_dir.join("_progress.json").exists());
    for key in ["RC_2006-01", "RS_2006-01", "RC_2006-02", "RS_2006-02"] {
        assert!(
            tmp_dir.join(format!(".part_{key}.jsonl")).exists(),
            "missing resume part {key}"
        );
    }

    // Resume fingerprints now include selected corpus file identity. If the
    // corpus changes after a checkpoint is written, RETL must not stitch stale
    // parts; it should rebuild and surface the decode error from the changed
    // sources instead.
    for subdir in ["comments", "submissions"] {
        for entry in fs::read_dir(base.join(subdir)).unwrap() {
            fs::write(entry.unwrap().path(), b"not a zstd frame").unwrap();
        }
    }

    let err = RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .sources(Sources::Both)
        .date_range(Some(months[0]), Some(months[1]))
        .progress(false)
        .resume(true)
        .scan()
        .subreddit("programming")
        .extract_to_jsonl(&out1)
        .expect_err("changed corpus identity should force a rebuild and fail on corrupt sources");

    let msg = format!("{err:#}");
    assert!(msg.contains("zstd decode error"), "unexpected error: {msg}");
    assert_eq!(fs::read_to_string(&out1).unwrap(), first);
}

#[test]
fn contains_url_false_reuses_default_resume_fingerprint() {
    let base = tempfile::tempdir().unwrap().keep();
    let work_dir = base.join("work_contains_url_false");
    write_marker_corpus(&base, "default_url_filter");

    let out = base.join("out.jsonl");
    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .resume(true)
        .scan()
        .extract_to_jsonl(&out)
        .unwrap();
    let first = fs::read_to_string(&out).unwrap();
    assert!(first.contains("default_url_filter"));

    let first_tmp_dir = find_extract_resume_dir(&work_dir);

    // `.contains_url(false)` should normalize to the same query fingerprint as
    // omitting the URL filter, so the second run should reuse the same resume
    // directory rather than creating a fresh checkpoint namespace.
    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .resume(true)
        .scan()
        .contains_url(false)
        .extract_to_jsonl(&out)
        .unwrap();

    assert_eq!(find_extract_resume_dir(&work_dir), first_tmp_dir);
    assert_eq!(fs::read_to_string(&out).unwrap(), first);
}

#[test]
fn jsonl_resume_fingerprint_includes_corpus_paths() {
    let root = tempfile::tempdir().unwrap().keep();
    let corpus_a = root.join("corpus_a");
    let corpus_b = root.join("corpus_b");
    write_marker_corpus(&corpus_a, "from_corpus_a");
    write_marker_corpus(&corpus_b, "from_corpus_b");

    let work_dir = root.join("work");
    let out = root.join("out.jsonl");

    RedditETL::new()
        .base_dir(&corpus_a)
        .work_dir(&work_dir)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .resume(true)
        .scan()
        .subreddit("programming")
        .extract_to_jsonl(&out)
        .unwrap();
    assert!(fs::read_to_string(&out).unwrap().contains("from_corpus_a"));

    RedditETL::new()
        .base_dir(&corpus_b)
        .work_dir(&work_dir)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .resume(true)
        .scan()
        .subreddit("programming")
        .extract_to_jsonl(&out)
        .unwrap();

    let second = fs::read_to_string(&out).unwrap();
    assert!(
        second.contains("from_corpus_b"),
        "corpus path change must rebuild extract parts, got {second}"
    );
    assert!(
        !second.contains("from_corpus_a"),
        "stale extract part from corpus A was reused"
    );
}

#[test]
fn concurrent_jsonl_extracts_share_work_dir_without_interference() {
    let root = tempfile::tempdir().unwrap().keep();
    let base = root.join("corpus");
    let months: Vec<_> = (1..=12).map(|month| YearMonth::new(2006, month)).collect();
    let per_subreddit = 1_000;
    write_dual_subreddit_corpus(&base, &months, per_subreddit);

    let work_dir = root.join("work");
    let programming_out = root.join("programming.jsonl");
    let rust_out = root.join("rust.jsonl");

    let first_base = base.clone();
    let first_work = work_dir.clone();
    let first_out = programming_out.clone();
    let first = std::thread::spawn(move || {
        RedditETL::new()
            .base_dir(&first_base)
            .work_dir(&first_work)
            .sources(Sources::Comments)
            .progress(false)
            .scan()
            .subreddit("programming")
            .extract_to_jsonl(&first_out)
    });

    assert!(
        wait_for_part_before_output(&work_dir, &programming_out, Duration::from_secs(10)),
        "first extract finished before the test could overlap the second extract"
    );

    let second_base = base.clone();
    let second_work = work_dir.clone();
    let second_out = rust_out.clone();
    let second = std::thread::spawn(move || {
        RedditETL::new()
            .base_dir(&second_base)
            .work_dir(&second_work)
            .sources(Sources::Comments)
            .progress(false)
            .scan()
            .subreddit("rust")
            .extract_to_jsonl(&second_out)
    });

    first.join().unwrap().unwrap();
    second.join().unwrap().unwrap();

    let programming = read_jsonl_values(&programming_out);
    let rust = read_jsonl_values(&rust_out);
    let expected = months.len() * per_subreddit;
    assert_eq!(programming.len(), expected, "programming output incomplete");
    assert_eq!(rust.len(), expected, "rust output incomplete");
    assert!(programming
        .iter()
        .all(|record| record["subreddit"] == "programming"));
    assert!(rust.iter().all(|record| record["subreddit"] == "rust"));
}
