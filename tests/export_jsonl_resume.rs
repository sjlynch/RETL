#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{RedditETL, Sources, YearMonth};
use std::fs;

#[test]
fn jsonl_resume_reuses_valid_month_parts_without_reading_sources() {
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

    let tmp_dir = work_dir.join("extract_jsonl_q_tmp");
    assert!(tmp_dir.join("_progress.json").exists());
    for key in ["RC_2006-01", "RS_2006-01", "RC_2006-02", "RS_2006-02"] {
        assert!(
            tmp_dir.join(format!(".part_{key}.jsonl")).exists(),
            "missing resume part {key}"
        );
    }

    // If resume fails to skip the completed parts, these invalid sources will
    // make the second extraction fail during zstd decode. A successful second
    // run therefore proves it stitched the validated checkpoint parts instead.
    for subdir in ["comments", "submissions"] {
        for entry in fs::read_dir(base.join(subdir)).unwrap() {
            fs::write(entry.unwrap().path(), b"not a zstd frame").unwrap();
        }
    }

    let out2 = base.join("second.jsonl");
    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .sources(Sources::Both)
        .date_range(Some(months[0]), Some(months[1]))
        .progress(false)
        .resume(true)
        .scan()
        .subreddit("programming")
        .extract_to_jsonl(&out2)
        .unwrap();

    assert_eq!(fs::read_to_string(&out2).unwrap(), first);
}
