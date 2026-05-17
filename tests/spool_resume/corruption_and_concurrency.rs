use crate::common::{make_corpus_multi_month, make_truncated_zst, write_zst_lines};
use retl::{RedditETL, Sources, YearMonth};
use serde_json::{json, Value};
use std::fs;

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
    // 6 months × 2 sources = 12 monthly shards. With file_concurrency(8) all
    // 8 worker slots stay saturated for most of the run, which is what
    // exercises the concurrent manifest-commit codepath. The original 12
    // months / 24 shards didn't strengthen the invariant (the manifest must
    // still record *every* shard regardless of count) but doubled the wall
    // time of this test.
    let months: Vec<_> = (1..=6).map(|m| YearMonth::new(2006, m)).collect();
    let base = make_corpus_multi_month(&months);
    let out_dir = base.join("spool_concurrent_manifest");

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 6)))
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
    assert_eq!(months_obj.len(), 12, "all RC/RS months must be committed");
}
