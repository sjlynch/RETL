use crate::common::make_corpus_multi_month;
use crate::common::spool::{assert_manifest_months, spool_part_names};
use retl::{RedditETL, Sources, YearMonth};
use std::fs;

#[test]
fn non_resume_spool_rerun_prunes_stale_owned_parts() {
    let jan = YearMonth::new(2006, 1);
    let feb = YearMonth::new(2006, 2);
    let base = make_corpus_multi_month(&[jan, feb]);
    let out_dir = base.join("spool_reuse_cleanup");

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(jan), Some(feb))
        .progress(false)
        .scan()
        .subreddit("programming")
        .extract_spool_monthly(&out_dir)
        .unwrap();
    assert!(spool_part_names(&out_dir).contains(&"part_RS_2006-02.jsonl".to_string()));
    fs::write(out_dir.join("notes.txt"), "do not delete").unwrap();

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(jan), Some(jan))
        .progress(false)
        .scan()
        .subreddit("programming")
        .extract_spool_monthly(&out_dir)
        .unwrap();

    assert_eq!(spool_part_names(&out_dir), vec!["part_RC_2006-01.jsonl"]);
    assert_eq!(
        fs::read_to_string(out_dir.join("notes.txt")).unwrap(),
        "do not delete"
    );
}

#[test]
fn resume_spool_without_manifest_prunes_untracked_owned_parts() {
    let jan = YearMonth::new(2006, 1);
    let feb = YearMonth::new(2006, 2);
    let base = make_corpus_multi_month(&[jan, feb]);
    let out_dir = base.join("spool_resume_missing_manifest_cleanup");

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(jan), Some(feb))
        .progress(false)
        .scan()
        .subreddit("programming")
        .extract_spool_monthly(&out_dir)
        .unwrap();
    assert!(!out_dir.join("_progress.json").exists());
    assert!(out_dir.join("part_RS_2006-02.jsonl").exists());

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(jan), Some(jan))
        .progress(false)
        .resume(true)
        .scan()
        .subreddit("programming")
        .extract_spool_monthly(&out_dir)
        .unwrap();

    assert_eq!(spool_part_names(&out_dir), vec!["part_RC_2006-01.jsonl"]);
    assert_manifest_months(&out_dir, &["RC_2006-01"]);
}
