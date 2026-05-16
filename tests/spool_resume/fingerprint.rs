use crate::common::spool::{
    make_over18_submission_corpus, make_two_subreddit_comment_corpus, write_marker_comment_corpus,
};
use retl::{RedditETL, Sources, YearMonth};
use std::fs;

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
