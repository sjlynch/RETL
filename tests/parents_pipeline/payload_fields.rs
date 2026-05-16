use crate::common::parents::{read_comment_payload_cache, write_comment_parent_corpus};
use crate::common::{make_corpus_basic, read_jsonl_values};
use retl::{ParentIds, ParentPayloadSpec, RedditETL, YearMonth};
use std::fs;

#[test]
fn configured_parent_fields_are_attached_under_parent() {
    let base = make_corpus_basic();
    let work_dir = base.join("work_parent_fields");
    let ym = YearMonth::new(2006, 1);
    let spool_dir = work_dir.join("spool");
    fs::create_dir_all(&spool_dir).unwrap();
    let spool = spool_dir.join("part_RC_2006-01.jsonl");
    fs::write(
        &spool,
        concat!(
            "{\"id\":\"child_comment\",\"body\":\"child\",\"parent_id\":\"t1_c1\",\"created_utc\":1136074700}\n",
            "{\"id\":\"child_submission\",\"body\":\"child\",\"parent_id\":\"t3_s1\",\"created_utc\":1136074700}\n"
        ),
    )
    .unwrap();

    let mut ids = ParentIds::new();
    ids.extend_prefixed(["t1_c1", "t3_s1"]);
    let parents = RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .parent_fields([
            "author",
            "body",
            "created_utc",
            "domain",
            "score",
            "subreddit",
            "title",
            "url",
        ])
        .resolve_parent_maps(&ids, &work_dir.join("cache"), false)
        .unwrap();

    let (attached, stats) = RedditETL::new()
        .progress(false)
        .attach_parents_jsonls_parallel_with_stats(
            vec![spool],
            &work_dir.join("attached"),
            &parents,
            false,
        )
        .unwrap();
    assert_eq!(stats.resolved, 2);
    assert_eq!(stats.unresolved, 0);

    let values = read_jsonl_values(&attached[0]);
    let comment_parent = values[0].get("parent").unwrap();
    assert_eq!(
        comment_parent.pointer("/kind").and_then(|v| v.as_str()),
        Some("comment")
    );
    assert_eq!(
        comment_parent.pointer("/id").and_then(|v| v.as_str()),
        Some("c1")
    );
    assert_eq!(
        comment_parent.pointer("/author").and_then(|v| v.as_str()),
        Some("alice")
    );
    assert_eq!(
        comment_parent.pointer("/body").and_then(|v| v.as_str()),
        Some("I love Rust http://rust-lang.org")
    );
    assert_eq!(
        comment_parent.pointer("/score").and_then(|v| v.as_i64()),
        Some(2)
    );
    assert_eq!(
        comment_parent
            .pointer("/created_utc")
            .and_then(|v| v.as_i64()),
        Some(1136074600)
    );
    assert!(comment_parent.get("title").is_none());

    let submission_parent = values[1].get("parent").unwrap();
    assert_eq!(
        submission_parent.pointer("/kind").and_then(|v| v.as_str()),
        Some("submission")
    );
    assert_eq!(
        submission_parent.pointer("/id").and_then(|v| v.as_str()),
        Some("s1")
    );
    assert_eq!(
        submission_parent
            .pointer("/author")
            .and_then(|v| v.as_str()),
        Some("bob")
    );
    assert_eq!(
        submission_parent.pointer("/title").and_then(|v| v.as_str()),
        Some("Rust news")
    );
    assert_eq!(
        submission_parent
            .pointer("/domain")
            .and_then(|v| v.as_str()),
        Some("example.com")
    );
    assert_eq!(
        submission_parent.pointer("/url").and_then(|v| v.as_str()),
        Some("http://example.com/x")
    );
}

#[test]
fn resolve_resume_rebuilds_when_parent_fields_change() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().join("corpus");
    let work_dir = tmp.path().join("work");
    let cache_dir = tmp.path().join("parents_cache");
    let ym = YearMonth::new(2006, 1);

    write_comment_parent_corpus(&base, ym, &[("p1", "parent body")]);
    let mut ids = ParentIds::new();
    assert!(ids.insert_t1("p1"));

    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .parent_fields(["body"])
        .resolve_parent_maps(&ids, &cache_dir, true)
        .unwrap();
    let narrow = read_comment_payload_cache(&cache_dir, ym);
    assert_eq!(
        narrow["p1"].get("body").and_then(|v| v.as_str()),
        Some("parent body")
    );
    assert!(narrow["p1"].get("author").is_none());

    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .parent_fields(["author", "body", "score"])
        .resolve_parent_maps(&ids, &cache_dir, true)
        .unwrap();
    let wide = read_comment_payload_cache(&cache_dir, ym);
    assert_eq!(
        wide["p1"].get("author").and_then(|v| v.as_str()),
        Some("parent_author")
    );
    assert_eq!(wide["p1"].get("score").and_then(|v| v.as_i64()), Some(1));
}

#[test]
fn parent_payload_spec_full_record_ignores_fields() {
    let spec = ParentPayloadSpec::from_fields(["author", "body"]).with_full_record(true);
    assert!(spec.is_full_record());
    assert!(spec.fields().is_empty());
}
