use crate::common::parents::*;
use retl::{RedditETL, YearMonth};

#[test]
fn resolve_resume_rebuilds_when_parent_ids_change() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().join("corpus");
    let work_dir = tmp.path().join("work");
    let cache_dir = tmp.path().join("parents_cache");
    let ym = YearMonth::new(2006, 1);

    write_comment_parent_corpus(&base, ym, &[("p1", "parent one"), ("p2", "parent two")]);
    let spool1 = tmp.path().join("spool1.jsonl");
    let spool2 = tmp.path().join("spool2.jsonl");
    write_parent_ref_spool(&spool1, "t1_p1");
    write_parent_ref_spool(&spool2, "t1_p2");

    let ids1 = collect_parent_ids_for_test(&base, &work_dir, &spool1);
    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&ids1, &cache_dir, true)
        .unwrap();
    let first = read_comment_cache(&cache_dir, ym);
    assert_eq!(first.get("p1").map(String::as_str), Some("parent one"));
    assert!(!first.contains_key("p2"));

    let ids2 = collect_parent_ids_for_test(&base, &work_dir, &spool2);
    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&ids2, &cache_dir, true)
        .unwrap();
    let second = read_comment_cache(&cache_dir, ym);
    assert_eq!(second.get("p2").map(String::as_str), Some("parent two"));
    assert!(
        !second.contains_key("p1"),
        "resume reused a parent-cache shard from the prior ParentIds"
    );
}

#[test]
fn resolve_resume_rebuilds_when_corpus_path_changes() {
    let tmp = tempfile::tempdir().unwrap();
    let base1 = tmp.path().join("corpus1");
    let base2 = tmp.path().join("corpus2");
    let work_dir = tmp.path().join("work");
    let cache_dir = tmp.path().join("parents_cache");
    let ym = YearMonth::new(2006, 1);

    write_comment_parent_corpus(&base1, ym, &[("p1", "old corpus body")]);
    write_comment_parent_corpus(&base2, ym, &[("p1", "new corpus body")]);
    let spool = tmp.path().join("spool.jsonl");
    write_parent_ref_spool(&spool, "t1_p1");
    let ids = collect_parent_ids_for_test(&base1, &work_dir, &spool);

    RedditETL::new()
        .base_dir(&base1)
        .work_dir(&work_dir)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&ids, &cache_dir, true)
        .unwrap();
    assert_eq!(
        read_comment_cache(&cache_dir, ym)
            .get("p1")
            .map(String::as_str),
        Some("old corpus body")
    );

    RedditETL::new()
        .base_dir(&base2)
        .work_dir(&work_dir)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&ids, &cache_dir, true)
        .unwrap();
    assert_eq!(
        read_comment_cache(&cache_dir, ym)
            .get("p1")
            .map(String::as_str),
        Some("new corpus body"),
        "resume reused a parent-cache shard from a different corpus path"
    );
}
