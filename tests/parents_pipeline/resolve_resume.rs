use crate::common::parents::*;
use retl::{RedditETL, YearMonth};
use std::fs;

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
fn resolve_resume_prunes_shards_outside_a_narrowed_window() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().join("corpus");
    let work_dir = tmp.path().join("work");
    let cache_dir = tmp.path().join("parents_cache");
    let jan = YearMonth::new(2006, 1);
    let feb = YearMonth::new(2006, 2);

    write_comment_parent_corpus(&base, jan, &[("p1", "january parent")]);
    write_comment_parent_corpus(&base, feb, &[("p2", "february parent")]);

    let spool = tmp.path().join("spool.jsonl");
    fs::write(
        &spool,
        "{\"id\":\"c1\",\"body\":\"child\",\"parent_id\":\"t1_p1\",\"created_utc\":1136073600}\n\
         {\"id\":\"c2\",\"body\":\"child\",\"parent_id\":\"t1_p2\",\"created_utc\":1138752000}\n",
    )
    .unwrap();
    let ids = collect_parent_ids_for_test(&base, &work_dir, &spool);

    // Wide window resolves both months into the cache.
    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .date_range(Some(jan), Some(feb))
        .progress(false)
        .resolve_parent_maps(&ids, &cache_dir, true)
        .unwrap();
    let jan_shard = cache_dir.join("comments").join("RC_2006-01.json");
    let feb_shard = cache_dir.join("comments").join("RC_2006-02.json");
    let feb_sidecar = cache_dir
        .join("comments")
        .join("RC_2006-02.json.parents-resolve.json");
    assert!(jan_shard.exists());
    assert!(feb_shard.exists());
    assert!(feb_sidecar.exists());

    // Re-running with a narrowed window must prune the now-out-of-window
    // February shard (and its resolver sidecar) instead of leaving it on
    // disk where it could leak into resolution.
    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .date_range(Some(jan), Some(jan))
        .progress(false)
        .resolve_parent_maps(&ids, &cache_dir, true)
        .unwrap();
    assert!(
        jan_shard.exists(),
        "in-window shard must survive a narrowed re-run"
    );
    assert!(
        !feb_shard.exists(),
        "out-of-window shard must be pruned on a narrowed re-run"
    );
    assert!(
        !feb_sidecar.exists(),
        "out-of-window resolver sidecar must be pruned alongside its shard"
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
