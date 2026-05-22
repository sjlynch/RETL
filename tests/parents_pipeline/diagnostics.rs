use crate::common::parents::*;
use crate::common::{make_truncated_zst, write_zst_lines};
use retl::{ParentIds, ParentMaps, RedditETL, Sources, YearMonth};
use std::collections::HashMap;
use std::fs;

#[test]
fn resolve_parent_maps_errors_on_zero_planned_files() {
    let tmp = tempfile::tempdir().unwrap();
    let ym = YearMonth::new(2006, 1);
    let err = match RedditETL::new()
        .base_dir(tmp.path().join("empty_corpus"))
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&ParentIds::new(), &tmp.path().join("cache"), true)
    {
        Ok(_) => panic!("resolve_parent_maps unexpectedly succeeded"),
        Err(err) => err,
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("planned zero corpus files") || msg.contains("no input files found"),
        "unexpected error: {msg}"
    );
}

#[test]
fn resolve_parent_maps_propagates_cache_write_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().join("corpus");
    let work_dir = tmp.path().join("work");
    let cache_dir = tmp.path().join("parents_cache");
    let ym = YearMonth::new(2006, 1);

    write_comment_parent_corpus(&base, ym, &[("p1", "parent body")]);
    let spool = tmp.path().join("spool.jsonl");
    write_parent_ref_spool(&spool, "t1_p1");
    let ids = collect_parent_ids_for_test(&base, &work_dir, &spool);

    // Pre-creating a directory at the cache file destination forces the
    // atomic-publish rename to fail with ERROR_ACCESS_DENIED, which is
    // correctly retriable in production (AV / sharing hiccups). Under
    // `--features test-utils` we cap the budget to 1 try / 0 ms delay so
    // the test surfaces the failure immediately instead of waiting out
    // ~10–20 s of retries. The "write parent shard" assertion is
    // independent of retry timing.
    fs::create_dir_all(cache_dir.join("comments").join("RC_2006-01.json")).unwrap();
    #[cfg(feature = "test-utils")]
    let _backoff_cap = retl::cap_backoff_budget_for_test(1, 0);
    let err = match RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&ids, &cache_dir, false)
    {
        Ok(_) => panic!("resolve_parent_maps unexpectedly succeeded"),
        Err(err) => err,
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("write parent shard"),
        "unexpected error: {msg}"
    );
}

#[test]
fn collect_parent_ids_errors_on_malformed_spool_json_with_location() {
    let tmp = tempfile::tempdir().unwrap();
    let spool = tmp.path().join("spool.jsonl");
    fs::write(
        &spool,
        "{\"id\":\"child\",\"body\":\"child\",\"parent_id\":\"t1_p1\",\"link_id\":\"t3_s1\",\"created_utc\":1136073600}\n{bad json}\n",
    )
    .unwrap();

    let err = match RedditETL::new()
        .work_dir(tmp.path().join("work"))
        .progress(false)
        .collect_parent_ids_from_jsonls(vec![spool.clone()])
    {
        Ok(_) => panic!("collect_parent_ids_from_jsonls unexpectedly succeeded"),
        Err(err) => err,
    };
    let msg = format!("{err:#}");
    assert!(msg.contains("malformed JSON"), "unexpected error: {msg}");
    assert!(
        msg.contains(&spool.display().to_string()),
        "unexpected error: {msg}"
    );
    assert!(msg.contains("line 2"), "unexpected error: {msg}");
}

#[test]
fn resolve_parent_maps_errors_on_malformed_corpus_json_with_location() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().join("corpus");
    let work_dir = tmp.path().join("work");
    let cache_dir = tmp.path().join("parents_cache");
    let ym = YearMonth::new(2006, 1);
    let corpus_path = base.join("comments").join("RC_2006-01.zst");

    write_zst_lines(
        &corpus_path,
        &[
            serde_json::json!({
                "author": "parent_author",
                "body": "parent body",
                "created_utc": 1136073600,
                "id": "p1",
                "score": 1,
                "subreddit": "programming"
            })
            .to_string(),
            "{bad json}".to_string(),
        ],
    );
    let spool = tmp.path().join("spool.jsonl");
    write_parent_ref_spool(&spool, "t1_p1");
    let ids = collect_parent_ids_for_test(&base, &work_dir, &spool);

    let err = match RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .sources(Sources::Comments)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&ids, &cache_dir, true)
    {
        Ok(_) => panic!("resolve_parent_maps unexpectedly succeeded"),
        Err(err) => err,
    };
    let msg = format!("{err:#}");
    assert!(msg.contains("malformed JSON"), "unexpected error: {msg}");
    assert!(
        msg.contains(&corpus_path.display().to_string()),
        "unexpected error: {msg}"
    );
    assert!(msg.contains("line 2"), "unexpected error: {msg}");
    assert!(
        !cache_dir.join("comments").join("RC_2006-01.json").exists(),
        "malformed resolver input must not publish a parent shard"
    );
}

#[test]
fn resolve_parent_maps_errors_on_truncated_corpus_and_does_not_publish_cache() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().join("corpus");
    let work_dir = tmp.path().join("work");
    let cache_dir = tmp.path().join("parents_cache");
    let ym = YearMonth::new(2006, 1);
    let corpus_path = base.join("comments").join("RC_2006-01.zst");

    make_truncated_zst(&corpus_path, 2000, 16);
    let spool = tmp.path().join("spool.jsonl");
    write_parent_ref_spool(&spool, "t1_rec000000");
    let ids = collect_parent_ids_for_test(&base, &work_dir, &spool);

    let err = match RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .sources(Sources::Comments)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&ids, &cache_dir, true)
    {
        Ok(_) => panic!("resolve_parent_maps unexpectedly succeeded"),
        Err(err) => err,
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("incomplete zstd decode") || msg.contains("zstd"),
        "unexpected error: {msg}"
    );
    assert!(
        msg.contains(&corpus_path.display().to_string()),
        "unexpected error: {msg}"
    );

    let shard = cache_dir.join("comments").join("RC_2006-01.json");
    let sidecar = cache_dir
        .join("comments")
        .join("RC_2006-01.json.parents-resolve.json");
    assert!(
        !shard.exists(),
        "truncated resolver input must not publish a parent shard"
    );
    assert!(
        !sidecar.exists(),
        "truncated resolver input must not mark a parent shard complete"
    );
}

#[test]
fn attach_parents_rejects_duplicate_input_basenames() {
    let tmp = tempfile::tempdir().unwrap();
    let dir_a = tmp.path().join("a");
    let dir_b = tmp.path().join("b");
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();
    // Two inputs from different directories sharing one basename would both
    // map to <out_dir>/part_RC_2006-01.jsonl, so the last writer would
    // silently win. The collision must be rejected up front.
    let in_a = dir_a.join("part_RC_2006-01.jsonl");
    let in_b = dir_b.join("part_RC_2006-01.jsonl");
    fs::write(&in_a, "{\"id\":\"a\",\"created_utc\":1136073600}\n").unwrap();
    fs::write(&in_b, "{\"id\":\"b\",\"created_utc\":1136073600}\n").unwrap();

    let parents = empty_parent_maps();
    let err = RedditETL::new()
        .progress(false)
        .attach_parents_jsonls_parallel_with_stats(
            vec![in_a.clone(), in_b.clone()],
            &tmp.path().join("attached"),
            &parents,
            false,
        )
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("collide on basename"),
        "unexpected error: {msg}"
    );
    assert!(
        msg.contains(&in_a.display().to_string()),
        "unexpected error: {msg}"
    );
    assert!(
        msg.contains(&in_b.display().to_string()),
        "unexpected error: {msg}"
    );
}

#[test]
fn attach_parents_counts_unprefixed_parent_id_records_as_unresolved() {
    let tmp = tempfile::tempdir().unwrap();
    let input = tmp.path().join("part_RC_2006-01.jsonl");
    // c1 has a prefixed-but-unresolvable parent_id; c2 has a bare numeric
    // parent_id. Both records claim a parent, so both must be tallied —
    // `total()` would otherwise undercount c2 (silently dropped pre-fix).
    fs::write(
        &input,
        "{\"id\":\"c1\",\"body\":\"x\",\"parent_id\":\"t1_p1\",\"created_utc\":1136073600}\n\
         {\"id\":\"c2\",\"body\":\"y\",\"parent_id\":\"12345\",\"created_utc\":1136073600}\n",
    )
    .unwrap();
    let parents = empty_parent_maps();

    let (_paths, stats) = RedditETL::new()
        .progress(false)
        .attach_parents_jsonls_parallel_with_stats(
            vec![input.clone()],
            &tmp.path().join("attached"),
            &parents,
            false,
        )
        .unwrap();
    assert_eq!(stats.resolved, 0, "no parents are resolvable");
    assert_eq!(
        stats.unresolved, 2,
        "both the prefixed-unresolvable and the bare parent_id record count"
    );
    assert_eq!(stats.total(), 2, "every record claiming a parent is tallied");
}

#[test]
fn attach_parents_reports_corrupt_cache_shard_with_recovery_hint() {
    let tmp = tempfile::tempdir().unwrap();
    let input = tmp.path().join("part_RC_2006-01.jsonl");
    fs::write(
        &input,
        "{\"id\":\"c1\",\"body\":\"x\",\"parent_id\":\"t1_p1\",\"created_utc\":1136073600}\n",
    )
    .unwrap();

    // A truncated/corrupt cached parent shard JSON must fail with an error
    // that names it as cache corruption and gives the delete + --resume
    // recovery, not a bare "parse parent shard" propagation.
    let corrupt_shard = tmp.path().join("RC_2006-01.json");
    fs::write(&corrupt_shard, "{ this is not valid json").unwrap();
    let mut comment_shards = HashMap::new();
    comment_shards.insert(YearMonth::new(2006, 1), corrupt_shard.clone());
    let parents = ParentMaps {
        comments: HashMap::new(),
        submissions: HashMap::new(),
        comment_shards: Some(comment_shards),
        submission_shards: Some(HashMap::new()),
        payload_spec: Default::default(),
    };

    let err = RedditETL::new()
        .progress(false)
        .attach_parents_jsonls_parallel_with_stats(
            vec![input.clone()],
            &tmp.path().join("attached"),
            &parents,
            false,
        )
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("corrupt"), "unexpected error: {msg}");
    assert!(msg.contains("--resume"), "unexpected error: {msg}");
    assert!(
        msg.contains(&corrupt_shard.display().to_string()),
        "unexpected error: {msg}"
    );
}

#[test]
fn attach_parents_errors_on_malformed_spool_json_with_location() {
    let tmp = tempfile::tempdir().unwrap();
    let input = tmp.path().join("part_RC_2006-01.jsonl");
    fs::write(
        &input,
        "{\"id\":\"ok\",\"body\":\"child\",\"parent_id\":\"t1_p1\",\"created_utc\":1136073600}\n{bad json}\n",
    )
    .unwrap();
    let parents = ParentMaps {
        comments: HashMap::new(),
        submissions: HashMap::new(),
        comment_shards: Some(HashMap::new()),
        submission_shards: Some(HashMap::new()),
        payload_spec: Default::default(),
    };

    let err = RedditETL::new()
        .progress(false)
        .attach_parents_jsonls_parallel_with_stats(
            vec![input.clone()],
            &tmp.path().join("attached"),
            &parents,
            false,
        )
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains(&input.display().to_string()),
        "unexpected error: {msg}"
    );
    assert!(msg.contains("line 2"), "unexpected error: {msg}");
}
