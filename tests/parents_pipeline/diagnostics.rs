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
