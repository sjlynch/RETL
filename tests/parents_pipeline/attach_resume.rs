use crate::common::write_zst_lines;
use retl::{ParentAttachStats, ParentMaps, RedditETL, Sources, YearMonth};
use std::collections::HashMap;
use std::fs;

#[test]
fn parent_attach_stats_unresolved_rate_boundaries() {
    assert_eq!(ParentAttachStats::default().total(), 0);
    assert_eq!(ParentAttachStats::default().unresolved_rate(), 0.0);

    let all_resolved = ParentAttachStats {
        resolved: 20,
        unresolved: 0,
    };
    assert_eq!(all_resolved.total(), 20);
    assert_eq!(all_resolved.unresolved_rate(), 0.0);

    let threshold = ParentAttachStats {
        resolved: 95,
        unresolved: 5,
    };
    assert_eq!(threshold.total(), 100);
    assert!((threshold.unresolved_rate() - 0.05).abs() < f64::EPSILON);

    let high = ParentAttachStats {
        resolved: 1,
        unresolved: 1,
    };
    assert_eq!(high.total(), 2);
    assert_eq!(high.unresolved_rate(), 0.5);
}

#[test]
fn unresolved_parent_is_absent_and_counted() {
    let tmp = tempfile::tempdir().unwrap();
    let input = tmp.path().join("part_RC_2006-01.jsonl");
    fs::write(
        &input,
        r#"{"id":"c_missing","body":"child","parent_id":"t1_missing","created_utc":1136073600}"#,
    )
    .unwrap();

    let parents = ParentMaps {
        comments: HashMap::new(),
        submissions: HashMap::new(),
        comment_shards: Some(HashMap::new()),
        submission_shards: Some(HashMap::new()),
        payload_spec: Default::default(),
    };

    let (attached_paths, stats) = RedditETL::new()
        .progress(false)
        .attach_parents_jsonls_parallel_with_stats(
            vec![input],
            &tmp.path().join("attached"),
            &parents,
            false,
        )
        .unwrap();

    assert_eq!(
        stats,
        ParentAttachStats {
            resolved: 0,
            unresolved: 1,
        }
    );
    let line = fs::read_to_string(&attached_paths[0]).unwrap();
    let v: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert!(
        v.get("parent").is_none(),
        "unresolved parent must be absent"
    );
}

#[test]
fn attach_parents_rerun_prunes_stale_owned_parts_when_inputs_shrink() {
    let tmp = tempfile::tempdir().unwrap();
    let spool_dir = tmp.path().join("spool");
    fs::create_dir_all(&spool_dir).unwrap();
    let jan = spool_dir.join("part_RC_2006-01.jsonl");
    let feb = spool_dir.join("part_RC_2006-02.jsonl");
    fs::write(
        &jan,
        "{\"id\":\"c1\",\"body\":\"child\",\"parent_id\":\"t1_missing\",\"created_utc\":1136073600}\n",
    )
    .unwrap();
    fs::write(
        &feb,
        "{\"id\":\"c2\",\"body\":\"child\",\"parent_id\":\"t1_missing\",\"created_utc\":1138752000}\n",
    )
    .unwrap();

    let parents = ParentMaps {
        comments: HashMap::new(),
        submissions: HashMap::new(),
        comment_shards: Some(HashMap::new()),
        submission_shards: Some(HashMap::new()),
        payload_spec: Default::default(),
    };
    let out_dir = tmp.path().join("attached");

    let (_paths, first_stats) = RedditETL::new()
        .progress(false)
        .attach_parents_jsonls_parallel_with_stats(
            vec![jan.clone(), feb.clone()],
            &out_dir,
            &parents,
            false,
        )
        .unwrap();
    assert_eq!(first_stats.unresolved, 2);
    assert!(out_dir.join("part_RC_2006-02.jsonl").exists());
    assert!(out_dir
        .join("part_RC_2006-02.jsonl.parents-attach.json")
        .exists());
    fs::write(out_dir.join("notes.txt"), "do not delete").unwrap();

    let (paths, second_stats) = RedditETL::new()
        .progress(false)
        .attach_parents_jsonls_parallel_with_stats(vec![jan.clone()], &out_dir, &parents, false)
        .unwrap();

    assert_eq!(paths, vec![out_dir.join("part_RC_2006-01.jsonl")]);
    assert_eq!(second_stats.unresolved, 1);
    assert!(out_dir.join("part_RC_2006-01.jsonl").exists());
    assert!(!out_dir.join("part_RC_2006-02.jsonl").exists());
    assert!(!out_dir
        .join("part_RC_2006-02.jsonl.parents-attach.json")
        .exists());
    assert_eq!(
        fs::read_to_string(out_dir.join("notes.txt")).unwrap(),
        "do not delete"
    );
}

#[test]
fn attach_parents_resume_rebuilds_corrupt_published_output() {
    let tmp = tempfile::tempdir().unwrap();
    let input = tmp.path().join("part_RC_2006-01.jsonl");
    fs::write(
        &input,
        "{\"id\":\"c1\",\"body\":\"child\",\"parent_id\":\"t1_p1\",\"created_utc\":1136073600}\n",
    )
    .unwrap();

    let out_dir = tmp.path().join("attached");
    fs::create_dir_all(&out_dir).unwrap();
    fs::write(out_dir.join("part_RC_2006-01.jsonl"), "{\"id\":").unwrap();

    let mut comments = HashMap::new();
    comments.insert("p1".to_string(), "parent body".to_string());
    let parents = ParentMaps {
        comments,
        submissions: HashMap::new(),
        comment_shards: Some(HashMap::new()),
        submission_shards: Some(HashMap::new()),
        payload_spec: Default::default(),
    };

    let (attached_paths, stats) = RedditETL::new()
        .progress(false)
        .attach_parents_jsonls_parallel_with_stats(vec![input], &out_dir, &parents, true)
        .unwrap();

    assert_eq!(stats.resolved, 1);
    assert_eq!(stats.unresolved, 0);
    let line = fs::read_to_string(&attached_paths[0]).unwrap();
    let v: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(
        v.pointer("/parent/body").and_then(|v| v.as_str()),
        Some("parent body")
    );
}

#[test]
fn attach_resume_rebuilds_when_resolution_window_expands() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().join("corpus");
    let work_dir = tmp.path().join("work");
    let spool_dir = work_dir.join("spool");
    let attached_dir = work_dir.join("attached");
    let cache_dir = work_dir.join("parents_cache");
    let lib_tmp = work_dir.join("lib_tmp");

    let parent_month = YearMonth::new(2005, 12);
    let child_month = YearMonth::new(2006, 1);

    write_zst_lines(
        &base.join("submissions").join("RS_2005-12.zst"),
        &[serde_json::json!({
            "author": "parent_author",
            "created_utc": 1133395200,
            "domain": "example.com",
            "id": "s_dec",
            "score": 10,
            "selftext": "parent selftext",
            "title": "December parent",
            "subreddit": "programming"
        })
        .to_string()],
    );
    write_zst_lines(
        &base.join("comments").join("RC_2006-01.zst"),
        &[serde_json::json!({
            "author": "child_author",
            "body": "child body",
            "created_utc": 1136073600,
            "id": "c_child",
            "link_id": "t3_s_dec",
            "parent_id": "t3_s_dec",
            "score": 1,
            "subreddit": "programming"
        })
        .to_string()],
    );

    let (spool_parts, total_written) = RedditETL::new()
        .base_dir(&base)
        .work_dir(&lib_tmp)
        .sources(Sources::Comments)
        .date_range(Some(child_month), Some(child_month))
        .progress(false)
        .scan()
        .subreddit("programming")
        .extract_spool_monthly(&spool_dir)
        .unwrap();
    assert_eq!(total_written, 1);

    let ids = RedditETL::new()
        .base_dir(&base)
        .work_dir(&lib_tmp)
        .progress(false)
        .collect_parent_ids_from_jsonls(spool_parts.clone())
        .unwrap();

    let narrow_parents = RedditETL::new()
        .base_dir(&base)
        .work_dir(&lib_tmp)
        .date_range(Some(child_month), Some(child_month))
        .progress(false)
        .resolve_parent_maps(&ids, &cache_dir, true)
        .unwrap();

    let (first_paths, first_stats) = RedditETL::new()
        .base_dir(&base)
        .work_dir(&lib_tmp)
        .date_range(Some(child_month), Some(child_month))
        .progress(false)
        .attach_parents_jsonls_parallel_with_stats(
            spool_parts.clone(),
            &attached_dir,
            &narrow_parents,
            false,
        )
        .unwrap();
    assert_eq!(first_stats.resolved, 0);
    assert_eq!(first_stats.unresolved, 1);
    let first_line = fs::read_to_string(&first_paths[0]).unwrap();
    let first_value: serde_json::Value = serde_json::from_str(first_line.trim()).unwrap();
    assert!(first_value.get("parent").is_none());

    let wider_parents = RedditETL::new()
        .base_dir(&base)
        .work_dir(&lib_tmp)
        .date_range(Some(parent_month), Some(child_month))
        .progress(false)
        .resolve_parent_maps(&ids, &cache_dir, true)
        .unwrap();

    let (second_paths, second_stats) = RedditETL::new()
        .base_dir(&base)
        .work_dir(&lib_tmp)
        .date_range(Some(parent_month), Some(child_month))
        .progress(false)
        .attach_parents_jsonls_parallel_with_stats(spool_parts, &attached_dir, &wider_parents, true)
        .unwrap();

    assert_eq!(second_stats.resolved, 1);
    assert_eq!(second_stats.unresolved, 0);
    assert_eq!(second_paths[0], first_paths[0]);
    let second_line = fs::read_to_string(&second_paths[0]).unwrap();
    let second_value: serde_json::Value = serde_json::from_str(second_line.trim()).unwrap();
    assert_eq!(
        second_value
            .pointer("/parent/title")
            .and_then(|v| v.as_str()),
        Some("December parent")
    );
}
