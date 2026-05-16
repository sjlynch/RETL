
#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress_manifest::testing::fail_saves_after_attempts_for_tests;
    use crate::{KeyExtractor, RedditETL, ScanPlan, Sources, YearMonth};
    use serde_json::{json, Value};
    use std::fs::{self, File};
    use std::io::Write;
    use std::path::{Path, PathBuf};

    fn write_zst_lines(path: &Path, lines: &[String]) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let file = File::create(path).unwrap();
        let mut enc = zstd::stream::write::Encoder::new(file, 3).unwrap();
        for line in lines {
            writeln!(&mut enc, "{line}").unwrap();
        }
        enc.finish().unwrap();
    }

    fn make_one_comment_corpus() -> PathBuf {
        let base = tempfile::tempdir().unwrap().keep();
        let line = json!({
            "id":"c1",
            "author":"alice",
            "subreddit":"programming",
            "created_utc":1136073600_i64,
            "score":1,
            "body":"hello",
        })
        .to_string();
        write_zst_lines(&base.join("comments").join("RC_2006-01.zst"), &[line]);
        fs::create_dir_all(base.join("submissions")).unwrap();
        base
    }

    fn one_month_scan(base: &Path) -> ScanPlan {
        RedditETL::new()
            .base_dir(base)
            .sources(Sources::Comments)
            .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
            .progress(false)
            .resume(true)
            .scan()
            .subreddit("programming")
    }

    fn manifest_json(out_dir: &Path) -> Value {
        serde_json::from_slice(&fs::read(out_dir.join("_progress.json")).unwrap()).unwrap()
    }

    fn comment_line(id: &str, author: &str, created_utc: i64) -> String {
        json!({
            "id": id,
            "author": author,
            "subreddit": "programming",
            "created_utc": created_utc,
            "score": 1,
            "body": "hello",
            "parent_id": "t3_s1",
            "link_id": "t3_s1",
        })
        .to_string()
    }

    fn make_two_month_comment_corpus() -> PathBuf {
        let base = tempfile::tempdir().unwrap().keep();
        write_zst_lines(
            &base.join("comments").join("RC_2006-01.zst"),
            &[comment_line("jan_real", "jan_real", 1_136_073_600)],
        );
        write_zst_lines(
            &base.join("comments").join("RC_2006-02.zst"),
            &[comment_line("feb_real", "feb_real", 1_138_752_000)],
        );
        fs::create_dir_all(base.join("submissions")).unwrap();
        base
    }

    fn two_month_scan(base: &Path, work_dir: &Path) -> ScanPlan {
        RedditETL::new()
            .base_dir(base)
            .work_dir(work_dir)
            .sources(Sources::Comments)
            .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 2)))
            .progress(false)
            .resume(true)
            .scan()
            .subreddit("programming")
    }

    fn seed_scan_checkpoint_part(
        plan: &ScanPlan,
        key: &str,
        contents: &str,
        lines: u64,
    ) -> PathBuf {
        let files = plan_pipeline_files(&plan.etl, Some(&plan.query)).unwrap();
        let fingerprint = build_resume_fingerprint(
            &plan.etl,
            &plan.query,
            ANALYTICS_CHECKPOINT_OPERATION,
            plan.limit,
            &files,
        )
        .unwrap();
        let work_dir = plan.etl.ensure_work_dir().unwrap();
        let checkpoint_dir = scan_checkpoint_dir(&work_dir, &fingerprint);
        fs::create_dir_all(&checkpoint_dir).unwrap();
        let part = checkpoint_part_path(&checkpoint_dir, key);
        fs::write(&part, contents).unwrap();
        let mut months = std::collections::HashMap::new();
        months.insert(
            key.to_string(),
            MonthEntry {
                size: fs::metadata(&part).unwrap().len(),
                lines,
                sha256: None,
            },
        );
        crate::progress_manifest::save(&checkpoint_dir, &months, Some(&fingerprint)).unwrap();
        checkpoint_dir
    }

    fn read_tsv_i64(path: &Path) -> std::collections::HashMap<String, i64> {
        fs::read_to_string(path)
            .unwrap()
            .lines()
            .filter_map(|line| {
                let (key, value) = line.split_once('\t')?;
                Some((key.to_string(), value.parse::<i64>().unwrap()))
            })
            .collect()
    }

    fn spool_fingerprint_for(plan: ScanPlan) -> String {
        let plan = plan.build().unwrap();
        let files = plan_pipeline_files(&plan.etl, Some(&plan.query)).unwrap();
        build_resume_fingerprint(&plan.etl, &plan.query, "spool", plan.limit, &files).unwrap()
    }

    #[test]
    fn dedupe_resume_reuses_checkpointed_month_and_scans_missing_month() {
        let base = make_two_month_comment_corpus();
        let work = base.join("work_dedupe_resume");
        let checkpoint_line = format!(
            "{}\n",
            comment_line("checkpoint_jan", "checkpoint_only", 1_136_073_600)
        );
        let plan = two_month_scan(&base, &work).build().unwrap();
        seed_scan_checkpoint_part(&plan, "RC_2006-01", &checkpoint_line, 1);

        let out = base.join("dedupe_resume.txt");
        let stats = two_month_scan(&base, &work)
            .dedupe_keys_to_lines_with_stats(&KeyExtractor::author_lowercase_fast(), &out)
            .unwrap();

        let got: Vec<String> = fs::read_to_string(&out)
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect();
        assert_eq!(got, vec!["checkpoint_only", "feb_real"]);
        assert_eq!(stats.matched_records, 2);
    }

    #[test]
    fn count_author_resume_reuses_checkpointed_month_and_scans_missing_month() {
        let base = make_two_month_comment_corpus();
        let work = base.join("work_count_resume");
        let checkpoint_contents = format!(
            "{}\n{}\n",
            comment_line("checkpoint_a", "checkpoint_only", 1_136_073_600),
            comment_line("checkpoint_b", "checkpoint_only", 1_136_073_601)
        );
        let plan = two_month_scan(&base, &work).build().unwrap();
        seed_scan_checkpoint_part(&plan, "RC_2006-01", &checkpoint_contents, 2);

        let out = base.join("count_resume.tsv");
        two_month_scan(&base, &work)
            .author_counts_to_tsv(&out)
            .unwrap();

        let got = read_tsv_i64(&out);
        assert_eq!(got.get("checkpoint_only").copied(), Some(2));
        assert_eq!(got.get("feb_real").copied(), Some(1));
        assert!(!got.contains_key("jan_real"));
    }

    #[test]
    fn first_seen_resume_reuses_checkpointed_month_and_scans_missing_month() {
        let base = make_two_month_comment_corpus();
        let work = base.join("work_first_seen_resume");
        let checkpoint_contents = format!(
            "{}\n{}\n",
            comment_line("checkpoint_late", "checkpoint_only", 1_136_073_650),
            comment_line("checkpoint_early", "checkpoint_only", 1_136_073_610)
        );
        let plan = two_month_scan(&base, &work).build().unwrap();
        seed_scan_checkpoint_part(&plan, "RC_2006-01", &checkpoint_contents, 2);

        let out = base.join("first_seen_resume.tsv");
        two_month_scan(&base, &work)
            .build_first_seen_index_to_tsv(&out)
            .unwrap();

        let got = read_tsv_i64(&out);
        assert_eq!(got.get("checkpoint_only").copied(), Some(1_136_073_610));
        assert_eq!(got.get("feb_real").copied(), Some(1_138_752_000));
        assert!(!got.contains_key("jan_real"));
    }

    #[test]
    fn resume_rebuilds_invalid_checkpoint_part() {
        let base = make_two_month_comment_corpus();
        let work = base.join("work_invalid_checkpoint");
        let plan = two_month_scan(&base, &work).build().unwrap();
        let checkpoint_dir = seed_scan_checkpoint_part(&plan, "RC_2006-01", "{not json\n", 1);

        let out = base.join("count_rebuilt.tsv");
        two_month_scan(&base, &work)
            .author_counts_to_tsv(&out)
            .unwrap();

        let got = read_tsv_i64(&out);
        assert_eq!(got.get("jan_real").copied(), Some(1));
        assert_eq!(got.get("feb_real").copied(), Some(1));
        assert!(!got.contains_key("checkpoint_only"));
        let manifest = manifest_json(&checkpoint_dir);
        assert!(manifest["months"]
            .as_object()
            .unwrap()
            .contains_key("RC_2006-01"));
    }

    #[test]
    fn resume_fingerprint_includes_new_text_and_url_filters() {
        let base = make_one_comment_corpus();
        let base_fp = spool_fingerprint_for(one_month_scan(&base));

        let keyword_all_fp = spool_fingerprint_for(one_month_scan(&base).keywords_all(["hello"]));
        let exclude_keyword_fp =
            spool_fingerprint_for(one_month_scan(&base).exclude_keywords(["spam"]));
        let text_regex_fp = spool_fingerprint_for(one_month_scan(&base).text_regex("hello"));
        let no_url_fp = spool_fingerprint_for(one_month_scan(&base).no_url());

        assert_ne!(base_fp, keyword_all_fp);
        assert_ne!(base_fp, exclude_keyword_fp);
        assert_ne!(base_fp, text_regex_fp);
        assert_ne!(base_fp, no_url_fp);
    }

    #[test]
    fn resume_fingerprint_changes_when_timestamp_bounds_change() {
        let base = make_one_comment_corpus();
        let plan_a = RedditETL::new()
            .base_dir(&base)
            .sources(Sources::Comments)
            .progress(false)
            .scan()
            .created_utc_gte(1_136_073_600)
            .created_utc_lt(1_136_160_000)
            .build()
            .unwrap();
        let plan_b = RedditETL::new()
            .base_dir(&base)
            .sources(Sources::Comments)
            .progress(false)
            .scan()
            .created_utc_gte(1_136_073_601)
            .created_utc_lt(1_136_160_000)
            .build()
            .unwrap();

        let files_a = plan_pipeline_files(&plan_a.etl, Some(&plan_a.query)).unwrap();
        let files_b = plan_pipeline_files(&plan_b.etl, Some(&plan_b.query)).unwrap();
        let fp_a =
            build_resume_fingerprint(&plan_a.etl, &plan_a.query, "extract", plan_a.limit, &files_a)
                .expect("fingerprint a");
        let fp_b =
            build_resume_fingerprint(&plan_b.etl, &plan_b.query, "extract", plan_b.limit, &files_b)
                .expect("fingerprint b");
        assert_ne!(fp_a, fp_b);
    }

    #[test]
    fn resume_fingerprint_changes_when_record_id_filter_changes() {
        let base = make_one_comment_corpus();
        let plan_c1 = one_month_scan(&base).ids(["c1"]).build().unwrap();
        let plan_c2 = one_month_scan(&base).ids(["c2"]).build().unwrap();

        let files_c1 = plan_pipeline_files(&plan_c1.etl, Some(&plan_c1.query)).unwrap();
        let files_c2 = plan_pipeline_files(&plan_c2.etl, Some(&plan_c2.query)).unwrap();
        let fp_c1 =
            build_resume_fingerprint(&plan_c1.etl, &plan_c1.query, "spool", plan_c1.limit, &files_c1)
                .unwrap();
        let fp_c2 =
            build_resume_fingerprint(&plan_c2.etl, &plan_c2.query, "spool", plan_c2.limit, &files_c2)
                .unwrap();

        assert_ne!(fp_c1, fp_c2);
    }

    #[test]
    fn spool_resume_commit_failure_after_publish_returns_error_and_next_run_succeeds() {
        let base = make_one_comment_corpus();
        let out_dir = base.join("spool_manifest_failure");

        let err = {
            // The first save is the initial pruned manifest; fail the per-month
            // commit that happens only after the spool part has been published.
            let _guard = fail_saves_after_attempts_for_tests(&out_dir, 1, 1);
            one_month_scan(&base)
                .extract_spool_monthly(&out_dir)
                .expect_err("manifest commit failure must fail the run")
        };
        assert!(
            err.to_string()
                .contains("durably update resume progress manifest"),
            "unexpected error: {err:#}"
        );

        let part = out_dir.join("part_RC_2006-01.jsonl");
        assert!(
            part.exists(),
            "data publish should remain atomic and durable"
        );
        let manifest = manifest_json(&out_dir);
        assert!(
            manifest["months"].as_object().unwrap().is_empty(),
            "failed commit must not claim the month is resumable: {manifest}"
        );

        let (_parts, written) = one_month_scan(&base)
            .extract_spool_monthly(&out_dir)
            .expect("subsequent run without manifest failure should succeed");
        assert_eq!(written, 1);
        let manifest = manifest_json(&out_dir);
        assert!(manifest["months"]
            .as_object()
            .unwrap()
            .contains_key("RC_2006-01"));
    }

    #[test]
    fn partitioned_resume_commit_failure_after_publish_returns_error() {
        let base = make_one_comment_corpus();
        let out_dir = base.join("partitioned_manifest_failure");

        let err = {
            let _guard = fail_saves_after_attempts_for_tests(&out_dir, 1, 1);
            one_month_scan(&base)
                .export_partitioned(&out_dir, ExportFormat::Jsonl)
                .expect_err("manifest commit failure must fail the partitioned export")
        };
        assert!(
            err.to_string()
                .contains("durably update partitioned export progress manifest"),
            "unexpected error: {err:#}"
        );

        let part = out_dir.join("comments").join("RC_2006-01.jsonl");
        assert!(
            part.exists(),
            "partition should remain published after commit failure"
        );
        let manifest = manifest_json(&out_dir);
        assert!(
            manifest["months"].as_object().unwrap().is_empty(),
            "failed commit must not mark the partition resumable: {manifest}"
        );

        one_month_scan(&base)
            .export_partitioned(&out_dir, ExportFormat::Jsonl)
            .expect("subsequent partitioned export should succeed");
        let manifest = manifest_json(&out_dir);
        assert!(manifest["months"]
            .as_object()
            .unwrap()
            .contains_key("RC_2006-01"));
    }

    #[test]
    fn resume_fingerprint_includes_record_limit() {
        let base = make_one_comment_corpus();
        let no_limit_plan = one_month_scan(&base).build().unwrap();
        let limit_one_plan = one_month_scan(&base).limit(1).build().unwrap();
        let limit_two_plan = one_month_scan(&base).limit(2).build().unwrap();

        let no_limit_files =
            plan_pipeline_files(&no_limit_plan.etl, Some(&no_limit_plan.query)).unwrap();
        let limit_one_files =
            plan_pipeline_files(&limit_one_plan.etl, Some(&limit_one_plan.query)).unwrap();
        let limit_two_files =
            plan_pipeline_files(&limit_two_plan.etl, Some(&limit_two_plan.query)).unwrap();

        let no_limit = build_resume_fingerprint(
            &no_limit_plan.etl,
            &no_limit_plan.query,
            "extract",
            no_limit_plan.limit,
            &no_limit_files,
        )
        .unwrap();
        let limit_one = build_resume_fingerprint(
            &limit_one_plan.etl,
            &limit_one_plan.query,
            "extract",
            limit_one_plan.limit,
            &limit_one_files,
        )
        .unwrap();
        let limit_two = build_resume_fingerprint(
            &limit_two_plan.etl,
            &limit_two_plan.query,
            "extract",
            limit_two_plan.limit,
            &limit_two_files,
        )
        .unwrap();

        assert_ne!(no_limit, limit_one);
        assert_ne!(limit_one, limit_two);
    }

    #[test]
    fn extract_resume_commit_failure_after_temp_part_publish_returns_error_and_next_run_stitches() {
        let base = make_one_comment_corpus();
        let work_dir = base.join("work_manifest_failure");
        let out = base.join("out.jsonl");

        let plan = RedditETL::new()
            .base_dir(&base)
            .work_dir(&work_dir)
            .sources(Sources::Comments)
            .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
            .progress(false)
            .resume(true)
            .scan()
            .subreddit("programming")
            .build()
            .unwrap();
        let files = plan_pipeline_files(&plan.etl, Some(&plan.query)).unwrap();
        let fingerprint =
            build_resume_fingerprint(&plan.etl, &plan.query, "extract", plan.limit, &files)
                .unwrap();
        let tmp_dir = extract_scratch_dir(
            &work_dir,
            "extract_jsonl_q_tmp",
            &out,
            true,
            Some(&fingerprint),
        )
        .unwrap();

        let err = {
            let _guard = fail_saves_after_attempts_for_tests(&tmp_dir, 1, 1);
            RedditETL::new()
                .base_dir(&base)
                .work_dir(&work_dir)
                .sources(Sources::Comments)
                .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
                .progress(false)
                .resume(true)
                .scan()
                .subreddit("programming")
                .extract_to_jsonl(&out)
                .expect_err("manifest commit failure must fail extract_to_jsonl")
        };
        assert!(
            err.to_string()
                .contains("durably update extract progress manifest"),
            "unexpected error: {err:#}"
        );

        let tmp_part = tmp_dir.join(".part_RC_2006-01.jsonl");
        assert!(
            tmp_part.exists(),
            "resume temp part should remain published"
        );
        assert!(
            !out.exists(),
            "final extract output should not be stitched after a failed checkpoint"
        );
        let manifest = manifest_json(&tmp_dir);
        assert!(
            manifest["months"].as_object().unwrap().is_empty(),
            "failed commit must not mark the temp part resumable: {manifest}"
        );

        RedditETL::new()
            .base_dir(&base)
            .work_dir(&work_dir)
            .sources(Sources::Comments)
            .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
            .progress(false)
            .resume(true)
            .scan()
            .subreddit("programming")
            .extract_to_jsonl(&out)
            .expect("subsequent extract should commit the temp part and stitch output");
        assert!(out.exists());
        let manifest = manifest_json(&tmp_dir);
        assert!(manifest["months"]
            .as_object()
            .unwrap()
            .contains_key("RC_2006-01"));
    }
}
