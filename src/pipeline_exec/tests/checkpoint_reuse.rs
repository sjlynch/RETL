use super::fixtures::*;
use super::*;

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
