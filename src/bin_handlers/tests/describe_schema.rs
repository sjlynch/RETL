use super::fixtures::{describe_args, make_multi_month_corpus, schema_args, write_zst_at};
use super::*;
use retl::YearMonth;

// -----------------------------------------------------------------------
// `describe` / `schema` stdout assertions — migrated from
// tests/cli_describe.rs. Each formerly spawned the `retl` binary and
// grepped stdout; now each parses argv through clap and calls
// `run_describe_to` / `run_schema_to` with a captured Vec<u8>.
// -----------------------------------------------------------------------

/// Run `run_describe_to` against argv and return captured stdout as a String.
fn describe_stdout(args: crate::bin_args::DescribeArgs) -> String {
    let mut buf: Vec<u8> = Vec::new();
    run_describe_to(args, &mut buf).expect("describe must succeed");
    String::from_utf8(buf).expect("describe output must be UTF-8")
}

#[test]
fn describe_reports_available_ranges_counts_and_bytes() {
    let base = make_multi_month_corpus(&[
        YearMonth::new(2006, 1),
        YearMonth::new(2006, 2),
        YearMonth::new(2006, 3),
    ]);
    let rc_feb = base.join("comments").join("RC_2006-02.zst");
    let rs_feb = base.join("submissions").join("RS_2006-02.zst");
    let rc_bytes = fs::metadata(&rc_feb).unwrap().len();
    let rs_bytes = fs::metadata(&rs_feb).unwrap().len();
    let total_bytes = rc_bytes + rs_bytes;

    let out = describe_stdout(describe_args(&[
        "--data-dir",
        base.to_str().unwrap(),
        "--start",
        "2006-02",
        "--end",
        "2006-02",
    ]));

    assert!(out.contains("source\tavailable\tfiles_in_range\tcompressed_bytes"));
    assert!(out.contains(&format!("rc\t2006-01..=2006-03\t1\t{rc_bytes}")));
    assert!(out.contains(&format!("rs\t2006-01..=2006-03\t1\t{rs_bytes}")));
    assert!(out.contains(&format!("total\t\t2\t{total_bytes}")));
}

#[test]
fn describe_reports_missing_month_holes() {
    let base = make_multi_month_corpus(&[YearMonth::new(2006, 1), YearMonth::new(2006, 3)]);
    let out = describe_stdout(describe_args(&["--data-dir", base.to_str().unwrap()]));
    assert!(out.contains("missing_month_count\tmissing_months"));
    assert!(out.contains("rc\t2006-01..=2006-03\t2"));
    assert!(out.contains("\t1\t2006-02"));
    assert!(out.contains("rs\t2006-01..=2006-03\t2"));
    assert!(out.contains("total\t\t4"));
    assert!(out.contains("\t2\t-"));
}

#[test]
fn describe_honors_source_selection() {
    let base = make_multi_month_corpus(&[YearMonth::new(2006, 1)]);
    let rc = base.join("comments").join("RC_2006-01.zst");
    let rc_bytes = fs::metadata(&rc).unwrap().len();

    let out = describe_stdout(describe_args(&[
        "--data-dir",
        base.to_str().unwrap(),
        "--source",
        "rc",
    ]));
    assert!(out.contains(&format!("rc\t2006-01..=2006-01\t1\t{rc_bytes}")));
    assert!(out.contains(&format!("total\t\t1\t{rc_bytes}")));
    assert!(
        !out.contains("rs\t"),
        "rs row leaked into rc-only output: {out}"
    );
}

#[test]
fn describe_errors_when_source_path_is_regular_file() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();
    fs::write(base.join("comments"), "not a directory").unwrap();
    fs::create_dir(base.join("submissions")).unwrap();

    let args = describe_args(&["--data-dir", base.to_str().unwrap(), "--source", "rc"]);
    let mut buf = Vec::new();
    let err = run_describe_to(args, &mut buf).expect_err("non-dir comments path must fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("failed to discover comments corpus directory"),
        "expected discovery error, got: {msg}"
    );
}

#[test]
fn describe_skips_invalid_month_filenames() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path();
    let comments = base.join("comments");
    let submissions = base.join("submissions");
    fs::create_dir(&comments).unwrap();
    fs::create_dir(&submissions).unwrap();
    fs::write(comments.join("RC_2024-00.zst"), b"").unwrap();
    fs::write(comments.join("RC_2024-01.zst"), b"").unwrap();
    fs::write(submissions.join("RS_2024-99.zst"), b"").unwrap();

    let out = describe_stdout(describe_args(&["--data-dir", base.to_str().unwrap()]));
    assert!(out.contains("rc\t2024-01..=2024-01\t1"));
    assert!(out.contains("rs\t<none>\t0"));
}

#[test]
fn describe_schema_runs_with_explicit_schema_format_flags() {
    // `--schema-sample` / `--schema-format` (here via its `--format` alias)
    // are honored when `--schema` is set.
    let base = make_multi_month_corpus(&[YearMonth::new(2006, 1)]);
    let out = describe_stdout(describe_args(&[
        "--data-dir",
        base.to_str().unwrap(),
        "--schema",
        "--schema-sample",
        "1",
        "--format",
        "json",
    ]));
    assert!(
        out.trim_start().starts_with('['),
        "schema --format json should emit a JSON array, got: {out}"
    );
}

#[test]
fn describe_rejects_schema_format_without_schema() {
    // Without `--schema`, `describe` prints the plain discovery table; the
    // schema-only `--format`/`--schema-format` flag must be rejected, not
    // silently ignored.
    let args = describe_args(&["--format", "json"]);
    let mut buf = Vec::new();
    let err =
        run_describe_to(args, &mut buf).expect_err("--format without --schema must be rejected");
    assert!(
        format!("{err}").contains("--schema-format/--format only applies"),
        "unexpected error: {err}"
    );
}

#[test]
fn describe_rejects_schema_sample_without_schema() {
    let args = describe_args(&["--schema-sample", "50"]);
    let mut buf = Vec::new();
    let err = run_describe_to(args, &mut buf)
        .expect_err("--schema-sample without --schema must be rejected");
    assert!(
        format!("{err}").contains("--schema-sample only applies"),
        "unexpected error: {err}"
    );
}

#[test]
fn schema_reports_top_level_fields_as_tsv() {
    // Build a single-month rs-only corpus inline; schema's contract is
    // about the *type* and *presence* of top-level fields, not the
    // record count, so one record is sufficient.
    let dir = tempfile::tempdir().unwrap();
    let base = dir.keep();
    let rs_path = base.join("submissions").join("RS_2006-01.zst");
    let rs_lines = [serde_json::json!({
        "archived": false, "author":"bob", "created_utc":1136073600,
        "domain":"example.com", "id":"s1", "is_self":false, "is_video":false,
        "num_comments":10, "over_18":false, "score":183, "selftext":"",
        "title":"Rust news", "subreddit":"programming", "subreddit_id":"t5_x",
        "url":"http://example.com/x"
    })
    .to_string()];
    write_zst_at(&rs_path, &rs_lines);
    fs::create_dir_all(base.join("comments")).unwrap();

    let args = schema_args(&[
        "--data-dir",
        base.to_str().unwrap(),
        "--source",
        "rs",
        "--sample",
        "1",
    ]);
    let mut buf = Vec::new();
    run_schema_to(args, &mut buf).expect("schema must succeed");
    let out = String::from_utf8(buf).unwrap();
    assert!(out.contains("field\ttype\tpresence_pct"), "got: {out}");
    assert!(out.contains("author\tstring\t100.00"), "got: {out}");
    assert!(out.contains("score\tnumber\t100.00"), "got: {out}");
}
