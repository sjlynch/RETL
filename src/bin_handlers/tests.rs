
#[cfg(test)]
mod tests {
    use super::*;
    use crate::bin_args::{Cli, Command};
    use clap::Parser;

    #[test]
    fn staged_text_write_error_preserves_existing_final() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.txt");
        fs::write(&out, "old\n").unwrap();

        let res: Result<()> = write_text_atomic(&out, CLI_TEXT_WRITE_BUF_BYTES, |w| {
            writeln!(w, "new")?;
            anyhow::bail!("synthetic write failure")
        });

        assert!(res.is_err());
        assert_eq!(fs::read_to_string(&out).unwrap(), "old\n");
        let staging = dir.path().join("_staging");
        let leftovers = fs::read_dir(staging).map(|it| it.count()).unwrap_or(0);
        assert_eq!(leftovers, 0, "failed staged write should be cleaned up");
    }

    // ----- Handler-level validations (formerly tested by spawning `retl`) ---

    /// `aggregate --resume` is intentionally not supported; the handler should
    /// bail with the helpful pointer to the resumable spool workflow.
    /// Migrated from tests/cli_help.rs::aggregate_resume_flag_fails_with_supported_workflow_hint.
    #[test]
    fn aggregate_resume_surfaces_supported_workflow_hint() {
        let cli =
            Cli::try_parse_from(["retl", "aggregate", "--resume", "--out", "out.json"]).unwrap();
        let args = match cli.command {
            Command::Aggregate(a) => a,
            other => panic!("expected aggregate command, got {other:?}"),
        };
        let err = run_aggregate(args).expect_err("--resume must surface helpful error");
        let msg = format!("{err}");
        assert!(
            msg.contains("does not support --resume"),
            "missing helpful prefix: {msg}"
        );
        assert!(
            msg.contains("export --format spool --resume"),
            "missing pointer to spool workflow: {msg}"
        );
    }

    /// `integrity --resume` is also intentionally not supported; the handler
    /// should bail with a pointer to scoping flags / resumable workflows.
    /// Migrated from tests/cli_help.rs::integrity_resume_flag_fails_with_supported_workflow_hint.
    #[test]
    fn integrity_resume_surfaces_supported_workflow_hint() {
        let cli = Cli::try_parse_from(["retl", "integrity", "--resume"]).unwrap();
        let args = match cli.command {
            Command::Integrity(a) => a,
            other => panic!("expected integrity command, got {other:?}"),
        };
        let err = run_integrity(args).expect_err("--resume must surface helpful error");
        let msg = format!("{err}");
        assert!(
            msg.contains("does not support --resume"),
            "missing helpful prefix: {msg}"
        );
        assert!(
            msg.contains("--start/--end"),
            "missing pointer to scoping flags: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // Scan filter validation — migrated from tests/cli_query_validation.rs.
    // Each formerly spawned the `retl` binary to confirm validation fires
    // BEFORE any zstd file is opened. In-process the same assertion is one
    // call to `run_scan(args)` plus a Display-string check on the error.
    // The "must not contain 'zstd'" predicate is preserved verbatim.
    // -----------------------------------------------------------------------

    fn corrupt_scan_corpus() -> tempfile::TempDir {
        let base = tempfile::tempdir().unwrap();
        fs::create_dir_all(base.path().join("comments")).unwrap();
        fs::create_dir_all(base.path().join("submissions")).unwrap();
        // Intentionally not a zstd frame: if validation regresses and the scan
        // tries to read this file, the resulting error mentions "zstd" and the
        // test fails.
        fs::write(
            base.path().join("comments").join("RC_2006-01.zst"),
            b"not a zstd frame",
        )
        .unwrap();
        base
    }

    fn scan_argv_for(base: &tempfile::TempDir, extra: &[&str]) -> Vec<String> {
        let mut v: Vec<String> = vec![
            "retl".into(),
            "scan".into(),
            "--data-dir".into(),
            base.path().display().to_string(),
            "--work-dir".into(),
            base.path().join("work").display().to_string(),
            "--source".into(),
            "rc".into(),
            "--start".into(),
            "2006-01".into(),
            "--end".into(),
            "2006-01".into(),
            "--no-progress".into(),
        ];
        for e in extra {
            v.push((*e).to_string());
        }
        v
    }

    fn assert_scan_blank_filter_fails(extra: &[&str], field: &str) {
        let base = corrupt_scan_corpus();
        let argv = scan_argv_for(&base, extra);
        let argv_refs: Vec<&str> = argv.iter().map(String::as_str).collect();
        let cli = Cli::try_parse_from(&argv_refs).expect("clap parse");
        let args = match cli.command {
            Command::Scan(a) => a,
            other => panic!("expected scan command, got {other:?}"),
        };
        let err = run_scan(args).expect_err("validation must reject blank filter");
        let msg = format!("{err:#}");
        assert!(
            msg.contains(field),
            "expected field `{field}` in error, got: {msg}"
        );
        assert!(
            msg.contains("blank entries are not allowed"),
            "expected blank-entry wording in error, got: {msg}"
        );
        assert!(
            !msg.contains("zstd"),
            "validation must fire before any zstd open; got: {msg}"
        );
    }

    #[test]
    fn scan_rejects_blank_subreddit_before_scanning() {
        assert_scan_blank_filter_fails(&["--subreddit", " "], "subreddits");
    }

    #[test]
    fn scan_rejects_blank_id_before_scanning() {
        assert_scan_blank_filter_fails(&["--id", "t1_"], "ids_in");
    }

    #[test]
    fn scan_rejects_blank_author_before_scanning() {
        assert_scan_blank_filter_fails(&["--author", ""], "authors_in");
    }

    #[test]
    fn scan_rejects_blank_excluded_author_before_scanning() {
        assert_scan_blank_filter_fails(&["--exclude-author", "\t"], "authors_out");
    }

    #[test]
    fn scan_rejects_blank_domain_before_scanning() {
        assert_scan_blank_filter_fails(&["--domain", " "], "domains_in");
    }

    #[test]
    fn scan_rejects_blank_keyword_before_scanning() {
        assert_scan_blank_filter_fails(&["--keyword", ""], "keywords_any");
    }

    #[test]
    fn scan_rejects_blank_keyword_all_before_scanning() {
        assert_scan_blank_filter_fails(&["--keyword-all", ""], "keywords_all");
    }

    #[test]
    fn scan_rejects_blank_excluded_keyword_before_scanning() {
        assert_scan_blank_filter_fails(&["--exclude-keyword", ""], "keywords_exclude");
    }

    #[test]
    fn scan_rejects_invalid_text_regex_before_scanning() {
        let base = corrupt_scan_corpus();
        let argv = scan_argv_for(&base, &["--text-regex", "["]);
        let argv_refs: Vec<&str> = argv.iter().map(String::as_str).collect();
        let cli = Cli::try_parse_from(&argv_refs).expect("clap parse");
        let args = match cli.command {
            Command::Scan(a) => a,
            other => panic!("expected scan command, got {other:?}"),
        };
        let err = run_scan(args).expect_err("invalid regex must be rejected");
        let msg = format!("{err:#}");
        assert!(msg.contains("text_regex"), "got: {msg}");
        assert!(msg.contains("invalid"), "got: {msg}");
        assert!(!msg.contains("zstd"), "got: {msg}");
    }

    #[test]
    fn scan_rejects_blank_text_regex_before_scanning() {
        let base = corrupt_scan_corpus();
        let argv = scan_argv_for(&base, &["--text-regex", " "]);
        let argv_refs: Vec<&str> = argv.iter().map(String::as_str).collect();
        let cli = Cli::try_parse_from(&argv_refs).expect("clap parse");
        let args = match cli.command {
            Command::Scan(a) => a,
            other => panic!("expected scan command, got {other:?}"),
        };
        let err = run_scan(args).expect_err("blank regex must be rejected");
        let msg = format!("{err:#}");
        assert!(msg.contains("text_regex"), "got: {msg}");
        assert!(msg.contains("blank regex patterns"), "got: {msg}");
        assert!(!msg.contains("zstd"), "got: {msg}");
    }

    // -----------------------------------------------------------------------
    // `describe` / `schema` stdout assertions — migrated from
    // tests/cli_describe.rs. Each formerly spawned the `retl` binary and
    // grepped stdout; now each parses argv through clap and calls
    // `run_describe_to` / `run_schema_to` with a captured Vec<u8>.
    // -----------------------------------------------------------------------

    use retl::YearMonth;

    /// Build a `DescribeArgs` from CLI-shaped argv. Mirrors what `main.rs`
    /// would do for a `retl describe ...` invocation.
    fn describe_args(argv: &[&str]) -> crate::bin_args::DescribeArgs {
        let mut v = vec!["retl", "describe"];
        v.extend_from_slice(argv);
        let cli = Cli::try_parse_from(&v).expect("clap parse");
        match cli.command {
            Command::Describe(a) => a,
            other => panic!("expected describe command, got {other:?}"),
        }
    }

    /// Build a `SchemaArgs` from CLI-shaped argv.
    fn schema_args(argv: &[&str]) -> crate::bin_args::SchemaArgs {
        let mut v = vec!["retl", "schema"];
        v.extend_from_slice(argv);
        let cli = Cli::try_parse_from(&v).expect("clap parse");
        match cli.command {
            Command::Schema(a) => a,
            other => panic!("expected schema command, got {other:?}"),
        }
    }

    /// Build the tiny 3-month corpus from the integration-test helpers, so
    /// in-process describe tests share the same fixtures the binary-spawn
    /// tests used. `tests/common/mod.rs` isn't visible to the bin crate, so
    /// reconstruct just what these tests need inline.
    fn make_multi_month_corpus(months: &[YearMonth]) -> std::path::PathBuf {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let base = dir.keep();
        for ym in months {
            let label = format!("{:04}-{:02}", ym.year, ym.month);
            let ts = ym_to_epoch_first_day(ym.year, ym.month);
            let rs_lines = [
                json!({
                    "archived": false, "author": format!("user_{}", label),
                    "created_utc": ts, "domain":"example.com", "id": format!("s_{}", label),
                    "is_self":false, "is_video":false, "num_comments":1, "over_18":false,
                    "score":10, "selftext":"", "title":"hi", "subreddit":"programming",
                    "subreddit_id":"t5_x", "url":"http://example.com/x"
                })
                .to_string(),
                json!({
                    "archived": false, "author":"AutoModerator",
                    "created_utc": ts + 1, "domain":"reddit.com", "id": format!("sb_{}", label),
                    "is_self":false, "is_video":false, "num_comments":1, "over_18":false,
                    "score":1, "selftext":"", "title":"meta", "subreddit":"programming",
                    "subreddit_id":"t5_x", "url":"http://reddit.com/rules"
                })
                .to_string(),
            ];
            let rs_path = base.join("submissions").join(format!("RS_{}.zst", label));
            write_zst_at(&rs_path, &rs_lines);
            let rc_lines = [
                json!({
                    "controversiality":0, "body":"hi", "subreddit_id":"t5_x",
                    "link_id": format!("t3_s_{}", label), "stickied":false,
                    "subreddit":"programming", "score":2, "ups":2,
                    "author": format!("user_{}", label), "id": format!("c1_{}", label),
                    "edited":false, "parent_id": format!("t3_s_{}", label),
                    "gilded":0, "distinguished":null,
                    "created_utc": ts + 100, "retrieved_on": ts + 200
                })
                .to_string(),
                json!({
                    "controversiality":0, "body":"yo", "subreddit_id":"t5_x",
                    "link_id": format!("t3_s_{}", label), "stickied":false,
                    "subreddit":"programming", "score":3, "ups":3,
                    "author": format!("commenter_{}", label), "id": format!("c2_{}", label),
                    "edited":false, "parent_id": format!("t3_s_{}", label),
                    "gilded":0, "distinguished":null,
                    "created_utc": ts + 200, "retrieved_on": ts + 300
                })
                .to_string(),
            ];
            let rc_path = base.join("comments").join(format!("RC_{}.zst", label));
            write_zst_at(&rc_path, &rc_lines);
        }
        base
    }

    fn write_zst_at(path: &Path, lines: &[String]) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let f = fs::File::create(path).unwrap();
        let mut enc = zstd::stream::write::Encoder::new(f, 3).unwrap();
        for l in lines {
            writeln!(enc, "{l}").unwrap();
        }
        enc.finish().unwrap();
    }

    fn ym_to_epoch_first_day(year: u16, month: u8) -> i64 {
        use time::{Date, Month, OffsetDateTime, Time, UtcOffset};
        let m = Month::try_from(month).unwrap();
        let d = Date::from_calendar_date(year as i32, m, 1).unwrap();
        OffsetDateTime::new_in_offset(d, Time::MIDNIGHT, UtcOffset::UTC).unix_timestamp()
    }

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
        assert!(!out.contains("rs\t"), "rs row leaked into rc-only output: {out}");
    }

    #[test]
    fn describe_errors_when_source_path_is_regular_file() {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path();
        fs::write(base.join("comments"), "not a directory").unwrap();
        fs::create_dir(base.join("submissions")).unwrap();

        let args = describe_args(&[
            "--data-dir",
            base.to_str().unwrap(),
            "--source",
            "rc",
        ]);
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

    // -----------------------------------------------------------------------
    // Pure-error-path / pure-file-output tests migrated from
    // tests/cli_export.rs. The tracing-stdout-warning tests stay as spawns
    // because `tracing_subscriber` is process-global and capturing it
    // in-process requires a separate per-test subscriber.
    // -----------------------------------------------------------------------

    fn export_args(argv: &[&str]) -> crate::bin_args::ExportArgs {
        let mut v = vec!["retl", "export"];
        v.extend_from_slice(argv);
        match Cli::try_parse_from(&v).expect("clap parse").command {
            Command::Export(a) => a,
            other => panic!("expected export command, got {other:?}"),
        }
    }

    fn count_args(argv: &[&str]) -> crate::bin_args::CountArgs {
        let mut v = vec!["retl", "count"];
        v.extend_from_slice(argv);
        match Cli::try_parse_from(&v).expect("clap parse").command {
            Command::Count(a) => a,
            other => panic!("expected count command, got {other:?}"),
        }
    }

    fn sample_args(argv: &[&str]) -> crate::bin_args::SampleArgs {
        let mut v = vec!["retl", "sample"];
        v.extend_from_slice(argv);
        match Cli::try_parse_from(&v).expect("clap parse").command {
            Command::Sample(a) => a,
            other => panic!("expected sample command, got {other:?}"),
        }
    }

    fn convert_args(argv: &[&str]) -> crate::bin_args::ConvertArgs {
        let mut v = vec!["retl", "convert"];
        v.extend_from_slice(argv);
        match Cli::try_parse_from(&v).expect("clap parse").command {
            Command::Convert(a) => a,
            other => panic!("expected convert command, got {other:?}"),
        }
    }

    fn dedupe_args(argv: &[&str]) -> crate::bin_args::DedupeArgs {
        let mut v = vec!["retl", "dedupe"];
        v.extend_from_slice(argv);
        match Cli::try_parse_from(&v).expect("clap parse").command {
            Command::Dedupe(a) => a,
            other => panic!("expected dedupe command, got {other:?}"),
        }
    }

    /// Build the same tiny `make_corpus_basic`-shaped fixture that
    /// `tests/common/mod.rs` produces. The integration-test `common` module
    /// isn't visible to the bin crate, so reconstruct the small set of
    /// records here. Cached encoding isn't needed — each test in this module
    /// runs once per `cargo test` invocation.
    fn make_basic_corpus_inline() -> std::path::PathBuf {
        use serde_json::json;
        let dir = tempfile::tempdir().unwrap();
        let base = dir.keep();
        let rs_lines = [
            json!({
                "archived": false, "author":"bob", "created_utc":1136073600,
                "domain":"example.com", "id":"s1", "is_self":false, "is_video":false,
                "num_comments":10, "over_18":false, "score":183, "selftext":"",
                "title":"Rust news", "subreddit":"programming", "subreddit_id":"t5_x",
                "url":"http://example.com/x"
            }).to_string(),
            json!({
                "archived": false, "author":"AutoModerator", "created_utc":1136073601,
                "domain":"nytimes.com", "id":"s2", "is_self":false, "is_video":false,
                "num_comments":1, "over_18":false, "score":1, "selftext":"",
                "title":"Meta: rules", "subreddit":"programming", "subreddit_id":"t5_x",
                "url":"http://reddit.com/rules"
            }).to_string(),
        ];
        write_zst_at(&base.join("submissions").join("RS_2006-01.zst"), &rs_lines);
        let rc_lines = [
            json!({
                "controversiality":0, "body":"I love Rust http://rust-lang.org", "subreddit_id":"t5_x",
                "link_id":"t3_s1", "stickied":false, "subreddit":"programming", "score":2,
                "ups":2, "author":"alice", "id":"c1", "edited":false, "parent_id":"t3_s1",
                "gilded":0, "distinguished":null, "created_utc":1136074600, "retrieved_on":1136075600
            }).to_string(),
            json!({
                "controversiality":0, "body":"reply to alice", "subreddit_id":"t5_x",
                "link_id":"t3_s1", "stickied":false, "subreddit":"programming", "score":5,
                "ups":5, "author":"charlie", "id":"c2", "edited":false, "parent_id":"t1_c1",
                "gilded":0, "distinguished":null, "created_utc":1136074700, "retrieved_on":1136075700
            }).to_string(),
            json!({
                "controversiality":0, "body":"[deleted msg]", "subreddit_id":"t5_x",
                "link_id":"t3_s1", "stickied":false, "subreddit":"programming", "score":0,
                "ups":0, "author":"[deleted]", "id":"c3", "edited":false, "parent_id":"t3_s1",
                "gilded":0, "distinguished":null, "created_utc":1136074800, "retrieved_on":1136075800
            }).to_string(),
        ];
        write_zst_at(&base.join("comments").join("RC_2006-01.zst"), &rc_lines);
        base
    }

    /// Decompress a `.zst` file into JSONL lines, mirroring the helper in
    /// `tests/common/mod.rs::decompress_zst_lines`.
    fn decompress_zst(path: &Path) -> Vec<String> {
        use std::io::{BufRead, BufReader};
        let f = fs::File::open(path).unwrap();
        let dec = zstd::stream::read::Decoder::new(f).unwrap();
        BufReader::new(dec)
            .lines()
            .filter_map(|l| l.ok())
            .filter(|s| !s.is_empty())
            .collect()
    }

    #[test]
    fn export_csv_rejects_human_timestamps() {
        let cwd = tempfile::tempdir().unwrap();
        let args = export_args(&[
            "--data-dir",
            cwd.path().join("missing_data").to_str().unwrap(),
            "--format",
            "csv",
            "--whitelist",
            "id",
            "--human-timestamps",
            "--no-progress",
            "--out",
            cwd.path().join("out.csv").to_str().unwrap(),
        ]);
        let err = run_export(args).expect_err("--human-timestamps must be rejected for csv");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("--human-timestamps is not supported"),
            "got: {msg}"
        );
    }

    #[test]
    fn export_tsv_rejects_resume() {
        let cwd = tempfile::tempdir().unwrap();
        let args = export_args(&[
            "--data-dir",
            cwd.path().join("missing_data").to_str().unwrap(),
            "--format",
            "tsv",
            "--whitelist",
            "id",
            "--resume",
            "--no-progress",
            "--out",
            cwd.path().join("out.tsv").to_str().unwrap(),
        ]);
        let err = run_export(args).expect_err("--resume must be rejected for tsv");
        let msg = format!("{err:#}");
        assert!(msg.contains("--resume is not supported"), "got: {msg}");
    }

    #[test]
    fn sample_csv_rejects_human_timestamps() {
        let cwd = tempfile::tempdir().unwrap();
        let args = sample_args(&[
            "--data-dir",
            cwd.path().join("missing_data").to_str().unwrap(),
            "--format",
            "csv",
            "--whitelist",
            "id",
            "--human-timestamps",
            "--no-progress",
            "--out",
            cwd.path().join("sample.csv").to_str().unwrap(),
        ]);
        let err = run_sample(args).expect_err("--human-timestamps must be rejected for csv");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("--human-timestamps is not supported"),
            "got: {msg}"
        );
    }

    #[test]
    fn count_rejects_export_only_whitelist_flag() {
        // `count` doesn't accept `--whitelist`; the clap parser should reject it.
        let err = Cli::try_parse_from(["retl", "count", "--whitelist", "author"])
            .expect_err("--whitelist on count must fail at parse time");
        let msg = err.to_string();
        assert!(msg.contains("--whitelist"), "got: {msg}");
    }

    #[test]
    fn cli_invalid_author_regex_errors_before_scan() {
        let base = make_basic_corpus_inline();
        let cwd = tempfile::tempdir().unwrap();
        let work_dir = cwd.path().join("work");
        let args = count_args(&[
            "--data-dir",
            base.to_str().unwrap(),
            "--work-dir",
            work_dir.to_str().unwrap(),
            "--author-regex",
            "[",
            "--no-progress",
        ]);
        let mut sink: Vec<u8> = Vec::new();
        let err = run_count_to(args, &mut sink).expect_err("invalid regex must be rejected");
        let msg = format!("{err:#}");
        assert!(msg.contains("author_regex"), "got: {msg}");
    }

    #[test]
    fn export_csv_renders_header_and_rows() {
        let base = make_basic_corpus_inline();
        let cwd = tempfile::tempdir().unwrap();
        let out = cwd.path().join("submissions.csv");

        let args = export_args(&[
            "--data-dir",
            base.to_str().unwrap(),
            "--source",
            "rs",
            "--format",
            "csv",
            "--whitelist",
            "author,title,score",
            "--no-progress",
            "--out",
            out.to_str().unwrap(),
        ]);
        run_export(args).expect("csv export must succeed");

        let csv = fs::read_to_string(&out).unwrap();
        assert!(csv.starts_with("author,title,score\r\n"), "{csv:?}");
        assert!(csv.contains("bob,Rust news,183\r\n"), "{csv:?}");
    }

    #[test]
    fn export_csv_requires_whitelist() {
        let base = make_basic_corpus_inline();
        let cwd = tempfile::tempdir().unwrap();
        let args = export_args(&[
            "--data-dir",
            base.to_str().unwrap(),
            "--source",
            "rs",
            "--format",
            "csv",
            "--no-progress",
            "--out",
            cwd.path().join("bad.csv").to_str().unwrap(),
        ]);
        let err = run_export(args).expect_err("csv export without --whitelist must fail");
        let msg = format!("{err:#}");
        assert!(msg.contains("requires --whitelist"), "got: {msg}");
    }

    #[test]
    fn export_format_zst_writes_partitioned_decodable_corpus() {
        let base = make_basic_corpus_inline();
        let cwd = tempfile::tempdir().unwrap();
        let work_dir = cwd.path().join("work");
        let out_dir = cwd.path().join("mini_corpus");

        let args = export_args(&[
            "--data-dir",
            base.to_str().unwrap(),
            "--work-dir",
            work_dir.to_str().unwrap(),
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--subreddit",
            "programming",
            "--include-deleted",
            "--format",
            "zst",
            "--zst-level",
            "1",
            "--whitelist",
            "id,author,created_utc,subreddit",
            "--human-timestamps",
            "--no-progress",
            "--out",
            out_dir.to_str().unwrap(),
        ]);
        run_export(args).expect("zst export must succeed");

        let rc = out_dir.join("comments").join("RC_2006-01.zst");
        assert!(rc.exists(), "RC zst not found at {}", rc.display());

        let lines = decompress_zst(&rc);
        assert_eq!(lines.len(), 3, "comments (RC) should have 3 lines");

        use std::collections::BTreeSet;
        let allowed: BTreeSet<&str> = ["id", "author", "created_utc", "subreddit"]
            .into_iter()
            .collect();
        for line in lines {
            let value: serde_json::Value = serde_json::from_str(&line).unwrap();
            let obj = value.as_object().expect("exported records are objects");
            assert!(
                obj.keys().all(|k| allowed.contains(k.as_str())),
                "non-whitelisted key leaked into {obj:?}"
            );
            assert!(
                obj.get("created_utc").is_some_and(|v| v.is_string()),
                "created_utc should be human-readable in {obj:?}"
            );
        }
    }

    #[test]
    fn cli_author_allow_and_deny_filters_dedupe() {
        let base = make_basic_corpus_inline();
        let cwd = tempfile::tempdir().unwrap();
        let work_dir = cwd.path().join("work");
        let out = cwd.path().join("authors.txt");

        let args = dedupe_args(&[
            "--data-dir",
            base.to_str().unwrap(),
            "--work-dir",
            work_dir.to_str().unwrap(),
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--author",
            "alice",
            "--author-in",
            "bob",
            "--exclude-author",
            "AutoModerator",
            "--key",
            "author",
            "--no-progress",
            "--out",
            out.to_str().unwrap(),
        ]);
        let mut sink: Vec<u8> = Vec::new();
        run_dedupe_to(args, &mut sink).expect("dedupe must succeed");

        let lines: Vec<String> = fs::read_to_string(&out)
            .unwrap()
            .lines()
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        assert_eq!(lines, vec!["alice", "bob"]);
    }

    #[test]
    fn convert_cli_flattens_parent_enriched_jsonl_to_csv() {
        let cwd = tempfile::tempdir().unwrap();
        let input = cwd.path().join("with_parent.jsonl");
        fs::write(
            &input,
            r#"{"id":"c1","body":"child","parent":{"kind":"t1","id":"p1","body":"parent","author":"bob"}}
"#,
        )
        .unwrap();
        let out = cwd.path().join("parents.csv");

        let args = convert_args(&[
            "--format",
            "csv",
            "--field",
            "id,body,parent.kind,parent.id,parent.body,parent.author",
            "--out",
            out.to_str().unwrap(),
            input.to_str().unwrap(),
        ]);
        run_convert(args).expect("convert must succeed");

        let csv = fs::read_to_string(&out).unwrap();
        assert_eq!(
            csv,
            "id,body,parent.kind,parent.id,parent.body,parent.author\r\nc1,child,t1,p1,parent,bob\r\n"
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
}
