
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
}
