use super::fixtures::{export_args, sample_args};
use super::*;

// ----- Handler-level validations (formerly tested by spawning `retl`) ---

/// `aggregate --resume` is intentionally not supported; the handler should
/// bail with the helpful pointer to the resumable spool workflow.
/// Migrated from tests/cli_help.rs::aggregate_resume_flag_fails_with_supported_workflow_hint.
#[test]
fn aggregate_resume_surfaces_supported_workflow_hint() {
    let cli = Cli::try_parse_from(["retl", "aggregate", "--resume", "--out", "out.json"]).unwrap();
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

/// `aggregate --scientific` is a grouped-metric display toggle and has no
/// effect on the rec-count JSON output produced without `--by`. Like the
/// sibling `--metric` / `--top` flags, passing it without `--by` should fail
/// loudly instead of being silently ignored.
#[test]
fn aggregate_scientific_without_by_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let cli = Cli::try_parse_from([
        "retl",
        "aggregate",
        "--out",
        dir.path().join("out.json").to_str().unwrap(),
        "--shards-dir",
        dir.path().join("shards").to_str().unwrap(),
        "--scientific",
        dir.path().join("missing.jsonl").to_str().unwrap(),
    ])
    .unwrap();
    let args = match cli.command {
        Command::Aggregate(a) => a,
        other => panic!("expected aggregate command, got {other:?}"),
    };
    let err = run_aggregate(args).expect_err("--scientific without --by must error");
    assert!(
        format!("{err}").contains("--scientific requires --by"),
        "unexpected error: {err}"
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

/// `integrity` validates whole `.zst` files and never builds a record-level
/// scan plan, so the shared `--subreddit` filter would be a silent no-op
/// (the whole corpus is checked regardless). Reject it loudly.
#[test]
fn integrity_rejects_subreddit_record_filter() {
    let cli = Cli::try_parse_from(["retl", "integrity", "--subreddit", "rust"]).unwrap();
    let args = match cli.command {
        Command::Integrity(a) => a,
        other => panic!("expected integrity command, got {other:?}"),
    };
    let err = run_integrity(args).expect_err("--subreddit on integrity must error");
    assert!(
        format!("{err}").contains("--subreddit/-s has no effect on `retl integrity`"),
        "unexpected error: {err}"
    );
}

/// `--include-deleted` is likewise a record-level filter with no meaning for
/// `integrity`, which never inspects individual records.
#[test]
fn integrity_rejects_include_deleted_record_filter() {
    let cli = Cli::try_parse_from(["retl", "integrity", "--include-deleted"]).unwrap();
    let args = match cli.command {
        Command::Integrity(a) => a,
        other => panic!("expected integrity command, got {other:?}"),
    };
    let err = run_integrity(args).expect_err("--include-deleted on integrity must error");
    assert!(
        format!("{err}").contains("--include-deleted has no effect on `retl integrity`"),
        "unexpected error: {err}"
    );
}

/// `--window-months` only shapes spool mode's discovered-range slack;
/// direct parent-ID resolution scopes its scan with `--start`/`--end`. The
/// flag must be rejected in direct mode, mirroring how spool mode rejects
/// `--start`/`--end`/`--id-kind`.
#[test]
fn parents_direct_mode_rejects_window_months() {
    let dir = tempfile::tempdir().unwrap();
    let cli = Cli::try_parse_from([
        "retl",
        "parents",
        "--parent-id",
        "t1_abc",
        "--cache",
        dir.path().join("cache").to_str().unwrap(),
        "--out",
        dir.path().join("out.jsonl").to_str().unwrap(),
        "--window-months",
        "5",
    ])
    .unwrap();
    let args = match cli.command {
        Command::Parents(a) => a,
        other => panic!("expected parents command, got {other:?}"),
    };
    let err = run_parents(args).expect_err("--window-months in direct mode must error");
    assert!(
        format!("{err}").contains("--window-months only applies to --spool mode"),
        "unexpected error: {err}"
    );
}

/// `--pretty` only field-indents the `json` array; for any other `--format`
/// it is a silent no-op, so `export` rejects it up front.
#[test]
fn export_rejects_pretty_for_non_json_format() {
    let cwd = tempfile::tempdir().unwrap();
    let args = export_args(&[
        "--data-dir",
        cwd.path().join("missing_data").to_str().unwrap(),
        "--format",
        "jsonl",
        "--pretty",
        "--no-progress",
        "--out",
        cwd.path().join("out.jsonl").to_str().unwrap(),
    ]);
    let err = run_export(args).expect_err("--pretty for jsonl must be rejected");
    assert!(
        format!("{err:#}").contains("--pretty only applies to --format json"),
        "unexpected error: {err}"
    );
}

/// `--zst-level` only sets the `.zst` compression level; for non-zst formats
/// it has no effect, so `export` rejects it up front.
#[test]
fn export_rejects_zst_level_for_non_zst_format() {
    let cwd = tempfile::tempdir().unwrap();
    let args = export_args(&[
        "--data-dir",
        cwd.path().join("missing_data").to_str().unwrap(),
        "--format",
        "jsonl",
        "--zst-level",
        "5",
        "--no-progress",
        "--out",
        cwd.path().join("out.jsonl").to_str().unwrap(),
    ]);
    let err = run_export(args).expect_err("--zst-level for jsonl must be rejected");
    assert!(
        format!("{err:#}").contains("--zst-level only applies to --format zst"),
        "unexpected error: {err}"
    );
}

/// `sample` mirrors `export`: `--pretty` is rejected for any non-json format.
#[test]
fn sample_rejects_pretty_for_non_json_format() {
    let cwd = tempfile::tempdir().unwrap();
    let args = sample_args(&[
        "--data-dir",
        cwd.path().join("missing_data").to_str().unwrap(),
        "--format",
        "jsonl",
        "--pretty",
        "--no-progress",
        "--out",
        cwd.path().join("sample.jsonl").to_str().unwrap(),
    ]);
    let err = run_sample(args).expect_err("--pretty for jsonl must be rejected");
    assert!(
        format!("{err:#}").contains("--pretty only applies to --format json"),
        "unexpected error: {err}"
    );
}
