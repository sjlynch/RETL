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
