//! Snapshot-style tests for the `retl` CLI's `--help` output.
//!
//! These guard against accidental drift in the public CLI surface — if a
//! subcommand is renamed/removed or an advertised flag disappears, these
//! tests fail with a clear diff in CI before users see a regression.

use assert_cmd::Command;
use predicates::prelude::*;
use predicates::str::contains;

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

#[test]
fn root_help_lists_all_subcommands() {
    let assert = retl().arg("--help").assert().success();
    let pred = contains("scan")
        .and(contains("dedupe"))
        .and(contains("export"))
        .and(contains("count"))
        .and(contains("integrity"))
        .and(contains("aggregate"))
        .and(contains("parents"))
        .and(contains("first-seen"));
    assert.stdout(pred);
}

#[test]
fn parents_help_advertises_required_flags() {
    let assert = retl().args(["parents", "--help"]).assert().success();
    let pred = contains("--spool")
        .and(contains("--cache"))
        .and(contains("--out"))
        .and(contains("--window-months"))
        .and(contains("--resume"));
    assert.stdout(pred);
}

#[test]
fn first_seen_help_advertises_out() {
    let assert = retl().args(["first-seen", "--help"]).assert().success();
    assert.stdout(contains("--out"));
}

#[test]
fn export_help_advertises_export_only_flags() {
    let assert = retl().args(["export", "--help"]).assert().success();
    let pred = contains("--zst-level")
        .and(contains("--resume"))
        .and(contains("--whitelist"))
        .and(contains("--strict-whitelist"))
        .and(contains("--human-timestamps"))
        .and(contains("--inflight-bytes"));
    assert.stdout(pred);
}

#[test]
fn dedupe_help_advertises_key_out_and_inflight() {
    let assert = retl().args(["dedupe", "--help"]).assert().success();
    let pred = contains("--key")
        .and(contains("--out"))
        .and(contains("--inflight-bytes"))
        .and(contains("json:/pointer"));
    assert.stdout(pred);
}

#[test]
fn scan_help_advertises_common_flags() {
    let assert = retl().args(["scan", "--help"]).assert().success();
    let pred = contains("--data-dir")
        .and(contains("--work-dir"))
        .and(contains("--start"))
        .and(contains("--end"))
        .and(contains("--parallelism"))
        .and(contains("--file-concurrency"))
        .and(contains("--no-progress"))
        .and(contains("--source"))
        .and(contains("--subreddit"))
        .and(contains("--include-deleted"))
        .and(contains("--whitelist").not())
        .and(contains("--strict-whitelist").not())
        .and(contains("--human-timestamps").not());
    assert.stdout(pred);
}

#[test]
fn export_help_advertises_format_and_out() {
    let assert = retl().args(["export", "--help"]).assert().success();
    let pred = contains("--format")
        .and(contains("jsonl"))
        .and(contains("json"))
        .and(contains("spool"))
        .and(contains("zst"))
        .and(contains("partitioned-jsonl"))
        .and(contains("--out"))
        .and(contains("--pretty"));
    assert.stdout(pred);
}

#[test]
fn count_help_advertises_modes() {
    let assert = retl().args(["count", "--help"]).assert().success();
    let pred = contains("--mode")
        .and(contains("month"))
        .and(contains("author"));
    assert.stdout(pred);
}

#[test]
fn integrity_help_advertises_modes_and_sample() {
    let assert = retl().args(["integrity", "--help"]).assert().success();
    let pred = contains("--mode")
        .and(contains("quick"))
        .and(contains("full"))
        .and(contains("--sample-bytes"));
    assert.stdout(pred);
}

#[test]
fn aggregate_help_requires_inputs_and_out() {
    let assert = retl().args(["aggregate", "--help"]).assert().success();
    let pred = contains("INPUTS")
        .and(contains("--out"))
        .and(contains("--by"))
        .and(contains("--metric"))
        .and(contains("--top"));
    assert.stdout(pred);
}

#[test]
fn version_flag_works() {
    retl().arg("--version").assert().success();
}

#[test]
fn unknown_subcommand_fails_clean() {
    retl()
        .arg("not-a-real-subcommand")
        .assert()
        .failure()
        .stderr(predicate::str::contains("error"));
}
