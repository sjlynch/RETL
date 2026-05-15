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
    let pred = contains("describe")
        .and(contains("scan"))
        .and(contains("dedupe"))
        .and(contains("export"))
        .and(contains("convert"))
        .and(contains("count"))
        .and(contains("integrity"))
        .and(contains("aggregate"))
        .and(contains("parents"))
        .and(contains("first-seen"));
    assert.stdout(pred);
}

#[test]
fn describe_help_advertises_discovery_flags() {
    let assert = retl().args(["describe", "--help"]).assert().success();
    let pred = contains("--data-dir")
        .and(contains("--source"))
        .and(contains("--start"))
        .and(contains("--end"));
    assert.stdout(pred);
}

#[test]
fn parents_help_advertises_required_flags() {
    let assert = retl().args(["parents", "--help"]).assert().success();
    let pred = contains("--spool")
        .and(contains("--cache"))
        .and(contains("--out"))
        .and(contains("--window-months"))
        .and(contains("--parent-fields"))
        .and(contains("--parent-full"))
        .and(contains("--resume"))
        .and(contains("--inflight-bytes"))
        .and(contains("--inflight-groups"));
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
        .and(contains("--inflight-bytes"))
        .and(contains("--inflight-groups"));
    assert.stdout(pred);
}

#[test]
fn dedupe_help_advertises_key_out_and_inflight() {
    let assert = retl().args(["dedupe", "--help"]).assert().success();
    let pred = contains("--key")
        .and(contains("--out"))
        .and(contains("--inflight-bytes"))
        .and(contains("--inflight-groups"))
        .and(contains("--strict-key"))
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
        .and(contains("--id"))
        .and(contains("--ids-file"))
        .and(contains("--author"))
        .and(contains("--exclude-author"))
        .and(contains("--exclude-common-bots"))
        .and(contains("--author-regex"))
        .and(contains("--keyword"))
        .and(contains("--min-score"))
        .and(contains("--max-score"))
        .and(contains("--contains-url"))
        .and(contains("--domain"))
        .and(contains("--json"))
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
        .and(contains("--pretty"))
        .and(contains("Field-indent the JSON array"));
    assert.stdout(pred);
}

#[test]
fn convert_help_advertises_fields_spool_format_and_out() {
    let assert = retl().args(["convert", "--help"]).assert().success();
    let pred = contains("--field")
        .and(contains("--spool"))
        .and(contains("--format"))
        .and(contains("csv"))
        .and(contains("tsv"))
        .and(contains("--out"));
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
        .and(contains("--sample-bytes"))
        .and(contains("--collect"));
    assert.stdout(pred);
}

#[test]
fn aggregate_help_advertises_spool_inputs_out_and_runtime_flags_only() {
    let assert = retl().args(["aggregate", "--help"]).assert().success();
    let pred = contains("INPUTS")
        .and(contains("--spool"))
        .and(contains("--out"))
        .and(contains("--by"))
        .and(contains("--metric"))
        .and(contains("--top"))
        .and(contains("--scientific"))
        .and(contains("Field-indent the final JSON"))
        .and(contains("--parallelism"))
        .and(contains("--no-progress"))
        .and(contains("--shards-dir"))
        .and(contains("--data-dir").not())
        .and(contains("--work-dir").not())
        .and(contains("--start").not())
        .and(contains("--end").not())
        .and(contains("--source").not())
        .and(contains("--subreddit").not())
        .and(contains("--include-deleted").not())
        .and(contains("--exclude-common-bots").not())
        .and(contains("--file-concurrency").not());
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
