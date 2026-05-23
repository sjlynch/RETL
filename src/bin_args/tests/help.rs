use super::render_help;

// Snapshot-style guards on the public CLI surface. Migrated from the
// former tests/cli_help.rs (which spawned the `retl` binary 18 times to
// do exactly this) into in-process clap help rendering — zero process
// spawn cost, same coverage.

#[test]
fn root_help_lists_all_subcommands() {
    let h = render_help(&[]);
    for s in [
        "describe",
        "scan",
        "dedupe",
        "export",
        "quickstart",
        "convert",
        "count",
        "integrity",
        "aggregate",
        "parents",
        "first-seen",
        "load",
    ] {
        assert!(h.contains(s), "root --help should mention `{s}`:\n{h}");
    }
}

#[test]
fn describe_help_advertises_discovery_flags() {
    let h = render_help(&["describe"]);
    for s in ["--data-dir", "--source", "--start", "--end"] {
        assert!(h.contains(s), "describe --help missing `{s}`:\n{h}");
    }
}

#[test]
fn quickstart_help_advertises_out_dir() {
    let h = render_help(&["quickstart"]);
    assert!(h.contains("--out-dir"));
}

#[test]
fn parents_help_advertises_required_flags() {
    let h = render_help(&["parents"]);
    for s in [
        "--spool",
        "--ids-file",
        "--parent-id",
        "--id-kind",
        "--cache",
        "--out",
        "--start",
        "--end",
        "--window-months",
        "--parent-fields",
        "--parent-full",
        "--resume",
        "--inflight-bytes",
        "--inflight-groups",
    ] {
        assert!(h.contains(s), "parents --help missing `{s}`:\n{h}");
    }
}

#[test]
fn first_seen_help_advertises_out() {
    let h = render_help(&["first-seen"]);
    assert!(h.contains("--out"));
    assert!(h.contains("--resume"));
}

#[test]
fn export_help_advertises_export_only_flags() {
    let h = render_help(&["export"]);
    for s in [
        "--zst-level",
        "--resume",
        "--whitelist",
        "--strict-whitelist",
        "--human-timestamps",
        "--inflight-bytes",
        "--inflight-groups",
    ] {
        assert!(h.contains(s), "export --help missing `{s}`:\n{h}");
    }
}

#[test]
fn dedupe_help_advertises_key_out_and_inflight() {
    let h = render_help(&["dedupe"]);
    for s in [
        "--key",
        "--out",
        "--inflight-bytes",
        "--inflight-groups",
        "--strict-key",
        "--resume",
        "json:/pointer",
    ] {
        assert!(h.contains(s), "dedupe --help missing `{s}`:\n{h}");
    }
}

#[test]
fn scan_help_advertises_common_flags() {
    let h = render_help(&["scan"]);
    for s in [
        "--data-dir",
        "--work-dir",
        "--start",
        "--end",
        "--parallelism",
        "--file-concurrency",
        "--no-progress",
        "--source",
        "--subreddit",
        "--id",
        "--ids-file",
        "--author",
        "--exclude-author",
        "--exclude-common-bots",
        "--author-regex",
        "--keyword",
        "--keyword-all",
        "--exclude-keyword",
        "--text-regex",
        "--min-score",
        "--max-score",
        "--after",
        "--before",
        "inclusive",
        "exclusive",
        "--contains-url",
        "--no-url",
        "--domain",
        "--json",
        "--include-deleted",
        "--resume",
    ] {
        assert!(h.contains(s), "scan --help missing `{s}`:\n{h}");
    }
    for s in ["--whitelist", "--strict-whitelist", "--human-timestamps"] {
        assert!(!h.contains(s), "scan --help should NOT mention `{s}`:\n{h}");
    }
}

#[test]
fn export_help_advertises_format_and_out() {
    let h = render_help(&["export"]);
    for s in [
        "--format",
        "jsonl",
        "json",
        "spool",
        "zst",
        "partitioned-jsonl",
        "--out",
        "--pretty",
        "Field-indent the JSON array",
    ] {
        assert!(h.contains(s), "export --help missing `{s}`:\n{h}");
    }
}

#[test]
fn convert_help_advertises_fields_spool_format_and_out() {
    let h = render_help(&["convert"]);
    for s in ["--field", "--spool", "--format", "csv", "tsv", "--out"] {
        assert!(h.contains(s), "convert --help missing `{s}`:\n{h}");
    }
}

#[test]
fn count_help_advertises_modes() {
    let h = render_help(&["count"]);
    for s in ["--mode", "month", "author", "--resume"] {
        assert!(h.contains(s), "count --help missing `{s}`:\n{h}");
    }
}

#[test]
fn integrity_help_advertises_modes_and_sample() {
    let h = render_help(&["integrity"]);
    for s in ["--mode", "quick", "full", "--sample-bytes", "--collect"] {
        assert!(h.contains(s), "integrity --help missing `{s}`:\n{h}");
    }
}

#[test]
fn load_help_advertises_target_flags() {
    let h = render_help(&["load"]);
    for s in [
        "--from",
        "--to",
        "--table",
        "--mode",
        "view",
        "table",
        "--if-exists",
        "fail",
        "replace",
        "append",
        "duckdb://",
    ] {
        assert!(h.contains(s), "load --help missing `{s}`:\n{h}");
    }
}

#[test]
fn aggregate_help_advertises_spool_inputs_out_and_runtime_flags_only() {
    let h = render_help(&["aggregate"]);
    for s in [
        "INPUTS",
        "--spool",
        "--out",
        "--by",
        "--metric",
        "--top",
        "--scientific",
        "--strict",
        "Field-indent the final JSON",
        "--parallelism",
        "--no-progress",
        "--shards-dir",
    ] {
        assert!(h.contains(s), "aggregate --help missing `{s}`:\n{h}");
    }
    for s in [
        "--data-dir",
        "--work-dir",
        "--start",
        "--end",
        "--source",
        "--subreddit",
        "--include-deleted",
        "--exclude-common-bots",
        "--file-concurrency",
    ] {
        assert!(
            !h.contains(s),
            "aggregate --help should NOT mention `{s}`:\n{h}"
        );
    }
}
