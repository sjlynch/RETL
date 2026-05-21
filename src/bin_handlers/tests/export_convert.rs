use super::fixtures::{
    convert_args, count_args, decompress_zst, dedupe_args, export_args, make_basic_corpus_inline,
    sample_args,
};
use super::*;

// -----------------------------------------------------------------------
// Pure-error-path / pure-file-output tests migrated from
// tests/cli_export.rs. The tracing-stdout-warning tests stay as spawns
// because `tracing_subscriber` is process-global and capturing it
// in-process requires a separate per-test subscriber.
// -----------------------------------------------------------------------

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
fn export_csv_rejects_resume() {
    let cwd = tempfile::tempdir().unwrap();
    let args = export_args(&[
        "--data-dir",
        cwd.path().join("missing_data").to_str().unwrap(),
        "--format",
        "csv",
        "--whitelist",
        "id",
        "--resume",
        "--no-progress",
        "--out",
        cwd.path().join("out.csv").to_str().unwrap(),
    ]);
    let err = run_export(args).expect_err("--resume must be rejected for csv");
    let msg = format!("{err:#}");
    assert!(msg.contains("--resume is not supported"), "got: {msg}");
}

#[test]
fn export_tsv_rejects_human_timestamps() {
    let cwd = tempfile::tempdir().unwrap();
    let args = export_args(&[
        "--data-dir",
        cwd.path().join("missing_data").to_str().unwrap(),
        "--format",
        "tsv",
        "--whitelist",
        "id",
        "--human-timestamps",
        "--no-progress",
        "--out",
        cwd.path().join("out.tsv").to_str().unwrap(),
    ]);
    let err = run_export(args).expect_err("--human-timestamps must be rejected for tsv");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("--human-timestamps is not supported"),
        "got: {msg}"
    );
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
