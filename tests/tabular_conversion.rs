#[path = "common/mod.rs"]
mod common;

use common::write_zst_lines;
use retl::{convert_jsonl_to_csv, convert_jsonl_to_tsv, RedditETL, Sources, TabularExportOptions};
use serde_json::json;
use std::fs;

fn make_multiline_comment_corpus() -> std::path::PathBuf {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.keep();
    fs::create_dir_all(base.join("submissions")).unwrap();
    let rc_path = base.join("comments").join("RC_2006-01.zst");
    write_zst_lines(
        &rc_path,
        &[json!({
            "id":"c1",
            "author":"alice",
            "subreddit":"programming",
            "created_utc":1136073600_i64,
            "score":1,
            "body":"hello\nworld"
        })
        .to_string()],
    );
    base
}

#[test]
fn tsv_rejects_line_breaks_without_publishing_final_output_and_csv_quotes_them() {
    let base = make_multiline_comment_corpus();
    let tsv_out = base.join("comments.tsv");

    let err = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .progress(false)
        .scan()
        .extract_to_tsv(&tsv_out, ["id", "body"], Default::default())
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("line feed") && msg.contains("body") && msg.contains("--format csv"),
        "unexpected TSV error: {msg}"
    );
    assert!(
        !tsv_out.exists(),
        "failed TSV export must not publish a partial final file"
    );

    let csv_out = base.join("comments.csv");
    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .progress(false)
        .scan()
        .extract_to_csv(&csv_out, ["id", "body"], Default::default())
        .unwrap();
    let csv = fs::read_to_string(&csv_out).unwrap();
    assert!(csv.contains("c1,\"hello\nworld\"\r\n"), "CSV was {csv:?}");
}

#[test]
fn library_tabular_exports_reject_unsupported_timestamp_and_resume_options() {
    let base = make_multiline_comment_corpus();

    let human_err = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .progress(false)
        .timestamps_human_readable(true)
        .scan()
        .extract_to_csv(&base.join("human.csv"), ["id"], Default::default())
        .unwrap_err();
    assert!(
        human_err
            .to_string()
            .contains("--human-timestamps is not supported"),
        "unexpected error: {human_err}"
    );

    let resume_err = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .progress(false)
        .resume(true)
        .scan()
        .extract_to_tsv(&base.join("resume.tsv"), ["id"], Default::default())
        .unwrap_err();
    assert!(
        resume_err.to_string().contains("--resume is not supported"),
        "unexpected error: {resume_err}"
    );
}

#[test]
fn convert_parent_enriched_jsonl_to_csv_and_tsv_with_nested_fields() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("with_parent.jsonl");
    fs::write(
        &input,
        format!(
            "{}\n",
            json!({
                "id":"c1",
                "body":"child text",
                "parent": {
                    "kind":"t1",
                    "id":"p1",
                    "body":"parent text",
                    "author":"bob"
                }
            })
        ),
    )
    .unwrap();

    let csv_out = dir.path().join("parents.csv");
    convert_jsonl_to_csv(
        [&input],
        &csv_out,
        [
            "id",
            "body",
            "parent.kind",
            "parent.id",
            "parent.body",
            "parent.author",
        ],
        TabularExportOptions::default(),
    )
    .unwrap();
    let csv = fs::read_to_string(&csv_out).unwrap();
    assert_eq!(
        csv,
        "id,body,parent.kind,parent.id,parent.body,parent.author\r\nc1,child text,t1,p1,parent text,bob\r\n"
    );

    let tsv_out = dir.path().join("parents.tsv");
    convert_jsonl_to_tsv(
        [&input],
        &tsv_out,
        ["id", "/parent/author", "/parent/body"],
        TabularExportOptions::default(),
    )
    .unwrap();
    let tsv = fs::read_to_string(&tsv_out).unwrap();
    assert_eq!(
        tsv,
        "id\t/parent/author\t/parent/body\nc1\tbob\tparent text\n"
    );
}

#[test]
fn convert_rejects_zst_input_with_clear_capability_message() {
    // RETL's own corpus and partitioned-zst exports are .zst; pointing
    // `convert` at one used to surface a baffling "malformed JSON ... line 1"
    // from serde_json choking on raw compressed bytes.
    let base = make_multiline_comment_corpus();
    let zst_input = base.join("comments").join("RC_2006-01.zst");
    let err = convert_jsonl_to_csv(
        [&zst_input],
        &base.join("from_zst.csv"),
        ["id"],
        TabularExportOptions::default(),
    )
    .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("zstd-compressed")
            && msg.contains("plain JSONL")
            && msg.contains("RC_2006-01.zst"),
        "expected a clear zstd capability message, got: {msg}"
    );
    assert!(
        !msg.contains("malformed JSON"),
        "should not surface a JSON parse error: {msg}"
    );
}

#[test]
fn convert_rejects_zstd_magic_input_even_without_zst_extension() {
    // A zstd file mislabelled with a .jsonl extension is still caught via the
    // frame magic bytes, so the user never reaches the serde_json trap.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("mislabelled.jsonl");
    write_zst_lines(&input, &[json!({ "id": "c1" }).to_string()]);
    let err = convert_jsonl_to_tsv(
        [&input],
        &dir.path().join("out.tsv"),
        ["id"],
        TabularExportOptions::default(),
    )
    .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("zstd-compressed") && msg.contains("plain JSONL"),
        "expected zstd magic-byte detection, got: {msg}"
    );
}

#[test]
fn convert_malformed_json_reports_path_and_line() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("bad.jsonl");
    fs::write(&input, "{\"id\":\"ok\"}\n{bad json}\n").unwrap();
    let err = convert_jsonl_to_csv(
        [&input],
        &dir.path().join("bad.csv"),
        ["id"],
        TabularExportOptions::default(),
    )
    .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("bad.jsonl") && msg.contains("line 2"),
        "error lacked path/line context: {msg}"
    );
}
