mod common;

use common::{make_corpus_n_records, write_zst_lines};
use retl::{RedditETL, Sources};
use serde_json::json;
use std::fs;
use std::io;
use std::sync::{Arc, Mutex};
use tracing_subscriber::fmt::MakeWriter;

#[derive(Clone, Default)]
struct CaptureLogs {
    buf: Arc<Mutex<Vec<u8>>>,
}

impl CaptureLogs {
    fn contents(&self) -> String {
        let bytes = self.buf.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }
}

struct CaptureWriter {
    buf: Arc<Mutex<Vec<u8>>>,
}

impl io::Write for CaptureWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.buf.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CaptureLogs {
    type Writer = CaptureWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CaptureWriter {
            buf: self.buf.clone(),
        }
    }
}

#[test]
fn typo_whitelist_warns_after_initial_sample_matches_zero_fields() {
    let base = make_corpus_n_records(100);
    let out = base.join("typo_whitelist.jsonl");
    let logs = CaptureLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .with_writer(logs.clone())
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        RedditETL::new()
            .base_dir(&base)
            .sources(Sources::Comments)
            .progress(false)
            .whitelist_fields(["not_a_real_field"])
            .scan()
            .extract_to_jsonl(&out)
            .unwrap();
    });

    let logs = logs.contents();
    assert!(
        logs.contains("--whitelist matched zero fields on the first 100 records"),
        "expected typo whitelist warning, got logs: {logs:?}"
    );
    assert!(logs.contains("Comments use `body`/`parent_id`/`link_id`"));
    assert!(logs.contains("submissions use `title`/`selftext`/`domain`"));
}

#[test]
fn small_typo_whitelist_warns_at_end_of_stream() {
    let base = make_corpus_n_records(1);
    let out = base.join("small_typo_whitelist.jsonl");
    let logs = CaptureLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .with_writer(logs.clone())
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        RedditETL::new()
            .base_dir(&base)
            .sources(Sources::Comments)
            .progress(false)
            .whitelist_fields(["not_a_real_field"])
            .scan()
            .extract_to_jsonl(&out)
            .unwrap();
    });

    let logs = logs.contents();
    assert!(
        logs.contains("--whitelist matched zero fields on all 1 records"),
        "expected small-sample typo whitelist warning, got logs: {logs:?}"
    );
}

#[test]
fn strict_whitelist_errors_on_one_record_and_publishes_no_output() {
    let base = make_corpus_n_records(1);
    let out = base.join("strict_one_record_typo.jsonl");

    let err = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .progress(false)
        .whitelist_fields(["not_a_real_field"])
        .strict_whitelist(true)
        .scan()
        .extract_to_jsonl(&out)
        .unwrap_err();

    let chain = format!("{err:#}");
    assert!(
        chain.contains("--whitelist matched zero fields on all 1 records"),
        "unexpected error: {chain}"
    );
    assert!(
        !out.exists(),
        "strict whitelist failure must not publish output"
    );
}

#[test]
fn strict_whitelist_errors_instead_of_warning() {
    let base = make_corpus_n_records(100);
    let out = base.join("strict_typo_whitelist.jsonl");

    let err = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .progress(false)
        .whitelist_fields(["not_a_real_field"])
        .strict_whitelist(true)
        .scan()
        .extract_to_jsonl(&out)
        .unwrap_err();

    let chain = format!("{err:#}");
    assert!(
        chain.contains("--whitelist matched zero fields on the first 100 records"),
        "unexpected error: {chain}"
    );
}

#[test]
fn strict_whitelist_title_passes_when_submissions_are_scanned_after_comments() {
    let base = common::make_corpus_basic();
    let out = base.join("both_title.jsonl");

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .progress(false)
        .whitelist_fields(["title"])
        .strict_whitelist(true)
        .scan()
        .extract_to_jsonl(&out)
        .expect("title should be observed in RS files even though RC files are planned first");

    let written = fs::read_to_string(&out).unwrap();
    assert!(written.contains("\"title\""), "output was {written:?}");
}

#[test]
fn mixed_valid_and_typo_whitelist_reports_the_missing_field() {
    let base = make_corpus_n_records(1);
    let out = base.join("mixed_typo_whitelist.jsonl");
    let logs = CaptureLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .with_writer(logs.clone())
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        RedditETL::new()
            .base_dir(&base)
            .sources(Sources::Comments)
            .progress(false)
            .whitelist_fields(["author", "autor_typo"])
            .scan()
            .extract_to_jsonl(&out)
            .unwrap();
    });

    let logs = logs.contents();
    assert!(
        logs.contains("autor_typo") && logs.contains("fields never matched"),
        "expected field-level typo warning, got logs: {logs:?}"
    );

    let strict_err = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .progress(false)
        .whitelist_fields(["author", "autor_typo"])
        .strict_whitelist(true)
        .scan()
        .extract_to_jsonl(&base.join("mixed_typo_strict.jsonl"))
        .unwrap_err();
    let chain = format!("{strict_err:#}");
    assert!(chain.contains("autor_typo"), "unexpected error: {chain}");
}

#[test]
fn tabular_strict_whitelist_counts_field_presence_not_rendered_cell_contents() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.keep();
    let rc_path = base.join("comments").join("RC_2006-01.zst");
    fs::create_dir_all(base.join("submissions")).unwrap();
    write_zst_lines(
        &rc_path,
        &[json!({
            "id":"c1", "author":"alice", "subreddit":"programming",
            "created_utc":1136073600_i64, "score":1, "distinguished": null,
            "body":""
        })
        .to_string()],
    );
    let out = base.join("null_cell.csv");

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .progress(false)
        .strict_whitelist(true)
        .scan()
        .extract_to_csv(&out, ["distinguished"], Default::default())
        .expect("present null field renders empty but must satisfy whitelist validation");

    let csv = fs::read_to_string(&out).unwrap();
    assert_eq!(csv, "distinguished\r\n\r\n");
}

/// Build a comment corpus where every record's `created_utc` key is written
/// using the escaped-unicode form `created_utc`. The JSON parser decodes
/// `c` to `c`, so the canonical key is `created_utc` — the same name
/// the whitelist compares against — but the raw bytes differ. This
/// exercises the tokenizer's escape-decoding branch end-to-end and confirms
/// the strict-whitelist verdict is decided on fast-path semantics (the
/// projected output contains the whitelisted field) rather than on raw byte
/// equality of the key.
fn make_corpus_with_escaped_unicode_key(n: usize) -> std::path::PathBuf {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.keep();
    let rc_path = base.join("comments").join("RC_2006-01.zst");
    fs::create_dir_all(rc_path.parent().unwrap()).unwrap();
    fs::create_dir_all(base.join("submissions")).unwrap();

    // Key bytes for the escaped-unicode form of `created_utc`. The leading
    // `c` JSON-decodes to `c`. Built as a regular Rust string so the
    // backslash is escaped at the Rust level and ends up literal in the
    // emitted JSON.
    let escaped_key = "\\u0063reated_utc";

    let mut lines: Vec<String> = Vec::with_capacity(n);
    for i in 0..n {
        let ts = 1136073600_i64 + i as i64;
        lines.push(format!(
            "{{\"id\":\"rc{i:08}\",\"author\":\"alice\",\"subreddit\":\"programming\",\"{escaped_key}\":{ts},\"body\":\"hi\"}}"
        ));
    }
    write_zst_lines(&rc_path, &lines);
    base
}

#[test]
fn strict_whitelist_verdict_follows_fast_path_for_escaped_unicode_key() {
    // Every record's `created_utc` key is written as `created_utc`, the
    // JSON escape that decodes to `created_utc`. The strict-whitelist
    // verdict must reflect the fast-path semantics (the projected output
    // contains the whitelisted field once the key escape is decoded), not
    // raw byte equality, so the run must succeed without warning.
    let base = make_corpus_with_escaped_unicode_key(100);
    let out = base.join("unicode_whitelist.jsonl");
    let logs = CaptureLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .with_writer(logs.clone())
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        RedditETL::new()
            .base_dir(&base)
            .sources(Sources::Comments)
            .progress(false)
            .whitelist_fields(["created_utc"])
            .strict_whitelist(true)
            .scan()
            .extract_to_jsonl(&out)
            .expect(
                "strict_whitelist must not fire when the projected output contains \
                 the whitelisted field, even when its key is escape-encoded",
            );
    });

    let captured = logs.contents();
    assert!(
        !captured.contains("--whitelist matched zero fields"),
        "zero-match warning must not fire when the escape-decoded key matches: {captured:?}"
    );

    // Sanity-check the projected output: every record contributed a line
    // and at least one carries the whitelisted field (raw escape bytes are
    // preserved verbatim by the fast tokenizer path).
    let written = fs::read_to_string(&out).unwrap();
    let nonempty_lines: Vec<&str> = written.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(nonempty_lines.len(), 100);
    assert!(
        nonempty_lines
            .iter()
            .any(|l| l.contains("created_utc") || l.contains("\\u0063reated_utc")),
        "projected output must contain the whitelisted key (raw or escaped): {nonempty_lines:?}"
    );
}

/// Names of published per-month spool parts (`part_RC_*.jsonl` /
/// `part_RS_*.jsonl`) currently in `out_dir`.
fn published_spool_parts(out_dir: &std::path::Path) -> Vec<String> {
    let Ok(entries) = fs::read_dir(out_dir) else {
        return Vec::new();
    };
    entries
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| {
            (name.starts_with("part_RC_") || name.starts_with("part_RS_"))
                && name.ends_with(".jsonl")
        })
        .collect()
}

/// Regression: a strict `--whitelist` failure on a *resumable* spool run must
/// not leave committed state (`_progress.json` + published parts) that a
/// resumed run would skip. The strict verdict is post-hoc — by the time the
/// tracker knows a field never matched, every month is already published and
/// recorded. Before the fix, a resumed run loaded `_progress.json`, skipped
/// every month, observed zero fresh records, and silently "succeeded" with
/// empty-projection output.
#[test]
fn strict_whitelist_spool_failure_leaves_no_resume_skippable_state() {
    let base = make_corpus_n_records(100);
    let out_dir = base.join("strict_spool_out");

    let run = || {
        RedditETL::new()
            .base_dir(&base)
            .sources(Sources::Comments)
            .progress(false)
            .resume(true)
            .whitelist_fields(["not_a_real_field"])
            .strict_whitelist(true)
            .scan()
            .extract_spool_monthly(&out_dir)
    };

    // First strict run: the whitelisted field never matches, so it fails.
    let err = run().unwrap_err();
    assert!(
        format!("{err:#}").contains("--whitelist matched zero fields"),
        "unexpected error: {err:#}"
    );

    // The failed strict run must leave nothing a resumed run would honor.
    assert!(
        !out_dir.join("_progress.json").exists(),
        "strict-whitelist failure left a resume manifest a resumed run would skip months from"
    );
    let parts = published_spool_parts(&out_dir);
    assert!(
        parts.is_empty(),
        "strict-whitelist failure left published spool parts behind: {parts:?}"
    );

    // The resumed run must re-stream every month and re-trigger the strict
    // check — not silently succeed by skipping months marked complete.
    let resumed_err = run().unwrap_err();
    assert!(
        format!("{resumed_err:#}").contains("--whitelist matched zero fields"),
        "resumed strict run skipped the affected months instead of re-failing: {resumed_err:#}"
    );
}

/// Regression: same guarantee for a resumable JSONL `extract`. `out_path` is
/// only stitched after the strict check, so a strict failure never publishes
/// it directly — but the cached `.part_*.jsonl` files and `_progress.json` in
/// the work dir would let a resumed run skip every month and stitch the bad
/// (empty-projection) output. The resumed run must re-fail instead.
#[test]
fn strict_whitelist_extract_failure_does_not_let_resume_stitch_bad_output() {
    let base = make_corpus_n_records(100);
    let out = base.join("strict_extract_out.jsonl");

    let run = || {
        RedditETL::new()
            .base_dir(&base)
            .sources(Sources::Comments)
            .progress(false)
            .resume(true)
            .whitelist_fields(["not_a_real_field"])
            .strict_whitelist(true)
            .scan()
            .extract_to_jsonl(&out)
    };

    let err = run().unwrap_err();
    assert!(
        format!("{err:#}").contains("--whitelist matched zero fields"),
        "unexpected error: {err:#}"
    );
    assert!(
        !out.exists(),
        "strict-whitelist failure published the final extract output"
    );

    // Resumed run must re-stream and re-fail, not skip cached months and
    // stitch the bad output into `out`.
    let resumed_err = run().unwrap_err();
    assert!(
        format!("{resumed_err:#}").contains("--whitelist matched zero fields"),
        "resumed strict extract skipped cached months instead of re-failing: {resumed_err:#}"
    );
    assert!(
        !out.exists(),
        "resumed strict-whitelist extract stitched bad output instead of re-failing"
    );
}

#[test]
fn empty_whitelist_after_normalization_is_rejected() {
    let base = make_corpus_n_records(1);
    let out = base.join("empty_whitelist.jsonl");

    let err = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .progress(false)
        .whitelist_fields([""])
        .scan()
        .extract_to_jsonl(&out)
        .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("--whitelist") && msg.contains("non-empty field"),
        "unexpected error: {msg}"
    );
}
