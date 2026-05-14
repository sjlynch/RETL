mod common;

use common::{make_corpus_n_records, write_zst_lines};
use retl::{RedditETL, Sources};
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
