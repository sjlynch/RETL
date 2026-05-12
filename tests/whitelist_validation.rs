mod common;

use common::make_corpus_n_records;
use retl::{RedditETL, Sources};
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
