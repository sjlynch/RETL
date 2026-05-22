//! Regression: the deprecated `RedditETL::usernames` shim reads `subreddit`,
//! `parallelism`, `file_concurrency` and `allow_partial` from `ETLOptions` but
//! never reads `resume`. A caller that set `.resume(true)` and then used the
//! deprecated path silently got a full non-resumed re-scan. The shim now logs
//! a warning so that silent downgrade is visible; `ScanPlan::usernames` is the
//! resumable replacement.

#[path = "common/mod.rs"]
mod common;

use common::make_corpus_basic;
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
        String::from_utf8(self.buf.lock().unwrap().clone()).unwrap()
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

/// Run the deprecated `RedditETL::usernames` shim against the shared tiny
/// corpus, capturing WARN-level tracing output. Default `parallelism` (None)
/// keeps the shim on this thread, so the `with_default` subscriber sees the
/// warning, which is emitted before any thread pool is built.
fn run_deprecated_usernames(resume: bool) -> String {
    let base = make_corpus_basic();
    let logs = CaptureLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .with_writer(logs.clone())
        .finish();

    tracing::subscriber::with_default(subscriber, || {
        #[allow(deprecated)]
        let stream = RedditETL::new()
            .base_dir(&base)
            .sources(Sources::Both)
            .progress(false)
            .resume(resume)
            .subreddit("programming")
            .usernames()
            .expect("deprecated usernames() shim must still run");
        // Drain the stream so its run completes and scratch cleanup happens.
        let _: Vec<String> = stream.collect();
    });

    logs.contents()
}

#[test]
fn deprecated_usernames_warns_when_resume_is_requested() {
    let logs = run_deprecated_usernames(true);
    assert!(
        logs.contains("RedditETL::usernames() ignores resume"),
        "deprecated usernames() must warn when resume is requested, got: {logs:?}"
    );
}

#[test]
fn deprecated_usernames_is_quiet_when_resume_is_not_requested() {
    let logs = run_deprecated_usernames(false);
    assert!(
        !logs.contains("ignores resume"),
        "deprecated usernames() must not warn when resume was not requested, got: {logs:?}"
    );
}
