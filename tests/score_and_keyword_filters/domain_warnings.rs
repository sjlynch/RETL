use crate::common::make_corpus_basic;
use retl::{RedditETL, Sources, YearMonth};
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
fn domains_in_filter_rejects_comments_and_warns_when_sources_include_comments() {
    let base = make_corpus_basic();
    let logs = CaptureLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .with_writer(logs.clone())
        .finish();

    let counts = tracing::subscriber::with_default(subscriber, || {
        RedditETL::new()
            .base_dir(&base)
            .sources(Sources::Both)
            .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
            .progress(false)
            .scan()
            .subreddit("programming")
            .domains_in(["example.com"])
            .count_by_month()
            .unwrap()
    });

    // Only the s1 submission has domain=example.com; comments are discarded.
    assert_eq!(counts.get(&YearMonth::new(2006, 1)).copied(), Some(1));

    let logs = logs.contents();
    assert!(
        logs.contains("domains_in filters Reddit's submission-only `domain` field"),
        "expected domains_in warning, got logs: {logs:?}"
    );
    assert!(logs.contains("comment records have no domain and will be dropped"));
}
