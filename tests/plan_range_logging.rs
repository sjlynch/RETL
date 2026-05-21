//! PART 3 regression: when a query carries BOTH an explicit date range and a
//! `created_utc` timestamp bound, the explicit range silently governs which
//! months are planned/scanned. File planning must log which range governed so
//! a user wondering why an `--after` scan reads more (or fewer) months than
//! expected has something to go on. Results stay correct either way — the
//! timestamp bound is still applied as a record-level filter.

#[path = "common/mod.rs"]
mod common;

use common::make_corpus_basic;
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

#[test]
fn explicit_date_range_with_timestamp_bound_logs_governing_range() {
    let base = make_corpus_basic();
    let logs = CaptureLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .with_writer(logs.clone())
        .finish();

    // Explicit start month + a `created_utc` lower bound that derives the same
    // side. Default `parallelism` (None) keeps planning on this thread, so the
    // thread-local subscriber installed by `with_default` sees the log.
    let counts = tracing::subscriber::with_default(subscriber, || {
        RedditETL::new()
            .base_dir(&base)
            .sources(Sources::Comments)
            .date_range(Some(YearMonth::new(2006, 1)), None)
            .progress(false)
            .scan()
            .subreddit("programming")
            .created_utc_gte(1_136_073_600)
            .count_by_month()
            .unwrap()
    });

    assert!(
        !counts.is_empty(),
        "the timestamp bound is record-level only; matching records must still survive"
    );

    let log = logs.contents();
    assert!(
        log.contains("file planning start month governed by the explicit date range"),
        "expected a log line naming which range governed file planning, got:\n{log}"
    );
}
