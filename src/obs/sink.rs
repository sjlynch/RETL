//! Event sinks. `JsonlSink` writes one JSON object per line, line-buffered,
//! mutex-guarded so an `on_event` from any thread produces a complete line.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};

use super::events::Event;

/// Counter shared with the status snapshot writer so a watcher can detect
/// drops vs. silent stalls.
#[derive(Debug, Default)]
pub(crate) struct SinkCounters {
    pub(crate) emitted: AtomicU64,
    pub(crate) dropped: AtomicU64,
}

impl SinkCounters {
    pub(crate) fn emitted(&self) -> u64 {
        self.emitted.load(Ordering::Relaxed)
    }
    pub(crate) fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

pub(crate) trait EventSink: Send + Sync {
    fn write(&self, event: &Event);
    fn flush(&self);
    fn counters(&self) -> Arc<SinkCounters>;
}

pub(crate) type SharedSink = Arc<dyn EventSink>;

/// No-op sink used when `--events` isn't set. Counters still tick so
/// downstream code can be uniform.
pub(crate) struct NullSink {
    counters: Arc<SinkCounters>,
}

impl NullSink {
    pub(crate) fn new() -> Self {
        Self {
            counters: Arc::new(SinkCounters::default()),
        }
    }
}

impl EventSink for NullSink {
    fn write(&self, _event: &Event) {
        // Still count emissions so MonitorHandle drop can report a
        // total — useful for diagnostics even when no file is set.
        self.counters.emitted.fetch_add(1, Ordering::Relaxed);
    }
    fn flush(&self) {}
    fn counters(&self) -> Arc<SinkCounters> {
        self.counters.clone()
    }
}

/// File-backed NDJSON sink. One serialized JSON object per line. The file
/// is truncated on open — operators tailing it should expect a fresh start
/// per `retl` invocation.
pub(crate) struct JsonlSink {
    inner: Mutex<BufWriter<File>>,
    #[allow(dead_code)]
    path: PathBuf,
    counters: Arc<SinkCounters>,
}

impl JsonlSink {
    pub(crate) fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("creating events parent dir {}", parent.display()))?;
            }
        }
        let file = File::create(&path)
            .with_context(|| format!("creating events file {}", path.display()))?;
        let inner = Mutex::new(BufWriter::with_capacity(64 * 1024, file));
        Ok(Self {
            inner,
            path,
            counters: Arc::new(SinkCounters::default()),
        })
    }

    #[allow(dead_code)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl EventSink for JsonlSink {
    fn write(&self, event: &Event) {
        let line = match serde_json::to_string(event) {
            Ok(s) => s,
            Err(_) => {
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if guard.write_all(line.as_bytes()).is_err() || guard.write_all(b"\n").is_err() {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        // Line-flush so watchers see events promptly. BufWriter still
        // amortizes small writes per syscall when bursts arrive.
        let _ = guard.flush();
        self.counters.emitted.fetch_add(1, Ordering::Relaxed);
    }

    fn flush(&self) {
        if let Ok(mut g) = self.inner.lock() {
            let _ = g.flush();
        }
    }

    fn counters(&self) -> Arc<SinkCounters> {
        self.counters.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::super::events::{Event, LifecycleEvent};
    use super::*;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    #[test]
    fn jsonl_sink_writes_one_line_per_event() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("events.ndjson");
        let sink = JsonlSink::create(&path).unwrap();
        sink.write(&Event::lifecycle(
            LifecycleEvent::RunStart,
            "hi",
            BTreeMap::new(),
        ));
        sink.write(&Event::lifecycle(
            LifecycleEvent::RunSummary,
            "bye",
            BTreeMap::new(),
        ));
        sink.flush();
        let body = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2);
        for line in &lines {
            let _: serde_json::Value =
                serde_json::from_str(line).expect("each line must parse as JSON");
        }
        assert_eq!(sink.counters().emitted(), 2);
        assert_eq!(sink.counters().dropped(), 0);
    }

    #[test]
    fn null_sink_counts_emissions() {
        let sink = NullSink::new();
        let c = sink.counters();
        for _ in 0..5 {
            sink.write(&Event::lifecycle(
                LifecycleEvent::Heartbeat,
                "tick",
                BTreeMap::new(),
            ));
        }
        assert_eq!(c.emitted(), 5);
    }
}
