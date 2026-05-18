//! Event types emitted on the NDJSON event stream.
//!
//! Every line on the stream serializes one [`Event`]. The on-disk schema is
//! versioned by the `schema` field (`retl.v1`); additive changes preserve
//! compatibility, removals or renames bump the version.
//!
//! An LLM watcher prompted against `retl.v1` should always be able to read
//! at least `{ts, schema, kind, msg}` from every line.

use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

pub(crate) const EVENT_SCHEMA: &str = "retl.v1";
pub(crate) const STATUS_SCHEMA: &str = "retl.status.v1";

/// Top-level partition of the event stream. A watcher filtering for cap
/// breaches need only read `kind == "watchdog"`; one filtering for
/// lifecycle bookends reads `kind == "lifecycle"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    /// Explicit lifecycle marker emitted by the monitor itself (run start,
    /// knobs echo, run summary). Carries a stable [`LifecycleEvent`] name in
    /// the `event` field.
    Lifecycle,
    /// Forwarded from `tracing::{info,warn,error}` calls in the library or
    /// binary. Free-form `target` + `msg` + arbitrary fields.
    Tracing,
    /// Periodic status snapshot mirrored from the status file.
    Status,
    /// Watchdog signals — cap breaches, stop-file detection, shutdown.
    Watchdog,
}

impl EventKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            EventKind::Lifecycle => "lifecycle",
            EventKind::Tracing => "tracing",
            EventKind::Status => "status",
            EventKind::Watchdog => "watchdog",
        }
    }
}

/// Stable lifecycle event names. Watchers should pattern-match these
/// strings rather than the free-form `msg`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleEvent {
    /// First event after monitor install. Carries pid + start timestamp.
    RunStart,
    /// Resolved configuration echoed once at startup so watchers can
    /// confirm the effective query/knobs.
    RunKnobs,
    /// Periodic heartbeat (status snapshot mirror).
    Heartbeat,
    /// Final event before the monitor exits. Carries outcome + counters.
    RunSummary,
}

impl LifecycleEvent {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            LifecycleEvent::RunStart => "run.start",
            LifecycleEvent::RunKnobs => "run.knobs",
            LifecycleEvent::Heartbeat => "heartbeat",
            LifecycleEvent::RunSummary => "run.summary",
        }
    }
}

/// One event line on the NDJSON stream. Public for downstream watchers that
/// want to deserialize directly; constructors are crate-private so the
/// versioning contract stays under our control.
#[derive(Debug, Clone, Serialize)]
pub struct Event {
    /// ISO-8601 / RFC3339 timestamp with millisecond precision, UTC.
    pub ts: String,
    /// Versioned schema identifier (`retl.v1`).
    pub schema: &'static str,
    /// Event partition; see [`EventKind`].
    pub kind: &'static str,
    /// `info` / `warn` / `error` / `debug` / `trace`.
    pub level: &'static str,
    /// For lifecycle/watchdog events: a stable event name like
    /// `run.start`, `watchdog.rss_exceeded`. Empty for `tracing` /
    /// `status` lines.
    #[serde(skip_serializing_if = "str::is_empty")]
    pub event: &'static str,
    /// Tracing target (`module::path`). Empty for non-tracing events.
    #[serde(skip_serializing_if = "str::is_empty")]
    pub target: &'static str,
    /// Innermost active span name, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span: Option<String>,
    /// Human-readable message. For tracing events this is the format
    /// string + interpolated args.
    pub msg: String,
    /// Arbitrary structured fields. For tracing events, the visitor
    /// captures all `key = value` pairs.
    #[serde(skip_serializing_if = "Map::is_empty")]
    pub fields: Map<String, Value>,
}

pub(crate) fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

impl Event {
    pub(crate) fn lifecycle(
        ev: LifecycleEvent,
        msg: impl Into<String>,
        fields: BTreeMap<String, Value>,
    ) -> Self {
        Self {
            ts: now_rfc3339(),
            schema: EVENT_SCHEMA,
            kind: EventKind::Lifecycle.as_str(),
            level: "info",
            event: ev.as_str(),
            target: "",
            span: None,
            msg: msg.into(),
            fields: btree_to_map(fields),
        }
    }

    pub(crate) fn watchdog(
        event_name: &'static str,
        level: &'static str,
        msg: impl Into<String>,
        fields: BTreeMap<String, Value>,
    ) -> Self {
        Self {
            ts: now_rfc3339(),
            schema: EVENT_SCHEMA,
            kind: EventKind::Watchdog.as_str(),
            level,
            event: event_name,
            target: "",
            span: None,
            msg: msg.into(),
            fields: btree_to_map(fields),
        }
    }

    pub(crate) fn status_mirror(snapshot_json: Value) -> Self {
        let mut fields = Map::new();
        if let Value::Object(obj) = snapshot_json {
            for (k, v) in obj {
                fields.insert(k, v);
            }
        }
        Self {
            ts: now_rfc3339(),
            schema: EVENT_SCHEMA,
            kind: EventKind::Status.as_str(),
            level: "info",
            event: LifecycleEvent::Heartbeat.as_str(),
            target: "",
            span: None,
            msg: String::new(),
            fields,
        }
    }
}

fn btree_to_map(b: BTreeMap<String, Value>) -> Map<String, Value> {
    b.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_event_serializes_minimum_fields() {
        let ev = Event::lifecycle(LifecycleEvent::RunStart, "starting", BTreeMap::new());
        let s = serde_json::to_string(&ev).unwrap();
        assert!(s.contains("\"schema\":\"retl.v1\""));
        assert!(s.contains("\"kind\":\"lifecycle\""));
        assert!(s.contains("\"event\":\"run.start\""));
        assert!(s.contains("\"msg\":\"starting\""));
        // No empty target string in serialized form.
        assert!(!s.contains("\"target\":\"\""));
    }

    #[test]
    fn watchdog_event_carries_event_name() {
        let mut f = BTreeMap::new();
        f.insert("rss_mb".to_string(), Value::from(1024u64));
        let ev = Event::watchdog("watchdog.rss_exceeded", "error", "RSS over cap", f);
        let s = serde_json::to_string(&ev).unwrap();
        assert!(s.contains("\"event\":\"watchdog.rss_exceeded\""));
        assert!(s.contains("\"rss_mb\":1024"));
        assert!(s.contains("\"level\":\"error\""));
    }

    #[test]
    fn status_mirror_unpacks_object_into_fields() {
        let snap = serde_json::json!({"current_rss_mb": 412, "elapsed_sec": 12.5});
        let ev = Event::status_mirror(snap);
        let s = serde_json::to_string(&ev).unwrap();
        assert!(s.contains("\"current_rss_mb\":412"));
        assert!(s.contains("\"elapsed_sec\":12.5"));
        assert!(s.contains("\"kind\":\"status\""));
    }

    #[test]
    fn rfc3339_now_parses_back() {
        let ts = now_rfc3339();
        // Smoke test: parseable.
        let _ = OffsetDateTime::parse(&ts, &Rfc3339).expect("rfc3339 should round-trip");
    }
}
