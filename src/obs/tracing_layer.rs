//! `tracing_subscriber::Layer` that forwards info+ events to an
//! [`EventSink`] as structured `kind=tracing` lines. Anything the library
//! emits via `tracing::{info,warn,error,debug}!` becomes a JSON line for
//! free — no library changes required.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use serde_json::Value;
use tracing::field::{Field, Visit};
use tracing::{Event as TracingEvent, Level, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

use super::events::{Event, EventKind, EVENT_SCHEMA, now_rfc3339};
use super::sink::SharedSink;
use super::status::StatusShared;

/// Custom layer; install alongside the standard fmt layer.
pub(crate) struct EventLayer {
    sink: SharedSink,
    status: Arc<StatusShared>,
}

impl EventLayer {
    pub(crate) fn new(sink: SharedSink, status: Arc<StatusShared>) -> Self {
        Self { sink, status }
    }
}

impl<S> Layer<S> for EventLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &TracingEvent<'_>, ctx: Context<'_, S>) {
        let meta = event.metadata();
        let level = level_str(*meta.level());

        // Capture fields into a Map.
        let mut visitor = JsonVisitor::default();
        event.record(&mut visitor);
        let JsonVisitor { fields, message } = visitor;

        let span_name = ctx
            .lookup_current()
            .map(|span_ref| span_ref.name().to_string());

        let msg = message.unwrap_or_default();
        let target: &'static str = meta.target();

        // Status side-channel: record the most-recent tracing event so the
        // snapshot file always reflects "what's happening now".
        self.status
            .record_event(EventKind::Tracing.as_str(), &msg);

        // Move fields into a serde_json::Map.
        let mut map = serde_json::Map::new();
        for (k, v) in fields.into_iter() {
            map.insert(k, v);
        }

        let ev = Event {
            ts: now_rfc3339(),
            schema: EVENT_SCHEMA,
            kind: EventKind::Tracing.as_str(),
            level,
            event: "",
            target,
            span: span_name,
            msg,
            fields: map,
        };
        self.sink.write(&ev);
    }
}

fn level_str(level: Level) -> &'static str {
    match level {
        Level::ERROR => "error",
        Level::WARN => "warn",
        Level::INFO => "info",
        Level::DEBUG => "debug",
        Level::TRACE => "trace",
    }
}

#[derive(Default)]
struct JsonVisitor {
    fields: BTreeMap<String, Value>,
    message: Option<String>,
}

impl JsonVisitor {
    fn insert(&mut self, field: &Field, value: Value) {
        let name = field.name();
        if name == "message" {
            // Stringify the message field separately so it lands in `msg`.
            if let Value::String(s) = value {
                self.message = Some(s);
            } else {
                self.message = Some(value.to_string());
            }
        } else {
            self.fields.insert(name.to_string(), value);
        }
    }
}

impl Visit for JsonVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.insert(field, Value::from(value));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.insert(field, Value::from(value));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.insert(field, Value::from(value));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.insert(field, Value::from(value));
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.insert(field, Value::from(value));
    }
    fn record_i128(&mut self, field: &Field, value: i128) {
        // serde_json doesn't represent i128 directly; downcast to string
        // to avoid silent precision loss.
        self.insert(field, Value::from(value.to_string()));
    }
    fn record_u128(&mut self, field: &Field, value: u128) {
        self.insert(field, Value::from(value.to_string()));
    }
    fn record_error(
        &mut self,
        field: &Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.insert(field, Value::from(value.to_string()));
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.insert(field, Value::from(format!("{:?}", value)));
    }
}
