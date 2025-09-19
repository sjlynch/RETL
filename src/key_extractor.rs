use crate::zstd_jsonl::parse_minimal;
use serde_json::Value;
use std::sync::Arc;

/// A reusable key extractor with fast-paths for Reddit fields,
/// plus JSON-pointer and custom extractors.
///
/// Two calling styles:
///  - `key_from_value(&Value)`
///  - `key_from_line(&str)`
///
/// Notes:
///  - `author_lowercase_fast` / `subreddit_lowercase_fast` prefer `MinimalRecord`.
///  - `json_pointer("/user")` works for arbitrary JSON (serde parse).
pub enum KeyExtractor {
    AuthorLowerFast,
    SubredditLowerFast,
    JsonPointer(String),
    ByValue(Arc<dyn Fn(&Value) -> Option<String> + Send + Sync>),
}

impl KeyExtractor {
    pub fn author_lowercase_fast() -> Self { Self::AuthorLowerFast }
    pub fn subreddit_lowercase_fast() -> Self { Self::SubredditLowerFast }
    pub fn json_pointer(ptr: impl Into<String>) -> Self { Self::JsonPointer(ptr.into()) }
    pub fn by_value(f: impl Fn(&Value) -> Option<String> + Send + Sync + 'static) -> Self {
        Self::ByValue(Arc::new(f))
    }

    /// Extract the key from a full `serde_json::Value`.
    pub fn key_from_value(&self, v: &Value) -> Option<String> {
        match self {
            KeyExtractor::AuthorLowerFast => v.get("author").and_then(|x| x.as_str()).map(|s| s.to_lowercase()),
            KeyExtractor::SubredditLowerFast => v.get("subreddit").and_then(|x| x.as_str()).map(|s| s.to_lowercase()),
            KeyExtractor::JsonPointer(ptr) => v.pointer(ptr).and_then(|x| x.as_str()).map(|s| s.to_string()),
            KeyExtractor::ByValue(f) => f(v),
        }
    }

    /// Extract the key directly from a raw JSON line using the MinimalRecord fast path,
    /// or fallback to serde parsing for pointer/custom variants.
    pub fn key_from_line(&self, line: &str) -> Option<String> {
        match self {
            KeyExtractor::AuthorLowerFast => {
                let rec = parse_minimal(line).ok()?;
                rec.author.map(|s| s.to_lowercase())
            }
            KeyExtractor::SubredditLowerFast => {
                let rec = parse_minimal(line).ok()?;
                rec.subreddit.map(|s| s.to_lowercase())
            }
            KeyExtractor::JsonPointer(ptr) => {
                let v: Value = serde_json::from_str(line).ok()?;
                v.pointer(ptr).and_then(|x| x.as_str()).map(|s| s.to_string())
            }
            KeyExtractor::ByValue(f) => {
                let v: Value = serde_json::from_str(line).ok()?;
                f(&v)
            }
        }
    }
}

/// Convenience helper when you want to adapt a `KeyExtractor` to `Fn(&Value)->Option<String>`.
pub fn extractor_from_value_adapter(ex: &KeyExtractor) -> impl '_ + Fn(&Value) -> Option<String> {
    move |v| ex.key_from_value(v)
}

/// Convenience helper when you need a line-based closure (serde-only for pointer/custom).
pub fn extractor_from_line_adapter(ex: &KeyExtractor) -> impl '_ + Fn(&str) -> Option<String> {
    move |s| ex.key_from_line(s)
}
