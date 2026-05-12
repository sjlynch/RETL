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
///  - `json_pointer("/user")` works for arbitrary JSON (serde parse), and
///    coerces pointed-to scalar values to text.
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
            KeyExtractor::JsonPointer(ptr) => v.pointer(ptr).and_then(json_pointer_value_to_key),
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
                v.pointer(ptr).and_then(json_pointer_value_to_key)
            }
            KeyExtractor::ByValue(f) => {
                let v: Value = serde_json::from_str(line).ok()?;
                f(&v)
            }
        }
    }
}

fn json_pointer_value_to_key(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(v).ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::KeyExtractor;
    use serde_json::json;

    #[test]
    fn json_pointer_coerces_non_string_scalar_values() {
        let key = KeyExtractor::json_pointer("/score");
        assert_eq!(key.key_from_line(r#"{"score":42}"#).as_deref(), Some("42"));
        assert_eq!(
            key.key_from_value(&json!({ "score": -7 })).as_deref(),
            Some("-7")
        );

        let key = KeyExtractor::json_pointer("/stickied");
        assert_eq!(
            key.key_from_line(r#"{"stickied":true}"#).as_deref(),
            Some("true")
        );
        assert_eq!(
            key.key_from_value(&json!({ "stickied": false })).as_deref(),
            Some("false")
        );
    }

    #[test]
    fn json_pointer_ignores_null_and_missing_values() {
        let key = KeyExtractor::json_pointer("/score");
        assert_eq!(key.key_from_line(r#"{"score":null}"#), None);
        assert_eq!(key.key_from_line(r#"{"other":42}"#), None);
    }
}
