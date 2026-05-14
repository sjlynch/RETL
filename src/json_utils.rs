use serde_json::Value;

/// Extract lowercased author from a JSON value.
pub fn author_lower(v: &Value) -> Option<String> {
    v.get("author")
        .and_then(|x| x.as_str())
        .map(|s| s.to_lowercase())
}

/// Extract lowercased subreddit from a JSON value.
pub fn subreddit_lower(v: &Value) -> Option<String> {
    v.get("subreddit")
        .and_then(|x| x.as_str())
        .map(|s| s.to_lowercase())
}

/// Heuristic: a record is a comment if it has a `body` and a `parent_id`.
pub fn is_comment_record(v: &Value) -> bool {
    v.get("body").is_some() && v.get("parent_id").is_some()
}

/// Parent-attach matching is intentionally more permissive than the historical
/// public [`is_comment_record`] helper: whitelist-stripped comments may omit
/// `body`, but a Reddit comment can still be identified by a canonical
/// `parent_id` prefix.
pub(crate) fn is_comment_record_for_parent_attach(v: &Value) -> bool {
    v.get("parent_id")
        .and_then(|x| x.as_str())
        .map(|parent_id| parent_id.starts_with("t1_") || parent_id.starts_with("t3_"))
        .unwrap_or(false)
}
