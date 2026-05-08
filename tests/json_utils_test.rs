//! Direct tests for the small `json_utils` helpers: `author_lower`,
//! `subreddit_lower`, `is_comment_record`. These are publicly re-exported but
//! until now had no direct coverage.

use retl::{author_lower, is_comment_record, subreddit_lower};
use serde_json::json;

#[test]
fn author_lower_returns_lowercase_when_present_as_string() {
    let v = json!({"author": "AliceBob"});
    assert_eq!(author_lower(&v).as_deref(), Some("alicebob"));
}

#[test]
fn author_lower_handles_already_lowercase_input() {
    let v = json!({"author": "alice"});
    assert_eq!(author_lower(&v).as_deref(), Some("alice"));
}

#[test]
fn author_lower_returns_none_when_field_missing() {
    let v = json!({"subreddit": "programming"});
    assert!(author_lower(&v).is_none());
}

#[test]
fn author_lower_returns_none_when_field_is_not_a_string() {
    // `author` set to a number or null shouldn't crash; helper only takes strings.
    assert!(author_lower(&json!({"author": null})).is_none());
    assert!(author_lower(&json!({"author": 42})).is_none());
    assert!(author_lower(&json!({"author": true})).is_none());
    assert!(author_lower(&json!({"author": ["array"]})).is_none());
}

#[test]
fn author_lower_lowercases_non_ascii_when_possible() {
    // "Δ" (Greek capital delta) lowercases to "δ" via Rust's str::to_lowercase.
    let v = json!({"author": "ΔELTA"});
    assert_eq!(author_lower(&v).as_deref(), Some("δelta"));
}

#[test]
fn subreddit_lower_returns_lowercase_when_present_as_string() {
    let v = json!({"subreddit": "AskScience"});
    assert_eq!(subreddit_lower(&v).as_deref(), Some("askscience"));
}

#[test]
fn subreddit_lower_returns_none_when_missing_or_non_string() {
    assert!(subreddit_lower(&json!({"author": "alice"})).is_none());
    assert!(subreddit_lower(&json!({"subreddit": 7})).is_none());
    assert!(subreddit_lower(&json!({"subreddit": null})).is_none());
}

#[test]
fn is_comment_record_requires_both_body_and_parent_id() {
    // Comment-shaped record (RC): has both
    let comment = json!({
        "body": "hi", "parent_id": "t3_abc", "author": "alice"
    });
    assert!(is_comment_record(&comment));

    // Submission-shaped record (RS): has neither
    let submission = json!({
        "title": "hi", "selftext": "", "author": "bob", "domain": "x.com"
    });
    assert!(!is_comment_record(&submission));

    // Has body but no parent_id (e.g., a self-post might have body in some schemas)
    let body_only = json!({"body": "x", "author": "c"});
    assert!(!is_comment_record(&body_only));

    // Has parent_id but no body
    let parent_only = json!({"parent_id": "t1_abc", "author": "c"});
    assert!(!is_comment_record(&parent_only));

    // Empty object
    assert!(!is_comment_record(&json!({})));
}

#[test]
fn is_comment_record_does_not_inspect_field_values() {
    // Helper only checks presence — values can be null and it still matches.
    // This is documented behavior in the helper.
    let v = json!({"body": null, "parent_id": null});
    assert!(is_comment_record(&v));
}
