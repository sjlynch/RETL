use crate::paths::FileKind;
use crate::query::QuerySpec;
use serde_json::Value;

use super::ci::list_contains_ci;

/// Full-parse checks for arbitrary JSON-pointer predicates.
pub fn matches_full(val: &Value, kind: FileKind, q: &QuerySpec) -> bool {
    if let Some(ref domains) = q.domains_in {
        if let FileKind::Submission = kind {
            let d = val.get("domain").and_then(|v| v.as_str());
            match d {
                Some(dom) if list_contains_ci(domains, dom) => {}
                _ => return false,
            }
        } else {
            return false;
        }
    }

    q.json_predicates
        .iter()
        .all(|predicate| predicate.matches(val))
}
