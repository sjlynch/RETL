/// Resolve target subreddits from both the deprecated ETL single-subreddit
/// default and the query-level multi-subreddit selector. When both are set,
/// the selections are merged rather than letting one silently override the
/// other. Returns None to indicate "all subreddits".
pub fn resolve_target_subs_from(
    etl_subreddit: &Option<String>,
    q_subreddits: &Option<Vec<String>>,
) -> Option<Vec<String>> {
    let mut v = Vec::new();
    if let Some(ref single) = etl_subreddit {
        v.push(single.clone().to_lowercase());
    }
    if let Some(ref subs) = q_subreddits {
        v.extend(subs.iter().cloned());
    }
    if v.is_empty() {
        return None;
    }
    v.sort();
    v.dedup();
    Some(v)
}
