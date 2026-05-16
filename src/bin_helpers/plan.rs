
/// Build a `ScanPlan` from `etl` with common CLI scan selections applied.
/// Kept as a macro so the binary call sites stay concise while sharing the
/// same public builder surface external users get from `retl::ScanPlan`.
macro_rules! plan {
    ($etl:expr, $common:expr, $query:expr) => {{
        let common = &$common;
        let query = &$query;
        let mut scan = $etl.scan();
        if !common.subreddits.is_empty() {
            scan = scan.subreddits(common.subreddits.iter().map(String::as_str));
        }
        if !query.ids.is_empty() || !query.ids_files.is_empty() {
            let mut id_selectors = query.ids.clone();
            for ids_file in &query.ids_files {
                id_selectors.extend(retl::read_record_ids_file(ids_file)?);
            }
            scan = scan.ids_in(id_selectors.iter().map(String::as_str));
        }
        if !query.authors.is_empty() {
            scan = scan.authors_in(query.authors.iter().map(String::as_str));
        }
        if !query.exclude_authors.is_empty() {
            scan = scan.authors_out(query.exclude_authors.iter().map(String::as_str));
        }
        if query.exclude_common_bots {
            scan = scan.exclude_common_bots();
        }
        if let Some(author_regex) = &query.author_regex {
            scan = scan.author_regex(author_regex.as_str());
        }
        if !query.keywords.is_empty() {
            scan = scan.keywords_any(query.keywords.iter().map(String::as_str));
        }
        if !query.keywords_all.is_empty() {
            scan = scan.keywords_all(query.keywords_all.iter().map(String::as_str));
        }
        if !query.exclude_keywords.is_empty() {
            scan = scan.exclude_keywords(query.exclude_keywords.iter().map(String::as_str));
        }
        if let Some(text_regex) = &query.text_regex {
            scan = scan.text_regex(text_regex.as_str());
        }
        if let Some(min_score) = query.min_score {
            scan = scan.min_score(min_score);
        }
        if let Some(max_score) = query.max_score {
            scan = scan.max_score(max_score);
        }
        if query.after.is_some() || query.before.is_some() {
            scan = scan.timestamp_bounds(query.after, query.before);
        }
        if query.contains_url {
            scan = scan.contains_url(true);
        }
        if query.no_url {
            scan = scan.no_url();
        }
        if !query.domains.is_empty() {
            scan = scan.domains_in(query.domains.iter().map(String::as_str));
        }
        for json_predicate in &query.json_predicates {
            scan = scan.json_predicate($crate::bin_helpers::parse_json_predicate(json_predicate)?);
        }
        if common.include_deleted {
            scan = scan.include_pseudo_users();
        }
        scan
    }};
}
pub(crate) use plan;

// -----------------------------------------------------------------------------
// CLI-only path / I/O helpers.
// -----------------------------------------------------------------------------
