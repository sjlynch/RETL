//! Behavioral coverage for score, keyword, URL, and domain filter branches.

#[path = "common/mod.rs"]
mod common;

#[path = "score_and_keyword_filters/domain_warnings.rs"]
mod domain_warnings;
#[path = "score_and_keyword_filters/keyword_matching.rs"]
mod keyword_matching;
#[path = "score_and_keyword_filters/score_bounds.rs"]
mod score_bounds;
#[path = "score_and_keyword_filters/url_filters.rs"]
mod url_filters;
