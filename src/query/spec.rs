
/// High-level query/filter spec for advanced scans.
/// All string lists are matched case-insensitively (Unicode-aware for keywords;
/// subreddit/author/domain matching normalizes non-ASCII values via lowercase).
#[derive(Debug, Default)]
pub struct QuerySpec {
    pub subreddits: Option<Vec<String>>,
    /// Bare Reddit record IDs (without `t1_` / `t3_`) that match either source.
    /// Prefer [`ScanPlan::ids`](crate::ScanPlan::ids) /
    /// [`ScanPlan::ids_in`](crate::ScanPlan::ids_in) when accepting user input;
    /// those builders also understand prefixed fullnames and preserve the
    /// source constraint.
    pub ids_in: Option<Vec<String>>,
    pub(crate) comment_ids_in: Option<Vec<String>>,
    pub(crate) submission_ids_in: Option<Vec<String>>,
    pub authors_in: Option<Vec<String>>,
    pub authors_out: Option<Vec<String>>,
    pub(crate) authors_out_explicit: bool,
    /// When true, [`ScanPlan::build`](crate::ScanPlan::build) merges RETL's
    /// default bot/service deny-list and `ETL_EXCLUDE_AUTHORS*` augments into
    /// `authors_out`. Keeping this as intent until build time avoids
    /// order-dependent builder calls.
    pub exclude_common_bots: bool,
    /// Compiled author regex used by the hot-path filter. Builders that accept
    /// raw patterns also keep `author_regex_pattern` so invalid regexes can be
    /// surfaced as [`QueryBuildError`] from `ScanPlan::build()` instead of panicking
    /// during builder construction.
    pub author_regex: Option<Regex>,
    pub(crate) author_regex_pattern: Option<String>,
    pub min_score: Option<i64>,
    pub max_score: Option<i64>,
    /// Exact Unix timestamp bounds for the top-level `created_utc` field.
    /// Lower bound is inclusive; upper bound is exclusive.
    pub timestamp_bounds: TimestampBounds,
    /// Keep records where at least one keyword is present in `body`, `selftext`, or `title`.
    pub keywords_any: Option<Vec<String>>, // substring in body/selftext/title (case-insensitive)
    /// Keep records only when every keyword is present across `body`, `selftext`, and `title`.
    pub keywords_all: Option<Vec<String>>,
    /// Reject records when any excluded keyword is present in `body`, `selftext`, or `title`.
    pub keywords_exclude: Option<Vec<String>>,
    /// Regex matched against `body`, `selftext`, or `title` on the MinimalRecord fast path.
    pub text_regex: Option<Regex>,
    pub(crate) text_regex_pattern: Option<String>,
    /// Submissions only: matches the top-level `domain` field. Comments do not
    /// have this field and are rejected when the filter is active.
    pub domains_in: Option<Vec<String>>,
    /// Positive URL-presence filter. `Some(true)` keeps only records with
    /// http(s) in text or a link-submission `url` whose value starts with
    /// http(s). `Some(false)` is canonicalized to `None` during normalization;
    /// use [`QuerySpec::no_url`] / [`ScanPlan::no_url`](crate::ScanPlan::no_url)
    /// for the negative predicate.
    pub contains_url: Option<bool>,
    /// Negative URL-presence filter. When true, rejects records with http(s) in
    /// text or a link-submission `url` whose value starts with http(s).
    pub no_url: bool,
    /// Full-record predicates evaluated against arbitrary JSON Pointer paths.
    ///
    /// Regex predicates compile their pattern lazily (once, via an internal
    /// `OnceLock`) on first evaluation, so a directly-constructed `QuerySpec`
    /// no longer recompiles the regex per record. Prefer routing through
    /// [`ScanPlan::build`](crate::ScanPlan::build) (or calling
    /// [`QuerySpec::validate`]) anyway: that compiles eagerly and rejects an
    /// invalid pattern as a [`QueryBuildError`] up front, whereas the lazy
    /// path *panics* mid-scan on a pattern that never passed validation.
    pub json_predicates: Vec<JsonPointerPredicate>,
    pub filter_pseudo_users: bool, // exclude [deleted]/[removed]/empty author; default true

    // Lazily-built case-insensitive automatons over keyword families.
    // Built once per QuerySpec on first call to the corresponding accessor.
    pub(crate) compiled_keywords_any: OnceLock<Arc<AhoCorasick>>,
    pub(crate) compiled_keywords_all: OnceLock<Arc<AhoCorasick>>,
    pub(crate) compiled_keywords_exclude: OnceLock<Arc<AhoCorasick>>,
}

fn clone_keyword_cache(cache: &OnceLock<Arc<AhoCorasick>>) -> OnceLock<Arc<AhoCorasick>> {
    let cloned = OnceLock::new();
    if let Some(ac) = cache.get() {
        let _ = cloned.set(Arc::clone(ac));
    }
    cloned
}

impl Clone for QuerySpec {
    fn clone(&self) -> Self {
        Self {
            subreddits: self.subreddits.clone(),
            ids_in: self.ids_in.clone(),
            comment_ids_in: self.comment_ids_in.clone(),
            submission_ids_in: self.submission_ids_in.clone(),
            authors_in: self.authors_in.clone(),
            authors_out: self.authors_out.clone(),
            authors_out_explicit: self.authors_out_explicit,
            exclude_common_bots: self.exclude_common_bots,
            author_regex: self.author_regex.clone(),
            author_regex_pattern: self.author_regex_pattern.clone(),
            min_score: self.min_score,
            max_score: self.max_score,
            timestamp_bounds: self.timestamp_bounds,
            keywords_any: self.keywords_any.clone(),
            keywords_all: self.keywords_all.clone(),
            keywords_exclude: self.keywords_exclude.clone(),
            text_regex: self.text_regex.clone(),
            text_regex_pattern: self.text_regex_pattern.clone(),
            domains_in: self.domains_in.clone(),
            contains_url: self.contains_url,
            no_url: self.no_url,
            json_predicates: self.json_predicates.clone(),
            filter_pseudo_users: self.filter_pseudo_users,
            compiled_keywords_any: clone_keyword_cache(&self.compiled_keywords_any),
            compiled_keywords_all: clone_keyword_cache(&self.compiled_keywords_all),
            compiled_keywords_exclude: clone_keyword_cache(&self.compiled_keywords_exclude),
        }
    }
}

impl QuerySpec {
    /// Normalize to lowercase and sort list filters. Most string lists are
    /// deduplicated; record-ID filters intentionally keep duplicates so
    /// [`QuerySpec::validate`] can reject accidental repeated IDs.
    pub fn normalize(mut self) -> Self {
        let lower_sort_dedup = |v: &mut Option<Vec<String>>| {
            if let Some(list) = v.as_mut() {
                for s in list.iter_mut() {
                    *s = normalize_str(s);
                }
                list.sort();
                list.dedup();
            }
        };

        lower_sort_dedup(&mut self.subreddits);
        normalize_id_filters(&mut self);
        lower_sort_dedup(&mut self.authors_in);
        lower_sort_dedup(&mut self.authors_out);

        normalize_trim_lower_list(&mut self.keywords_any);
        normalize_trim_lower_list(&mut self.keywords_all);
        normalize_trim_lower_list(&mut self.keywords_exclude);
        normalize_trim_lower_list(&mut self.domains_in);

        // `contains_url(false)` is a no-op/clear request, not a negative URL
        // predicate. Canonicalize direct QuerySpec construction too so resume
        // fingerprints and selectivity match the builder default.
        if self.contains_url == Some(false) {
            self.contains_url = None;
        }

        // Reset any prior compiled caches; keywords may have changed shape.
        self.compiled_keywords_any = OnceLock::new();
        self.compiled_keywords_all = OnceLock::new();
        self.compiled_keywords_exclude = OnceLock::new();

        self
    }

    /// Validate contradictory or malformed query filters before a scan starts.
    /// Error messages name the offending field(s) so callers can surface them
    /// directly to users.
    pub fn validate(&self) -> Result<(), QueryBuildError> {
        if let (Some(min), Some(max)) = (self.min_score, self.max_score) {
            if min > max {
                return Err(QueryBuildError::new(format!(
                    "min_score ({min}) cannot be greater than max_score ({max})"
                )));
            }
        }
        self.timestamp_bounds.validate()?;

        validate_string_list_filter("subreddits", &self.subreddits)?;
        validate_id_list_filter("ids_in", &self.ids_in)?;
        validate_id_list_filter("comment_ids_in", &self.comment_ids_in)?;
        validate_id_list_filter("submission_ids_in", &self.submission_ids_in)?;
        validate_id_filter_overlaps(self)?;
        validate_string_list_filter("authors_in", &self.authors_in)?;
        validate_string_list_filter("authors_out", &self.authors_out)?;
        validate_string_list_filter("domains_in", &self.domains_in)?;
        validate_string_list_filter("keywords_any", &self.keywords_any)?;
        validate_string_list_filter("keywords_all", &self.keywords_all)?;
        validate_string_list_filter("keywords_exclude", &self.keywords_exclude)?;

        if self.contains_url == Some(true) && self.no_url {
            return Err(QueryBuildError::new(
                "contains_url and no_url cannot both be set; choose either --contains-url or --no-url",
            ));
        }

        validate_text_regex_filter(&self.text_regex_pattern, &self.text_regex)?;

        if let (Some(allow), Some(deny)) = (&self.authors_in, &self.authors_out) {
            if let Some(author) = allow.iter().find(|a| deny.iter().any(|d| d == *a)) {
                return Err(QueryBuildError::new(format!(
                    "authors_in and authors_out both contain '{author}'"
                )));
            }
        }

        if let Some(pattern) = &self.author_regex_pattern {
            validate_author_regex_pattern(pattern)?;
        }

        for predicate in &self.json_predicates {
            predicate.validate()?;
        }

        Ok(())
    }

    pub(crate) fn compile_author_regex(mut self) -> Result<Self, QueryBuildError> {
        if let Some(pattern) = &self.author_regex_pattern {
            validate_author_regex_pattern(pattern)?;
            let re = Regex::new(pattern).map_err(|e| {
                QueryBuildError::new(format!("author_regex is invalid: {e}; pattern={pattern:?}"))
            })?;
            self.author_regex = Some(re);
        }
        Ok(self)
    }

    pub(crate) fn compile_text_regex(mut self) -> Result<Self, QueryBuildError> {
        if let Some(pattern) = self.text_regex_pattern.clone() {
            validate_text_regex_pattern(&pattern)?;
            let re = Regex::new(&pattern).map_err(|e| {
                QueryBuildError::new(format!("text_regex is invalid: {e}; pattern={pattern:?}"))
            })?;
            self.text_regex = Some(re);
        }
        Ok(self)
    }

    pub(crate) fn compile_json_predicates(mut self) -> Result<Self, QueryBuildError> {
        self.json_predicates = self
            .json_predicates
            .into_iter()
            .map(JsonPointerPredicate::compile_regex)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(self)
    }

    /// Only JSON-pointer predicates require full-record parsing; all fixed
    /// filters, including `created_utc` timestamp bounds, are handled on the
    /// MinimalRecord fast path.
    pub fn requires_full_parse(&self) -> bool {
        !self.json_predicates.is_empty()
    }

    /// Returns a lazily-built case-insensitive Aho-Corasick automaton over
    /// `keywords_any`, or `None` when there are no keywords to match.
    /// The automaton is built once per `QuerySpec` and reused across records.
    pub fn keywords_automaton(&self) -> Option<&AhoCorasick> {
        self.keywords_any_automaton()
    }

    pub fn keywords_any_automaton(&self) -> Option<&AhoCorasick> {
        keyword_automaton_for(
            &self.keywords_any,
            &self.compiled_keywords_any,
            MatchKind::LeftmostFirst,
        )
    }

    pub(crate) fn keywords_all_automaton(&self) -> Option<&AhoCorasick> {
        keyword_automaton_for(
            &self.keywords_all,
            &self.compiled_keywords_all,
            MatchKind::Standard,
        )
    }

    pub(crate) fn keywords_exclude_automaton(&self) -> Option<&AhoCorasick> {
        keyword_automaton_for(
            &self.keywords_exclude,
            &self.compiled_keywords_exclude,
            MatchKind::LeftmostFirst,
        )
    }

    pub(crate) fn keywords_any_all_ascii(&self) -> bool {
        keyword_list_all_ascii(&self.keywords_any)
    }

    pub(crate) fn keywords_all_all_ascii(&self) -> bool {
        keyword_list_all_ascii(&self.keywords_all)
    }

    pub(crate) fn keywords_exclude_all_ascii(&self) -> bool {
        keyword_list_all_ascii(&self.keywords_exclude)
    }

    pub(crate) fn has_unqualified_id_selectors(&self) -> bool {
        self.ids_in.as_ref().is_some_and(|v| !v.is_empty())
    }

    pub(crate) fn has_comment_id_selectors(&self) -> bool {
        self.comment_ids_in.as_ref().is_some_and(|v| !v.is_empty())
    }

    pub(crate) fn has_submission_id_selectors(&self) -> bool {
        self.submission_ids_in
            .as_ref()
            .is_some_and(|v| !v.is_empty())
    }

    pub(crate) fn has_id_filters(&self) -> bool {
        self.has_unqualified_id_selectors()
            || self.has_comment_id_selectors()
            || self.has_submission_id_selectors()
    }

    pub(crate) fn id_source_hint(&self) -> Option<RecordIdKind> {
        if self.has_unqualified_id_selectors() {
            return None;
        }
        match (
            self.has_comment_id_selectors(),
            self.has_submission_id_selectors(),
        ) {
            (true, false) => Some(RecordIdKind::Comment),
            (false, true) => Some(RecordIdKind::Submission),
            _ => None,
        }
    }

    pub(crate) fn id_filter_matches(&self, kind: RecordIdKind, raw_id: Option<&str>) -> bool {
        if !self.has_id_filters() {
            return true;
        }
        let Some(raw_id) = raw_id else {
            return false;
        };
        let bare = normalize_record_id_value(raw_id);
        if bare.is_empty() {
            return false;
        }
        sorted_id_list_contains(self.ids_in.as_ref(), &bare)
            || match kind {
                RecordIdKind::Comment => {
                    sorted_id_list_contains(self.comment_ids_in.as_ref(), &bare)
                }
                RecordIdKind::Submission => {
                    sorted_id_list_contains(self.submission_ids_in.as_ref(), &bare)
                }
            }
    }

    pub(crate) fn json_predicates_fingerprint(&self) -> Vec<Value> {
        self.json_predicates
            .iter()
            .map(JsonPointerPredicate::fingerprint_value)
            .collect()
    }

    pub(crate) fn has_selective_filters(&self) -> bool {
        self.subreddits.as_ref().is_some_and(|v| !v.is_empty())
            || self.has_id_filters()
            || self.authors_in.as_ref().is_some_and(|v| !v.is_empty())
            || self.author_regex.is_some()
            || self.author_regex_pattern.is_some()
            || (self.authors_out_explicit
                && self.authors_out.as_ref().is_some_and(|v| !v.is_empty()))
            || self.min_score.is_some()
            || self.max_score.is_some()
            || self.timestamp_bounds.is_active()
            || self.keywords_any.as_ref().is_some_and(|v| !v.is_empty())
            || self.keywords_all.as_ref().is_some_and(|v| !v.is_empty())
            || self
                .keywords_exclude
                .as_ref()
                .is_some_and(|v| !v.is_empty())
            || self.text_regex.is_some()
            || self.text_regex_pattern.is_some()
            || self.domains_in.as_ref().is_some_and(|v| !v.is_empty())
            || self.contains_url == Some(true)
            || self.no_url
            || !self.json_predicates.is_empty()
    }
}
