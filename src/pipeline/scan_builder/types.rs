// ----------------- Advanced ScanPlan -----------------

pub struct ScanPlan {
    pub(crate) etl: RedditETL,
    pub(crate) query: QuerySpec,
    pub(crate) limit: Option<u64>,
}

/// Input accepted by [`ScanPlan::author_regex`]. Passing a raw pattern defers
/// compilation until [`ScanPlan::build`], so malformed regexes return a
/// structured [`QueryBuildError`] instead of panicking during builder construction.
#[doc(hidden)]
pub enum AuthorRegexInput {
    Compiled(Regex),
    Pattern(String),
}

#[doc(hidden)]
pub trait IntoAuthorRegex {
    fn into_author_regex(self) -> AuthorRegexInput;
}

impl IntoAuthorRegex for Regex {
    fn into_author_regex(self) -> AuthorRegexInput {
        AuthorRegexInput::Compiled(self)
    }
}

impl IntoAuthorRegex for &str {
    fn into_author_regex(self) -> AuthorRegexInput {
        AuthorRegexInput::Pattern(self.to_string())
    }
}

impl IntoAuthorRegex for String {
    fn into_author_regex(self) -> AuthorRegexInput {
        AuthorRegexInput::Pattern(self)
    }
}

impl IntoAuthorRegex for &String {
    fn into_author_regex(self) -> AuthorRegexInput {
        AuthorRegexInput::Pattern(self.clone())
    }
}
