use super::{CorpusManifest, CorpusManifestError};
use crate::corpus_manifest::types::SUPPORTED_MANIFEST_VERSION;
use std::io::Read;

const BUILTIN_MANIFEST_JSON: &str = include_str!("../../manifests/reddit_corpus_manifest.v1.json");

impl CorpusManifest {
    /// Parse the built-in conservative manifest.
    pub fn builtin() -> Result<Self, CorpusManifestError> {
        Self::from_json_str(BUILTIN_MANIFEST_JSON)
    }

    /// Return the built-in manifest JSON exactly as shipped with RETL.
    pub fn builtin_json() -> &'static str {
        BUILTIN_MANIFEST_JSON
    }

    pub fn from_json_str(raw: &str) -> Result<Self, CorpusManifestError> {
        let manifest: Self = serde_json::from_str(raw)?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn from_reader<R: Read>(reader: R) -> Result<Self, CorpusManifestError> {
        let manifest: Self = serde_json::from_reader(reader)?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<(), CorpusManifestError> {
        if self.version != SUPPORTED_MANIFEST_VERSION {
            return Err(CorpusManifestError::UnsupportedVersion {
                version: self.version,
            });
        }
        for source in &self.sources {
            if let (Some(first), Some(last)) = (source.first_month, source.last_month) {
                if first > last {
                    return Err(CorpusManifestError::InvalidSourceRange {
                        source: source.source,
                        first_month: first,
                        last_month: last,
                    });
                }
            }
            for range in &source.unavailable {
                if range.start > range.end {
                    return Err(CorpusManifestError::InvalidUnavailableRange {
                        source: source.source,
                        start: range.start,
                        end: range.end,
                    });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::corpus_manifest::CorpusSource;
    use crate::date::YearMonth;

    #[test]
    fn validate_rejects_inverted_source_month_range() {
        let err = CorpusManifest::from_json_str(
            r#"{
              "version": 1,
              "sources": [{
                "source": "rc",
                "first_month": "2020-06",
                "last_month": "2019-01"
              }]
            }"#,
        )
        .unwrap_err();

        match err {
            CorpusManifestError::InvalidSourceRange {
                source,
                first_month,
                last_month,
            } => {
                assert_eq!(source, CorpusSource::Rc);
                assert_eq!(first_month, YearMonth::new(2020, 6));
                assert_eq!(last_month, YearMonth::new(2019, 1));
            }
            other => panic!("expected InvalidSourceRange, got {other:?}"),
        }
    }

    #[test]
    fn validate_accepts_equal_first_and_last_month() {
        // A bounded one-month source is a legitimate (degenerate) range.
        CorpusManifest::from_json_str(
            r#"{
              "version": 1,
              "sources": [{
                "source": "rc",
                "first_month": "2020-01",
                "last_month": "2020-01"
              }]
            }"#,
        )
        .expect("first_month == last_month must validate");
    }

    #[test]
    fn validate_accepts_open_ended_range_with_only_one_bound() {
        // Only one bound set: nothing to compare, so this must not error.
        CorpusManifest::from_json_str(
            r#"{
              "version": 1,
              "sources": [{ "source": "rc", "first_month": "2020-06" }]
            }"#,
        )
        .expect("a source with only first_month set must validate");
    }

    #[test]
    fn builtin_manifest_validates() {
        CorpusManifest::builtin().expect("built-in manifest must validate");
    }
}
