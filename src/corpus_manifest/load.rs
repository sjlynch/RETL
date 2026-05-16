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
