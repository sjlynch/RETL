use crate::date::YearMonth;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
use std::path::PathBuf;

pub(crate) const SUPPORTED_MANIFEST_VERSION: u32 = 1;

/// Source selector used by corpus manifests.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CorpusSource {
    /// Reddit comments (`RC_YYYY-MM.zst`) under `comments/`.
    Rc,
    /// Reddit submissions (`RS_YYYY-MM.zst`) under `submissions/`.
    Rs,
}

impl CorpusSource {
    pub fn label(self) -> &'static str {
        match self {
            Self::Rc => "rc",
            Self::Rs => "rs",
        }
    }

    pub fn directory(self) -> &'static str {
        match self {
            Self::Rc => "comments",
            Self::Rs => "submissions",
        }
    }

    pub fn prefix(self) -> &'static str {
        match self {
            Self::Rc => "RC",
            Self::Rs => "RS",
        }
    }
}

/// Versioned manifest consumed by `retl corpus plan` and the optional
/// manifest checks on `describe` / `integrity`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CorpusManifest {
    /// Manifest schema version. RETL currently supports version 1.
    pub version: u32,
    /// Human-readable manifest name.
    #[serde(default)]
    pub name: Option<String>,
    /// Optional human-readable notes.
    #[serde(default)]
    pub description: Option<String>,
    /// Per-source defaults and file overrides.
    #[serde(default)]
    pub sources: Vec<CorpusSourceManifest>,
}

/// Per-source manifest defaults plus optional per-month overrides.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CorpusSourceManifest {
    pub source: CorpusSource,
    /// Destination directory under the corpus base dir.
    #[serde(default)]
    pub directory: Option<String>,
    /// File-name prefix, normally `RC` or `RS`.
    #[serde(default)]
    pub prefix: Option<String>,
    /// First known available month for this source.
    #[serde(default)]
    pub first_month: Option<YearMonth>,
    /// Last known available month for this source, if the manifest is bounded.
    #[serde(default)]
    pub last_month: Option<YearMonth>,
    /// URL template. Supports `{file_name}`, `{prefix}`, `{month}`, `{yyyy}`, `{mm}`.
    #[serde(default)]
    pub url_template: Option<String>,
    /// Torrent/search label template. Supports `{file_name}`, `{prefix}`, `{month}`, `{yyyy}`, `{mm}`.
    #[serde(default)]
    pub torrent_template: Option<String>,
    /// Inclusive unavailable ranges with reasons.
    #[serde(default)]
    pub unavailable: Vec<CorpusUnavailableRange>,
    /// Per-month file metadata overrides.
    #[serde(default)]
    pub files: Vec<CorpusManifestFile>,
}

/// Inclusive unavailable range.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CorpusUnavailableRange {
    pub start: YearMonth,
    pub end: YearMonth,
    #[serde(default)]
    pub reason: Option<String>,
}

/// Optional per-month metadata. Omitted fields fall back to the source defaults.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CorpusManifestFile {
    pub month: YearMonth,
    #[serde(default)]
    pub file_name: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub torrent: Option<String>,
    #[serde(default)]
    pub compressed_bytes: Option<u64>,
    #[serde(default)]
    pub sha256: Option<String>,
    /// Mark this specific source/month unavailable even if it falls within the
    /// source default range.
    #[serde(default)]
    pub unavailable: bool,
    #[serde(default)]
    pub note: Option<String>,
}

/// Availability of a requested source/month in the manifest.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CorpusAvailability {
    Available,
    Unavailable,
}

/// Local file status for an acquisition-plan item.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CorpusLocalStatus {
    Missing,
    Present {
        actual_bytes: u64,
        /// `None` means the manifest did not provide an expected byte count.
        size_matches: Option<bool>,
        /// `None` means checksum verification was not requested, the manifest
        /// did not provide a SHA-256 value, or computing the hash failed (in
        /// which case `sha256_error` is set).
        sha256_matches: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        sha256_actual: Option<String>,
        /// `Some` when checksum verification was requested but computing the
        /// SHA-256 failed (e.g. a transient read error mid-hash). The file is
        /// still present and may still have the expected size; this is a
        /// warning, not grounds for reporting the file inaccessible.
        #[serde(skip_serializing_if = "Option::is_none")]
        sha256_error: Option<String>,
    },
    Inaccessible {
        error: String,
    },
}

/// One row in a corpus acquisition plan.
#[derive(Clone, Debug, Serialize)]
pub struct CorpusPlanItem {
    pub source: CorpusSource,
    pub month: YearMonth,
    pub availability: CorpusAvailability,
    pub file_name: String,
    pub expected_path: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compressed_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub torrent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    pub local: CorpusLocalStatus,
}

impl CorpusPlanItem {
    pub fn needs_download(&self) -> bool {
        self.availability == CorpusAvailability::Available
            && matches!(self.local, CorpusLocalStatus::Missing)
    }

    pub fn has_local_problem(&self) -> bool {
        if self.availability == CorpusAvailability::Unavailable {
            return true;
        }
        match &self.local {
            CorpusLocalStatus::Missing | CorpusLocalStatus::Inaccessible { .. } => true,
            CorpusLocalStatus::Present {
                size_matches,
                sha256_matches,
                ..
            } => matches!(size_matches, Some(false)) || matches!(sha256_matches, Some(false)),
        }
    }
}

/// Errors raised while loading or planning from a corpus manifest.
#[derive(Debug)]
pub enum CorpusManifestError {
    Json(serde_json::Error),
    UnsupportedVersion {
        version: u32,
    },
    MissingSource {
        source: CorpusSource,
    },
    InvalidDateRange {
        start: YearMonth,
        end: YearMonth,
    },
    InvalidUnavailableRange {
        source: CorpusSource,
        start: YearMonth,
        end: YearMonth,
    },
    InvalidSourceRange {
        source: CorpusSource,
        first_month: YearMonth,
        last_month: YearMonth,
    },
}

impl fmt::Display for CorpusManifestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(e) => write!(f, "invalid corpus manifest JSON: {e}"),
            Self::UnsupportedVersion { version } => write!(
                f,
                "unsupported corpus manifest version {version}; supported version is {SUPPORTED_MANIFEST_VERSION}"
            ),
            Self::MissingSource { source } => {
                write!(f, "corpus manifest does not define source {}", source.label())
            }
            Self::InvalidDateRange { start, end } => {
                write!(f, "invalid date range: start {start} is after end {end}")
            }
            Self::InvalidUnavailableRange { source, start, end } => write!(
                f,
                "corpus manifest source {} has invalid unavailable range {start}..={end}",
                source.label()
            ),
            Self::InvalidSourceRange {
                source,
                first_month,
                last_month,
            } => write!(
                f,
                "corpus manifest source {} has first_month {first_month} after last_month {last_month}",
                source.label()
            ),
        }
    }
}

impl Error for CorpusManifestError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Json(e) => Some(e),
            _ => None,
        }
    }
}

impl From<serde_json::Error> for CorpusManifestError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}
