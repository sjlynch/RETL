//! Versioned corpus manifest and acquisition-plan helpers.
//!
//! The built-in manifest is intentionally conservative: it knows the canonical
//! RC/RS file names, destination directories, public-corpus start months, and
//! Academic Torrents search URLs. Projects that maintain verified byte counts or
//! SHA-256 values can pass their own JSON manifest with the same schema.

use crate::config::Sources;
use crate::date::{iter_year_months, YearMonth};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

const BUILTIN_MANIFEST_JSON: &str = include_str!("../manifests/reddit_corpus_manifest.v1.json");
const SUPPORTED_MANIFEST_VERSION: u32 = 1;

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

    fn selected_by(self, sources: Sources) -> bool {
        matches!(
            (self, sources),
            (Self::Rc, Sources::Comments | Sources::Both)
                | (Self::Rs, Sources::Submissions | Sources::Both)
        )
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
        /// `None` means checksum verification was not requested or the manifest
        /// did not provide a SHA-256 value.
        sha256_matches: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        sha256_actual: Option<String>,
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

    pub fn source(&self, source: CorpusSource) -> Option<&CorpusSourceManifest> {
        self.sources.iter().find(|s| s.source == source)
    }

    /// Build a desired-vs-local acquisition plan for an inclusive month range.
    ///
    /// `verify_checksums` computes SHA-256 only for present files that have a
    /// manifest checksum. Leave it off for cheap planning over multi-GB files.
    pub fn plan(
        &self,
        sources: Sources,
        start: YearMonth,
        end: YearMonth,
        dest: &Path,
        verify_checksums: bool,
    ) -> Result<Vec<CorpusPlanItem>, CorpusManifestError> {
        if start > end {
            return Err(CorpusManifestError::InvalidDateRange { start, end });
        }
        let mut rows = Vec::new();
        for source in [CorpusSource::Rc, CorpusSource::Rs] {
            if !source.selected_by(sources) {
                continue;
            }
            let spec = self
                .source(source)
                .ok_or(CorpusManifestError::MissingSource { source })?;
            for ym in iter_year_months(start, end) {
                rows.push(spec.plan_month(ym, dest, verify_checksums));
            }
        }
        Ok(rows)
    }
}

impl CorpusSourceManifest {
    fn plan_month(&self, month: YearMonth, dest: &Path, verify_checksums: bool) -> CorpusPlanItem {
        let file = self.files.iter().find(|file| file.month == month);
        let prefix = self
            .prefix
            .as_deref()
            .unwrap_or_else(|| self.source.prefix());
        let file_name = file
            .and_then(|f| f.file_name.clone())
            .unwrap_or_else(|| format!("{prefix}_{month}.zst"));
        let directory = self
            .directory
            .as_deref()
            .unwrap_or_else(|| self.source.directory());
        let expected_path = dest.join(directory).join(&file_name);
        let compressed_bytes = file.and_then(|f| f.compressed_bytes);
        let sha256 = file.and_then(|f| f.sha256.clone());
        let url = file.and_then(|f| f.url.clone()).or_else(|| {
            self.url_template
                .as_ref()
                .map(|t| apply_template(t, prefix, month, &file_name))
        });
        let torrent = file.and_then(|f| f.torrent.clone()).or_else(|| {
            self.torrent_template
                .as_ref()
                .map(|t| apply_template(t, prefix, month, &file_name))
        });
        let (availability, note) = self.availability_note(month, file);
        let local = inspect_local_file(
            &expected_path,
            compressed_bytes,
            sha256.as_deref(),
            verify_checksums,
        );

        CorpusPlanItem {
            source: self.source,
            month,
            availability,
            file_name,
            expected_path,
            compressed_bytes,
            sha256,
            url,
            torrent,
            note,
            local,
        }
    }

    fn availability_note(
        &self,
        month: YearMonth,
        file: Option<&CorpusManifestFile>,
    ) -> (CorpusAvailability, Option<String>) {
        if let Some(file) = file {
            if file.unavailable {
                return (
                    CorpusAvailability::Unavailable,
                    file.note.clone().or_else(|| {
                        Some("manifest marks this source/month unavailable".to_string())
                    }),
                );
            }
        }
        if let Some(first) = self.first_month {
            if month < first {
                return (
                    CorpusAvailability::Unavailable,
                    Some(format!(
                        "{} is before first known available {} month {first}",
                        month,
                        self.source.label()
                    )),
                );
            }
        }
        if let Some(last) = self.last_month {
            if month > last {
                return (
                    CorpusAvailability::Unavailable,
                    Some(format!(
                        "{} is after last known available {} month {last}",
                        month,
                        self.source.label()
                    )),
                );
            }
        }
        for range in &self.unavailable {
            if range.start <= month && month <= range.end {
                return (
                    CorpusAvailability::Unavailable,
                    range.reason.clone().or_else(|| {
                        Some(format!(
                            "manifest marks {} month range {}..={} unavailable",
                            self.source.label(),
                            range.start,
                            range.end
                        ))
                    }),
                );
            }
        }
        (
            CorpusAvailability::Available,
            file.and_then(|f| f.note.clone()),
        )
    }
}

fn apply_template(template: &str, prefix: &str, month: YearMonth, file_name: &str) -> String {
    template
        .replace("{file_name}", file_name)
        .replace("{prefix}", prefix)
        .replace("{month}", &month.to_string())
        .replace("{yyyy}", &format!("{:04}", month.year))
        .replace("{mm}", &format!("{:02}", month.month))
}

fn inspect_local_file(
    path: &Path,
    expected_bytes: Option<u64>,
    expected_sha256: Option<&str>,
    verify_checksums: bool,
) -> CorpusLocalStatus {
    let meta = match fs::metadata(path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return CorpusLocalStatus::Missing,
        Err(e) => {
            return CorpusLocalStatus::Inaccessible {
                error: e.to_string(),
            }
        }
    };
    if !meta.is_file() {
        return CorpusLocalStatus::Inaccessible {
            error: "path exists but is not a file".to_string(),
        };
    }

    let actual_bytes = meta.len();
    let size_matches = expected_bytes.map(|expected| expected == actual_bytes);
    let (sha256_matches, sha256_actual) = if verify_checksums {
        match expected_sha256 {
            Some(expected) => match sha256_file(path) {
                Ok(actual) => (Some(actual.eq_ignore_ascii_case(expected)), Some(actual)),
                Err(e) => {
                    return CorpusLocalStatus::Inaccessible {
                        error: format!("failed to compute sha256: {e}"),
                    }
                }
            },
            None => (None, None),
        }
    } else {
        (None, None)
    };

    CorpusLocalStatus::Present {
        actual_bytes,
        size_matches,
        sha256_matches,
        sha256_actual,
    }
}

fn sha256_file(path: &Path) -> io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 128 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_manifest_marks_pre_corpus_comment_month_unavailable() {
        let manifest = CorpusManifest::builtin().unwrap();
        let rows = manifest
            .plan(
                Sources::Comments,
                YearMonth::new(2005, 11),
                YearMonth::new(2005, 12),
                Path::new("data"),
                false,
            )
            .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].availability, CorpusAvailability::Unavailable);
        assert_eq!(rows[0].file_name, "RC_2005-11.zst");
        assert_eq!(rows[1].availability, CorpusAvailability::Available);
        assert_eq!(
            rows[1].expected_path,
            Path::new("data/comments/RC_2005-12.zst")
        );
    }

    #[test]
    fn custom_manifest_size_and_checksum_are_checked_when_requested() {
        let manifest = CorpusManifest::from_json_str(
            r#"{
              "version": 1,
              "sources": [{
                "source": "rc",
                "directory": "comments",
                "prefix": "RC",
                "files": [{
                  "month": "2020-01",
                  "compressed_bytes": 3,
                  "sha256": "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
                }]
              }]
            }"#,
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let comments = dir.path().join("comments");
        fs::create_dir(&comments).unwrap();
        fs::write(comments.join("RC_2020-01.zst"), b"abc").unwrap();

        let rows = manifest
            .plan(
                Sources::Comments,
                YearMonth::new(2020, 1),
                YearMonth::new(2020, 1),
                dir.path(),
                true,
            )
            .unwrap();

        match &rows[0].local {
            CorpusLocalStatus::Present {
                actual_bytes,
                size_matches,
                sha256_matches,
                ..
            } => {
                assert_eq!(*actual_bytes, 3);
                assert_eq!(*size_matches, Some(true));
                assert_eq!(*sha256_matches, Some(true));
            }
            other => panic!("unexpected local status: {other:?}"),
        }
    }
}
