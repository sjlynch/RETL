use super::local::inspect_local_file;
use super::{
    CorpusAvailability, CorpusManifest, CorpusManifestError, CorpusManifestFile, CorpusPlanItem,
    CorpusSource, CorpusSourceManifest,
};
use crate::config::Sources;
use crate::date::{iter_year_months, YearMonth};
use std::path::Path;

impl CorpusManifest {
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
            if !source_selected_by(source, sources) {
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

fn source_selected_by(source: CorpusSource, sources: Sources) -> bool {
    matches!(
        (source, sources),
        (CorpusSource::Rc, Sources::Comments | Sources::Both)
            | (CorpusSource::Rs, Sources::Submissions | Sources::Both)
    )
}

fn apply_template(template: &str, prefix: &str, month: YearMonth, file_name: &str) -> String {
    template
        .replace("{file_name}", file_name)
        .replace("{prefix}", prefix)
        .replace("{month}", &month.to_string())
        .replace("{yyyy}", &format!("{:04}", month.year))
        .replace("{mm}", &format!("{:02}", month.month))
}

#[cfg(test)]
mod tests {
    use super::super::CorpusLocalStatus;
    use super::*;
    use std::fs;

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
