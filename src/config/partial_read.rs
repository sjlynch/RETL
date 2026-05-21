
/// A single input file skipped because `allow_partial` tolerated a zstd
/// decode error.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SkippedFile {
    pub path: PathBuf,
    pub error: String,
}

/// Machine-readable snapshot of partial-read skips observed by a run.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct PartialReadReport {
    pub skipped_file_count: usize,
    pub skipped_files: Vec<SkippedFile>,
}

/// Shared collector for tolerated partial zstd reads.
///
/// Clone the handle before starting a consuming operation, then call
/// [`PartialReadReporter::snapshot`] afterwards to inspect the skipped paths.
///
/// The handle is cloned by `Arc`, so the same collector is shared across
/// `ETLOptions::clone()`. Each consuming operation calls [`Self::clear`]
/// before processing any month, so a snapshot reflects only the most recent
/// operation rather than accumulating skips across every run on a builder.
#[derive(Clone, Debug, Default)]
pub struct PartialReadReporter {
    inner: Arc<Mutex<Vec<SkippedFile>>>,
}

impl PartialReadReporter {
    pub fn record(&self, path: &Path, error: impl fmt::Display) {
        self.inner.lock().push(SkippedFile {
            path: path.to_path_buf(),
            error: error.to_string(),
        });
    }

    pub fn snapshot(&self) -> PartialReadReport {
        let skipped_files = self.inner.lock().clone();
        PartialReadReport {
            skipped_file_count: skipped_files.len(),
            skipped_files,
        }
    }

    /// Drop all recorded skips. Called at the start of each consuming
    /// operation so a reused builder's reporter does not accumulate skips
    /// across runs.
    pub fn clear(&self) {
        self.inner.lock().clear();
    }
}
