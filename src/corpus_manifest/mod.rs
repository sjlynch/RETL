//! Versioned corpus manifest and acquisition-plan helpers.
//!
//! The built-in manifest is intentionally conservative: it knows the canonical
//! RC/RS file names, destination directories, public-corpus start months, and
//! Academic Torrents search URLs. Projects that maintain verified byte counts or
//! SHA-256 values can pass their own JSON manifest with the same schema.

mod load;
mod local;
mod plan;
mod types;

pub use types::{
    CorpusAvailability, CorpusLocalStatus, CorpusManifest, CorpusManifestError, CorpusManifestFile,
    CorpusPlanItem, CorpusSource, CorpusSourceManifest, CorpusUnavailableRange,
};
