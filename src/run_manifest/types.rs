use crate::atomic_write::{ensure_staging_dir, write_jsonl_atomic};
use crate::config::{ETLOptions, PartialReadReport, Sources};
use crate::paths::{FileJob, FileKind};
use crate::query::QuerySpec;
use crate::util::{stable_fnv1a_hex, system_time_parts};
use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

pub const MANIFEST_SCHEMA_VERSION: u32 = 1;
pub const FILE_MANIFEST_SUFFIX: &str = ".retl-manifest.json";
pub const DIR_MANIFEST_NAME: &str = "_retl_manifest.json";
const MANIFEST_WRITE_BUF_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug)]
pub struct RunManifestStart {
    started_at: String,
    instant: Instant,
}

impl RunManifestStart {
    pub fn now() -> Self {
        Self {
            started_at: now_rfc3339(),
            instant: Instant::now(),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct GeneratedBy {
    pub name: &'static str,
    pub version: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_hash: Option<&'static str>,
}

#[derive(Clone, Debug, Serialize)]
pub struct FileIdentity {
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub month: Option<String>,
    pub exists: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified_unix_secs: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified_nanos: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CorpusSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comments_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub submissions_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sources: Option<String>,
    pub selected_files: Vec<FileIdentity>,
}

#[derive(Clone, Debug, Serialize)]
pub struct OutputSnapshot {
    pub path: String,
    pub kind: String,
    pub format: String,
    pub manifest_path: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResumeSnapshot {
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_fingerprint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_path: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct UpstreamManifest {
    pub path: String,
    pub exists: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RunManifest {
    pub schema_version: u32,
    pub generated_by: GeneratedBy,
    pub manifest_fingerprint: String,
    pub operation: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_operation: Option<String>,
    pub started_at: String,
    pub finished_at: String,
    pub duration_ms: u128,
    pub query: Value,
    pub options: Value,
    pub corpus: CorpusSnapshot,
    pub inputs: Vec<FileIdentity>,
    pub output: OutputSnapshot,
    pub counts: BTreeMap<String, u64>,
    pub partial_read: PartialReadReport,
    pub resume: ResumeSnapshot,
    pub warnings: Vec<String>,
    pub upstream_manifests: Vec<UpstreamManifest>,
}

#[derive(Clone, Debug)]
pub enum ManifestDestination {
    File(PathBuf),
    Directory(PathBuf),
}

#[derive(Clone, Debug)]
pub struct RunManifestInput {
    pub start: RunManifestStart,
    pub operation: String,
    pub command: Option<String>,
    pub api_operation: Option<String>,
    pub query: Value,
    pub options: Value,
    pub corpus: CorpusSnapshot,
    pub inputs: Vec<FileIdentity>,
    pub output_format: String,
    pub counts: BTreeMap<String, u64>,
    pub partial_read: PartialReadReport,
    pub resume_enabled: bool,
    pub checkpoint_fingerprint: Option<String>,
    pub checkpoint_path: Option<PathBuf>,
    pub warnings: Vec<String>,
    pub upstream_manifests: Vec<UpstreamManifest>,
}

impl RunManifestInput {
    pub fn new(operation: impl Into<String>) -> Self {
        Self {
            start: RunManifestStart::now(),
            operation: operation.into(),
            command: None,
            api_operation: None,
            query: Value::Null,
            options: Value::Null,
            corpus: CorpusSnapshot::empty(),
            inputs: Vec::new(),
            output_format: "unknown".to_string(),
            counts: BTreeMap::new(),
            partial_read: PartialReadReport::default(),
            resume_enabled: false,
            checkpoint_fingerprint: None,
            checkpoint_path: None,
            warnings: Vec::new(),
            upstream_manifests: Vec::new(),
        }
    }
}

impl CorpusSnapshot {
    pub fn empty() -> Self {
        Self {
            base_dir: None,
            comments_dir: None,
            submissions_dir: None,
            sources: None,
            selected_files: Vec::new(),
        }
    }
}
