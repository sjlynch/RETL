//! User-facing run provenance manifests.
//!
//! These sidecars are separate from RETL's resumable `_progress.json`: they are
//! stable, machine-readable descriptions of a completed user-facing output.

use crate::atomic_write::{ensure_staging_dir, write_jsonl_atomic};
use crate::config::{ETLOptions, PartialReadReport, Sources};
use crate::paths::{FileJob, FileKind};
use crate::query::QuerySpec;
use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
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

pub fn manifest_path_for_file(path: &Path) -> PathBuf {
    let mut sidecar = OsString::from(path.as_os_str());
    sidecar.push(FILE_MANIFEST_SUFFIX);
    PathBuf::from(sidecar)
}

pub fn manifest_path_for_directory(dir: &Path) -> PathBuf {
    dir.join(DIR_MANIFEST_NAME)
}

pub fn write_run_manifest(
    input: RunManifestInput,
    destination: ManifestDestination,
) -> Result<PathBuf> {
    let (output_path, output_kind, manifest_path) = match destination {
        ManifestDestination::File(path) => {
            let manifest_path = manifest_path_for_file(&path);
            (path, "file".to_string(), manifest_path)
        }
        ManifestDestination::Directory(path) => {
            let manifest_path = manifest_path_for_directory(&path);
            (path, "directory".to_string(), manifest_path)
        }
    };

    let finished_at = now_rfc3339();
    let output = OutputSnapshot {
        path: path_to_stable_string(&output_path),
        kind: output_kind,
        format: input.output_format,
        manifest_path: path_to_stable_string(&manifest_path),
    };
    let resume = ResumeSnapshot {
        enabled: input.resume_enabled,
        checkpoint_fingerprint: input.checkpoint_fingerprint,
        checkpoint_path: input
            .checkpoint_path
            .as_ref()
            .map(|path| path_to_stable_string(path)),
    };

    let fingerprint = manifest_fingerprint(
        &input.operation,
        input.command.as_deref(),
        input.api_operation.as_deref(),
        &input.query,
        &input.options,
        &input.corpus,
        &input.inputs,
        &output,
        &input.counts,
        &resume,
    )?;

    let mut warnings = input.warnings;
    if input.partial_read.skipped_file_count > 0 {
        warnings.push(format!(
            "allow_partial skipped {} input file(s); see partial_read.skipped_files",
            input.partial_read.skipped_file_count
        ));
    }

    let manifest = RunManifest {
        schema_version: MANIFEST_SCHEMA_VERSION,
        generated_by: GeneratedBy {
            name: "retl",
            version: env!("CARGO_PKG_VERSION"),
            git_hash: git_hash(),
        },
        manifest_fingerprint: fingerprint,
        operation: input.operation,
        command: input.command,
        api_operation: input.api_operation,
        started_at: input.start.started_at,
        finished_at,
        duration_ms: input.start.instant.elapsed().as_millis(),
        query: input.query,
        options: input.options,
        corpus: input.corpus,
        inputs: input.inputs,
        output,
        counts: input.counts,
        partial_read: input.partial_read,
        resume,
        warnings,
        upstream_manifests: input.upstream_manifests,
    };

    let parent = manifest_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let staging_dir = ensure_staging_dir(parent)
        .with_context(|| format!("creating manifest staging dir under {}", parent.display()))?;
    write_jsonl_atomic(
        &staging_dir,
        &manifest_path,
        MANIFEST_WRITE_BUF_BYTES,
        |w| {
            serde_json::to_writer_pretty(&mut *w, &manifest)?;
            w.write_all(b"\n")?;
            Ok(())
        },
    )
    .with_context(|| format!("writing run manifest {}", manifest_path.display()))?;

    Ok(manifest_path)
}

pub(crate) fn maybe_write_run_manifest(
    emit: bool,
    input: RunManifestInput,
    destination: ManifestDestination,
) -> Result<Option<PathBuf>> {
    if !emit {
        return Ok(None);
    }
    write_run_manifest(input, destination).map(Some)
}

pub(crate) fn corpus_snapshot_from_etl(etl: &ETLOptions, files: &[FileJob]) -> CorpusSnapshot {
    CorpusSnapshot {
        base_dir: Some(path_to_stable_string(&etl.base_dir)),
        comments_dir: Some(path_to_stable_string(&etl.comments_dir)),
        submissions_dir: Some(path_to_stable_string(&etl.submissions_dir)),
        sources: Some(sources_label(etl.sources).to_string()),
        selected_files: selected_file_identities(files),
    }
}

pub(crate) fn selected_file_identities(files: &[FileJob]) -> Vec<FileIdentity> {
    let mut out: Vec<FileIdentity> = files
        .iter()
        .map(|job| {
            let kind = match job.kind {
                FileKind::Comment => "comment",
                FileKind::Submission => "submission",
            };
            file_identity_with_context(&job.path, Some(kind.to_string()), Some(job.ym.to_string()))
        })
        .collect();
    out.sort_by(|a, b| {
        a.kind
            .cmp(&b.kind)
            .then_with(|| a.month.cmp(&b.month))
            .then_with(|| a.path.cmp(&b.path))
    });
    out
}

pub fn file_identities(paths: &[PathBuf]) -> Vec<FileIdentity> {
    let mut out: Vec<FileIdentity> = paths
        .iter()
        .map(|path| file_identity_with_context(path, None, None))
        .collect();
    out.sort_by(|a, b| a.path.cmp(&b.path));
    out
}

pub fn file_identity(path: &Path) -> FileIdentity {
    file_identity_with_context(path, None, None)
}

pub(crate) fn scan_query_value(query: &QuerySpec, limit: Option<u64>) -> Value {
    json!({
        "subreddits": query.subreddits.as_ref(),
        "authors_in": query.authors_in.as_ref(),
        "authors_out": query.authors_out.as_ref(),
        "exclude_common_bots": query.exclude_common_bots,
        "author_regex": query.author_regex.as_ref().map(|re| re.as_str()),
        "author_regex_pattern": query.author_regex_pattern.as_ref(),
        "min_score": query.min_score,
        "max_score": query.max_score,
        "keywords_any": query.keywords_any.as_ref(),
        "domains_in": query.domains_in.as_ref(),
        "contains_url": query.contains_url,
        "json_predicates": query.json_predicates_fingerprint(),
        "filter_pseudo_users": query.filter_pseudo_users,
        "limit": limit,
    })
}

pub(crate) fn etl_options_value(etl: &ETLOptions, limit: Option<u64>, extra: Value) -> Value {
    json!({
        "sources": sources_label(etl.sources),
        "start": etl.opts_start_string(),
        "end": etl.opts_end_string(),
        "legacy_subreddit": etl.subreddit.as_ref(),
        "whitelist_fields": etl.whitelist_fields.as_ref(),
        "strict_whitelist": etl.strict_whitelist,
        "strict_key": etl.strict_key,
        "parallelism": etl.parallelism,
        "file_concurrency": etl.file_concurrency,
        "shard_count": etl.shard_count,
        "work_dir": etl.work_dir.as_ref().map(|p| path_to_stable_string(p)),
        "read_buffer_bytes": etl.read_buffer_bytes,
        "write_buffer_bytes": etl.write_buffer_bytes,
        "human_readable_timestamps": etl.human_readable_timestamps,
        "zst_level": etl.zst_level,
        "inflight_bytes": etl.inflight_bytes,
        "inflight_groups": etl.inflight_groups,
        "resume": etl.resume,
        "allow_partial": etl.allow_partial,
        "emit_manifest": etl.emit_manifest,
        "parent_payload": {
            "full_record": etl.parent_payload_spec.is_full_record(),
            "fields": etl.parent_payload_spec.fields(),
        },
        "limit": limit,
        "extra": extra,
    })
}

pub fn path_to_stable_string(path: &Path) -> String {
    stable_path(path).to_string_lossy().into_owned()
}

pub fn discover_upstream_manifests_from_inputs(inputs: &[PathBuf]) -> Vec<UpstreamManifest> {
    let mut dirs: Vec<PathBuf> = inputs
        .iter()
        .filter_map(|path| path.parent().map(Path::to_path_buf))
        .collect();
    dirs.sort();
    dirs.dedup();
    dirs.into_iter()
        .map(|dir| upstream_manifest_for_directory(&dir))
        .filter(|m| m.exists)
        .collect()
}

pub fn upstream_manifest_for_directory(dir: &Path) -> UpstreamManifest {
    let path = manifest_path_for_directory(dir);
    let mut fingerprint = None;
    let mut operation = None;
    let exists = path.exists();
    if exists {
        if let Ok(bytes) = fs::read(&path) {
            if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                fingerprint = value
                    .get("manifest_fingerprint")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                operation = value
                    .get("operation")
                    .and_then(Value::as_str)
                    .map(str::to_string);
            }
        }
    }
    UpstreamManifest {
        path: path_to_stable_string(&path),
        exists,
        fingerprint,
        operation,
    }
}

fn file_identity_with_context(
    path: &Path,
    kind: Option<String>,
    month: Option<String>,
) -> FileIdentity {
    let metadata = fs::metadata(path).ok();
    let modified = metadata
        .as_ref()
        .and_then(|m| m.modified().ok())
        .map(system_time_parts);
    FileIdentity {
        path: path_to_stable_string(path),
        kind,
        month,
        exists: metadata.is_some(),
        size_bytes: metadata.as_ref().map(|m| m.len()),
        modified_unix_secs: modified.map(|(secs, _)| secs),
        modified_nanos: modified.map(|(_, nanos)| nanos),
        digest: None,
    }
}

fn manifest_fingerprint(
    operation: &str,
    command: Option<&str>,
    api_operation: Option<&str>,
    query: &Value,
    options: &Value,
    corpus: &CorpusSnapshot,
    inputs: &[FileIdentity],
    output: &OutputSnapshot,
    counts: &BTreeMap<String, u64>,
    resume: &ResumeSnapshot,
) -> Result<String> {
    let output_for_hash = json!({
        "kind": output.kind.as_str(),
        "format": output.format.as_str(),
    });
    let payload = json!({
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "operation": operation,
        "command": command,
        "api_operation": api_operation,
        "query": query,
        "options": options,
        "corpus": corpus,
        "inputs": inputs,
        "output": output_for_hash,
        "counts": counts,
        "resume": resume,
    });
    let bytes = serde_json::to_vec(&payload).context("serialize run manifest fingerprint input")?;
    Ok(stable_fnv1a_hex(&bytes))
}

fn stable_fnv1a_hex(bytes: &[u8]) -> String {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001B3;
    let mut hash = FNV_OFFSET;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("fnv1a64:{hash:016x}")
}

fn stable_path(path: &Path) -> PathBuf {
    if path.exists() {
        fs::canonicalize(path).unwrap_or_else(|_| absolutize(path))
    } else if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        let parent = if parent.exists() {
            fs::canonicalize(parent).unwrap_or_else(|_| absolutize(parent))
        } else {
            absolutize(parent)
        };
        match path.file_name() {
            Some(name) => parent.join(name),
            None => parent,
        }
    } else {
        absolutize(path)
    }
}

fn absolutize(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

fn system_time_parts(t: SystemTime) -> (i64, u32) {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => (
            i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
            d.subsec_nanos(),
        ),
        Err(e) => {
            let d = e.duration();
            (
                -i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
                d.subsec_nanos(),
            )
        }
    }
}

fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

fn git_hash() -> Option<&'static str> {
    option_env!("RETL_GIT_HASH")
        .or(option_env!("VERGEN_GIT_SHA"))
        .or(option_env!("GITHUB_SHA"))
        .or(option_env!("GIT_HASH"))
}

fn sources_label(sources: Sources) -> &'static str {
    match sources {
        Sources::Comments => "comments",
        Sources::Submissions => "submissions",
        Sources::Both => "both",
    }
}

trait EtlOptionDates {
    fn opts_start_string(&self) -> Option<String>;
    fn opts_end_string(&self) -> Option<String>;
}

impl EtlOptionDates for ETLOptions {
    fn opts_start_string(&self) -> Option<String> {
        self.start.map(|ym| ym.to_string())
    }

    fn opts_end_string(&self) -> Option<String> {
        self.end.map(|ym| ym.to_string())
    }
}
