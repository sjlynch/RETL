use anyhow::{Context, Result};
use retl::{
    convert_jsonl_to_csv, convert_jsonl_to_tsv, discover_sources_checked,
    discover_upstream_manifests_from_inputs, file_identities, file_identity, for_each_line_cfg,
    format_year_month_ranges, missing_month_diagnostics, output_parent,
    path_to_stable_string, plan_files, plan_files_checked, read_line_capped,
    replace_file_atomic_backoff, total_compressed_size, upstream_manifest_for_directory,
    write_run_manifest, AggregateBuildReport, CorpusAvailability, CorpusLocalStatus, CorpusManifest,
    CorpusPlanItem, CorpusSnapshot, ExportFormat, FileIdentity, FileKind, IntegrityMode,
    KeyExtractor, ManifestDestination, ParentIds, ParentPayloadSpec, RedditETL, RunManifestInput,
    RunManifestStart, Sources, TabularExportOptions, YearMonth, DEFAULT_MAX_LINE_BYTES,
};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
#[cfg(test)]
use std::fs;
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::bin_args::{
    AggregateArgs, CommonOpts, ConvertArgs, ConvertFmt, CorpusArgs, CorpusCommand,
    CorpusManifestArgs, CorpusPlanArgs, CorpusPlanFmt, CountArgs, CountMode, DedupeArgs,
    DescribeArgs, ExportArgs, ExportFmt, FirstSeenArgs, IntegrityArgs, IntegrityModeArg,
    ParentIdKindArg, ParentsArgs, QueryOpts, QuickstartArgs, SampleArgs, ScanArgs, SchemaArgs,
    SchemaFmt, SourceArg,
};
use crate::bin_helpers::{
    build_etl, discover_spool_parts, emit_partial_read_report, plan, stream_extract_to_stdout,
    stream_path_output_to_stdout, GroupBySpec, GroupMetricAgg, MetricSpec, RecCount,
};

const CLI_TEXT_WRITE_BUF_BYTES: usize = 64 * 1024;
const QUICK_SAMPLE_WARN_BELOW_BYTES: u64 = 4096;
const QUICKSTART_SAMPLE_JSONL: &str = include_str!("../../benches/data/sample.jsonl");
const QUICKSTART_ZST_LEVEL: i32 = 3;
static CLI_STAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

fn write_text_file_atomic<T, F>(final_path: &Path, body: F) -> Result<T>
where
    F: FnOnce(&mut dyn Write) -> Result<T>,
{
    let parent = output_parent(final_path);
    let staging_dir = parent.join("_staging");
    retl::create_dir_all_with_default_backoff(&staging_dir)
        .with_context(|| format!("creating staging dir {}", staging_dir.display()))?;

    let file_name = final_path
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("output path has no file name: {}", final_path.display()))?;
    let mut staged_name = file_name.to_os_string();
    let counter = CLI_STAGE_COUNTER.fetch_add(1, Ordering::Relaxed);
    staged_name.push(format!(".{}.{}.inprogress", std::process::id(), counter));
    let staged = staging_dir.join(staged_name);

    let file = retl::create_new_with_default_backoff(&staged)
        .with_context(|| format!("creating staged output {}", staged.display()))?;
    let mut w = BufWriter::with_capacity(CLI_TEXT_WRITE_BUF_BYTES, file);

    let result = match body(&mut w) {
        Ok(v) => v,
        Err(e) => {
            drop(w);
            let _ = retl::remove_with_short_backoff(&staged);
            return Err(e).with_context(|| format!("writing staged output {}", staged.display()));
        }
    };

    if let Err(e) = w.flush() {
        drop(w);
        let _ = retl::remove_with_short_backoff(&staged);
        return Err(e).with_context(|| format!("flushing staged output {}", staged.display()));
    }
    drop(w);

    let parent = output_parent(final_path);
    retl::create_dir_all_with_default_backoff(parent)
        .with_context(|| format!("creating output parent {}", parent.display()))?;

    if let Err(e) = replace_file_atomic_backoff(&staged, final_path) {
        let _ = retl::remove_with_short_backoff(&staged);
        return Err(e).with_context(|| {
            format!(
                "publishing staged output {} -> {}",
                staged.display(),
                final_path.display()
            )
        });
    }

    Ok(result)
}
