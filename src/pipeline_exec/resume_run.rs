
/// Outcome of [`prepare_resume_run`]: the manifest entries surviving validation,
/// a snapshot of their keys (used by the per-file loop's resume-skip check),
/// and the live [`ManifestAccumulator`] when resume is enabled.
struct ResumePrelude {
    initial_months: HashMap<String, MonthEntry>,
    completed_keys: HashSet<String>,
    accumulator: Option<ManifestAccumulator>,
}

/// Tracing labels for resume-prelude logs. Each caller fills these in to keep
/// the existing log wording (and downstream log-scrapers) intact.
struct ResumeLogLabels {
    /// Logged on fingerprint mismatch.
    fingerprint_mismatch: &'static str,
    /// Logged when a recorded key is no longer in the planned-file set.
    out_of_plan: &'static str,
    /// Logged when a recorded entry fails its per-caller validation.
    stale_entry: &'static str,
}

/// Shared scaffold for the three resumable-publish pipelines
/// (`spool`, `partitioned`, `extract_common`).
///
/// Runs the prelude that each caller used to reimplement by hand:
///
/// 1. If `!resume`, calls `clear_all_outputs` and returns empty state.
/// 2. Loads `_progress.json` from `manifest_dir`.
///    - On fingerprint mismatch (and when `warn_clear_when_empty || months
///      not empty`), warns, calls `clear_all_outputs`, and returns empty state.
///    - Otherwise iterates the recorded entries: drops any whose key is not
///      in `planned_keys` (when `Some`), then runs `validate_entry`; on `Err`
///      drops the entry with an info log.
/// 3. Pre-saves the pruned manifest.
/// 4. Calls `prune_outputs_except(&completed_keys)` to remove unowned files,
///    then constructs a `ManifestAccumulator`.
///
/// Step 3 deliberately precedes step 4: rewriting `_progress.json` to the
/// surviving key set *before* deleting any output keeps the manifest no more
/// optimistic than the filesystem if the save fails — a manifest listing
/// fewer months than exist on disk is harmless, but one listing months whose
/// files were already pruned is the desync inter-run tooling trips on.
///
/// `validate_entry` returns the canonical `MonthEntry` to keep on success;
/// callers that recompute line counts (e.g. `extract_common`) can return a
/// refreshed entry. Errors are caught and logged; never propagated.
fn prepare_resume_run<C, P, V>(
    manifest_dir: &Path,
    fingerprint: &str,
    planned_keys: Option<&HashSet<String>>,
    resume: bool,
    warn_clear_when_empty: bool,
    labels: ResumeLogLabels,
    clear_all_outputs: C,
    prune_outputs_except: P,
    mut validate_entry: V,
) -> Result<ResumePrelude>
where
    C: FnOnce() -> Result<()>,
    P: FnOnce(&HashSet<String>) -> Result<()>,
    V: FnMut(&str, MonthEntry) -> Result<MonthEntry>,
{
    if !resume {
        clear_all_outputs()?;
        return Ok(ResumePrelude {
            initial_months: HashMap::new(),
            completed_keys: HashSet::new(),
            accumulator: None,
        });
    }

    let manifest = crate::progress_manifest::load(manifest_dir);
    let fingerprint_mismatches = manifest.fingerprint.as_deref() != Some(fingerprint);
    let should_discard = fingerprint_mismatches
        && (warn_clear_when_empty || !manifest.months.is_empty());

    let keep: HashMap<String, MonthEntry> = if should_discard {
        tracing::warn!(
            path=%crate::progress_manifest::manifest_path(manifest_dir).display(),
            stored=?manifest.fingerprint,
            current=%fingerprint,
            "{}",
            labels.fingerprint_mismatch,
        );
        clear_all_outputs()?;
        HashMap::new()
    } else {
        let mut keep: HashMap<String, MonthEntry> = HashMap::new();
        for (key, entry) in manifest.months {
            if let Some(planned) = planned_keys {
                if !planned.contains(&key) {
                    tracing::info!(key = %key, "{}", labels.out_of_plan);
                    continue;
                }
            }
            match validate_entry(&key, entry) {
                Ok(refreshed) => {
                    keep.insert(key, refreshed);
                }
                Err(e) => {
                    tracing::info!(key = %key, error = %e, "{}", labels.stale_entry);
                }
            }
        }
        keep
    };

    let completed_keys: HashSet<String> = keep.keys().cloned().collect();
    // Persist the pruned manifest BEFORE deleting any output file. If this
    // save fails the run aborts with `_progress.json` still describing the
    // larger, fully-intact output set — conservative, and harmless. Pruning
    // first would risk aborting with files already deleted but the manifest
    // still listing them, leaving inter-run tooling reading a manifest of
    // months whose outputs no longer exist.
    crate::progress_manifest::save(manifest_dir, &keep, Some(fingerprint))?;
    prune_outputs_except(&completed_keys)?;
    let accumulator =
        ManifestAccumulator::new(manifest_dir, keep.clone(), Some(fingerprint.to_string()));

    Ok(ResumePrelude {
        initial_months: keep,
        completed_keys,
        accumulator: Some(accumulator),
    })
}

/// Verify that every async manifest save observed by `accumulator` succeeded.
/// Called after the per-file loop completes so a silent late-save failure
/// cannot masquerade as a durable checkpoint.
fn ensure_resume_manifest_durable(
    accumulator: Option<&ManifestAccumulator>,
    operation: &str,
) -> Result<()> {
    if let Some(error) = accumulator.and_then(ManifestAccumulator::last_save_error) {
        anyhow::bail!(
            "{operation} resume progress manifest is not durable; earlier save failed: {error}"
        );
    }
    Ok(())
}

/// Run the strict `--whitelist` finalization, discarding this run's published
/// outputs and resume manifest if it fails.
///
/// [`WhitelistMatchTracker::finalize`] is unavoidably post-hoc: it can only
/// decide that a requested `--whitelist` field never matched any record after
/// the per-file loop has already atomically published each processed month's
/// output *and* durably committed its `_progress.json` entry. A bare
/// `finalize()?` would therefore abort the run while leaving those bad (empty /
/// wrong-projection) outputs on disk — and with `resume = true` a re-run would
/// load `_progress.json`, skip every month, observe zero fresh records, and
/// never re-trigger the strict check, silently "succeeding" with bad output.
///
/// To keep `--strict-whitelist`'s "fail before producing bad output" guarantee
/// meaningful, a strict failure here discards the whole output set (via
/// `discard_all_outputs` — the same cleanup used on a resume-fingerprint
/// mismatch) and removes the resume manifest, so a resumed run starts fresh,
/// re-streams every month, and re-evaluates the whitelist. The original strict
/// error is always propagated; a cleanup failure is logged but never masks it.
///
/// `finalize` only returns `Err` in strict mode (non-strict runs merely warn),
/// so the rollback path is reached only when `--strict-whitelist` was set.
fn finalize_whitelist_strict<C>(
    tracker: Option<&WhitelistMatchTracker>,
    manifest_dir: &Path,
    discard_all_outputs: C,
) -> Result<()>
where
    C: FnOnce() -> Result<()>,
{
    let Some(tracker) = tracker else {
        return Ok(());
    };
    let Err(strict_err) = tracker.finalize() else {
        return Ok(());
    };
    if let Err(cleanup_err) = discard_all_outputs() {
        tracing::warn!(
            error = %cleanup_err,
            "failed to discard published outputs after strict --whitelist failure; \
             a resumed run may skip the affected months",
        );
    }
    let manifest = crate::progress_manifest::manifest_path(manifest_dir);
    if let Err(cleanup_err) = crate::util::remove_with_short_backoff(&manifest) {
        tracing::warn!(
            path = %manifest.display(),
            error = %cleanup_err,
            "failed to remove resume manifest after strict --whitelist failure; \
             a resumed run may skip the affected months",
        );
    }
    Err(strict_err)
}

/// Remove every file under `dir` whose name passes `should_remove`. Used by
/// the per-caller prune/clear helpers; centralizes the directory scan + error
/// context so callers only have to supply their filename pattern.
fn remove_matching_files(dir: &Path, mut should_remove: impl FnMut(&str) -> bool) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    let entries = crate::util::read_dir_with_default_backoff(dir)
        .with_context(|| format!("read_dir {}", dir.display()))?;
    for entry in entries {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if should_remove(name) {
            crate::util::remove_with_short_backoff(&path)
                .with_context(|| format!("remove stale RETL-owned output {}", path.display()))?;
        }
    }
    Ok(())
}
