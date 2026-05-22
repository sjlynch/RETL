/// User-facing options with sensible defaults and builder chaining.
#[derive(Clone, Debug)]
pub struct ETLOptions {
    /// Corpus base directory containing `comments/` and `submissions/`.
    /// Defaults to `./data`, matching the `retl` CLI and README examples.
    pub base_dir: PathBuf,
    pub comments_dir: PathBuf,
    pub submissions_dir: PathBuf,
    pub subreddit: Option<String>, // normalized lowercase, no "r/"; deprecated single-subreddit default
    pub sources: Sources,
    pub start: Option<YearMonth>, // inclusive
    pub end: Option<YearMonth>,   // inclusive
    pub shard_count: usize,       // number of on-disk dedup shards, clamped to MAX_SHARDS
    pub whitelist_fields: Option<Vec<String>>,
    pub strict_whitelist: bool, // fail instead of warn when whitelisted keys match nothing
    pub strict_key: bool,       // fail dedupe when matching records lack the requested key
    /// Fail the whole aggregate run when any input is fatal (open error,
    /// malformed JSON, shard write failure). Default `false` keeps the
    /// historical tolerant behavior: a fatal input is reported but the run
    /// still merges whatever shards succeeded. Honored by
    /// [`RedditETL::aggregate_jsonls_parallel`]; the `retl aggregate --strict`
    /// flag enforces the same rule on the CLI path.
    pub aggregate_strict: bool,
    pub parallelism: Option<usize>, // Some(N) to set rayon threads (clamped), None to use default
    pub work_dir: Option<PathBuf>, // if None, create in base_dir/.reddit_etl_work/
    pub file_concurrency: usize, // limit monthly files processed concurrently, clamped to MAX_FILE_CONCURRENCY
    pub progress: bool,          // show progress bar
    pub progress_label: Option<String>, // optional label for progress bar

    // IO tuning
    pub read_buffer_bytes: usize,  // BufReader capacity
    pub write_buffer_bytes: usize, // BufWriter capacity

    // output formatting
    pub human_readable_timestamps: bool, // convert unix timestamps to RFC3339 strings

    // zstd compression level used by partitioned ZST writers
    pub zst_level: i32,

    /// Bound (in bytes) on data inflight between bucketing/dedupe producers
    /// and their downstream consumers. Sets `per_flush_cap = inflight_bytes /
    /// 2` for the producer-side map. Defaults to 256 MiB.
    ///
    /// **This is not the only memory lever.** [`inflight_groups`] adds a
    /// channel of buffered groups on top, so the worst-case bucketing peak is
    ///
    /// ```text
    /// peak ≈ (1 + inflight_groups) * (inflight_bytes / 2)
    /// ```
    ///
    /// (the dedupe pipeline pins channel capacity to 1, so its peak stays at
    /// `~inflight_bytes`). With the defaults
    /// (`inflight_bytes = 256 MiB`, `inflight_groups = 8`) the bucketing peak
    /// is ≈ 1.125 GiB, *not* 256 MiB. Use [`with_inflight_budget`] to set both
    /// values together so the declared budget matches the actual peak.
    ///
    /// [`inflight_groups`]: ETLOptions::inflight_groups
    /// [`with_inflight_budget`]: ETLOptions::with_inflight_budget
    pub inflight_bytes: usize,

    /// Number of buffered groups allowed between bucketing producers and
    /// consumers. Affects the bucketing stage only (the dedupe stage hard-codes
    /// channel capacity to 1). Defaults to 8.
    ///
    /// **Interacts with [`inflight_bytes`]:** each buffered group can hold up
    /// to `inflight_bytes / 2` bytes, so raising this value raises the
    /// bucketing memory peak proportionally. Worst-case peak:
    ///
    /// ```text
    /// peak ≈ (1 + inflight_groups) * (inflight_bytes / 2)
    /// ```
    ///
    /// The two values are NOT independent. To keep the peak ≤ the declared
    /// budget, use [`with_inflight_budget`] which sets both for you.
    /// `retl` emits a one-shot `tracing::warn!` when a configured pair would
    /// exceed roughly 2× the declared `inflight_bytes`.
    ///
    /// [`inflight_bytes`]: ETLOptions::inflight_bytes
    /// [`with_inflight_budget`]: ETLOptions::with_inflight_budget
    pub inflight_groups: usize,

    /// Adaptive-memory policy shared by bucketing/dedupe producers. Controls
    /// the free-memory fractions used to shrink/grow producer buffers and the
    /// minimum cooldown between target recomputations.
    pub adaptive_mem: AdaptiveMemCfg,

    /// Opt-in: when true, supported extract/export and analytics operations
    /// read/write a `_progress.json`-style sidecar and skip months already
    /// committed by a prior run. Default false to preserve current behavior.
    pub resume: bool,

    /// Parent payload fields attached by the parents pipeline. Defaults to the
    /// legacy output shape (`body` for comments, `title`/`selftext` for
    /// submissions).
    pub parent_payload_spec: ParentPayloadSpec,

    /// Emit user-facing provenance manifests next to file/directory outputs.
    /// Enabled by default; disable via [`ETLOptions::with_run_manifest`] or the
    /// CLI's `--no-manifest` when absolute local paths are too sensitive for a
    /// sidecar artifact.
    pub emit_manifest: bool,

    /// Opt-in lossy mode for corrupt zstd inputs. The default (`false`) treats
    /// zstd decode errors as fatal so scans/exports cannot silently return
    /// partial results. When `true`, corrupt monthly files are skipped, recorded
    /// in [`Self::partial_read_reporter`], and never committed to resume manifests.
    pub allow_partial: bool,
    pub partial_read_reporter: PartialReadReporter,

    #[doc(hidden)]
    pub build_error: Option<ConfigBuildError>,
}
