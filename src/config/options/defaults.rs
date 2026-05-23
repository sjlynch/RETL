pub(crate) const DEFAULT_BASE_DIR: &str = "./data";
pub(crate) const DEFAULT_FILE_CONCURRENCY: usize = 1;
pub(crate) const DEFAULT_READ_BUFFER_BYTES: usize = 256 * 1024;
pub(crate) const DEFAULT_WRITE_BUFFER_BYTES: usize = 256 * 1024;
pub(crate) const DEFAULT_ZST_LEVEL: i32 = 7;
pub(crate) const DEFAULT_INFLIGHT_BYTES: usize = 256 * 1024 * 1024;
pub(crate) const DEFAULT_INFLIGHT_GROUPS: usize = 8;
pub const DEFAULT_PARQUET_ROW_GROUP_SIZE: usize = 128 * 1024;
pub const DEFAULT_PARQUET_COMPRESSION: &str = "zstd:3";

impl Default for ETLOptions {
    fn default() -> Self {
        let base = PathBuf::from(DEFAULT_BASE_DIR);

        Self {
            comments_dir: base.join("comments"),
            submissions_dir: base.join("submissions"),
            base_dir: base,
            subreddit: None,
            sources: Sources::Both,
            start: None,
            end: None,
            shard_count: MAX_SHARDS,
            whitelist_fields: None,
            strict_whitelist: false,
            strict_key: false,
            aggregate_strict: false,
            parallelism: None,
            work_dir: None,
            file_concurrency: DEFAULT_FILE_CONCURRENCY, // safe default to prevent OOM on big .zst windows
            progress: true,
            progress_label: None,

            read_buffer_bytes: DEFAULT_READ_BUFFER_BYTES,
            write_buffer_bytes: DEFAULT_WRITE_BUFFER_BYTES,

            human_readable_timestamps: false,

            zst_level: DEFAULT_ZST_LEVEL,
            parquet_row_group_size: DEFAULT_PARQUET_ROW_GROUP_SIZE,
            parquet_compression: DEFAULT_PARQUET_COMPRESSION.to_string(),

            inflight_bytes: DEFAULT_INFLIGHT_BYTES,
            inflight_groups: DEFAULT_INFLIGHT_GROUPS,
            adaptive_mem: AdaptiveMemCfg::default(),
            resume: false,
            parent_payload_spec: ParentPayloadSpec::default(),
            emit_manifest: true,
            allow_partial: false,
            partial_read_reporter: PartialReadReporter::default(),
            build_error: None,
        }
    }
}
