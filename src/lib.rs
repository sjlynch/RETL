mod config;
mod date;
mod paths;
mod zstd_jsonl;
mod shard;
mod query;
mod kv_shard;

mod filters;
mod progress;
mod stitch;
mod concurrency;
mod streaming;
mod counting;
mod util;
mod mem;
mod atomic_write;
mod progress_manifest;
mod pipeline;

mod parents;
mod aggregate;
mod integrity;
mod partition;

mod bucketing;
mod json_utils;
mod json_whitelist;
mod ndjson;
mod key_extractor;
mod dedupe;

pub use crate::config::{ETLOptions, Sources};
pub use crate::date::YearMonth;
pub use crate::pipeline::{RedditETL, ExportFormat};
pub use crate::shard::UsernameStream;
pub use crate::query::QuerySpec;

pub use crate::parents::{ParentIds, ParentMaps};
pub use crate::aggregate::Aggregator;

#[doc(hidden)]
pub use crate::aggregate::{merge_aggregator_shards_parallel, merge_aggregator_shards_serial};

// Expose multiprogress and progress helpers.
pub use crate::progress::{set_global_multiprogress, make_count_progress, make_progress_bar_labeled, ProgressScope};

// Expose memory helpers for adaptive throttling from the binary.
pub use crate::mem::{available_memory_fraction, is_low_memory};

// Expose integrity checker mode, and (optionally) direct zstd validators.
pub use crate::integrity::IntegrityMode;
pub use crate::zstd_jsonl::{quick_validate_zst, validate_zst_full};

//export partition writers (lambda-capable)
pub use crate::partition::PartitionWriters;

//export robust file ops from util so binaries can import from crate root.
pub use crate::util::{open_with_backoff, create_with_backoff, remove_with_backoff, replace_file_atomic_backoff};

// Scoped rayon pool + opt-in tracing init for binaries.
pub use crate::util::{init_tracing_for_binary, with_thread_pool};

//export bucketing & json utils to application code
pub use crate::bucketing::{BucketingCfg, partition_stage1, bucketize_shard, process_bucket_streaming};
pub use crate::json_utils::{author_lower, subreddit_lower, is_comment_record};

// export NDJSON helpers
pub use crate::ndjson::{NdjsonReader, NdjsonWriter};

// export streaming whitelist tokenizer for tests/benches
#[doc(hidden)]
pub use crate::json_whitelist::{TokenizerError, WhitelistTokenizer};

// export KeyExtractor abstraction
pub use crate::key_extractor::KeyExtractor;

// export dedupe engine
pub use crate::dedupe::{DedupeCfg, build_runs_sorted, merge_runs_sorted};

// Test-only re-exports of internals so behavioral tests can drive them directly.
// Behavior is unchanged; these are additive exports used by tests/*.rs.
#[doc(hidden)]
pub use crate::shard::ShardedWriter;
#[doc(hidden)]
pub use crate::kv_shard::ShardedKVWriter;
#[doc(hidden)]
pub use crate::concurrency::for_each_file_limited;
#[doc(hidden)]
pub use crate::paths::{plan_files, discover_all, FileKind, FileJob, Discovered};
#[doc(hidden)]
pub use crate::date::iter_year_months;
#[doc(hidden)]
pub use crate::filters::{bounds_tuple, within_bounds};
#[doc(hidden)]
pub use crate::zstd_jsonl::{MinimalRecord, parse_minimal};
// Test-only re-exports of the human-timestamp rewriters so fuzz targets and
// behavioral tests can exercise them directly. Keep `#[doc(hidden)]` to avoid
// signalling these as part of the supported public API.
#[doc(hidden)]
pub use crate::streaming::{apply_human_timestamps, rewrite_human_timestamps_bytes};
