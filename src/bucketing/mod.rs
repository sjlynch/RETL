//! Stage-focused bucketing pipeline modules.
//!
//! Bucketing feeds dedupe/aggregation in three stages:
//! disk shard routing ([`partition_stage1`]), disk re-bucketing
//! ([`bucketize_shards`]), and adaptive in-memory micro-bucketing
//! ([`process_bucket_streaming`]). The public surface is re-exported here
//! so crate-root exports stay stable while each stage lives in its own file.

mod cfg;
mod hash;
mod micro;
mod routing;

pub use cfg::BucketingCfg;
pub use micro::process_bucket_streaming;
pub use routing::{bucketize_shards, partition_stage1};
