use crate::config::{clamp_shard_count, MAX_SHARDS};
use crate::ndjson::{read_line_capped, DEFAULT_MAX_LINE_BYTES};
use crate::parents::ParentIds;
use crate::pipeline::RedditETL;
use crate::progress::make_progress_bar_labeled;
use crate::shard_common;
use crate::util::with_thread_pool;
use crate::zstd_jsonl::malformed_json_error;
use ahash::{AHashSet, RandomState};
use anyhow::{Context, Result};
use rayon::prelude::*;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::{self, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

static PARENT_ID_SCRATCH_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub(crate) struct IdScratchRoot {
    path: PathBuf,
}

impl IdScratchRoot {
    fn create(work_dir: &Path) -> Result<Arc<Self>> {
        let parent = work_dir.join("parent_ids");
        crate::util::create_dir_all_with_default_backoff(&parent)
            .with_context(|| format!("create parent-id scratch parent {}", parent.display()))?;

        let pid = std::process::id();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let counter = PARENT_ID_SCRATCH_COUNTER.fetch_add(1, Ordering::Relaxed);

        for attempt in 0..1024usize {
            let path = parent.join(format!("run-p{pid}-{nanos:x}-{counter:x}-{attempt:x}"));
            match crate::util::create_dir_with_default_backoff(&path) {
                Ok(()) => return Ok(Arc::new(Self { path })),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => {
                    return Err(e).with_context(|| {
                        format!("create parent-id scratch root {}", path.display())
                    });
                }
            }
        }

        Err(anyhow::anyhow!(
            "could not allocate a unique parent-id scratch root under {}",
            parent.display()
        ))
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for IdScratchRoot {
    fn drop(&mut self) {
        if let Err(e) = crate::util::remove_dir_all_with_short_backoff(&self.path) {
            tracing::debug!(
                path = %self.path.display(),
                error = %e,
                "failed to remove parent-id scratch root"
            );
        }
    }
}
