
fn load_shard<A: Aggregator>(shard: &Path) -> Result<A> {
    let f = crate::util::open_with_default_backoff(shard)?;
    let r = BufReader::new(f);
    let part: A = serde_json::from_reader(r)?;
    Ok(part)
}

/// Parallel tree-merge of aggregator shards from disk.
/// `pb`, if provided, is incremented once per shard as it's loaded.
///
/// Uses `try_fold` + `try_reduce` so each rayon worker accumulates an
/// entire chunk locally (no cross-thread synchronization per shard), then
/// only the per-worker totals are combined via tree reduction.
#[doc(hidden)]
pub fn merge_aggregator_shards_parallel<A: Aggregator>(
    shards: &[PathBuf],
    pb: Option<&ProgressBar>,
) -> Result<A> {
    shards
        .par_iter()
        .try_fold(A::default, |mut acc, shard| -> Result<A> {
            let part: A = load_shard(shard)?;
            acc.merge(part);
            if let Some(pb) = pb {
                pb.inc(1);
            }
            Ok(acc)
        })
        .try_reduce(A::default, |mut left, right| {
            left.merge(right);
            Ok(left)
        })
}

/// Serial fold of aggregator shards from disk. Kept for benchmark
/// comparisons against the parallel path.
#[doc(hidden)]
pub fn merge_aggregator_shards_serial<A: Aggregator>(
    shards: &[PathBuf],
    pb: Option<&ProgressBar>,
) -> Result<A> {
    let mut total = A::default();
    for shard in shards {
        let part: A = load_shard(shard)?;
        total.merge(part);
        if let Some(pb) = pb {
            pb.inc(1);
        }
    }
    Ok(total)
}
