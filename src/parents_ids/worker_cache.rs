
/// Per-rayon-worker FIFO cache of parsed parent shard JSON files.
///
/// Eviction is plain FIFO with **no bump-on-hit** — same as the prior inline
/// implementation. Bumping on hit would require an `O(n)` `VecDeque` rescan
/// and is unnecessary in practice because shard load order is dominated by
/// own-month locality (the same shards stay hot naturally).
pub(crate) struct WorkerShardCache<V> {
    cache: HashMap<PathBuf, HashMap<String, V>>,
    order: VecDeque<PathBuf>,
    cap: usize,
}

impl<V: serde::de::DeserializeOwned + Clone> WorkerShardCache<V> {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            cache: HashMap::new(),
            order: VecDeque::new(),
            cap,
        }
    }

    pub(crate) fn get(&mut self, path: &Path, id: &str) -> Result<Option<V>> {
        if !self.cache.contains_key(path) {
            if self.cache.len() >= self.cap {
                if let Some(old) = self.order.pop_front() {
                    self.cache.remove(&old);
                }
            }
            let file = crate::util::open_with_default_backoff(path)
                .with_context(|| format!("open parent shard {}", path.display()))?;
            let rdr = BufReader::new(file);
            // A corrupt/truncated cached shard JSON would otherwise fail
            // attach for *every* input with an error naming only the shard
            // path. Name it explicitly as resolver-cache corruption and give
            // the one-line recovery: delete that shard and re-run with
            // --resume so the resolver rebuilds just that shard (the resolver
            // resume path in `build_id_shard_index` already validates and
            // rebuilds corrupt resolver shards).
            let map: HashMap<String, V> = serde_json::from_reader(rdr).map_err(|e| {
                anyhow::anyhow!(
                    "parent cache shard {} is corrupt: could not parse it as JSON ({e}). \
                     This is a damaged file in the resolver parent cache, not a problem with \
                     your input spool — delete {} and re-run with --resume to rebuild just \
                     that shard.",
                    path.display(),
                    path.display(),
                )
            })?;
            self.cache.insert(path.to_path_buf(), map);
            self.order.push_back(path.to_path_buf());
        }
        Ok(self.cache.get(path).and_then(|m| m.get(id).cloned()))
    }
}

/// Process-wide cache for membership-id shards (set of t1_/t3_ ids per shard
/// file). Shared across rayon workers so each shard is read+parsed at most
/// once globally rather than once per worker.
pub(crate) struct SharedIdsetCache {
    inner: parking_lot::RwLock<SharedIdsetState>,
    cap: usize,
}
