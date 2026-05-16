
struct SharedIdsetState {
    map: HashMap<PathBuf, Arc<AHashSet<String>>>,
    order: VecDeque<PathBuf>,
}

impl SharedIdsetCache {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            inner: parking_lot::RwLock::new(SharedIdsetState {
                map: HashMap::new(),
                order: VecDeque::new(),
            }),
            cap: cap.max(1),
        }
    }

    pub(crate) fn get_or_load(&self, path: &Path) -> Result<Arc<AHashSet<String>>> {
        if let Some(v) = self.inner.read().map.get(path) {
            return Ok(v.clone());
        }
        // Load outside the lock so concurrent workers loading different shards
        // don't serialize on the I/O.
        let f = crate::util::open_with_default_backoff(path)
            .with_context(|| format!("open idset shard {}", path.display()))?;
        let mut r = BufReader::new(f);
        let mut set: AHashSet<String> = AHashSet::with_capacity(64_000);
        let mut s = String::with_capacity(16 * 1024);
        loop {
            let n = read_line_capped(&mut r, &mut s, DEFAULT_MAX_LINE_BYTES, path)
                .with_context(|| format!("read idset shard {}", path.display()))?;
            if n == 0 {
                break;
            }
            if !s.is_empty() {
                set.insert(s.clone());
            }
        }
        let arc = Arc::new(set);

        let mut g = self.inner.write();
        // Another worker may have raced us to load the same shard; prefer
        // their copy so callers don't see different Arc identities.
        if let Some(existing) = g.map.get(path) {
            return Ok(existing.clone());
        }
        if g.map.len() >= self.cap {
            if let Some(old) = g.order.pop_front() {
                g.map.remove(&old);
            }
        }
        g.map.insert(path.to_path_buf(), arc.clone());
        g.order.push_back(path.to_path_buf());
        Ok(arc)
    }
}
