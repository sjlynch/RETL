
pub struct ParentIds {
    // Private fields to avoid exposing private types and to keep API clean.
    t1_ids_mem: Option<AHashSet<String>>,
    t3_ids_mem: Option<AHashSet<String>>,
    t1_ids_sharded: Option<IdShards>,
    t3_ids_sharded: Option<IdShards>,
}
impl ParentIds {
    pub fn new() -> Self {
        Self {
            t1_ids_mem: Some(AHashSet::new()),
            t3_ids_mem: Some(AHashSet::new()),
            t1_ids_sharded: None,
            t3_ids_sharded: None,
        }
    }

    pub(crate) fn from_shards(t1: IdShards, t3: IdShards) -> Self {
        Self {
            t1_ids_mem: None,
            t3_ids_mem: None,
            t1_ids_sharded: Some(t1),
            t3_ids_sharded: Some(t3),
        }
    }

    /// Insert a prefixed parent ID (`t1_...` for comments or `t3_...` for
    /// submissions). Returns `true` only when a valid, non-empty ID was newly
    /// inserted; malformed prefixes and duplicates are ignored safely.
    pub fn insert_prefixed(&mut self, id: impl AsRef<str>) -> bool {
        let id = id.as_ref().trim();
        if let Some(rest) = id.strip_prefix("t1_") {
            return self.insert_t1(rest);
        }
        if let Some(rest) = id.strip_prefix("t3_") {
            return self.insert_t3(rest);
        }
        false
    }

    /// Insert a comment parent ID. Accepts either a bare base36 ID (`abc`) or
    /// the matching prefixed form (`t1_abc`); `t3_...` and empty IDs are
    /// ignored.
    pub fn insert_t1(&mut self, id: impl AsRef<str>) -> bool {
        let Some(id) = normalize_parent_id_for_kind(id.as_ref(), "t1_") else {
            return false;
        };
        self.t1_ids_mem.get_or_insert_with(AHashSet::new).insert(id)
    }

    /// Insert a submission parent ID. Accepts either a bare base36 ID (`abc`)
    /// or the matching prefixed form (`t3_abc`); `t1_...` and empty IDs are
    /// ignored.
    pub fn insert_t3(&mut self, id: impl AsRef<str>) -> bool {
        let Some(id) = normalize_parent_id_for_kind(id.as_ref(), "t3_") else {
            return false;
        };
        self.t3_ids_mem.get_or_insert_with(AHashSet::new).insert(id)
    }

    /// Extend from prefixed `t1_...` / `t3_...` IDs. Returns the number of
    /// newly inserted unique IDs.
    pub fn extend_prefixed<I, S>(&mut self, ids: I) -> usize
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        ids.into_iter()
            .filter(|id| self.insert_prefixed(id.as_ref()))
            .count()
    }

    /// Extend the comment-ID set from bare or `t1_`-prefixed IDs.
    pub fn extend_t1<I, S>(&mut self, ids: I) -> usize
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        ids.into_iter()
            .filter(|id| self.insert_t1(id.as_ref()))
            .count()
    }

    /// Extend the submission-ID set from bare or `t3_`-prefixed IDs.
    pub fn extend_t3<I, S>(&mut self, ids: I) -> usize
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        ids.into_iter()
            .filter(|id| self.insert_t3(id.as_ref()))
            .count()
    }

    /// Return true when neither the comment (`t1_`) nor submission (`t3_`)
    /// parent-id backing contains any IDs.
    pub fn is_empty(&self) -> bool {
        fn backing_empty(mem: Option<&AHashSet<String>>, sharded: Option<&IdShards>) -> bool {
            mem.map_or(true, |ids| ids.is_empty())
                && sharded.map_or(true, |shards| shards.total_ids == 0)
        }

        backing_empty(self.t1_ids_mem.as_ref(), self.t1_ids_sharded.as_ref())
            && backing_empty(self.t3_ids_mem.as_ref(), self.t3_ids_sharded.as_ref())
    }

    #[inline]
    pub fn contains_t1<'a>(
        &self,
        id: &'a str,
        loader: &mut impl FnMut(&Path) -> Result<&AHashSet<String>>,
    ) -> Result<bool> {
        if self
            .t1_ids_mem
            .as_ref()
            .map(|mem| mem.contains(id))
            .unwrap_or(false)
        {
            return Ok(true);
        }
        if let Some(sh) = &self.t1_ids_sharded {
            let idx = sh.idx(id);
            let p = sh.path_for(idx);
            let set = loader(&p)?;
            return Ok(set.contains(id));
        }
        Ok(false)
    }

    #[inline]
    pub fn contains_t3<'a>(
        &self,
        id: &'a str,
        loader: &mut impl FnMut(&Path) -> Result<&AHashSet<String>>,
    ) -> Result<bool> {
        if self
            .t3_ids_mem
            .as_ref()
            .map(|mem| mem.contains(id))
            .unwrap_or(false)
        {
            return Ok(true);
        }
        if let Some(sh) = &self.t3_ids_sharded {
            let idx = sh.idx(id);
            let p = sh.path_for(idx);
            let set = loader(&p)?;
            return Ok(set.contains(id));
        }
        Ok(false)
    }
}

impl Default for ParentIds {
    fn default() -> Self {
        Self::new()
    }
}

fn normalize_parent_id_for_kind(id: &str, prefix: &str) -> Option<String> {
    let id = id.trim();
    if id.is_empty() {
        return None;
    }
    if let Some(rest) = id.strip_prefix(prefix) {
        let rest = rest.trim();
        return (!rest.is_empty()).then(|| rest.to_string());
    }
    if id.starts_with("t1_") || id.starts_with("t3_") {
        return None;
    }
    Some(id.to_string())
}

/// Parent shard cache lookup tables.
///
/// Replaces the prior per-id index (one entry PER parent id — tens of millions
/// at corpus scale). Now keyed by month: the resolver writes one shard JSON per
/// (YearMonth, FileKind), and consumers resolve a parent id to its shard via
/// the child record's own-month metadata + the parent's prefix-derived FileKind.
pub struct ParentMaps {
    /// Backwards-compatible eager comment cache (`id -> body`) used by the
    /// default parent payload spec.
    pub comments: HashMap<String, String>,
    /// Backwards-compatible eager submission cache (`id -> (title, selftext)`).
    pub submissions: HashMap<String, (String, String)>,
    pub comment_shards: Option<HashMap<YearMonth, PathBuf>>,
    pub submission_shards: Option<HashMap<YearMonth, PathBuf>>,
    pub payload_spec: ParentPayloadSpec,
}

impl Default for ParentMaps {
    fn default() -> Self {
        Self {
            comments: HashMap::new(),
            submissions: HashMap::new(),
            comment_shards: None,
            submission_shards: None,
            payload_spec: ParentPayloadSpec::default(),
        }
    }
}

impl ParentMaps {
    /// Helper: pick the shard that is most likely to own `_id`, given its
    /// FileKind (from the parent_id prefix) and the consuming record's own
    /// month. Reddit threads almost always live in a single month, so the
    /// child's own month is a strong prior for the parent's shard.
    pub fn shard_for(&self, kind: FileKind, own_month: YearMonth) -> Option<&PathBuf> {
        let map = match kind {
            FileKind::Comment => self.comment_shards.as_ref()?,
            FileKind::Submission => self.submission_shards.as_ref()?,
        };
        map.get(&own_month)
    }
}
