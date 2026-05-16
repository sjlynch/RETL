
pub(crate) struct IdShards {
    pub(crate) dir: PathBuf,
    pub(crate) count: usize,
    pub(crate) total_ids: usize,
    pub(crate) rs: RandomState,
    pub(crate) kind: String,
    pub(crate) _scratch_root: Arc<IdScratchRoot>,
}
impl IdShards {
    #[inline]
    pub(crate) fn idx(&self, id: &str) -> usize {
        shard_common::shard_index(&self.rs, id, self.count)
    }
    #[inline]
    pub(crate) fn path_for(&self, idx: usize) -> PathBuf {
        self.dir.join(format!("{}_ids_{:04}.txt", self.kind, idx))
    }
}
