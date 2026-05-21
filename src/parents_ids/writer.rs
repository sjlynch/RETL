
pub(crate) struct IdShardWriter {
    scratch_root: Arc<IdScratchRoot>,
    base_dir: PathBuf,
    count: usize,
    rs: RandomState,
    kind: &'static str,
    writers: shard_common::LineShardWriters,
}
impl IdShardWriter {
    pub(crate) fn create(
        scratch_root: Arc<IdScratchRoot>,
        kind: &'static str,
        count: usize,
    ) -> Result<Self> {
        let count = clamp_shard_count(count, "IdShardWriter::create");
        let dir = scratch_root.path().join(format!("{kind}_ids_shards"));
        crate::util::create_dir_all_with_default_backoff(&dir)
            .with_context(|| format!("create parent-id shard dir {}", dir.display()))?;
        let writers = shard_common::create_line_shard_writers(
            &dir,
            count,
            |i| format!("{kind}_ids_{i:04}.tmp"),
            "parent-id shard",
        )?;
        let rs = shard_common::seeded_state("parent_ids");
        Ok(Self {
            scratch_root,
            base_dir: dir,
            count,
            rs,
            kind,
            writers,
        })
    }
    #[inline]
    fn idx(&self, id: &str) -> usize {
        shard_common::shard_index(&self.rs, id, self.count)
    }
    #[inline]
    pub(crate) fn write(&self, id: &str) -> Result<()> {
        let i = self.idx(id);
        let mut w = self.writers[i].lock();
        w.write_all(id.as_bytes())?;
        w.write_all(b"\n")?;
        Ok(())
    }
    fn flush(&self) -> Result<()> {
        shard_common::flush_line_shard_writers(&self.writers)
    }
    pub(crate) fn dedup(self) -> Result<IdShards> {
        // `base_dir` (raw `<kind>_ids_shards/` scratch) and, once it exists,
        // `out_dir` (the partially-populated `<kind>_ids_dedup/` directory)
        // are scratch until this method returns `Ok`. Guard them so a
        // `dedup_one` failure — or a panic on a rayon worker — reclaims both
        // instead of leaving them solely to the `IdScratchRoot::drop`
        // fallback, which only runs once the last `Arc` clone is dropped. On
        // success the guard is disarmed: `out_dir` becomes the live dedup
        // output and only `base_dir` is disposable.
        let mut scratch = crate::util::ScratchGuard::new(self.base_dir.clone());

        self.flush()?;
        let IdShardWriter {
            scratch_root,
            base_dir,
            count,
            rs,
            kind,
            writers,
        } = self;
        drop(writers);

        let out_dir = base_dir
            .parent()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "parent-id scratch base_dir has no parent: {}",
                    base_dir.display()
                )
            })?
            .join(format!("{kind}_ids_dedup"));
        scratch.guard_also(out_dir.clone());
        crate::util::create_dir_all_with_default_backoff(&out_dir)
            .with_context(|| format!("create parent-id dedup dir {}", out_dir.display()))?;

        let tmp_paths: Vec<PathBuf> = (0..count)
            .map(|i| base_dir.join(format!("{kind}_ids_{:04}.tmp", i)))
            .collect();
        let shard_counts: Vec<usize> = tmp_paths
            .par_iter()
            .map(|p| -> Result<usize> {
                let file_name = p.file_name().ok_or_else(|| {
                    anyhow::anyhow!(
                        "parent-id shard scratch path has no file name: {}",
                        p.display()
                    )
                })?;
                let out = out_dir.join(file_name.to_string_lossy().replace(".tmp", ".txt"));
                dedup_one(p, &out)
            })
            .collect::<Result<Vec<_>>>()?;
        let total_ids = shard_counts.into_iter().sum();

        // Success: `out_dir` is now the live dedup output and must survive;
        // only the raw shard scratch (`base_dir`) is disposable. Disarm the
        // guard so it leaves `out_dir` alone, then remove `base_dir`
        // explicitly (logging, not failing, on a cleanup hiccup).
        scratch.disarm();
        if let Err(e) = crate::util::remove_dir_all_with_short_backoff(&base_dir) {
            tracing::debug!(
                path = %base_dir.display(),
                error = %e,
                "failed to remove parent-id raw shard scratch"
            );
        }

        Ok(IdShards {
            dir: out_dir,
            count,
            total_ids,
            rs,
            kind: kind.to_string(),
            _scratch_root: scratch_root,
        })
    }
}
