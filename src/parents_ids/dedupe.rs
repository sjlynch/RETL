
/// Reduce one line-oriented parent-id shard into a deduped ID file.
///
/// Thin wrapper over [`crate::shard_common::dedup_line_shard`] with
/// `sort = false`: parent-id shards have no sorted-layout requirement, so the
/// `AHashSet` iteration order is written directly (output order unspecified).
/// `shard::ShardedWriter` shares the same helper with `sort = true` — see
/// `dedup_line_shard` for why the two stages must not keep separate copies.
fn dedup_one(input: &Path, output: &Path) -> Result<usize> {
    crate::shard_common::dedup_line_shard(input, output, false)
}
