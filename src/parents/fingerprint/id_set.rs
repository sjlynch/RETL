
fn fingerprint_mem_id_set(kind: &str, ids: &AHashSet<String>) -> ParentIdSetFingerprint {
    let mut count = 0u64;
    let mut sum = 0u64;
    let mut xor = 0u64;
    for id in ids {
        update_unordered_id_digest(&mut count, &mut sum, &mut xor, id);
    }
    ParentIdSetFingerprint {
        kind: kind.to_string(),
        storage: "memory".to_string(),
        ids: count,
        digest: finish_unordered_digest_string(count, sum, xor),
        shard_count: 0,
        backing_shards: Vec::new(),
    }
}

fn fingerprint_empty_id_set(kind: &str) -> ParentIdSetFingerprint {
    ParentIdSetFingerprint {
        kind: kind.to_string(),
        storage: "empty".to_string(),
        ids: 0,
        digest: finish_unordered_digest_string(0, 0, 0),
        shard_count: 0,
        backing_shards: Vec::new(),
    }
}

fn fingerprint_id_shard_file(path: &Path) -> Result<(u64, u64, u64)> {
    let f = crate::util::open_with_default_backoff(path)
        .with_context(|| format!("open parent-id shard {}", path.display()))?;
    let mut r = BufReader::new(f);
    let mut count = 0u64;
    let mut sum = 0u64;
    let mut xor = 0u64;
    let mut id = String::new();
    loop {
        let n = read_line_capped(&mut r, &mut id, DEFAULT_MAX_LINE_BYTES, path)
            .with_context(|| format!("read parent-id shard {}", path.display()))?;
        if n == 0 {
            break;
        }
        if !id.is_empty() {
            update_unordered_id_digest(&mut count, &mut sum, &mut xor, &id);
        }
    }
    Ok((count, sum, xor))
}

fn fingerprint_sharded_id_set(kind: &str, shards: &IdShards) -> Result<ParentIdSetFingerprint> {
    let mut total_count = 0u64;
    let mut total_sum = 0u64;
    let mut total_xor = 0u64;
    let mut backing_shards = Vec::with_capacity(shards.count);

    for idx in 0..shards.count {
        let path = shards.path_for(idx);
        let metadata = fs::metadata(&path)
            .with_context(|| format!("stat parent-id shard {}", path.display()))?;
        let (count, sum, xor) = fingerprint_id_shard_file(&path)?;
        total_count += count;
        total_sum = total_sum.wrapping_add(sum);
        total_xor ^= xor;
        backing_shards.push(ParentIdShardFingerprint {
            index: idx as u64,
            ids: count,
            digest: finish_unordered_digest_string(count, sum, xor),
            len: metadata.len(),
        });
    }

    Ok(ParentIdSetFingerprint {
        kind: kind.to_string(),
        storage: "sharded".to_string(),
        ids: total_count,
        digest: finish_unordered_digest_string(total_count, total_sum, total_xor),
        shard_count: shards.count as u64,
        backing_shards,
    })
}

fn fingerprint_mixed_id_set(
    kind: &str,
    mem: &AHashSet<String>,
    shards: &IdShards,
) -> Result<ParentIdSetFingerprint> {
    let mem_fp = fingerprint_mem_id_set(kind, mem);
    let shard_fp = fingerprint_sharded_id_set(kind, shards)?;
    let ids = mem_fp.ids + shard_fp.ids;

    let mut hash = fnv1a_offset_basis();
    update_digest_str(&mut hash, &mem_fp.digest);
    update_digest_str(&mut hash, &shard_fp.digest);

    Ok(ParentIdSetFingerprint {
        kind: kind.to_string(),
        storage: "mixed".to_string(),
        ids,
        digest: finish_unordered_digest_string(ids, hash, 0),
        shard_count: shard_fp.shard_count,
        backing_shards: shard_fp.backing_shards,
    })
}

fn parent_id_set_fingerprint(
    kind: &str,
    mem: Option<&AHashSet<String>>,
    sharded: Option<&IdShards>,
) -> Result<ParentIdSetFingerprint> {
    match (mem, sharded) {
        (Some(ids), Some(shards)) if !ids.is_empty() => fingerprint_mixed_id_set(kind, ids, shards),
        (_, Some(shards)) => fingerprint_sharded_id_set(kind, shards),
        (Some(ids), None) => Ok(fingerprint_mem_id_set(kind, ids)),
        (None, None) => Ok(fingerprint_empty_id_set(kind)),
    }
}

fn parent_ids_fingerprint(ids: &ParentIds) -> Result<ParentIdsFingerprint> {
    Ok(ParentIdsFingerprint {
        t1: parent_id_set_fingerprint("t1", ids.t1_ids_mem.as_ref(), ids.t1_ids_sharded.as_ref())?,
        t3: parent_id_set_fingerprint("t3", ids.t3_ids_mem.as_ref(), ids.t3_ids_sharded.as_ref())?,
    })
}
