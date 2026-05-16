
fn update_digest_bytes(hash: &mut u64, bytes: &[u8]) {
    fnv1a_update(hash, bytes);
}

fn update_digest_bool(hash: &mut u64, value: bool) {
    update_digest_bytes(hash, &[u8::from(value)]);
}

fn update_digest_u32(hash: &mut u64, value: u32) {
    update_digest_bytes(hash, &value.to_le_bytes());
}

fn update_digest_u64(hash: &mut u64, value: u64) {
    update_digest_bytes(hash, &value.to_le_bytes());
}

fn update_digest_i64(hash: &mut u64, value: i64) {
    update_digest_bytes(hash, &value.to_le_bytes());
}

fn update_digest_str(hash: &mut u64, value: &str) {
    update_digest_u64(hash, value.len() as u64);
    update_digest_bytes(hash, value.as_bytes());
}

fn update_digest_option_u64(hash: &mut u64, value: Option<u64>) {
    update_digest_bool(hash, value.is_some());
    if let Some(value) = value {
        update_digest_u64(hash, value);
    }
}

fn update_digest_option_i64(hash: &mut u64, value: Option<i64>) {
    update_digest_bool(hash, value.is_some());
    if let Some(value) = value {
        update_digest_i64(hash, value);
    }
}

fn update_digest_option_u32(hash: &mut u64, value: Option<u32>) {
    update_digest_bool(hash, value.is_some());
    if let Some(value) = value {
        update_digest_u32(hash, value);
    }
}

fn update_file_identity_digest(hash: &mut u64, identity: &AttachFileIdentity) {
    update_digest_str(hash, &identity.path);
    update_digest_bool(hash, identity.exists);
    update_digest_option_u64(hash, identity.len);
    update_digest_option_i64(hash, identity.modified_unix_secs);
    update_digest_option_u32(hash, identity.modified_nanos);
}

fn finish_unordered_digest_string(entries: u64, sum: u64, xor: u64) -> String {
    let mut hash = fnv1a_offset_basis();
    update_digest_u64(&mut hash, entries);
    update_digest_u64(&mut hash, sum);
    update_digest_u64(&mut hash, xor);
    format!("{hash:016x}")
}

fn finish_unordered_digest(entries: usize, sum: u64, xor: u64) -> AttachMapDigest {
    AttachMapDigest {
        entries: entries as u64,
        digest: finish_unordered_digest_string(entries as u64, sum, xor),
    }
}

fn update_unordered_id_digest(count: &mut u64, sum: &mut u64, xor: &mut u64, id: &str) {
    let mut entry_hash = fnv1a_offset_basis();
    update_digest_str(&mut entry_hash, id);
    *count += 1;
    *sum = sum.wrapping_add(entry_hash);
    *xor ^= entry_hash.rotate_left((entry_hash & 63) as u32);
}
