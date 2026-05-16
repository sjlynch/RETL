
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct AttachFingerprint {
    version: u32,
    attach_format_version: u32,
    input: AttachFileIdentity,
    resolution_range: AttachResolutionRange,
    parent_cache: AttachParentCacheFingerprint,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct AttachResolutionRange {
    start: Option<String>,
    end: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct AttachParentCacheFingerprint {
    payload: ParentPayloadFingerprint,
    comment_shards: AttachShardSetFingerprint,
    submission_shards: AttachShardSetFingerprint,
    eager_comments: AttachMapDigest,
    eager_submissions: AttachMapDigest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct AttachShardSetFingerprint {
    index_present: bool,
    shards: u64,
    digest: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct AttachMapDigest {
    entries: u64,
    digest: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct AttachFileIdentity {
    path: String,
    exists: bool,
    len: Option<u64>,
    modified_unix_secs: Option<i64>,
    modified_nanos: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ResolverFingerprint {
    version: u32,
    resolver_format_version: u32,
    source: ResolverSourceFingerprint,
    resolution_range: AttachResolutionRange,
    parent_ids: ParentIdsFingerprint,
    payload: ParentPayloadFingerprint,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ResolverSourceFingerprint {
    kind: String,
    month: String,
    file: AttachFileIdentity,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ParentPayloadFingerprint {
    payload_format_version: u32,
    full_record: bool,
    fields: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ParentIdsFingerprint {
    t1: ParentIdSetFingerprint,
    t3: ParentIdSetFingerprint,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ParentIdSetFingerprint {
    kind: String,
    storage: String,
    ids: u64,
    digest: String,
    shard_count: u64,
    backing_shards: Vec<ParentIdShardFingerprint>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ParentIdShardFingerprint {
    index: u64,
    ids: u64,
    digest: String,
    len: u64,
}
