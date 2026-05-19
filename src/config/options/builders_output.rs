impl ETLOptions {
    // Output: human-readable timestamps
    pub fn with_human_timestamps(mut self, yes: bool) -> Self {
        self.human_readable_timestamps = yes;
        self
    }

    /// Set the zstd compression level used when writing partitioned `.zst`
    /// outputs. zstd's accepted range is 1..=22; values outside that band are
    /// clamped. Default: 7 (good ratio, ~5x faster than 19 on real workloads).
    pub fn with_zst_level(mut self, level: i32) -> Self {
        self.zst_level = level.clamp(1, 22);
        self
    }

    /// Opt in to resumable extract/export and analytics runs (`scan`/usernames,
    /// dedupe, count, first-seen, `extract_to_jsonl`, `extract_to_json`,
    /// `extract_spool_monthly`, `export_partitioned`, and parents helpers).
    pub fn with_resume(mut self, yes: bool) -> Self {
        self.resume = yes;
        self
    }

    /// Enable or disable user-facing provenance manifest sidecars next to
    /// file/directory outputs. Enabled by default.
    pub fn with_run_manifest(mut self, yes: bool) -> Self {
        self.emit_manifest = yes;
        self
    }

    /// Select top-level parent fields attached by `resolve_parent_maps` /
    /// `attach_parents_jsonls_parallel`.
    pub fn with_parent_fields<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.parent_payload_spec = ParentPayloadSpec::from_fields(fields);
        self
    }

    /// Attach the full source parent JSON record when resolving parents.
    pub fn with_parent_full(mut self, yes: bool) -> Self {
        self.parent_payload_spec = self.parent_payload_spec.with_full_record(yes);
        self
    }

    /// Replace the full parent-payload specification.
    pub fn with_parent_payload_spec(mut self, spec: ParentPayloadSpec) -> Self {
        self.parent_payload_spec = spec;
        self
    }

    /// Opt in to lossy corpus scans/exports that skip corrupt zstd monthly
    /// files instead of failing the operation. Skipped paths are collected in
    /// [`PartialReadReporter`] and incomplete months are not committed to
    /// resume manifests.
    pub fn with_allow_partial(mut self, yes: bool) -> Self {
        self.allow_partial = yes;
        self
    }
}
