
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ParentAttachDiagnostics {
    files_scanned: u64,
    resume_skipped_files: u64,
    parsed_records: u64,
    rc_records: u64,
    records_with_body: u64,
    records_with_parent_id: u64,
    records_with_link_id: u64,
    comment_shaped_records: u64,
    /// Records that carry a `parent_id` field whose value is not a string
    /// prefixed with `t1_`/`t3_` (unprefixed, numeric, or null). These fail
    /// `is_comment_record_for_parent_attach` and are silently skipped — see
    /// `warn_if_malformed_parent_ids`.
    records_with_unprefixed_parent_id: u64,
}

impl ParentAttachDiagnostics {
    fn add(&mut self, other: Self) {
        self.files_scanned += other.files_scanned;
        self.resume_skipped_files += other.resume_skipped_files;
        self.parsed_records += other.parsed_records;
        self.rc_records += other.rc_records;
        self.records_with_body += other.records_with_body;
        self.records_with_parent_id += other.records_with_parent_id;
        self.records_with_link_id += other.records_with_link_id;
        self.comment_shaped_records += other.comment_shaped_records;
        self.records_with_unprefixed_parent_id += other.records_with_unprefixed_parent_id;
    }
}

const ATTACH_INITIAL_DIAGNOSTIC_SAMPLE_RECORDS: u64 = 100;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ParentAttachInitialShape {
    parsed_records: u64,
    records_with_body: u64,
    records_with_parent_id: u64,
    records_with_link_id: u64,
    records_with_body_and_parent_id: u64,
    records_with_prefixed_parent_id: u64,
}

impl ParentAttachInitialShape {
    fn observe(&mut self, v: &Value) {
        let has_body = v.get("body").is_some();
        let parent_id = v.get("parent_id").and_then(|x| x.as_str());
        let has_parent_id = v.get("parent_id").is_some();
        let has_link_id = v.get("link_id").is_some();

        self.parsed_records += 1;
        if has_body {
            self.records_with_body += 1;
        }
        if has_parent_id {
            self.records_with_parent_id += 1;
        }
        if has_link_id {
            self.records_with_link_id += 1;
        }
        if has_body && has_parent_id {
            self.records_with_body_and_parent_id += 1;
        }
        if parent_id
            .map(|id| id.starts_with("t1_") || id.starts_with("t3_"))
            .unwrap_or(false)
        {
            self.records_with_prefixed_parent_id += 1;
        }
    }

    fn no_legacy_comment_shape(self) -> bool {
        self.parsed_records > 0 && self.records_with_body_and_parent_id == 0
    }

    fn observed_shape(self) -> &'static str {
        match (self.records_with_parent_id > 0, self.records_with_body > 0) {
            (true, false) => "sample records had `parent_id` but no `body`",
            (false, true) => "sample records had `body` but no `parent_id`",
            (false, false) => "sample records had neither `body` nor `parent_id`",
            (true, true) => {
                "sample records mixed `body` and `parent_id`, but no single record had both"
            }
        }
    }
}
