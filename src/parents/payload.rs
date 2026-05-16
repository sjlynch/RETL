
/// A resolved parent payload stored in structured parent-cache shards.
pub type ParentPayload = Map<String, Value>;

const DEFAULT_PARENT_PAYLOAD_FIELDS: &[&str] = &["body", "selftext", "title"];

/// Selects which top-level fields from a resolved parent record are attached
/// under the child record's `parent` object.
///
/// The default preserves RETL's original parents output: comment parents carry
/// `body`, while submission parents carry `title` and `selftext`. `kind` and
/// `id` metadata are always attached when a parent resolves successfully.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ParentPayloadSpec {
    fields: Vec<String>,
    full_record: bool,
}

impl Default for ParentPayloadSpec {
    fn default() -> Self {
        Self::from_fields(DEFAULT_PARENT_PAYLOAD_FIELDS)
    }
}

impl ParentPayloadSpec {
    /// Return the backwards-compatible default (`body,title,selftext`).
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a spec that copies the named top-level parent fields. Empty names
    /// are ignored and duplicates are de-duplicated after trimming.
    pub fn from_fields<I, S>(fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let fields = normalize_parent_payload_fields(fields);
        Self {
            fields,
            full_record: false,
        }
    }

    /// Attach the full source parent JSON record (plus RETL's `kind`/`id`
    /// metadata). When enabled, explicit field selections are ignored.
    pub fn full_record() -> Self {
        Self {
            fields: Vec::new(),
            full_record: true,
        }
    }

    /// Toggle full-record attachment on this spec. Enabling full-record mode
    /// clears the field list so equivalent full specs fingerprint the same way.
    pub fn with_full_record(mut self, yes: bool) -> Self {
        self.full_record = yes;
        if yes {
            self.fields.clear();
        }
        self
    }

    /// Selected top-level fields, sorted and de-duplicated. Empty when
    /// [`Self::is_full_record`] is true.
    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    /// Whether the full parent JSON record should be attached.
    pub fn is_full_record(&self) -> bool {
        self.full_record
    }

    fn is_legacy_default(&self) -> bool {
        !self.full_record
            && self.fields == normalize_parent_payload_fields(DEFAULT_PARENT_PAYLOAD_FIELDS)
    }

    fn payload_format_version(&self) -> u32 {
        if self.is_legacy_default() {
            LEGACY_PARENT_PAYLOAD_FORMAT_VERSION
        } else {
            STRUCTURED_PARENT_PAYLOAD_FORMAT_VERSION
        }
    }
}

fn normalize_parent_payload_fields<I, S>(fields: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    fields
        .into_iter()
        .filter_map(|field| {
            let field = field.as_ref().trim();
            if field.is_empty() {
                None
            } else {
                Some(field.to_string())
            }
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}
