// Field-selector parsing/validation for tabular (CSV/TSV) exports.
//
// Concern (1) split out of `pipeline_exec/tabular.rs`: parse and validate the
// user-supplied field names that drive CSV/TSV projection, and resolve each
// selector against a `serde_json::Value` record.

fn normalize_tabular_fields<I, S>(fields: I) -> Result<Vec<String>>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let fields: Vec<String> = fields
        .into_iter()
        .filter_map(|field| {
            let field = field.into();
            let field = field.trim();
            (!field.is_empty()).then(|| field.to_string())
        })
        .collect();
    if fields.is_empty() {
        anyhow::bail!("CSV/TSV export requires at least one whitelisted field");
    }
    Ok(fields)
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TabularFieldSelector {
    TopLevel(String),
    JsonPointer(String),
    Dotted(Vec<String>),
}

impl TabularFieldSelector {
    fn parse(raw: &str) -> Result<Self> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            anyhow::bail!("CSV/TSV field names must not be empty");
        }
        if let Some(pointer) = trimmed.strip_prefix("json:") {
            validate_tabular_json_pointer(pointer)?;
            return Ok(Self::JsonPointer(pointer.to_string()));
        }
        if trimmed.starts_with('/') {
            validate_tabular_json_pointer(trimmed)?;
            return Ok(Self::JsonPointer(trimmed.to_string()));
        }
        if trimmed.contains('.') {
            let parts: Vec<String> = trimmed
                .split('.')
                .map(str::trim)
                .map(str::to_string)
                .collect();
            if parts.iter().any(String::is_empty) {
                anyhow::bail!(
                    "bad dotted tabular field {trimmed:?}: path segments must not be empty; use JSON Pointer syntax for unusual keys"
                );
            }
            return Ok(Self::Dotted(parts));
        }
        Ok(Self::TopLevel(trimmed.to_string()))
    }

    fn value<'a>(&self, record: &'a Value) -> Option<&'a Value> {
        match self {
            Self::TopLevel(key) => record.as_object().and_then(|map| map.get(key)),
            Self::JsonPointer(pointer) => record.pointer(pointer),
            Self::Dotted(parts) => {
                let mut cur = record;
                for part in parts {
                    cur = cur.as_object()?.get(part)?;
                }
                Some(cur)
            }
        }
    }
}

fn parse_tabular_field_selectors(fields: &[String]) -> Result<Vec<TabularFieldSelector>> {
    fields
        .iter()
        .map(|field| TabularFieldSelector::parse(field))
        .collect()
}

fn validate_tabular_json_pointer(pointer: &str) -> Result<()> {
    if !pointer.starts_with('/') {
        anyhow::bail!("JSON Pointer tabular fields must start with '/': {pointer:?}");
    }
    let mut chars = pointer.chars();
    while let Some(ch) = chars.next() {
        if ch == '~' {
            match chars.next() {
                Some('0') | Some('1') => {}
                other => anyhow::bail!(
                    "bad JSON Pointer tabular field {pointer:?}: invalid escape near '~{:?}'; use '~0' for '~' and '~1' for '/'",
                    other
                ),
            }
        }
    }
    Ok(())
}
