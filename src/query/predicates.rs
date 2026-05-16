
/// Numeric operator used by [`JsonPointerPredicate::number`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericComparison {
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

impl NumericComparison {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GreaterThan => ">",
            Self::GreaterThanOrEqual => ">=",
            Self::LessThan => "<",
            Self::LessThanOrEqual => "<=",
        }
    }

    #[inline]
    fn matches(self, left: f64, right: f64) -> bool {
        match self {
            Self::GreaterThan => left > right,
            Self::GreaterThanOrEqual => left >= right,
            Self::LessThan => left < right,
            Self::LessThanOrEqual => left <= right,
        }
    }
}

#[derive(Debug, Clone)]
enum JsonPredicateKind {
    Exists,
    Equals(Value),
    NotEquals(Value),
    Number {
        op: NumericComparison,
        value: f64,
    },
    Regex {
        pattern: String,
        compiled: Option<Regex>,
    },
}

/// A predicate evaluated against an arbitrary JSON Pointer on the full record.
///
/// These predicates opt a query into the full-parse slow path; scans without
/// any JSON-pointer predicates continue to use only [`MinimalRecord`](crate::MinimalRecord)
/// fast-path fields.
#[derive(Debug, Clone)]
pub struct JsonPointerPredicate {
    pointer: String,
    kind: JsonPredicateKind,
}

impl JsonPointerPredicate {
    /// Match when the pointer resolves to any value, including JSON `null`.
    pub fn exists(pointer: impl Into<String>) -> Self {
        Self {
            pointer: pointer.into(),
            kind: JsonPredicateKind::Exists,
        }
    }

    /// Match when the pointer resolves to the provided scalar JSON value.
    pub fn equals(pointer: impl Into<String>, value: impl Into<Value>) -> Self {
        Self {
            pointer: pointer.into(),
            kind: JsonPredicateKind::Equals(value.into()),
        }
    }

    /// Match when the pointer resolves and is not equal to the provided scalar JSON value.
    pub fn not_equals(pointer: impl Into<String>, value: impl Into<Value>) -> Self {
        Self {
            pointer: pointer.into(),
            kind: JsonPredicateKind::NotEquals(value.into()),
        }
    }

    /// Match when the pointer resolves to a finite number (or numeric string)
    /// satisfying the numeric comparison.
    pub fn number(pointer: impl Into<String>, op: NumericComparison, value: f64) -> Self {
        Self {
            pointer: pointer.into(),
            kind: JsonPredicateKind::Number { op, value },
        }
    }

    /// Match when the pointer resolves to a string matched by `pattern`.
    /// The regex is compiled during [`ScanPlan::build`](crate::ScanPlan::build).
    pub fn regex(pointer: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self {
            pointer: pointer.into(),
            kind: JsonPredicateKind::Regex {
                pattern: pattern.into(),
                compiled: None,
            },
        }
    }

    pub fn pointer(&self) -> &str {
        &self.pointer
    }

    pub(crate) fn validate(&self) -> Result<(), QueryBuildError> {
        validate_json_pointer_syntax(&self.pointer)?;
        match &self.kind {
            JsonPredicateKind::Equals(value) | JsonPredicateKind::NotEquals(value) => {
                if !is_scalar_json(value) {
                    return Err(QueryBuildError::new(format!(
                        "JSON pointer predicate at {:?} uses a non-scalar equality value: {}",
                        self.pointer, value
                    )));
                }
            }
            JsonPredicateKind::Number { value, .. } => {
                if !value.is_finite() {
                    return Err(QueryBuildError::new(format!(
                        "JSON pointer predicate at {:?} uses a non-finite numeric comparison value: {}",
                        self.pointer, value
                    )));
                }
            }
            JsonPredicateKind::Regex { pattern, .. } => {
                Regex::new(pattern).map_err(|e| {
                    QueryBuildError::new(format!(
                        "JSON pointer predicate at {:?} has invalid regex value {:?}: {e}",
                        self.pointer, pattern
                    ))
                })?;
            }
            JsonPredicateKind::Exists => {}
        }
        Ok(())
    }

    pub(crate) fn compile_regex(mut self) -> Result<Self, QueryBuildError> {
        if let JsonPredicateKind::Regex { pattern, compiled } = &mut self.kind {
            let re = Regex::new(pattern).map_err(|e| {
                QueryBuildError::new(format!(
                    "JSON pointer predicate at {:?} has invalid regex value {:?}: {e}",
                    self.pointer, pattern
                ))
            })?;
            *compiled = Some(re);
        }
        Ok(self)
    }

    pub(crate) fn matches(&self, record: &Value) -> bool {
        match &self.kind {
            JsonPredicateKind::Exists => record.pointer(&self.pointer).is_some(),
            JsonPredicateKind::Equals(expected) => record
                .pointer(&self.pointer)
                .is_some_and(|actual| scalar_values_equal(actual, expected)),
            JsonPredicateKind::NotEquals(expected) => record
                .pointer(&self.pointer)
                .is_some_and(|actual| !scalar_values_equal(actual, expected)),
            JsonPredicateKind::Number { op, value } => record
                .pointer(&self.pointer)
                .and_then(value_to_f64)
                .is_some_and(|actual| op.matches(actual, *value)),
            JsonPredicateKind::Regex { pattern, compiled } => {
                let Some(text) = record.pointer(&self.pointer).and_then(Value::as_str) else {
                    return false;
                };
                match compiled {
                    Some(re) => re.is_match(text),
                    None => Regex::new(pattern)
                        .map(|re| re.is_match(text))
                        .unwrap_or(false),
                }
            }
        }
    }

    pub(crate) fn fingerprint_value(&self) -> Value {
        match &self.kind {
            JsonPredicateKind::Exists => serde_json::json!({
                "pointer": self.pointer.as_str(),
                "op": "exists",
            }),
            JsonPredicateKind::Equals(value) => serde_json::json!({
                "pointer": self.pointer.as_str(),
                "op": "=",
                "value": value,
            }),
            JsonPredicateKind::NotEquals(value) => serde_json::json!({
                "pointer": self.pointer.as_str(),
                "op": "!=",
                "value": value,
            }),
            JsonPredicateKind::Number { op, value } => serde_json::json!({
                "pointer": self.pointer.as_str(),
                "op": op.as_str(),
                "value": value,
            }),
            JsonPredicateKind::Regex { pattern, .. } => serde_json::json!({
                "pointer": self.pointer.as_str(),
                "op": "~=",
                "value": pattern,
            }),
        }
    }
}

fn is_scalar_json(value: &Value) -> bool {
    matches!(
        value,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)
    )
}

fn value_to_f64(value: &Value) -> Option<f64> {
    let n = match value {
        Value::Number(n) => n.as_f64()?,
        Value::String(s) => s.parse::<f64>().ok()?,
        _ => return None,
    };
    n.is_finite().then_some(n)
}

fn scalar_values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(a), Value::Number(b)) => match (a.as_f64(), b.as_f64()) {
            (Some(a), Some(b)) if a.is_finite() && b.is_finite() => a == b,
            _ => left == right,
        },
        _ => left == right,
    }
}

fn validate_json_pointer_syntax(pointer: &str) -> Result<(), QueryBuildError> {
    if !(pointer.is_empty() || pointer.starts_with('/')) {
        return Err(QueryBuildError::new(format!(
            "JSON pointer predicate has bad pointer {:?}; pointers must be empty or start with '/'",
            pointer
        )));
    }

    for token in pointer.split('/').skip(1) {
        let mut chars = token.chars();
        while let Some(ch) = chars.next() {
            if ch == '~' {
                match chars.next() {
                    Some('0') | Some('1') => {}
                    other => {
                        return Err(QueryBuildError::new(format!(
                            "JSON pointer predicate has bad pointer {:?}; '~' must be escaped as '~0' or '~1' (found {:?})",
                            pointer, other
                        )));
                    }
                }
            }
        }
    }

    Ok(())
}
