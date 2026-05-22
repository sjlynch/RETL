
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
        /// Compiled form of `pattern`. Populated eagerly by
        /// [`QuerySpec::compile_json_predicates`] (run by `ScanPlan::build`),
        /// or — for a directly-built `QuerySpec` that skipped the builder —
        /// lazily on the first [`JsonPointerPredicate::matches`] call. The
        /// `OnceLock` guarantees the regex is compiled at most once either
        /// way, so `matches` never recompiles per record.
        compiled: OnceLock<Regex>,
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
    ///
    /// [`ScanPlan::build`](crate::ScanPlan::build) compiles the pattern
    /// eagerly and surfaces an invalid pattern as a [`QueryBuildError`]. A
    /// [`QuerySpec`] constructed directly and scanned without going through
    /// `build` still compiles the regex only **once** — lazily, on the first
    /// match — but an invalid pattern then *panics* at scan time instead of
    /// failing the build. Call [`QuerySpec::validate`] (or use the builder)
    /// to surface a bad pattern up front.
    pub fn regex(pointer: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self {
            pointer: pointer.into(),
            kind: JsonPredicateKind::Regex {
                pattern: pattern.into(),
                compiled: OnceLock::new(),
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

    pub(crate) fn compile_regex(self) -> Result<Self, QueryBuildError> {
        if let JsonPredicateKind::Regex { pattern, compiled } = &self.kind {
            let re = Regex::new(pattern).map_err(|e| {
                QueryBuildError::new(format!(
                    "JSON pointer predicate at {:?} has invalid regex value {:?}: {e}",
                    self.pointer, pattern
                ))
            })?;
            // `set` only fails when already compiled (e.g. `compile_regex`
            // called twice); the existing regex is equivalent, so ignore it.
            let _ = compiled.set(re);
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
                // Compile once. `compile_json_predicates` (via `ScanPlan::build`)
                // normally populates this; a directly-built `QuerySpec` that
                // skipped the builder gets a single lazy compile here rather
                // than one `Regex::new` per record. An invalid, never-validated
                // pattern panics loudly instead of silently matching nothing.
                let re = compiled.get_or_init(|| {
                    Regex::new(pattern).unwrap_or_else(|e| {
                        panic!(
                            "JSON pointer predicate at {:?} has an invalid, \
                             never-validated regex {:?}: {e}; call \
                             QuerySpec::validate() or ScanPlan::build() before scanning",
                            self.pointer, pattern
                        )
                    })
                });
                re.is_match(text)
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
        // Two numbers, or a number paired with a string, compare numerically
        // via `value_to_f64` so `--json '/score=100'` agrees with
        // `--json '/score>=100'` (which already coerces) and with the
        // `--min-score` fast path on string-/float-typed `score`. A non-numeric
        // string falls back to structural inequality. String-vs-string stays an
        // exact match — coercing it would silently change unrelated `=` checks.
        (Value::Number(_), Value::Number(_))
        | (Value::Number(_), Value::String(_))
        | (Value::String(_), Value::Number(_)) => match (value_to_f64(left), value_to_f64(right)) {
            (Some(a), Some(b)) => a == b,
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
