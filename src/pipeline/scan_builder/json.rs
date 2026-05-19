impl ScanPlan {
    /// Add an arbitrary full-record JSON Pointer predicate.
    pub fn json_predicate(mut self, predicate: JsonPointerPredicate) -> Self {
        self.query.json_predicates.push(predicate);
        self
    }
    /// Add multiple arbitrary full-record JSON Pointer predicates.
    pub fn json_predicates<I>(mut self, predicates: I) -> Self
    where
        I: IntoIterator<Item = JsonPointerPredicate>,
    {
        self.query.json_predicates.extend(predicates);
        self
    }
    /// Keep records where `pointer` exists, including JSON `null` values.
    pub fn json_exists(self, pointer: impl Into<String>) -> Self {
        self.json_predicate(JsonPointerPredicate::exists(pointer))
    }
    /// Keep records where `pointer` equals a scalar JSON value.
    pub fn json_eq(self, pointer: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        self.json_predicate(JsonPointerPredicate::equals(pointer, value))
    }
    /// Keep records where `pointer` exists and does not equal a scalar JSON value.
    pub fn json_ne(self, pointer: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        self.json_predicate(JsonPointerPredicate::not_equals(pointer, value))
    }
    /// Keep records where `pointer` resolves to a finite number (or numeric string)
    /// satisfying `op` against `value`.
    pub fn json_number_cmp(
        self,
        pointer: impl Into<String>,
        op: NumericComparison,
        value: f64,
    ) -> Self {
        self.json_predicate(JsonPointerPredicate::number(pointer, op, value))
    }
    pub fn json_number_gt(self, pointer: impl Into<String>, value: f64) -> Self {
        self.json_number_cmp(pointer, NumericComparison::GreaterThan, value)
    }
    pub fn json_number_gte(self, pointer: impl Into<String>, value: f64) -> Self {
        self.json_number_cmp(pointer, NumericComparison::GreaterThanOrEqual, value)
    }
    pub fn json_number_lt(self, pointer: impl Into<String>, value: f64) -> Self {
        self.json_number_cmp(pointer, NumericComparison::LessThan, value)
    }
    pub fn json_number_lte(self, pointer: impl Into<String>, value: f64) -> Self {
        self.json_number_cmp(pointer, NumericComparison::LessThanOrEqual, value)
    }
    /// Keep records where `pointer` resolves to a string matched by `pattern`.
    pub fn json_regex(self, pointer: impl Into<String>, pattern: impl Into<String>) -> Self {
        self.json_predicate(JsonPointerPredicate::regex(pointer, pattern))
    }
}
