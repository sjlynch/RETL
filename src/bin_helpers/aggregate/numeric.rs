// -----------------------------------------------------------------------------
// Exact-arithmetic core: numeric value/sum/sort-key types plus the parsers
// and comparators used by the grouped aggregator.
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
enum MetricNumber {
    Int(i128),
    Float(f64),
}

impl MetricNumber {
    fn format(self, format: NumberFormat) -> String {
        match self {
            Self::Int(n) => n.to_string(),
            Self::Float(n) => format_number(n, format),
        }
    }

    fn sort_value(self) -> MetricSortValue {
        match self {
            Self::Int(n) => MetricSortValue::Int(n),
            Self::Float(n) => MetricSortValue::Float(n),
        }
    }

    fn cmp_numeric(&self, other: &Self) -> Ordering {
        match (*self, *other) {
            (Self::Int(a), Self::Int(b)) => a.cmp(&b),
            (Self::Float(a), Self::Float(b)) => a.total_cmp(&b),
            (Self::Int(a), Self::Float(b)) => cmp_i128_f64(a, b),
            (Self::Float(a), Self::Int(b)) => cmp_i128_f64(b, a).reverse(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
enum NumericSum {
    Int(i128),
    Float(f64),
}

impl Default for NumericSum {
    fn default() -> Self {
        Self::Int(0)
    }
}

impl NumericSum {
    fn add_number(&mut self, n: MetricNumber) {
        match (*self, n) {
            (Self::Int(sum), MetricNumber::Int(n)) => {
                *self = sum
                    .checked_add(n)
                    .map(Self::Int)
                    .unwrap_or_else(|| Self::Float(sum as f64 + n as f64));
            }
            (Self::Int(sum), MetricNumber::Float(n)) => {
                *self = Self::Float(sum as f64 + n);
            }
            (Self::Float(sum), MetricNumber::Int(n)) => {
                *self = Self::Float(sum + n as f64);
            }
            (Self::Float(sum), MetricNumber::Float(n)) => {
                *self = Self::Float(sum + n);
            }
        }
    }

    fn merge(&mut self, other: Self) {
        match other {
            Self::Int(n) => self.add_number(MetricNumber::Int(n)),
            Self::Float(n) => self.add_number(MetricNumber::Float(n)),
        }
    }

    fn format_sum(self, format: NumberFormat) -> String {
        match self {
            Self::Int(n) => n.to_string(),
            Self::Float(n) => format_number(n, format),
        }
    }

    fn format_avg(self, count: u64, format: NumberFormat) -> String {
        debug_assert!(count > 0);
        match self {
            Self::Int(sum) => format_integer_average(sum, count),
            Self::Float(sum) => format_number(sum / count as f64, format),
        }
    }

    fn sort_sum(self) -> MetricSortValue {
        match self {
            Self::Int(n) => MetricSortValue::Int(n),
            Self::Float(n) => MetricSortValue::Float(n),
        }
    }

    fn sort_avg(self, count: u64) -> MetricSortValue {
        debug_assert!(count > 0);
        match self {
            Self::Int(sum) => MetricSortValue::Ratio {
                numerator: sum,
                denominator: count,
            },
            Self::Float(sum) => MetricSortValue::Float(sum / count as f64),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum MetricSortValue {
    Int(i128),
    Float(f64),
    Ratio { numerator: i128, denominator: u64 },
}

impl MetricSortValue {
    fn cmp_numeric(&self, other: &Self) -> Ordering {
        match (*self, *other) {
            (Self::Int(a), Self::Int(b)) => a.cmp(&b),
            (Self::Float(a), Self::Float(b)) => a.total_cmp(&b),
            (
                Self::Ratio {
                    numerator: a_num,
                    denominator: a_den,
                },
                Self::Ratio {
                    numerator: b_num,
                    denominator: b_den,
                },
            ) => cmp_i128_ratios(a_num, a_den, b_num, b_den),
            (
                Self::Ratio {
                    numerator,
                    denominator,
                },
                Self::Int(n),
            ) => cmp_i128_ratios(numerator, denominator, n, 1),
            (
                Self::Int(n),
                Self::Ratio {
                    numerator,
                    denominator,
                },
            ) => cmp_i128_ratios(n, 1, numerator, denominator),
            (Self::Int(a), Self::Float(b)) => cmp_i128_f64(a, b),
            (Self::Float(a), Self::Int(b)) => cmp_i128_f64(b, a).reverse(),
            (Self::Float(a), other) => a.total_cmp(&other.as_f64_lossy()),
            (other, Self::Float(b)) => other.as_f64_lossy().total_cmp(&b),
        }
    }

    fn as_f64_lossy(self) -> f64 {
        match self {
            Self::Int(n) => n as f64,
            Self::Float(n) => n,
            Self::Ratio {
                numerator,
                denominator,
            } => numerator as f64 / denominator as f64,
        }
    }
}

fn value_to_metric_number(v: &Value) -> Option<MetricNumber> {
    match v {
        Value::Number(n) => metric_number_from_str(&n.to_string()),
        Value::String(s) => metric_number_from_str(s),
        _ => None,
    }
}

fn metric_number_from_str(raw: &str) -> Option<MetricNumber> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    if is_plain_integer_literal(s) {
        if let Ok(n) = s.parse::<i128>() {
            return Some(MetricNumber::Int(n));
        }
    }
    let n = s.parse::<f64>().ok()?;
    n.is_finite().then_some(MetricNumber::Float(n))
}

fn is_plain_integer_literal(s: &str) -> bool {
    let digits = s
        .strip_prefix('-')
        .or_else(|| s.strip_prefix('+'))
        .unwrap_or(s);
    !digits.is_empty() && digits.as_bytes().iter().all(u8::is_ascii_digit)
}

fn cmp_i128_ratios(a_num: i128, a_den: u64, b_num: i128, b_den: u64) -> Ordering {
    debug_assert!(a_den > 0 && b_den > 0);
    match (
        a_num.checked_mul(b_den as i128),
        b_num.checked_mul(a_den as i128),
    ) {
        (Some(a), Some(b)) => a.cmp(&b),
        _ => (a_num as f64 / a_den as f64).total_cmp(&(b_num as f64 / b_den as f64)),
    }
}

/// Exact ordering of an `i128` against an `f64`.
///
/// `i as f64` rounds for `|i| > 2^53`, so the old `(i as f64).total_cmp(&f)`
/// could report `Equal` for an integer that is strictly different from `f`
/// — and `MetricState::ingest_number` would then keep the wrong group
/// min/max. This compares exactly by bracketing `f` with `floor(f)`.
///
/// NaN is ordered deterministically rather than returned as `Equal`:
/// matching `f64::total_cmp`, a positive-signed NaN sorts above every
/// integer and a negative-signed NaN below every integer.
fn cmp_i128_f64(i: i128, f: f64) -> Ordering {
    if f.is_nan() {
        return if f.is_sign_negative() {
            // -NaN is the minimum: every integer is greater.
            Ordering::Greater
        } else {
            // +NaN is the maximum: every integer is less.
            Ordering::Less
        };
    }

    // 2^127 is exactly representable in f64. `i128::MAX < 2^127` and
    // `i128::MIN == -2^127`, so any `f` outside `[-2^127, 2^127)` (including
    // the infinities) is strictly beyond the integer's possible range.
    const TWO_POW_127: f64 = 170_141_183_460_469_231_731_687_303_715_884_105_728.0;
    if f >= TWO_POW_127 {
        return Ordering::Less; // i <= i128::MAX < 2^127 <= f
    }
    if f < -TWO_POW_127 {
        return Ordering::Greater; // i >= i128::MIN = -2^127 > f
    }

    // `f` is finite and within `[-2^127, 2^127)`, so `floor(f)` is an
    // integer-valued f64 that converts to `i128` losslessly.
    let floor = f.floor();
    match i.cmp(&(floor as i128)) {
        // i <= floor(f) - 1 < floor(f) <= f
        Ordering::Less => Ordering::Less,
        // i >= floor(f) + 1 > f
        Ordering::Greater => Ordering::Greater,
        // i == floor(f): equal only when f has no fractional part.
        Ordering::Equal => {
            if f == floor {
                Ordering::Equal
            } else {
                Ordering::Less
            }
        }
    }
}

#[cfg(test)]
mod tests_numeric {
    use super::*;

    #[test]
    fn metric_number_parses_large_integer_strings_exactly() {
        assert_eq!(
            metric_number_from_str("9007199254740993"),
            Some(MetricNumber::Int(9_007_199_254_740_993))
        );
    }

    #[test]
    fn value_to_metric_number_reads_large_json_integer_exactly() {
        let value: Value = serde_json::from_str("90071992547409931234").unwrap();
        assert_eq!(
            value_to_metric_number(&value),
            Some(MetricNumber::Int(90_071_992_547_409_931_234_i128))
        );
    }

    #[test]
    fn cmp_i128_f64_is_exact_past_f64_mantissa() {
        // 2^53 + 1 has no f64 representation; the old `i as f64` cast
        // rounded it to 2^53 and reported Equal against the float 2^53.
        let pow53 = 1_i128 << 53;
        let float_pow53 = (1_u64 << 53) as f64;
        assert_eq!(cmp_i128_f64(pow53 + 1, float_pow53), Ordering::Greater);
        assert_eq!(cmp_i128_f64(pow53, float_pow53), Ordering::Equal);
        assert_eq!(cmp_i128_f64(pow53 - 1, float_pow53), Ordering::Less);
    }

    #[test]
    fn cmp_i128_f64_brackets_fractional_floats() {
        assert_eq!(cmp_i128_f64(2, 2.5), Ordering::Less);
        assert_eq!(cmp_i128_f64(3, 2.5), Ordering::Greater);
        assert_eq!(cmp_i128_f64(-3, -2.5), Ordering::Less);
        assert_eq!(cmp_i128_f64(-2, -2.5), Ordering::Greater);
        assert_eq!(cmp_i128_f64(0, 0.0), Ordering::Equal);
    }

    #[test]
    fn cmp_i128_f64_orders_nan_and_infinities_deterministically() {
        assert_eq!(cmp_i128_f64(0, f64::INFINITY), Ordering::Less);
        assert_eq!(cmp_i128_f64(0, f64::NEG_INFINITY), Ordering::Greater);
        // +NaN sorts above every integer, -NaN below — never Equal.
        assert_eq!(cmp_i128_f64(i128::MAX, f64::NAN), Ordering::Less);
        assert_eq!(cmp_i128_f64(i128::MIN, -f64::NAN), Ordering::Greater);
    }

    #[test]
    fn cmp_i128_f64_handles_floats_beyond_i128_range() {
        assert_eq!(cmp_i128_f64(i128::MAX, 1e300), Ordering::Less);
        assert_eq!(cmp_i128_f64(i128::MIN, -1e300), Ordering::Greater);
    }

    #[test]
    fn metric_min_max_keeps_exact_value_past_f64_precision() {
        // The integer is strictly larger than the float, but both round to
        // the same f64. Min/max must keep the exact values regardless.
        let big_int = MetricNumber::Int((1_i128 << 53) + 1);
        let float_at_pow53 = MetricNumber::Float((1_u64 << 53) as f64);
        let mut state = MetricState::default();
        state.ingest_number(big_int);
        state.ingest_number(float_at_pow53);
        assert_eq!(state.max, Some(big_int));
        assert_eq!(state.min, Some(float_at_pow53));
    }
}
