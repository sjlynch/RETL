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

fn cmp_i128_f64(i: i128, f: f64) -> Ordering {
    if f.is_nan() {
        return Ordering::Equal;
    }
    if f.is_infinite() {
        return if f.is_sign_positive() {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    (i as f64).total_cmp(&f)
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
}
