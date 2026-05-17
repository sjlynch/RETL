// -----------------------------------------------------------------------------
// Pure number-to-string formatting. This submodule must not depend on the
// other aggregate submodules — that invariant keeps the "how does retl
// format averages?" answer self-contained.
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NumberFormat {
    Decimal,
    Scientific,
}

impl NumberFormat {
    fn from_scientific(scientific: bool) -> Self {
        if scientific {
            Self::Scientific
        } else {
            Self::Decimal
        }
    }
}

fn format_number(n: f64, format: NumberFormat) -> String {
    if !n.is_finite() {
        return n.to_string();
    }

    let rendered = n.to_string();
    let rendered = if format == NumberFormat::Decimal {
        expand_exponent_notation(&rendered).unwrap_or(rendered)
    } else {
        rendered
    };
    normalize_negative_zero(rendered)
}

const INTEGER_AVERAGE_FRACTIONAL_DIGITS: usize = 18;

fn format_integer_average(sum: i128, count: u64) -> String {
    debug_assert!(count > 0);
    let denom = count as u128;
    let negative = sum.is_negative();
    let abs_sum = sum.unsigned_abs();
    let mut integer = abs_sum / denom;
    let mut rem = abs_sum % denom;
    if rem == 0 {
        let mut out = String::new();
        if negative {
            out.push('-');
        }
        out.push_str(&integer.to_string());
        return normalize_negative_zero(out);
    }

    let mut digits = Vec::with_capacity(INTEGER_AVERAGE_FRACTIONAL_DIGITS);
    for _ in 0..INTEGER_AVERAGE_FRACTIONAL_DIGITS {
        rem *= 10;
        digits.push((rem / denom) as u8);
        rem %= denom;
    }

    if rem * 2 >= denom {
        let mut carry = true;
        for digit in digits.iter_mut().rev() {
            if *digit == 9 {
                *digit = 0;
            } else {
                *digit += 1;
                carry = false;
                break;
            }
        }
        if carry {
            integer += 1;
        }
    }

    while digits.last() == Some(&0) {
        digits.pop();
    }

    let mut out = String::new();
    if negative {
        out.push('-');
    }
    out.push_str(&integer.to_string());
    if !digits.is_empty() {
        out.push('.');
        for digit in digits {
            out.push(char::from(b'0' + digit));
        }
    }
    normalize_negative_zero(out)
}

fn expand_exponent_notation(s: &str) -> Option<String> {
    let e = s.find('e').or_else(|| s.find('E'))?;
    let exponent: i32 = s[e + 1..].parse().ok()?;
    let mut mantissa = &s[..e];
    let sign = if let Some(rest) = mantissa.strip_prefix('-') {
        mantissa = rest;
        "-"
    } else if let Some(rest) = mantissa.strip_prefix('+') {
        mantissa = rest;
        ""
    } else {
        ""
    };

    let integer_digits = mantissa.find('.').unwrap_or(mantissa.len());
    let digits: String = mantissa.chars().filter(|&ch| ch != '.').collect();
    if digits.is_empty() {
        return None;
    }

    let decimal_pos = integer_digits as i32 + exponent;
    let mut out = String::new();
    out.push_str(sign);
    if decimal_pos <= 0 {
        out.push_str("0.");
        for _ in 0..decimal_pos.unsigned_abs() {
            out.push('0');
        }
        out.push_str(&digits);
    } else if decimal_pos as usize >= digits.len() {
        out.push_str(&digits);
        for _ in 0..(decimal_pos as usize - digits.len()) {
            out.push('0');
        }
    } else {
        let split = decimal_pos as usize;
        out.push_str(&digits[..split]);
        out.push('.');
        out.push_str(&digits[split..]);
    }
    Some(normalize_negative_zero(out))
}

fn normalize_negative_zero(s: String) -> String {
    if s == "-0" {
        "0".to_string()
    } else {
        s
    }
}

#[cfg(test)]
mod tests_format {
    use super::*;

    #[test]
    fn format_number_integer_scores_use_plain_decimal() {
        let rendered = format_number(1_000_000_000_000_000.0, NumberFormat::Decimal);
        assert_eq!(rendered, "1000000000000000");
        assert!(!rendered.contains('e') && !rendered.contains('E'));
    }

    #[test]
    fn format_number_average_uses_stable_decimal() {
        let rendered = format_number(2.5, NumberFormat::Decimal);
        assert_eq!(rendered, "2.5");
        assert!(!rendered.contains('e') && !rendered.contains('E'));
    }

    #[test]
    fn format_integer_average_uses_documented_precision() {
        let rendered = format_integer_average(1, 7);
        assert_eq!(rendered, "0.142857142857142857");
        assert!(!rendered.contains('e') && !rendered.contains('E'));
    }
}
