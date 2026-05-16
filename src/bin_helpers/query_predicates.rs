
pub(crate) fn parse_json_predicate(raw: &str) -> Result<JsonPointerPredicate> {
    let raw = raw.trim();
    if raw.is_empty() {
        anyhow::bail!("bad --json predicate: predicate is empty");
    }

    if let Some(pointer) = raw.strip_prefix("exists:") {
        validate_cli_json_pointer(pointer, raw)?;
        return Ok(JsonPointerPredicate::exists(pointer.to_string()));
    }

    let (pointer, op, value) = split_json_predicate(raw)?;
    validate_cli_json_pointer(pointer, raw)?;

    match op {
        "=" => Ok(JsonPointerPredicate::equals(
            pointer.to_string(),
            parse_json_scalar(value, raw, op)?,
        )),
        "!=" => Ok(JsonPointerPredicate::not_equals(
            pointer.to_string(),
            parse_json_scalar(value, raw, op)?,
        )),
        ">" | ">=" | "<" | "<=" => {
            let number = value.parse::<f64>().map_err(|_| {
                anyhow::anyhow!(
                    "bad --json predicate {raw:?}: value {value:?} is not a number for operator {op:?}"
                )
            })?;
            if !number.is_finite() {
                anyhow::bail!(
                    "bad --json predicate {raw:?}: value {value:?} is not finite for operator {op:?}"
                );
            }
            let cmp = match op {
                ">" => NumericComparison::GreaterThan,
                ">=" => NumericComparison::GreaterThanOrEqual,
                "<" => NumericComparison::LessThan,
                "<=" => NumericComparison::LessThanOrEqual,
                _ => unreachable!(),
            };
            Ok(JsonPointerPredicate::number(
                pointer.to_string(),
                cmp,
                number,
            ))
        }
        "~=" => {
            if value.is_empty() {
                anyhow::bail!(
                    "bad --json predicate {raw:?}: value is empty for regex operator {op:?}"
                );
            }
            Ok(JsonPointerPredicate::regex(
                pointer.to_string(),
                value.to_string(),
            ))
        }
        _ => unreachable!("split_json_predicate only returns supported operators"),
    }
}

fn split_json_predicate(raw: &str) -> Result<(&str, &'static str, &str)> {
    let mut best: Option<(usize, &'static str)> = None;
    for op in ["~=", ">=", "<=", "!=", "=", ">", "<"] {
        if let Some(idx) = raw.find(op) {
            let replace = best
                .map(|(best_idx, best_op)| {
                    idx < best_idx || (idx == best_idx && op.len() > best_op.len())
                })
                .unwrap_or(true);
            if replace {
                best = Some((idx, op));
            }
        }
    }

    if let Some((idx, op)) = best {
        let pointer = &raw[..idx];
        let value = &raw[idx + op.len()..];
        if pointer.is_empty() {
            anyhow::bail!("bad --json predicate {raw:?}: pointer is empty before operator {op:?}");
        }
        if value.is_empty() {
            anyhow::bail!("bad --json predicate {raw:?}: value is empty for operator {op:?}");
        }
        return Ok((pointer, op, value));
    }

    anyhow::bail!(
        "bad --json predicate {raw:?}: missing or unsupported operator; expected exists:/path, /path=value, /path!=value, /path>value, /path>=value, /path<value, /path<=value, or /path~=REGEX"
    )
}

fn validate_cli_json_pointer(pointer: &str, raw: &str) -> Result<()> {
    if !pointer.starts_with('/') {
        anyhow::bail!("bad --json predicate {raw:?}: pointer {pointer:?} must start with '/'");
    }
    let mut chars = pointer.chars();
    while let Some(ch) = chars.next() {
        if ch == '~' {
            match chars.next() {
                Some('0') | Some('1') => {}
                other => anyhow::bail!(
                    "bad --json predicate {raw:?}: pointer {pointer:?} has invalid escape near '~{:?}'; use '~0' for '~' and '~1' for '/'",
                    other
                ),
            }
        }
    }
    Ok(())
}

fn parse_json_scalar(raw_value: &str, raw_predicate: &str, op: &str) -> Result<Value> {
    match serde_json::from_str::<Value>(raw_value) {
        Ok(value) if matches!(
            value,
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)
        ) => Ok(value),
        Ok(value) => anyhow::bail!(
            "bad --json predicate {raw_predicate:?}: value {raw_value:?} for operator {op:?} must be a JSON scalar, not {}",
            value_type_name(&value)
        ),
        Err(_) => Ok(Value::String(raw_value.to_string())),
    }
}

fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}
