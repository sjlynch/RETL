// Row writing for tabular (CSV/TSV) exports.
//
// Concern (2) split out of `pipeline_exec/tabular.rs`: serialize a row of
// pre-resolved cells into CSV (RFC-4180 quoting) or TSV (rejecting embedded
// tabs/line breaks). The selector → cell-string conversion lives here too so
// the two pieces of "format the value as text" stay together.

fn write_csv_cell<W: Write + ?Sized>(out: &mut W, cell: &str) -> Result<()> {
    let quote = cell
        .as_bytes()
        .iter()
        .any(|b| matches!(b, b',' | b'"' | b'\n' | b'\r'));
    if !quote {
        out.write_all(cell.as_bytes())?;
        return Ok(());
    }
    out.write_all(b"\"")?;
    for b in cell.as_bytes() {
        if *b == b'"' {
            out.write_all(b"\"\"")?;
        } else {
            out.write_all(&[*b])?;
        }
    }
    out.write_all(b"\"")?;
    Ok(())
}

fn value_to_tabular_cell(value: Option<&Value>) -> Result<String> {
    let Some(value) = value else {
        return Ok(String::new());
    };
    match value {
        Value::Null => Ok(String::new()),
        Value::String(s) => Ok(s.clone()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Array(_) | Value::Object(_) => Ok(serde_json::to_string(value)?),
    }
}

fn write_tabular_row<W: Write + ?Sized>(
    out: &mut W,
    fields: &[String],
    cells: &[String],
    format: TabularFormat,
) -> Result<()> {
    match format {
        TabularFormat::Csv => {
            for (i, cell) in cells.iter().enumerate() {
                if i > 0 {
                    out.write_all(b",")?;
                }
                write_csv_cell(out, cell)?;
            }
            out.write_all(b"\r\n")?;
        }
        TabularFormat::Tsv => {
            for (field, cell) in fields.iter().zip(cells.iter()) {
                if let Some(ch) = cell.chars().find(|ch| matches!(ch, '\t' | '\n' | '\r')) {
                    let desc = match ch {
                        '\t' => "tab",
                        '\n' => "line feed",
                        '\r' => "carriage return",
                        _ => unreachable!(),
                    };
                    tracing::warn!(
                        field,
                        character = desc,
                        "refusing to emit TSV value containing a tab or line break; use --format csv for robust escaping"
                    );
                    anyhow::bail!(
                        "TSV export cannot represent a {desc} in field {field:?}; use --format csv"
                    );
                }
            }
            for (i, cell) in cells.iter().enumerate() {
                if i > 0 {
                    out.write_all(b"\t")?;
                }
                out.write_all(cell.as_bytes())?;
            }
            out.write_all(b"\n")?;
        }
    }
    Ok(())
}

fn tabular_cells_from_value(
    value: &Value,
    selectors: &[TabularFieldSelector],
) -> Result<(Vec<String>, Vec<usize>)> {
    let mut matched_indices = Vec::new();
    let mut cells = Vec::with_capacity(selectors.len());
    for (idx, selector) in selectors.iter().enumerate() {
        let field_value = selector.value(value);
        if field_value.is_some() {
            matched_indices.push(idx);
        }
        cells.push(value_to_tabular_cell(field_value)?);
    }
    Ok((cells, matched_indices))
}

fn write_tabular_header<W: Write + ?Sized>(
    out: &mut W,
    fields: &[String],
    format: TabularFormat,
) -> Result<()> {
    write_tabular_row(out, fields, fields, format)
}
