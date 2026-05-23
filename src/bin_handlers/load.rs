
/// Validate that `name` is a safe SQL identifier (letters, digits, and
/// underscores; must not start with a digit). The string is later embedded
/// in `CREATE TABLE/VIEW <ident>` and in a few `DROP`s, so we whitelist a
/// strict ASCII subset rather than relying on quoting to neutralize hostile
/// input.
#[cfg_attr(not(feature = "duckdb-load"), allow(dead_code))]
fn validate_sql_identifier(name: &str, field: &str) -> Result<()> {
    if name.is_empty() {
        anyhow::bail!("{field} cannot be empty");
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap();
    if !(first.is_ascii_alphabetic() || first == '_') {
        anyhow::bail!(
            "{field} {name:?} must start with an ASCII letter or underscore (got {first:?})"
        );
    }
    for ch in chars {
        if !(ch.is_ascii_alphanumeric() || ch == '_') {
            anyhow::bail!(
                "{field} {name:?} contains invalid character {ch:?}; \
                 use only ASCII letters, digits, and underscores"
            );
        }
    }
    Ok(())
}

/// Escape a string literal for embedding inside a SQL single-quoted string by
/// doubling internal single quotes (`'` -> `''`). Used to inject the
/// user-supplied `--from` glob into `read_parquet('...')`.
#[cfg_attr(not(feature = "duckdb-load"), allow(dead_code))]
fn escape_sql_literal(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(feature = "duckdb-load")]
pub(crate) fn run_load(args: LoadArgs) -> Result<()> {
    validate_sql_identifier(&args.table, "--table")?;
    if args.from.trim().is_empty() {
        anyhow::bail!("--from cannot be empty");
    }
    if matches!(args.mode, LoadMode::View) && matches!(args.if_exists, IfExists::Append) {
        anyhow::bail!(
            "--if-exists append is not supported with --mode view (a view is not a value sink); \
             use --mode table to append rows"
        );
    }

    let from_literal = escape_sql_literal(&args.from);
    let table = &args.table;

    if let Some(parent) = args.to.parent() {
        if !parent.as_os_str().is_empty() {
            retl::create_dir_all_with_default_backoff(parent)
                .with_context(|| format!("creating DuckDB parent dir {}", parent.display()))?;
        }
    }

    let conn = duckdb::Connection::open(&args.to)
        .with_context(|| format!("opening DuckDB database at {}", args.to.display()))?;

    let object_kind = match args.mode {
        LoadMode::View => "view",
        LoadMode::Table => "table",
    };

    // Build the statement(s) for the chosen (mode, if-exists) pair. For
    // table-mode we wrap drop+create in a transaction so concurrent readers
    // never see a half-replaced table.
    let sql = match (args.mode, args.if_exists) {
        (LoadMode::View, IfExists::Replace) => format!(
            "CREATE OR REPLACE VIEW {table} AS SELECT * FROM read_parquet('{from_literal}');"
        ),
        (LoadMode::View, IfExists::Fail) => format!(
            "CREATE VIEW {table} AS SELECT * FROM read_parquet('{from_literal}');"
        ),
        (LoadMode::Table, IfExists::Replace) => format!(
            "BEGIN; \
             DROP TABLE IF EXISTS {table}; \
             DROP VIEW IF EXISTS {table}; \
             CREATE TABLE {table} AS SELECT * FROM read_parquet('{from_literal}'); \
             COMMIT;"
        ),
        (LoadMode::Table, IfExists::Fail) => format!(
            "CREATE TABLE {table} AS SELECT * FROM read_parquet('{from_literal}');"
        ),
        (LoadMode::Table, IfExists::Append) => format!(
            "BEGIN; \
             CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM read_parquet('{from_literal}') WHERE 1=0; \
             INSERT INTO {table} SELECT * FROM read_parquet('{from_literal}'); \
             COMMIT;"
        ),
        (LoadMode::View, IfExists::Append) => unreachable!("rejected above"),
    };

    conn.execute_batch(&sql).with_context(|| {
        format!(
            "creating DuckDB {object_kind} {table:?} in {} from {:?}",
            args.to.display(),
            args.from
        )
    })?;

    // Report row/object count so users get immediate feedback. View-mode
    // counts are sub-second; table-mode counts are O(1) on the freshly
    // materialized table.
    let row_count: i64 = conn
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
        .with_context(|| format!("counting rows in newly created {object_kind} {table:?}"))?;

    eprintln!(
        "Loaded {row_count} row(s) into DuckDB {object_kind} {table:?} at {} (from {:?})",
        args.to.display(),
        args.from
    );

    drop(conn);
    Ok(())
}

#[cfg(not(feature = "duckdb-load"))]
pub(crate) fn run_load(_args: LoadArgs) -> Result<()> {
    anyhow::bail!(
        "`retl load` was compiled out: rebuild with `cargo build --release --features duckdb-load` \
         to enable DuckDB loading (the bundled libduckdb adds ~10s to clean builds, so it is \
         off by default)"
    )
}

#[cfg(test)]
mod load_tests {
    use super::*;

    #[test]
    fn identifier_validation_accepts_safe_names() {
        for ok in ["comments", "_t", "tbl_2024", "RC_2024_01"] {
            validate_sql_identifier(ok, "--table").unwrap();
        }
    }

    #[test]
    fn identifier_validation_rejects_unsafe_names() {
        for bad in [
            "",
            "1leading_digit",
            "has space",
            "has-dash",
            "has;semicolon",
            "drop'tbl",
            "schema.table",
        ] {
            assert!(
                validate_sql_identifier(bad, "--table").is_err(),
                "expected reject for {bad:?}"
            );
        }
    }

    #[test]
    fn literal_escaping_doubles_quotes() {
        assert_eq!(escape_sql_literal("a'b"), "a''b");
        assert_eq!(escape_sql_literal("out/*.parquet"), "out/*.parquet");
    }
}
