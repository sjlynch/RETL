use clap::{Args, ValueEnum};
use std::path::PathBuf;

/// `retl load` — register Parquet output as a queryable DuckDB table or view.
///
/// Closes the L in ETL for the common research case: turn `retl export
/// --format parquet` output into a DuckDB database file readable from the
/// DuckDB CLI / Python / R bindings without standing up server infrastructure.
///
/// Only Parquet inputs are accepted; for JSONL, compose
/// `retl export --format parquet` first and then run `retl load`.
#[derive(Args, Debug, Clone)]
pub(crate) struct LoadArgs {
    /// Parquet file or glob to load (e.g. `out/*.parquet`).
    ///
    /// The string is passed verbatim to DuckDB's `read_parquet(...)`, which
    /// natively expands globs — there is no shell expansion step on Windows.
    /// Quote it if your shell would otherwise expand the glob client-side.
    #[arg(long, required = true)]
    pub(crate) from: String,

    /// Destination DuckDB database, formatted `duckdb://<path>` (e.g.
    /// `duckdb://reddit.duckdb`). The database file is created if missing.
    /// The `duckdb://` scheme is required so the flag is unambiguous if we
    /// add more loader targets later.
    #[arg(long, value_parser = parse_duckdb_target)]
    pub(crate) to: PathBuf,

    /// Destination table or view name (a SQL identifier — letters, digits,
    /// and underscores only; must not begin with a digit).
    #[arg(long)]
    pub(crate) table: String,

    /// Whether to register the Parquet as a `VIEW` (default, zero-copy) or
    /// materialize it as a `TABLE` (`CREATE TABLE AS SELECT`). Views are
    /// sub-second regardless of input size; tables pay the materialization
    /// cost up front in exchange for faster repeated reads.
    #[arg(long, value_enum, default_value_t = LoadMode::View)]
    pub(crate) mode: LoadMode,

    /// What to do if `<table>` already exists in the database.
    ///
    /// `append` is rejected for `--mode view` (a view is not a value sink).
    #[arg(long = "if-exists", value_enum, default_value_t = IfExists::Replace)]
    pub(crate) if_exists: IfExists,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LoadMode {
    /// Register as `CREATE [OR REPLACE] VIEW <name> AS SELECT * FROM read_parquet(...)`.
    View,
    /// Materialize as `CREATE TABLE <name> AS SELECT * FROM read_parquet(...)`.
    Table,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IfExists {
    /// Error if the table or view already exists.
    Fail,
    /// Drop and recreate (atomic under a `BEGIN; ... COMMIT;` for `--mode table`).
    Replace,
    /// Insert into the existing table. Only valid with `--mode table`.
    Append,
}

/// Parse `--to` values of the form `duckdb://<path>`. The scheme is required
/// so the flag stays unambiguous if we later support additional loader
/// targets (e.g. `sqlite://`, `postgres://`).
pub(crate) fn parse_duckdb_target(raw: &str) -> Result<PathBuf, String> {
    let s = raw.trim();
    if s.is_empty() {
        return Err("--to cannot be empty; expected duckdb://<path>".into());
    }
    let rest = s
        .strip_prefix("duckdb://")
        .ok_or_else(|| format!("--to must use the duckdb://<path> scheme (got {s:?})"))?;
    if rest.is_empty() {
        return Err("--to duckdb:// requires a database path after the scheme".into());
    }
    Ok(PathBuf::from(rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duckdb_target_strips_scheme() {
        assert_eq!(
            parse_duckdb_target("duckdb://reddit.duckdb").unwrap(),
            PathBuf::from("reddit.duckdb")
        );
        assert_eq!(
            parse_duckdb_target("duckdb:///tmp/r.duckdb").unwrap(),
            PathBuf::from("/tmp/r.duckdb")
        );
    }

    #[test]
    fn parse_duckdb_target_rejects_missing_scheme() {
        let err = parse_duckdb_target("reddit.duckdb").unwrap_err();
        assert!(err.contains("duckdb://"), "{err}");
    }

    #[test]
    fn parse_duckdb_target_rejects_empty_path() {
        let err = parse_duckdb_target("duckdb://").unwrap_err();
        assert!(err.contains("requires a database path"), "{err}");
    }

    #[test]
    fn parse_duckdb_target_rejects_empty_string() {
        let err = parse_duckdb_target("").unwrap_err();
        assert!(err.contains("cannot be empty"), "{err}");
    }
}
