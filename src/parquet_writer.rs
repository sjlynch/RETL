//! Parquet output sink for RETL.
//!
//! Exposes [`write_parquet_atomic_if`], an analog of [`write_jsonl_atomic_if`]
//! / [`write_zst_atomic_if`] from `atomic_write` but writing Apache Parquet.
//! The body is handed a `&mut dyn Write` that accepts newline-delimited JSON
//! lines; an internal adapter splits on `\n`, parses each line as
//! [`MinimalRecord`] (with the raw line carried in a `payload` column for
//! anything the minimal schema doesn't capture), batches rows into Arrow
//! `RecordBatch`es, and flushes them through `parquet::arrow::ArrowWriter`.
//!
//! The atomic-write contract from `atomic_write` is honored: every Parquet
//! file is staged at `<staging_dir>/<name>.retl-<pid>-<nonce>.inprogress`,
//! the Arrow writer is **closed before** the staged path is renamed onto
//! `final_dest`, and a discard (`should_publish` returns false) only strands
//! a PID-owned `.inprogress` in the staging dir — never a partial `.parquet`
//! at the published path.
//!
//! Without the `parquet` cargo feature this module compiles to a single
//! stub that returns a helpful build-flag error from
//! [`write_parquet_atomic_if`]; no Apache Arrow / Parquet dependencies are
//! pulled in.

use anyhow::Result;
use std::io::Write;
use std::path::Path;

// The `body` parameter to `write_parquet_atomic_if` is `impl FnOnce(&mut dyn
// Write) -> Result<u64>` — the user writes newline-delimited JSON lines to the
// supplied `&mut dyn Write`, the same shape `stream_job_with_partial_policy`
// already targets. The returned `u64` is opaque to the helper;
// `write_parquet_atomic_if` returns the count of rows actually written into
// the Parquet file.

#[cfg(feature = "parquet")]
mod imp {
    use super::*;
    use crate::atomic_write::{ensure_staging_dir, unique_inprogress_path};
    use crate::util::replace_file_atomic_backoff;
    use crate::zstd_jsonl::parse_minimal;
    use anyhow::{anyhow, Context};
    use arrow_array::builder::{BooleanBuilder, Int64Builder, StringBuilder};
    use arrow_array::{ArrayRef, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, GzipLevel, ZstdLevel};
    use parquet::file::properties::WriterProperties;
    use std::fs::File;
    use std::io::BufWriter;
    use std::sync::Arc;

    /// Parse a compression spec like `"zstd:3"`, `"gzip:6"`, `"snappy"`,
    /// `"uncompressed"` into a parquet [`Compression`].
    pub(super) fn parse_compression(spec: &str) -> Result<Compression> {
        let trimmed = spec.trim();
        if trimmed.is_empty() {
            return Ok(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()));
        }
        let (name, level) = match trimmed.split_once(':') {
            Some((n, l)) => (n.trim().to_ascii_lowercase(), Some(l.trim())),
            None => (trimmed.to_ascii_lowercase(), None),
        };
        let parse_level = |default: i32| -> Result<i32> {
            match level {
                Some(s) => s
                    .parse::<i32>()
                    .map_err(|e| anyhow!("invalid compression level '{s}' in '{spec}': {e}")),
                None => Ok(default),
            }
        };
        Ok(match name.as_str() {
            "uncompressed" | "none" => Compression::UNCOMPRESSED,
            "snappy" => Compression::SNAPPY,
            "gzip" => {
                let l = parse_level(6)?;
                Compression::GZIP(
                    GzipLevel::try_new(l as u32)
                        .map_err(|e| anyhow!("invalid gzip level {l}: {e}"))?,
                )
            }
            "lzo" => Compression::LZO,
            "brotli" => {
                let l = parse_level(1)?;
                // BrotliLevel range 0..=11.
                Compression::BROTLI(
                    parquet::basic::BrotliLevel::try_new(l as u32)
                        .map_err(|e| anyhow!("invalid brotli level {l}: {e}"))?,
                )
            }
            "lz4" => Compression::LZ4,
            "lz4_raw" => Compression::LZ4_RAW,
            "zstd" => {
                let l = parse_level(3)?;
                Compression::ZSTD(
                    ZstdLevel::try_new(l)
                        .map_err(|e| anyhow!("invalid zstd level {l}: {e}"))?,
                )
            }
            other => {
                return Err(anyhow!(
                    "unrecognized parquet compression codec '{other}' (expected one of: uncompressed, snappy, gzip, lzo, brotli, lz4, lz4_raw, zstd)"
                ));
            }
        })
    }

    /// Fixed RETL parquet schema: the [`MinimalRecord`] fast-path fields,
    /// plus `payload` carrying the raw JSON line so downstream readers can
    /// recover any field the minimal schema doesn't surface.
    pub(super) fn record_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("subreddit", DataType::Utf8, true),
            Field::new("author", DataType::Utf8, true),
            Field::new("created_utc", DataType::Int64, true),
            Field::new("score", DataType::Int64, true),
            Field::new("id", DataType::Utf8, true),
            Field::new("selftext", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
            Field::new("title", DataType::Utf8, true),
            Field::new("url", DataType::Utf8, true),
            Field::new("is_self", DataType::Boolean, true),
            Field::new("parent_id", DataType::Utf8, true),
            Field::new("domain", DataType::Utf8, true),
            Field::new("payload", DataType::Utf8, true),
        ]))
    }

    /// Builders for one record-batch worth of rows in the [`record_schema`].
    struct RecordBatchBuilders {
        subreddit: StringBuilder,
        author: StringBuilder,
        created_utc: Int64Builder,
        score: Int64Builder,
        id: StringBuilder,
        selftext: StringBuilder,
        body: StringBuilder,
        title: StringBuilder,
        url: StringBuilder,
        is_self: BooleanBuilder,
        parent_id: StringBuilder,
        domain: StringBuilder,
        payload: StringBuilder,
        rows: usize,
    }

    impl RecordBatchBuilders {
        fn with_capacity(cap: usize) -> Self {
            Self {
                subreddit: StringBuilder::with_capacity(cap, cap * 16),
                author: StringBuilder::with_capacity(cap, cap * 16),
                created_utc: Int64Builder::with_capacity(cap),
                score: Int64Builder::with_capacity(cap),
                id: StringBuilder::with_capacity(cap, cap * 12),
                selftext: StringBuilder::with_capacity(cap, cap * 8),
                body: StringBuilder::with_capacity(cap, cap * 64),
                title: StringBuilder::with_capacity(cap, cap * 16),
                url: StringBuilder::with_capacity(cap, cap * 16),
                is_self: BooleanBuilder::with_capacity(cap),
                parent_id: StringBuilder::with_capacity(cap, cap * 12),
                domain: StringBuilder::with_capacity(cap, cap * 16),
                payload: StringBuilder::with_capacity(cap, cap * 128),
                rows: 0,
            }
        }

        fn append_str(builder: &mut StringBuilder, value: Option<&str>) {
            match value {
                Some(s) => builder.append_value(s),
                None => builder.append_null(),
            }
        }

        fn push_line(&mut self, line: &str) -> Result<()> {
            // serde will succeed on essentially any JSON object since every
            // MinimalRecord field is Option-typed; a non-object line is the
            // only error path. Surface that as a hard error so a corrupt line
            // doesn't silently skew row counts.
            let rec = parse_minimal(line).with_context(|| {
                format!(
                    "parsing JSONL line for parquet writer (first 120 chars: {})",
                    truncated_preview(line)
                )
            })?;
            Self::append_str(&mut self.subreddit, rec.subreddit.as_deref());
            Self::append_str(&mut self.author, rec.author.as_deref());
            match rec.created_utc {
                Some(v) => self.created_utc.append_value(v),
                None => self.created_utc.append_null(),
            }
            match rec.score {
                Some(v) => self.score.append_value(v),
                None => self.score.append_null(),
            }
            Self::append_str(&mut self.id, rec.id.as_deref());
            Self::append_str(&mut self.selftext, rec.selftext.as_deref());
            Self::append_str(&mut self.body, rec.body.as_deref());
            Self::append_str(&mut self.title, rec.title.as_deref());
            Self::append_str(&mut self.url, rec.url.as_deref());
            match rec.is_self {
                Some(v) => self.is_self.append_value(v),
                None => self.is_self.append_null(),
            }
            Self::append_str(&mut self.parent_id, rec.parent_id.as_deref());
            Self::append_str(&mut self.domain, rec.domain.as_deref());
            // Raw JSON in payload so downstream readers can fall back to the
            // full record when they need a field outside the minimal schema.
            self.payload.append_value(line);
            self.rows += 1;
            Ok(())
        }

        fn finish_batch(&mut self, schema: Arc<Schema>) -> Result<RecordBatch> {
            let arrays: Vec<ArrayRef> = vec![
                Arc::new(self.subreddit.finish()),
                Arc::new(self.author.finish()),
                Arc::new(self.created_utc.finish()),
                Arc::new(self.score.finish()),
                Arc::new(self.id.finish()),
                Arc::new(self.selftext.finish()),
                Arc::new(self.body.finish()),
                Arc::new(self.title.finish()),
                Arc::new(self.url.finish()),
                Arc::new(self.is_self.finish()),
                Arc::new(self.parent_id.finish()),
                Arc::new(self.domain.finish()),
                Arc::new(self.payload.finish()),
            ];
            self.rows = 0;
            RecordBatch::try_new(schema, arrays).map_err(|e| anyhow!(e))
        }
    }

    fn truncated_preview(line: &str) -> String {
        const N: usize = 120;
        if line.len() <= N {
            line.to_string()
        } else {
            // Avoid splitting a multi-byte UTF-8 code point.
            let mut idx = N;
            while !line.is_char_boundary(idx) && idx > 0 {
                idx -= 1;
            }
            format!("{}…", &line[..idx])
        }
    }

    /// Sink that batches rows up to `row_group_size` and flushes each batch
    /// through the underlying `ArrowWriter`. `finish()` flushes any remaining
    /// rows and closes the writer (writing the parquet footer).
    pub(super) struct ParquetRowSink {
        writer: ArrowWriter<BufWriter<File>>,
        schema: Arc<Schema>,
        builders: RecordBatchBuilders,
        rows_written: u64,
        row_group_size: usize,
    }

    impl ParquetRowSink {
        pub(super) fn new(
            file: BufWriter<File>,
            row_group_size: usize,
            compression: &str,
        ) -> Result<Self> {
            let compression = parse_compression(compression)?;
            let props = WriterProperties::builder()
                .set_compression(compression)
                .set_max_row_group_size(row_group_size.max(1))
                .build();
            let schema = record_schema();
            let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
                .context("creating Parquet ArrowWriter")?;
            Ok(Self {
                writer,
                schema,
                builders: RecordBatchBuilders::with_capacity(row_group_size.max(1).min(64 * 1024)),
                rows_written: 0,
                row_group_size: row_group_size.max(1),
            })
        }

        pub(super) fn push_line(&mut self, line: &str) -> Result<()> {
            if line.is_empty() {
                return Ok(());
            }
            self.builders.push_line(line)?;
            if self.builders.rows >= self.row_group_size {
                self.flush_batch()?;
            }
            Ok(())
        }

        fn flush_batch(&mut self) -> Result<()> {
            if self.builders.rows == 0 {
                return Ok(());
            }
            let batch_rows = self.builders.rows as u64;
            let batch = self.builders.finish_batch(self.schema.clone())?;
            self.writer
                .write(&batch)
                .context("writing parquet record batch")?;
            self.rows_written += batch_rows;
            Ok(())
        }

        pub(super) fn finish(mut self) -> Result<u64> {
            self.flush_batch()?;
            self.writer.close().context("closing parquet writer")?;
            Ok(self.rows_written)
        }
    }

    /// `Write` adapter that buffers bytes and dispatches a line to
    /// `ParquetRowSink::push_line` for each `\n`. On `flush_residual` any
    /// trailing un-terminated line is flushed as a final record (Reddit dumps
    /// always end with `\n`, but a tolerated partial-read can truncate mid-
    /// line; calling flush_residual after the body returns is mandatory so
    /// the residual either errors or is preserved as a partial-row record).
    pub(super) struct LineSplittingWriter<'a> {
        sink: &'a mut ParquetRowSink,
        buf: Vec<u8>,
        deferred_error: Option<anyhow::Error>,
    }

    impl<'a> LineSplittingWriter<'a> {
        pub(super) fn new(sink: &'a mut ParquetRowSink) -> Self {
            Self {
                sink,
                buf: Vec::with_capacity(64 * 1024),
                deferred_error: None,
            }
        }

        /// Push any not-yet-terminated bytes as a final line, then surface a
        /// deferred parsing error (if any) raised during inline `write`s.
        pub(super) fn finish(mut self) -> Result<()> {
            if let Some(e) = self.deferred_error.take() {
                return Err(e);
            }
            if !self.buf.is_empty() {
                let line = std::str::from_utf8(&self.buf)
                    .map_err(|e| anyhow!("trailing parquet input is not valid UTF-8: {e}"))?;
                self.sink.push_line(line)?;
                self.buf.clear();
            }
            Ok(())
        }
    }

    impl<'a> Write for LineSplittingWriter<'a> {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            // A deferred error from a prior line is final — refuse further
            // writes so the body sees a write error and stops feeding rows.
            if self.deferred_error.is_some() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "parquet sink is already in an error state",
                ));
            }
            let mut start = 0;
            for (i, &b) in buf.iter().enumerate() {
                if b == b'\n' {
                    self.buf.extend_from_slice(&buf[start..i]);
                    let bytes = std::mem::take(&mut self.buf);
                    let line = match std::str::from_utf8(&bytes) {
                        Ok(s) => s.to_string(),
                        Err(e) => {
                            self.deferred_error = Some(anyhow!(
                                "parquet sink received non-UTF-8 line: {e}"
                            ));
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "parquet sink received non-UTF-8 line",
                            ));
                        }
                    };
                    if let Err(e) = self.sink.push_line(&line) {
                        self.deferred_error = Some(e);
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "parquet sink rejected line",
                        ));
                    }
                    start = i + 1;
                }
            }
            if start < buf.len() {
                self.buf.extend_from_slice(&buf[start..]);
            }
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    pub(super) fn write_parquet_atomic_if_impl<P, F>(
        staging_dir: &Path,
        final_dest: &Path,
        row_group_size: usize,
        compression: &str,
        write_buf_bytes: usize,
        should_publish: P,
        body: F,
    ) -> Result<u64>
    where
        P: FnOnce(&u64) -> bool,
        F: FnOnce(&mut dyn Write) -> Result<u64>,
    {
        crate::util::create_dir_all_with_default_backoff(staging_dir)
            .with_context(|| format!("create staging dir {}", staging_dir.display()))?;
        let staged = unique_inprogress_path(staging_dir, final_dest)?;

        let file = crate::util::create_new_with_default_backoff(&staged)
            .with_context(|| format!("create staged {}", staged.display()))?;
        let bw = BufWriter::with_capacity(write_buf_bytes, file);

        let mut sink = match ParquetRowSink::new(bw, row_group_size, compression) {
            Ok(s) => s,
            Err(e) => {
                let _ = crate::util::remove_with_short_backoff(&staged);
                return Err(e);
            }
        };

        let body_result = {
            let mut adapter = LineSplittingWriter::new(&mut sink);
            let body_outcome = body(&mut adapter);
            let finish_outcome = adapter.finish();
            body_outcome.and(finish_outcome.map(|()| 0u64))
        };

        if let Err(e) = body_result {
            // Close the writer to release the file handle before unlinking.
            let _ = sink.finish();
            let _ = crate::util::remove_with_short_backoff(&staged);
            return Err(e).with_context(|| format!("write staged {}", staged.display()));
        }

        let rows = match sink.finish() {
            Ok(n) => n,
            Err(e) => {
                let _ = crate::util::remove_with_short_backoff(&staged);
                return Err(e).with_context(|| format!("finalize staged {}", staged.display()));
            }
        };

        if !should_publish(&rows) {
            let _ = crate::util::remove_with_short_backoff(&staged);
            return Ok(rows);
        }

        if let Some(parent) = final_dest.parent() {
            if let Err(e) = crate::util::create_dir_all_with_default_backoff(parent) {
                let _ = crate::util::remove_with_short_backoff(&staged);
                return Err(e).context(format!("create dest parent {}", parent.display()));
            }
        }
        if let Err(e) = replace_file_atomic_backoff(&staged, final_dest) {
            let _ = crate::util::remove_with_short_backoff(&staged);
            return Err(e).context(format!(
                "publish staged {} -> {}",
                staged.display(),
                final_dest.display()
            ));
        }
        Ok(rows)
    }

    /// Ensure the staging dir exists (analog of `atomic_write::ensure_staging_dir`).
    /// Re-exported so the partitioned writer's pre-flight is shared.
    #[allow(dead_code)]
    pub(super) fn ensure_staging(out_root: &Path) -> Result<std::path::PathBuf> {
        ensure_staging_dir(out_root)
    }

    pub(super) fn write_kv_rows_atomic_impl<I, K, V>(
        staging_dir: &Path,
        final_dest: &Path,
        row_group_size: usize,
        compression: &str,
        write_buf_bytes: usize,
        rows: I,
    ) -> Result<u64>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        crate::util::create_dir_all_with_default_backoff(staging_dir)
            .with_context(|| format!("create staging dir {}", staging_dir.display()))?;
        let staged = unique_inprogress_path(staging_dir, final_dest)?;
        let file = crate::util::create_new_with_default_backoff(&staged)
            .with_context(|| format!("create staged {}", staged.display()))?;
        let bw = BufWriter::with_capacity(write_buf_bytes, file);

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]));

        let result = (|| -> Result<u64> {
            let comp = parse_compression(compression)?;
            let props = WriterProperties::builder()
                .set_compression(comp)
                .set_max_row_group_size(row_group_size.max(1))
                .build();
            let mut writer = ArrowWriter::try_new(bw, schema.clone(), Some(props))
                .context("creating aggregate parquet writer")?;
            let max_per_batch = row_group_size.max(1);
            let mut key_b = StringBuilder::with_capacity(max_per_batch, max_per_batch * 16);
            let mut val_b = StringBuilder::with_capacity(max_per_batch, max_per_batch * 16);
            let mut in_batch = 0_usize;
            let mut total = 0_u64;
            let flush = |key_b: &mut StringBuilder,
                         val_b: &mut StringBuilder,
                         writer: &mut ArrowWriter<BufWriter<File>>,
                         in_batch: &mut usize|
             -> Result<()> {
                if *in_batch == 0 {
                    return Ok(());
                }
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(key_b.finish()) as ArrayRef,
                        Arc::new(val_b.finish()) as ArrayRef,
                    ],
                )
                .map_err(|e| anyhow!(e))?;
                writer
                    .write(&batch)
                    .context("writing aggregate parquet batch")?;
                *in_batch = 0;
                Ok(())
            };
            for (k, v) in rows {
                key_b.append_value(k.as_ref());
                val_b.append_value(v.as_ref());
                in_batch += 1;
                total += 1;
                if in_batch >= max_per_batch {
                    flush(&mut key_b, &mut val_b, &mut writer, &mut in_batch)?;
                }
            }
            flush(&mut key_b, &mut val_b, &mut writer, &mut in_batch)?;
            writer
                .close()
                .context("closing aggregate parquet writer")?;
            Ok(total)
        })();

        let rows = match result {
            Ok(n) => n,
            Err(e) => {
                let _ = crate::util::remove_with_short_backoff(&staged);
                return Err(e).with_context(|| format!("write staged {}", staged.display()));
            }
        };

        if let Some(parent) = final_dest.parent() {
            if let Err(e) = crate::util::create_dir_all_with_default_backoff(parent) {
                let _ = crate::util::remove_with_short_backoff(&staged);
                return Err(e).context(format!("create dest parent {}", parent.display()));
            }
        }
        if let Err(e) = replace_file_atomic_backoff(&staged, final_dest) {
            let _ = crate::util::remove_with_short_backoff(&staged);
            return Err(e).context(format!(
                "publish aggregate parquet {} -> {}",
                staged.display(),
                final_dest.display()
            ));
        }
        Ok(rows)
    }
}


/// Atomically write a two-column (`key` STRING NOT NULL, `value` STRING)
/// Parquet rollup at `final_dest`. Used by `retl aggregate --format parquet`
/// to write grouped (key, value) rows.
///
/// Requires the `parquet` cargo feature. Without it returns a build-flag
/// error.
pub fn write_kv_rows_atomic<I, K, V>(
    staging_dir: &Path,
    final_dest: &Path,
    row_group_size: usize,
    compression: &str,
    write_buf_bytes: usize,
    rows: I,
) -> Result<u64>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    #[cfg(feature = "parquet")]
    {
        imp::write_kv_rows_atomic_impl(
            staging_dir,
            final_dest,
            row_group_size,
            compression,
            write_buf_bytes,
            rows,
        )
    }
    #[cfg(not(feature = "parquet"))]
    {
        let _ = (
            staging_dir,
            final_dest,
            row_group_size,
            compression,
            write_buf_bytes,
            rows,
        );
        Err(anyhow::anyhow!(
            "retl: Parquet output was requested but this binary was built without the `parquet` cargo feature. Rebuild with `cargo build --release --features parquet`."
        ))
    }
}

/// Stage a parquet write under `<staging_dir>/<name>.*.inprogress`, run
/// `body` against a `&mut dyn Write` that accepts newline-delimited JSON
/// lines, close the parquet footer, then atomically promote onto
/// `final_dest` when `should_publish` accepts the row count.
///
/// Returns the number of rows actually written into the parquet file
/// (post-flush). On `body` error or a discarded publish the staged file is
/// cleaned up.
///
/// **`parquet` feature required.** Without it this returns a build-flag
/// error explaining how to enable Parquet output.
pub fn write_parquet_atomic_if<P, F>(
    staging_dir: &Path,
    final_dest: &Path,
    row_group_size: usize,
    compression: &str,
    write_buf_bytes: usize,
    should_publish: P,
    body: F,
) -> Result<u64>
where
    P: FnOnce(&u64) -> bool,
    F: FnOnce(&mut dyn Write) -> Result<u64>,
{
    #[cfg(feature = "parquet")]
    {
        imp::write_parquet_atomic_if_impl(
            staging_dir,
            final_dest,
            row_group_size,
            compression,
            write_buf_bytes,
            should_publish,
            body,
        )
    }
    #[cfg(not(feature = "parquet"))]
    {
        let _ = (
            staging_dir,
            final_dest,
            row_group_size,
            compression,
            write_buf_bytes,
            should_publish,
            body,
        );
        Err(anyhow::anyhow!(
            "retl: Parquet output was requested but this binary was built without the `parquet` cargo feature. \
             Rebuild with `cargo build --release --features parquet` (or `cargo install --features parquet ...`) \
             to enable Arrow/Parquet writers."
        ))
    }
}

/// Convenience: always-publish variant.
pub fn write_parquet_atomic<F>(
    staging_dir: &Path,
    final_dest: &Path,
    row_group_size: usize,
    compression: &str,
    write_buf_bytes: usize,
    body: F,
) -> Result<u64>
where
    F: FnOnce(&mut dyn Write) -> Result<u64>,
{
    write_parquet_atomic_if(
        staging_dir,
        final_dest,
        row_group_size,
        compression,
        write_buf_bytes,
        |_| true,
        body,
    )
}

/// `true` when this binary was built with the `parquet` cargo feature so the
/// real writer is wired up. Useful for CLI error wording.
pub const fn parquet_feature_enabled() -> bool {
    cfg!(feature = "parquet")
}

#[cfg(all(test, feature = "parquet"))]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn tmp_root() -> PathBuf {
        tempfile::tempdir().unwrap().keep()
    }

    #[test]
    fn write_round_trip_via_minimal_schema() {
        let dir = tmp_root();
        let staging = dir.join("_staging");
        std::fs::create_dir_all(&staging).unwrap();
        let dest = dir.join("out.parquet");

        let lines = [
            r#"{"subreddit":"rust","author":"alice","created_utc":1700000000,"score":7,"id":"abc","body":"hi"}"#,
            r#"{"subreddit":"golang","author":"bob","created_utc":1700000010,"score":1,"id":"def","body":"hey"}"#,
        ];
        let rows = write_parquet_atomic(&staging, &dest, 1024, "zstd:3", 64 * 1024, |w| {
            for line in lines {
                w.write_all(line.as_bytes())?;
                w.write_all(b"\n")?;
            }
            Ok(lines.len() as u64)
        })
        .unwrap();
        assert_eq!(rows, 2);
        assert!(dest.exists());
    }

    #[test]
    fn parse_compression_round_trip() {
        assert!(imp::parse_compression("zstd:3").is_ok());
        assert!(imp::parse_compression("snappy").is_ok());
        assert!(imp::parse_compression("uncompressed").is_ok());
        assert!(imp::parse_compression("gzip:6").is_ok());
        assert!(imp::parse_compression("bogus").is_err());
    }
}
