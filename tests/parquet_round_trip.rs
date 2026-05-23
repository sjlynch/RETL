//! Round-trip test for the Parquet writer: write a small corpus through
//! `write_parquet_atomic`, then re-open it with the `parquet` crate's
//! reader and assert the row count plus a sampled field.
//!
//! Only built when the `parquet` cargo feature is on (the writer is a
//! no-op error stub otherwise). Tests in this file therefore require
//! `cargo test --features parquet`.

#![cfg(feature = "parquet")]

use arrow_array::Array;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::sync::Arc;

#[test]
fn write_then_read_back_minimal_records() {
    let tmp = tempfile::tempdir().unwrap();
    let staging = tmp.path().join("_staging");
    std::fs::create_dir_all(&staging).unwrap();
    let dest = tmp.path().join("out.parquet");

    let lines = vec![
        r#"{"subreddit":"rust","author":"alice","created_utc":1700000000,"score":7,"id":"abc","body":"hi"}"#,
        r#"{"subreddit":"golang","author":"bob","created_utc":1700000010,"score":1,"id":"def","body":"hey"}"#,
        r#"{"subreddit":"rust","author":"carol","created_utc":1700000020,"score":42,"id":"ghi","body":"hello"}"#,
    ];
    let lines_clone = lines.clone();

    let rows = retl::parquet_writer::write_parquet_atomic(
        &staging,
        &dest,
        1024,
        "zstd:3",
        64 * 1024,
        move |w: &mut dyn std::io::Write| {
            for line in &lines_clone {
                w.write_all(line.as_bytes())?;
                w.write_all(b"\n")?;
            }
            Ok(lines_clone.len() as u64)
        },
    )
    .expect("parquet write should succeed");

    assert_eq!(rows, lines.len() as u64);
    assert!(dest.exists(), "published parquet file must exist at {}", dest.display());
    // The staging dir should be empty after a successful publish.
    let leftover = std::fs::read_dir(&staging)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .count();
    assert_eq!(
        leftover, 0,
        "staging dir should be empty after successful publish",
    );

    // Read back via the parquet crate.
    let file = File::open(&dest).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = Arc::clone(builder.schema());
    let column_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(column_names.contains(&"subreddit"), "schema missing subreddit column: {:?}", column_names);
    assert!(column_names.contains(&"author"), "schema missing author column: {:?}", column_names);
    assert!(column_names.contains(&"payload"), "schema missing payload column: {:?}", column_names);

    let reader = builder.build().unwrap();
    let mut total_rows = 0_usize;
    let mut found_alice = false;
    for batch in reader {
        let batch = batch.unwrap();
        total_rows += batch.num_rows();

        let author_idx = schema.index_of("author").unwrap();
        let authors = batch
            .column(author_idx)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            if !authors.is_null(i) && authors.value(i) == "alice" {
                found_alice = true;
            }
        }
    }
    assert_eq!(total_rows, lines.len(), "row count mismatch");
    assert!(found_alice, "expected to find 'alice' in author column");
}

#[test]
fn declined_publish_leaves_no_file_at_dest() {
    let tmp = tempfile::tempdir().unwrap();
    let staging = tmp.path().join("_staging");
    std::fs::create_dir_all(&staging).unwrap();
    let dest = tmp.path().join("empty.parquet");

    let rows = retl::parquet_writer::write_parquet_atomic_if(
        &staging,
        &dest,
        1024,
        "zstd:3",
        64 * 1024,
        |&n: &u64| n > 0,
        |_w| Ok(0_u64),
    )
    .expect("declined publish should still return Ok");

    assert_eq!(rows, 0);
    assert!(
        !dest.exists(),
        "declined publish must not leave a file at the published path",
    );
}

#[test]
fn kv_rows_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let staging = tmp.path().join("_staging");
    std::fs::create_dir_all(&staging).unwrap();
    let dest = tmp.path().join("agg.parquet");

    let pairs: Vec<(String, String)> = vec![
        ("rust".to_string(), "100".to_string()),
        ("golang".to_string(), "42".to_string()),
    ];

    // Use uncompressed: the `parquet` crate is built with only the `zstd`
    // codec compiled in (matches our `features = ["arrow", "zstd"]` in
    // Cargo.toml). Other codecs would fail at write time with a "Disabled
    // feature at compile time" error.
    let rows = retl::parquet_writer::write_kv_rows_atomic(
        &staging,
        &dest,
        1024,
        "uncompressed",
        64 * 1024,
        pairs.clone(),
    )
    .unwrap();
    assert_eq!(rows, pairs.len() as u64);

    let file = File::open(&dest).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = Arc::clone(builder.schema());
    assert_eq!(
        schema.fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>(),
        vec!["key", "value"],
    );
    let mut total = 0;
    for batch in builder.build().unwrap() {
        total += batch.unwrap().num_rows();
    }
    assert_eq!(total, pairs.len());
}
