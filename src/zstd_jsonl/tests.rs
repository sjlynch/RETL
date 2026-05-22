
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;

    fn write_zst_with_checksum(path: &Path, payload: &[u8]) {
        let f = fs::File::create(path).unwrap();
        let mut enc = zstd::stream::write::Encoder::new(f, 3).unwrap();
        enc.include_checksum(true).unwrap();
        enc.write_all(payload).unwrap();
        enc.finish().unwrap();
    }

    /// Bit-flipping a byte mid-stream in a checksum-bearing zstd file must:
    ///   - cause `validate_zst_full` (Full integrity) to return an error
    ///   - cause `for_each_line_cfg` (the normal strict scanning path) to
    ///     return an error with path context
    ///   - cause explicit tolerant/status APIs to report the file incomplete
    #[test]
    fn bit_flipped_frame_fails_full_strict_scan_errors_and_tolerant_scan_skips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flipped.zst");

        // Enough payload that there's plenty of compressed data mid-stream.
        let mut payload = Vec::new();
        for i in 0..2000 {
            payload.extend_from_slice(
                format!(
                    "{{\"id\":\"r{}\",\"author\":\"u{}\",\"subreddit\":\"x\"}}\n",
                    i, i
                )
                .as_bytes(),
            );
        }
        write_zst_with_checksum(&path, &payload);

        // Sanity: original file validates cleanly.
        validate_zst_full(&path).expect("pristine file should validate");

        // Flip a couple of bytes well past the frame header and well before
        // the trailing checksum. Two bytes XOR'd with 0xFF makes the entropy
        // decoder reliably produce an error rather than (rarely) decoding
        // garbage that happens to checksum-match.
        let mut bytes = fs::read(&path).unwrap();
        let n = bytes.len();
        let off = n / 2;
        bytes[off] ^= 0xFF;
        bytes[off + 1] ^= 0xFF;
        fs::write(&path, &bytes).unwrap();

        // Full integrity must catch this.
        assert!(
            validate_zst_full(&path).is_err(),
            "validate_zst_full must reject a bit-flipped frame"
        );

        // The normal scan path is strict by default and must not silently
        // return partial results.
        let mut lines_seen = 0usize;
        let err = for_each_line_cfg(&path, 16 * 1024, |_line| {
            lines_seen += 1;
            Ok(())
        })
        .expect_err("for_each_line_cfg must fail corrupt zstd files by default");
        let msg = err.to_string();
        assert!(msg.contains("zstd decode error"), "unexpected error: {msg}");
        assert!(
            msg.contains(&path.display().to_string()),
            "unexpected error: {msg}"
        );

        let status = for_each_line_with_opts_status(
            &path,
            LineStreamOpts {
                read_buf_bytes: Some(16 * 1024),
                partial_read_policy: PartialReadPolicy::AllowPartial,
                ..Default::default()
            },
            |_line| Ok(()),
        )
        .expect("status API should not bubble corrupt-frame decode errors");
        assert!(
            !status,
            "status API must report the corrupt file as incomplete"
        );

        // The allow-partial + on_skip path must also skip gracefully AND
        // surface the skip event to the caller via `on_skip`. The path
        // passed to the callback must match the file we tried to read, and
        // the captured error must be non-empty.
        let mut skip_calls: Vec<(PathBuf, String)> = Vec::new();
        let mut lines_seen2 = 0usize;
        let mut on_skip = |p: &Path, e: &anyhow::Error| {
            skip_calls.push((p.to_path_buf(), e.to_string()));
        };
        let res = for_each_line_with_opts(
            &path,
            LineStreamOpts {
                read_buf_bytes: Some(16 * 1024),
                on_skip: Some(&mut on_skip),
                partial_read_policy: PartialReadPolicy::AllowPartial,
                ..Default::default()
            },
            |_line| {
                lines_seen2 += 1;
                Ok(())
            },
        );
        assert!(
            res.is_ok(),
            "allow-partial with on_skip should skip a corrupt file gracefully, got {:?}",
            res
        );
        assert_eq!(
            skip_calls.len(),
            1,
            "on_skip must fire exactly once for a corrupt file, got {:?}",
            skip_calls
        );
        let (skipped_path, err_msg) = &skip_calls[0];
        assert_eq!(
            skipped_path, &path,
            "on_skip must receive the original path"
        );
        assert!(
            !err_msg.is_empty(),
            "on_skip must receive a non-empty error description"
        );
    }

    /// On a healthy file, the *_with_skip variant must NOT invoke `on_skip`
    /// and must still deliver every line to `on_line`. This guards against
    /// regressions where the skip callback fires on the happy path.
    #[test]
    fn healthy_file_does_not_trigger_on_skip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("healthy.zst");

        let mut payload = Vec::new();
        for i in 0..50 {
            payload.extend_from_slice(format!("{{\"id\":\"r{}\"}}\n", i).as_bytes());
        }
        write_zst_with_checksum(&path, &payload);

        let mut skip_count = 0usize;
        let mut lines_seen = 0usize;
        let mut on_skip = |_: &Path, _: &anyhow::Error| skip_count += 1;
        let res = for_each_line_with_opts(
            &path,
            LineStreamOpts {
                read_buf_bytes: Some(16 * 1024),
                on_skip: Some(&mut on_skip),
                partial_read_policy: PartialReadPolicy::AllowPartial,
                ..Default::default()
            },
            |_line| {
                lines_seen += 1;
                Ok(())
            },
        );
        assert!(res.is_ok(), "healthy file must scan without error");
        assert_eq!(skip_count, 0, "on_skip must not fire on healthy files");
        assert_eq!(
            lines_seen, 50,
            "all lines must be delivered on healthy files"
        );
    }

    #[test]
    fn missing_file_open_error_propagates_without_skip_callback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.zst");

        let mut skip_count = 0usize;
        let mut on_skip = |_: &Path, _: &anyhow::Error| skip_count += 1;
        let err = for_each_line_with_opts(
            &path,
            LineStreamOpts {
                read_buf_bytes: Some(16 * 1024),
                on_skip: Some(&mut on_skip),
                partial_read_policy: PartialReadPolicy::AllowPartial,
                ..Default::default()
            },
            |_line| Ok(()),
        )
        .expect_err("missing input must be a fatal open error");

        assert_eq!(skip_count, 0, "on_skip must not fire for open errors");
        let msg = err.to_string();
        assert!(msg.contains("open zstd input"), "unexpected error: {msg}");
        assert!(msg.contains("missing.zst"), "unexpected error: {msg}");
    }

    #[test]
    fn callback_error_propagates_without_skip_callback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("callback-error.zst");
        write_zst_with_checksum(&path, b"{\"id\":\"r1\"}\n");

        let err = for_each_line_cfg(&path, 16 * 1024, |_line| {
            Err(anyhow::anyhow!("callback boom"))
        })
        .expect_err("callback errors must propagate from for_each_line_cfg");
        assert!(
            err.to_string().contains("callback boom"),
            "unexpected error: {err}"
        );

        let mut skip_count = 0usize;
        let mut on_skip = |_: &Path, _: &anyhow::Error| skip_count += 1;
        let err = for_each_line_with_opts(
            &path,
            LineStreamOpts {
                read_buf_bytes: Some(16 * 1024),
                on_skip: Some(&mut on_skip),
                partial_read_policy: PartialReadPolicy::AllowPartial,
                ..Default::default()
            },
            |_line| Err(anyhow::anyhow!("callback boom")),
        )
        .expect_err("callback errors must propagate from skip variants");
        assert!(
            err.to_string().contains("callback boom"),
            "unexpected error: {err}"
        );
        assert_eq!(
            skip_count, 0,
            "on_skip must not fire for caller callback errors"
        );
    }

    #[test]
    fn allow_partial_progress_reports_only_remaining_bytes_on_truncated_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated-progress.zst");

        let mut payload = Vec::new();
        for i in 0..500 {
            payload.extend_from_slice(format!("{{\"id\":\"r{i}\"}}\n").as_bytes());
        }
        write_zst_with_checksum(&path, &payload);

        let mut bytes = fs::read(&path).unwrap();
        bytes.truncate(bytes.len().saturating_sub(4));
        fs::write(&path, &bytes).unwrap();
        let file_len = fs::metadata(&path).unwrap().len();

        let mut progress_total = 0u64;
        let mut on_progress = |delta: u64| {
            progress_total = progress_total.saturating_add(delta);
        };
        let mut skip_count = 0usize;
        let mut on_skip = |_p: &Path, _e: &anyhow::Error| {
            skip_count += 1;
        };

        let complete = for_each_line_with_opts_status(
            &path,
            LineStreamOpts {
                read_buf_bytes: Some(256),
                progress: Some(&mut on_progress),
                on_skip: Some(&mut on_skip),
                partial_read_policy: PartialReadPolicy::AllowPartial,
                ..Default::default()
            },
            |_line| Ok(()),
        )
        .expect("allow_partial should tolerate the truncation");

        assert!(!complete, "truncated file must be reported incomplete");
        assert_eq!(skip_count, 1, "on_skip must fire exactly once");
        assert!(
            progress_total <= file_len,
            "progress over-reported: {progress_total} > {file_len}"
        );
        assert_eq!(
            progress_total, file_len,
            "allow_partial skip should advance progress exactly to metadata length"
        );
    }

    /// A zstd frame that decodes cleanly but contains a non-UTF-8 JSONL line
    /// is a *record-level* fault, not zstd-frame corruption. It must abort the
    /// file in BOTH strict and allow-partial modes: under `AllowPartial` it
    /// must NOT be quietly tolerated as a decode skip (which would silently
    /// drop the rest of the file and mark the month incomplete), and the
    /// surfaced message must describe a record-level problem rather than a
    /// "zstd decode error".
    #[test]
    fn invalid_utf8_line_is_fatal_in_strict_and_allow_partial() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad-utf8.zst");

        // Two healthy lines, then a line with bare 0xFF bytes (invalid UTF-8).
        // The zstd frame itself is valid and decodes cleanly.
        let mut payload = Vec::new();
        payload.extend_from_slice(b"{\"id\":\"r0\"}\n");
        payload.extend_from_slice(b"{\"id\":\"r1\"}\n");
        payload.extend_from_slice(&[0xFF, 0xFF]);
        payload.push(b'\n');
        write_zst_with_checksum(&path, &payload);

        // Strict mode: fatal, and the message must describe the record-level
        // UTF-8 fault — NOT a "zstd decode error".
        let err = for_each_line_cfg(&path, 16 * 1024, |_line| Ok(()))
            .expect_err("invalid-UTF-8 line must be fatal in strict mode");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("not valid UTF-8"),
            "strict error should name the UTF-8 fault: {msg}"
        );
        assert!(
            !msg.contains("zstd decode error"),
            "invalid UTF-8 must not be reported as a zstd-frame decode error: {msg}"
        );
        assert!(
            msg.contains(&path.display().to_string()),
            "error should name the offending path: {msg}"
        );

        // Allow-partial mode: still fatal. The file must NOT be tolerated as a
        // decode skip, so `on_skip` must NOT fire and the call must error.
        let mut skip_count = 0usize;
        let mut on_skip = |_: &Path, _: &anyhow::Error| skip_count += 1;
        let err = for_each_line_with_opts_status(
            &path,
            LineStreamOpts {
                read_buf_bytes: Some(16 * 1024),
                on_skip: Some(&mut on_skip),
                partial_read_policy: PartialReadPolicy::AllowPartial,
                ..Default::default()
            },
            |_line| Ok(()),
        )
        .expect_err("invalid-UTF-8 line must stay fatal under AllowPartial");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("not valid UTF-8"),
            "allow-partial error should name the UTF-8 fault: {msg}"
        );
        assert!(
            !msg.contains("zstd decode error"),
            "allow-partial must not classify a UTF-8 fault as a tolerated zstd skip: {msg}"
        );
        assert_eq!(
            skip_count, 0,
            "on_skip must not fire for a record-level UTF-8 fault"
        );
    }

    /// Reddit dumps occasionally store `score` / `created_utc` as JSON strings
    /// or floats. `parse_minimal` must coerce those encodings to `i64` so the
    /// fast-path filters (`matches_minimal`, `within_bounds`) see the same
    /// value regardless of how the number was written; a bare `as_i64()` would
    /// yield `None` and silently drop the record.
    #[test]
    fn parse_minimal_coerces_string_and_float_numeric_fields() {
        // Integer-typed: the baseline encoding.
        let int_rec = parse_minimal(r#"{"score":100,"created_utc":1136074600}"#).unwrap();
        assert_eq!(int_rec.score, Some(100));
        assert_eq!(int_rec.created_utc, Some(1136074600));

        // String-typed numbers coerce identically.
        let str_rec = parse_minimal(r#"{"score":"100","created_utc":"1136074600"}"#).unwrap();
        assert_eq!(str_rec.score, Some(100));
        assert_eq!(str_rec.created_utc, Some(1136074600));

        // Whole-number floats coerce too (`as_i64()` rejects these).
        let flt_rec = parse_minimal(r#"{"score":100.0,"created_utc":1136074600.0}"#).unwrap();
        assert_eq!(flt_rec.score, Some(100));
        assert_eq!(flt_rec.created_utc, Some(1136074600));

        // Negative values survive in every encoding.
        let neg = parse_minimal(r#"{"score":-5,"created_utc":"-5"}"#).unwrap();
        assert_eq!(neg.score, Some(-5));
        assert_eq!(neg.created_utc, Some(-5));

        // Fractional floats and non-numeric strings stay `None` rather than
        // being rounded or guessed — coercion only accepts integral values.
        let bad = parse_minimal(r#"{"score":100.5,"created_utc":"not-a-number"}"#).unwrap();
        assert_eq!(bad.score, None);
        assert_eq!(bad.created_utc, None);

        // Genuinely missing fields remain `None`.
        let missing = parse_minimal(r#"{"id":"c1"}"#).unwrap();
        assert_eq!(missing.score, None);
        assert_eq!(missing.created_utc, None);
    }
}
