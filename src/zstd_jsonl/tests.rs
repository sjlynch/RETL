
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

        let status = for_each_line_cfg_status(&path, 16 * 1024, |_line| Ok(()))
            .expect("status API should not bubble corrupt-frame decode errors");
        assert!(
            !status,
            "status API must report the corrupt file as incomplete"
        );

        // The *_with_skip variant must also skip gracefully AND surface the
        // skip event to the caller via `on_skip`. The path passed to the
        // callback must match the file we tried to read, and the captured
        // error must be non-empty.
        let mut skip_calls: Vec<(PathBuf, String)> = Vec::new();
        let mut lines_seen2 = 0usize;
        let res = for_each_line_cfg_with_skip(
            &path,
            16 * 1024,
            |p, e| skip_calls.push((p.to_path_buf(), e.to_string())),
            |_line| {
                lines_seen2 += 1;
                Ok(())
            },
        );
        assert!(
            res.is_ok(),
            "for_each_line_cfg_with_skip should skip a corrupt file gracefully, got {:?}",
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
        let res = for_each_line_cfg_with_skip(
            &path,
            16 * 1024,
            |_, _| skip_count += 1,
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
        let err =
            for_each_line_cfg_with_skip(&path, 16 * 1024, |_, _| skip_count += 1, |_line| Ok(()))
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
        let err = for_each_line_cfg_with_skip(
            &path,
            16 * 1024,
            |_, _| skip_count += 1,
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
}
