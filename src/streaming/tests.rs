
#[cfg(test)]
mod tests {
    use super::*;
    use crate::date::YearMonth;
    use crate::paths::FileKind;

    fn write_zst(path: &std::path::Path, payload: &[u8]) {
        let f = std::fs::File::create(path).unwrap();
        let mut enc = zstd::stream::write::Encoder::new(f, 3).unwrap();
        std::io::Write::write_all(&mut enc, payload).unwrap();
        enc.finish().unwrap();
    }

    struct FailsAfterWriter {
        remaining: usize,
    }

    impl std::io::Write for FailsAfterWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.remaining == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "writer boom",
                ));
            }
            let n = self.remaining.min(buf.len());
            self.remaining -= n;
            Ok(n)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn stream_job_propagates_writer_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("RC_2020-01.zst");
        write_zst(
            &path,
            b"{\"id\":\"c1\",\"author\":\"alice\",\"subreddit\":\"rust\",\"created_utc\":1577836800}\n",
        );

        let job = FileJob {
            kind: FileKind::Comment,
            ym: YearMonth::new(2020, 1),
            path,
        };
        let query = QuerySpec::default();
        let whitelist: Option<Vec<String>> = None;
        let mut writer = FailsAfterWriter { remaining: 8 };

        let res = stream_job(
            &job,
            &mut writer,
            None,
            &query,
            &whitelist,
            None,
            None,
            16 * 1024,
            false,
            None,
        );

        let err = res.expect_err("writer errors from stream_job must propagate");
        assert!(
            err.to_string().contains("writer boom"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn stream_job_skips_interior_blank_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("RC_2020-01.zst");
        // Two valid records with a stray interior blank line between them
        // (and a whitespace-only line), as produced by manual concatenation
        // or re-compression of monthly dumps. The blank lines must be skipped,
        // not abort the whole month with a misleading "malformed JSON" error.
        write_zst(
            &path,
            concat!(
                r#"{"id":"c1","author":"alice","subreddit":"rust","created_utc":1577836800}"#,
                "\n",
                "\n",
                "   \n",
                r#"{"id":"c2","author":"bob","subreddit":"rust","created_utc":1577836801}"#,
                "\n",
            )
            .as_bytes(),
        );

        let job = FileJob {
            kind: FileKind::Comment,
            ym: YearMonth::new(2020, 1),
            path,
        };
        let query = QuerySpec::default();
        let whitelist: Option<Vec<String>> = None;
        let mut out: Vec<u8> = Vec::new();

        let res = stream_job(
            &job,
            &mut out,
            None,
            &query,
            &whitelist,
            None,
            None,
            16 * 1024,
            false,
            None,
        )
        .expect("an interior blank line must not abort the month");

        assert_eq!(res.written, 2, "both valid records must be written");
        assert!(res.complete);
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("\"c1\"") && text.contains("\"c2\""));
    }

    #[test]
    fn whitelist_slow_path_reports_field_presence_independent_of_tokenizer_buffer() {
        let fields = vec!["id".to_string()];
        let tokenizer = WhitelistTokenizer::new(fields.iter().map(|s| s.as_str()));
        let mut tokenizer_buf = String::new();
        let mut matched_indices = Vec::new();
        let mut out = Vec::new();
        let mut written = 0_u64;

        let fast_used_slow_path = write_with_whitelist(
            &mut out,
            r#"{"id":"kept","subreddit":"programming","author":"a"}"#,
            &fields,
            &tokenizer,
            &mut tokenizer_buf,
            &mut matched_indices,
            false,
            &mut written,
            std::path::Path::new("test.jsonl"),
            1,
        )
        .unwrap();
        assert_eq!(matched_indices, vec![0], "sanity: projection contains id");
        assert!(!fast_used_slow_path);
        assert!(tokenizer_buf.contains("kept"));

        // Top-level arrays are valid JSON so the slow Value path can project
        // them, but there is no top-level object containing `id`. This must be
        // computed from the slow path, not from tokenizer_buf.
        let slow_used_slow_path = write_with_whitelist(
            &mut out,
            r#"[{"id":"not-a-top-level-object"}]"#,
            &fields,
            &tokenizer,
            &mut tokenizer_buf,
            &mut matched_indices,
            false,
            &mut written,
            std::path::Path::new("test.jsonl"),
            2,
        )
        .unwrap();
        assert!(
            matched_indices.is_empty(),
            "slow-path array projection should report no top-level id match"
        );
        assert!(
            slow_used_slow_path,
            "top-level array must take the Value slow path"
        );
        assert_eq!(written, 2);
    }

    #[test]
    fn strict_whitelist_ignores_slow_path_only_emissions() {
        // A run where 100% of emissions are slow-path must NOT trip the
        // strict_whitelist verdict because fast-path presence is the production
        // signal used by the checker.
        let fields = vec!["not_present".to_string()];
        let tokenizer = WhitelistTokenizer::new(fields.iter().map(|s| s.as_str()));
        let tracker = WhitelistMatchTracker::new(true, fields.iter().cloned());
        let mut tokenizer_buf = String::new();
        let mut matched_indices = Vec::new();
        let mut out = Vec::new();
        let mut written = 0_u64;

        for i in 0..(WHITELIST_ZERO_MATCH_SAMPLE * 2) {
            let used_slow_path = write_with_whitelist(
                &mut out,
                r#"[{"id":"slow"}]"#,
                &fields,
                &tokenizer,
                &mut tokenizer_buf,
                &mut matched_indices,
                false,
                &mut written,
                std::path::Path::new("test.jsonl"),
                i + 1,
            )
            .unwrap();
            assert!(used_slow_path);
            assert!(matched_indices.is_empty());
            tracker
                .observe(WhitelistEmission {
                    matched_fields: &matched_indices,
                    used_slow_path,
                })
                .expect("slow-path emissions must not trip strict_whitelist");
        }
        tracker
            .finalize()
            .expect("slow-path-only emissions must not trip strict_whitelist");
    }

    #[test]
    fn strict_whitelist_fires_on_fast_path_empty_projection_at_finalize() {
        let fields = vec!["not_present".to_string()];
        let tokenizer = WhitelistTokenizer::new(fields.iter().map(|s| s.as_str()));
        let tracker = WhitelistMatchTracker::new(true, fields.iter().cloned());
        let mut tokenizer_buf = String::new();
        let mut matched_indices = Vec::new();
        let mut out = Vec::new();
        let mut written = 0_u64;

        for i in 0..WHITELIST_ZERO_MATCH_SAMPLE {
            let used_slow_path = write_with_whitelist(
                &mut out,
                r#"{"id":"fast","subreddit":"programming","author":"a"}"#,
                &fields,
                &tokenizer,
                &mut tokenizer_buf,
                &mut matched_indices,
                false,
                &mut written,
                std::path::Path::new("test.jsonl"),
                i + 1,
            )
            .unwrap();
            assert!(!used_slow_path);
            assert!(matched_indices.is_empty());
            tracker
                .observe(WhitelistEmission {
                    matched_fields: &matched_indices,
                    used_slow_path,
                })
                .expect("observe only records field presence; finalization reports misses");
        }

        let msg = tracker
            .finalize()
            .expect_err("strict tracker must error at finalization for fast-path misses")
            .to_string();
        assert!(
            msg.contains("--whitelist matched zero fields") && msg.contains("not_present"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn fast_path_non_empty_emissions_suppress_warning_even_with_empty_slow_path() {
        // Inverse failure mode from the bug: when the fast path is healthy
        // (matches), interleaved slow-path empty projections must not push
        // the tracker into a false warning.
        let fields = vec!["id".to_string()];
        let tokenizer = WhitelistTokenizer::new(fields.iter().map(|s| s.as_str()));
        let tracker = WhitelistMatchTracker::new(true, fields.iter().cloned());
        let mut tokenizer_buf = String::new();
        let mut matched_indices = Vec::new();
        let mut out = Vec::new();
        let mut written = 0_u64;

        for i in 0..WHITELIST_ZERO_MATCH_SAMPLE {
            let line = if i % 2 == 0 {
                r#"{"id":"kept","subreddit":"programming","author":"a"}"#
            } else {
                r#"[{"id":"slow"}]"#
            };
            let used_slow_path = write_with_whitelist(
                &mut out,
                line,
                &fields,
                &tokenizer,
                &mut tokenizer_buf,
                &mut matched_indices,
                false,
                &mut written,
                std::path::Path::new("test.jsonl"),
                i + 1,
            )
            .unwrap();
            tracker
                .observe(WhitelistEmission {
                    matched_fields: &matched_indices,
                    used_slow_path,
                })
                .expect("healthy fast-path emissions must not trigger strict_whitelist");
        }
        tracker
            .finalize()
            .expect("healthy fast-path emissions must not trigger strict_whitelist");
    }

    #[test]
    fn legacy_usernames_normalizes_subreddit_and_pseudo_users() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("RC_2020-01.zst");
        // The subreddit is recorded as mixed-case "Programming"; one record's
        // author is a mixed-case `[Deleted]` pseudo-user; one record is in a
        // different subreddit.
        write_zst(
            &path,
            concat!(
                r#"{"id":"c1","author":"Alice","subreddit":"Programming","created_utc":1}"#,
                "\n",
                r#"{"id":"c2","author":"[Deleted]","subreddit":"Programming","created_utc":2}"#,
                "\n",
                r#"{"id":"c3","author":"bob","subreddit":"other","created_utc":3}"#,
                "\n",
            )
            .as_bytes(),
        );

        let job = FileJob {
            kind: FileKind::Comment,
            ym: YearMonth::new(2020, 1),
            path,
        };

        let shard_writer = ShardedWriter::create(dir.path(), "legacy_un", 4).unwrap();
        // The caller passes the subreddit *with* an `r/` prefix and odd casing:
        // the legacy path must normalize it the same way the canonical query
        // path does, otherwise it silently matches nothing.
        process_file_for_usernames_with_skip(
            &job,
            16 * 1024,
            "r/Programming",
            &shard_writer,
            None,
            false,
            None,
            |_p, _e| {},
        )
        .unwrap();

        let outs = shard_writer.dedup("legacy_un").unwrap();
        let mut names = std::collections::BTreeSet::new();
        for p in outs {
            let f = std::fs::File::open(&p).unwrap();
            for line in std::io::BufRead::lines(std::io::BufReader::new(f)) {
                let line = line.unwrap();
                if !line.is_empty() {
                    names.insert(line);
                }
            }
        }
        // `Alice` is kept (subreddit matched after `r/` strip + case-fold);
        // mixed-case `[Deleted]` is excluded (case-insensitive pseudo-user);
        // `bob` is excluded (different subreddit).
        assert_eq!(
            names,
            std::iter::once("Alice".to_string()).collect::<std::collections::BTreeSet<_>>()
        );
    }

    #[test]
    fn legacy_usernames_skips_interior_blank_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("RC_2020-01.zst");
        // A stray interior blank line between two valid records must be
        // skipped rather than aborting the whole month as malformed JSON.
        write_zst(
            &path,
            concat!(
                r#"{"id":"c1","author":"alice","subreddit":"rust","created_utc":1}"#,
                "\n",
                "\n",
                r#"{"id":"c2","author":"bob","subreddit":"rust","created_utc":2}"#,
                "\n",
            )
            .as_bytes(),
        );

        let job = FileJob {
            kind: FileKind::Comment,
            ym: YearMonth::new(2020, 1),
            path,
        };

        let shard_writer = ShardedWriter::create(dir.path(), "blank_un", 4).unwrap();
        process_file_for_usernames_with_skip(
            &job,
            16 * 1024,
            "rust",
            &shard_writer,
            None,
            false,
            None,
            |_p, _e| {},
        )
        .expect("an interior blank line must not abort the month");

        let outs = shard_writer.dedup("blank_un").unwrap();
        let mut names = std::collections::BTreeSet::new();
        for p in outs {
            let f = std::fs::File::open(&p).unwrap();
            for line in std::io::BufRead::lines(std::io::BufReader::new(f)) {
                let line = line.unwrap();
                if !line.is_empty() {
                    names.insert(line);
                }
            }
        }
        assert_eq!(
            names,
            ["alice".to_string(), "bob".to_string()]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
        );
    }

    #[test]
    fn nested_created_utc_left_untouched_by_all_three_human_timestamp_paths() {
        // Regression: an RS record nests full submission objects with their
        // own `created_utc` (here inside `crosspost_parent_list`). All three
        // human-timestamp rewriters — the byte path, the whitelist tokenizer,
        // and the slow `serde_json::Value` path — must rewrite only the
        // top-level timestamps and leave the nested ones as integers. The
        // byte path previously rewrote nested matches, diverging from the
        // other two whenever a whitelist or the tokenizer fallback was used.
        let raw = concat!(
            r#"{"id":"abc123","subreddit":"rust","created_utc":1136074600,"#,
            r#""edited":1136074800,"crosspost_parent_list":[{"id":"xpost1","#,
            r#""created_utc":1100000000,"edited":false}],"title":"hi"}"#,
        );
        // Round-trip through serde_json so `line` is already in the canonical
        // compact form the slow path re-emits — that makes a byte-identical
        // three-way comparison meaningful.
        let canonical: Value = serde_json::from_str(raw).unwrap();
        let line = serde_json::to_string(&canonical).unwrap();

        // Path 1: byte-level rewriter.
        let mut byte_out = String::new();
        rewrite_human_timestamps_bytes(&line, &mut byte_out);

        // Path 2: whitelist tokenizer with the fused timestamp rewrite. Every
        // top-level key is whitelisted, so the projection is the identity.
        let fields: Vec<String> = canonical
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        let tokenizer = WhitelistTokenizer::new(fields.iter().map(|s| s.as_str()));
        let mut tok_out = String::new();
        tokenizer
            .tokenize_and_rewrite_timestamps_into(&line, &mut tok_out)
            .expect("flat top-level object must tokenize");

        // Path 3: slow `serde_json::Value` path.
        let mut slow_val: Value = serde_json::from_str(&line).unwrap();
        apply_human_timestamps(&mut slow_val);
        let slow_out = serde_json::to_string(&slow_val).unwrap();

        assert_eq!(
            byte_out, tok_out,
            "byte path and whitelist tokenizer must agree byte-for-byte"
        );
        assert_eq!(
            byte_out, slow_out,
            "byte path and slow Value path must agree byte-for-byte"
        );

        // And explicitly: top-level timestamps became RFC3339 strings while
        // the nested ones inside `crosspost_parent_list` stayed integers.
        let got: Value = serde_json::from_str(&byte_out).unwrap();
        assert!(got["created_utc"].is_string(), "top-level created_utc");
        assert!(got["edited"].is_string(), "top-level edited");
        let nested = &got["crosspost_parent_list"][0];
        assert_eq!(
            nested["created_utc"].as_i64(),
            Some(1_100_000_000),
            "nested created_utc must stay an integer"
        );
        assert_eq!(
            nested["edited"].as_bool(),
            Some(false),
            "nested edited must stay a bool"
        );
    }
}
