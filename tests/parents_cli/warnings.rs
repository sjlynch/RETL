use crate::common::parents::*;
use crate::common::read_jsonl_values;

#[test]
fn parents_cli_warns_when_unresolved_rate_exceeds_threshold_only() {
    let base = make_cross_month_parent_corpus();
    let spool = write_cross_month_spool(&base);

    let tight = run_parents_for_warning_case(&base, &spool, "0", "tight");
    assert!(tight.status.success(), "tight run failed: {tight:?}");
    let tight_output = command_output_text(&tight);
    assert!(
        tight_output.contains("more than 5%"),
        "expected unresolved-rate warning in command output, got: {tight_output}"
    );

    let wide = run_parents_for_warning_case(&base, &spool, "1", "wide");
    assert!(wide.status.success(), "wide run failed: {wide:?}");
    let wide_output = command_output_text(&wide);
    assert!(
        !wide_output.contains("more than 5%"),
        "did not expect unresolved-rate warning in command output, got: {wide_output}"
    );

    let wide_values = read_jsonl_values(&base.join("parents_out_wide/part_RC_2006-01.jsonl"));
    assert_eq!(
        wide_values[0]
            .pointer("/parent/title")
            .and_then(|v| v.as_str()),
        Some("December parent")
    );
}
