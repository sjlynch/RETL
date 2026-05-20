
fn validate_text_regex_pattern(pattern: &str) -> Result<(), QueryBuildError> {
    if pattern.trim().is_empty() {
        return Err(QueryBuildError::new(
            "text_regex contains a blank pattern; blank regex patterns are not allowed",
        ));
    }
    Regex::new(pattern).map_err(|e| {
        QueryBuildError::new(format!("text_regex is invalid: {e}; pattern={pattern:?}"))
    })?;
    Ok(())
}

fn validate_author_regex_pattern(pattern: &str) -> Result<(), QueryBuildError> {
    // `Regex::new("")` succeeds and matches every author — a silent match-all.
    // Reject blank patterns the same way `text_regex` does so an empty
    // `--author-regex ''` fails loudly instead of widening the scan.
    if pattern.trim().is_empty() {
        return Err(QueryBuildError::new(
            "author_regex contains a blank pattern; blank regex patterns are not allowed",
        ));
    }
    Regex::new(pattern).map_err(|e| {
        QueryBuildError::new(format!("author_regex is invalid: {e}; pattern={pattern:?}"))
    })?;
    Ok(())
}

fn validate_text_regex_filter(
    pattern: &Option<String>,
    compiled: &Option<Regex>,
) -> Result<(), QueryBuildError> {
    if let Some(pattern) = pattern {
        validate_text_regex_pattern(pattern)?;
    }
    if let Some(re) = compiled {
        if re.as_str().trim().is_empty() {
            return Err(QueryBuildError::new(
                "text_regex contains a blank pattern; blank regex patterns are not allowed",
            ));
        }
    }
    Ok(())
}

fn sort_id_list(list: &mut Vec<String>) {
    list.sort();
}

fn normalize_id_filters(query: &mut QuerySpec) {
    let had_any_id_field = query.ids_in.is_some()
        || query.comment_ids_in.is_some()
        || query.submission_ids_in.is_some();

    let mut ids_any = Vec::new();
    let mut ids_comments = Vec::new();
    let mut ids_submissions = Vec::new();

    if let Some(list) = query.ids_in.take() {
        for raw in list {
            let normalized = normalize_record_id_selector(&raw);
            match normalized.kind {
                Some(RecordIdKind::Comment) => ids_comments.push(normalized.bare),
                Some(RecordIdKind::Submission) => ids_submissions.push(normalized.bare),
                None => ids_any.push(normalized.bare),
            }
        }
    }
    if let Some(list) = query.comment_ids_in.take() {
        for raw in list {
            ids_comments.push(normalize_record_id_selector(&raw).bare);
        }
    }
    if let Some(list) = query.submission_ids_in.take() {
        for raw in list {
            ids_submissions.push(normalize_record_id_selector(&raw).bare);
        }
    }

    sort_id_list(&mut ids_any);
    sort_id_list(&mut ids_comments);
    sort_id_list(&mut ids_submissions);

    let all_empty = ids_any.is_empty() && ids_comments.is_empty() && ids_submissions.is_empty();
    query.ids_in = if !ids_any.is_empty() || (had_any_id_field && all_empty) {
        Some(ids_any)
    } else {
        None
    };
    query.comment_ids_in = (!ids_comments.is_empty()).then_some(ids_comments);
    query.submission_ids_in = (!ids_submissions.is_empty()).then_some(ids_submissions);
}

fn sorted_id_list_contains(list: Option<&Vec<String>>, needle: &str) -> bool {
    list.is_some_and(|list| {
        list.binary_search_by(|candidate| candidate.as_str().cmp(needle))
            .is_ok()
    })
}

fn validate_id_filter_overlaps(query: &QuerySpec) -> Result<(), QueryBuildError> {
    let Some(any_ids) = query.ids_in.as_ref() else {
        return Ok(());
    };
    for id in any_ids {
        if sorted_id_list_contains(query.comment_ids_in.as_ref(), id)
            || sorted_id_list_contains(query.submission_ids_in.as_ref(), id)
        {
            return Err(QueryBuildError::new(format!(
                "ids_in contains duplicate ID '{id}' after normalization"
            )));
        }
    }
    Ok(())
}

fn validate_id_list_filter(
    field: &'static str,
    value: &Option<Vec<String>>,
) -> Result<(), QueryBuildError> {
    let Some(list) = value else {
        return Ok(());
    };
    if list.is_empty() {
        return Err(QueryBuildError::new(format!(
            "{field} cannot be an empty list; omit {field} to match all"
        )));
    }
    for id in list {
        if id.trim().is_empty() {
            return Err(QueryBuildError::new(format!(
                "{field} contains a blank entry after normalization; blank entries are not allowed"
            )));
        }
    }

    let mut sorted = list.clone();
    sorted.sort();
    if let Some(duplicate) = sorted
        .windows(2)
        .find_map(|pair| (pair[0] == pair[1]).then(|| pair[0].clone()))
    {
        return Err(QueryBuildError::new(format!(
            "{field} contains duplicate ID '{duplicate}' after normalization"
        )));
    }

    Ok(())
}

#[inline]
fn validate_string_list_filter(
    field: &'static str,
    value: &Option<Vec<String>>,
) -> Result<(), QueryBuildError> {
    let Some(list) = value else {
        return Ok(());
    };
    if list.is_empty() {
        return Err(QueryBuildError::new(format!(
            "{field} cannot be an empty list; omit {field} to match all"
        )));
    }
    if list.iter().any(|s| s.is_empty()) {
        return Err(QueryBuildError::new(format!(
            "{field} contains a blank entry after normalization; blank entries are not allowed"
        )));
    }
    Ok(())
}

pub fn normalize_str(s: &str) -> String {
    let s = s.trim().to_lowercase();
    if let Some(rest) = s.strip_prefix("r/") {
        rest.to_string()
    } else {
        s
    }
}
