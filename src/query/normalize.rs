
fn keyword_automaton_for<'a>(
    keywords: &'a Option<Vec<String>>,
    cache: &'a OnceLock<Arc<AhoCorasick>>,
    match_kind: MatchKind,
) -> Option<&'a AhoCorasick> {
    let kws = keywords.as_ref()?;
    if kws.is_empty() {
        return None;
    }
    let arc = cache.get_or_init(|| {
        let ac = AhoCorasickBuilder::new()
            .ascii_case_insensitive(true)
            .match_kind(match_kind)
            .build(kws.iter())
            .expect("aho-corasick build from non-empty keyword list");
        Arc::new(ac)
    });
    Some(arc.as_ref())
}

#[inline]
fn keyword_list_all_ascii(keywords: &Option<Vec<String>>) -> bool {
    keywords
        .as_ref()
        .map_or(true, |kws| kws.iter().all(|kw| kw.is_ascii()))
}

fn normalize_trim_lower_list(value: &mut Option<Vec<String>>) {
    if let Some(list) = value.as_mut() {
        for s in list.iter_mut() {
            *s = s.trim().to_lowercase();
        }
        list.sort();
        list.dedup();
    }
}
