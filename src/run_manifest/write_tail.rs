
fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

fn git_hash() -> Option<&'static str> {
    option_env!("RETL_GIT_HASH")
        .or(option_env!("VERGEN_GIT_SHA"))
        .or(option_env!("GITHUB_SHA"))
        .or(option_env!("GIT_HASH"))
}
