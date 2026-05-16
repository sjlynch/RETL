
fn sources_label(sources: Sources) -> &'static str {
    match sources {
        Sources::Comments => "comments",
        Sources::Submissions => "submissions",
        Sources::Both => "both",
    }
}

trait EtlOptionDates {
    fn opts_start_string(&self) -> Option<String>;
    fn opts_end_string(&self) -> Option<String>;
}

impl EtlOptionDates for ETLOptions {
    fn opts_start_string(&self) -> Option<String> {
        self.start.map(|ym| ym.to_string())
    }

    fn opts_end_string(&self) -> Option<String> {
        self.end.map(|ym| ym.to_string())
    }
}
