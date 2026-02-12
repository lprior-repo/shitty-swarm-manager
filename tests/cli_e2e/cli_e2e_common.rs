pub(crate) fn e2e_enabled() -> bool {
    std::env::var("SWARM_E2E")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

pub(crate) fn local_database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://shitty_swarm_manager:shitty_swarm_manager@localhost:5437/shitty_swarm_manager_db"
            .to_string()
    })
}
