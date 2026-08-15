use clickhouse::Client;

pub mod clickhouse_db;
pub mod sqlite;

/// Converts a Coolify-style internal ClickHouse URL
/// (`clickhouse://user:pass@host:9000/db`) into the HTTP URL the client speaks
/// (`http://host:8123`), returning the credentials and database embedded in it.
/// Any other scheme is passed through untouched.
pub fn normalize_clickhouse_url(
    raw: &str,
) -> (String, Option<String>, Option<String>, Option<String>) {
    let Some(rest) = raw.strip_prefix("clickhouse://") else {
        return (raw.to_string(), None, None, None);
    };
    if rest.is_empty() {
        return (raw.to_string(), None, None, None);
    }
    let (auth, rest) = match rest.split_once('@') {
        Some((a, r)) => (Some(a), r),
        None => (None, rest),
    };
    let (user, password) = match auth {
        Some(a) => match a.split_once(':') {
            Some((u, p)) => (Some(u.to_string()), Some(p.to_string())),
            None => (Some(a.to_string()), None),
        },
        None => (None, None),
    };
    let (host_port, db) = match rest.split_once('/') {
        Some((h, d)) => (h, Some(d.trim_end_matches('/').to_string())),
        None => (rest, None),
    };
    let host = match host_port.rsplit_once(':') {
        Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) => h,
        _ => host_port,
    };
    let host = host.trim_matches(|c| c == '[' || c == ']');
    let url = if host.contains(':') {
        format!("http://[{}]:8123", host)
    } else {
        format!("http://{}:8123", host)
    };
    (
        url,
        user.filter(|u| !u.is_empty()),
        password.filter(|p| !p.is_empty()),
        db.filter(|d| !d.is_empty()),
    )
}

pub struct Database {
    pub clickhouse: Client,
}

impl Database {
    pub fn new(
        clickhouse_url: &str,
        clickhouse_user: &str,
        clickhouse_password: &str,
        clickhouse_db: &str,
    ) -> Self {
        let clickhouse = Client::default()
            .with_url(clickhouse_url)
            .with_user(clickhouse_user)
            .with_password(clickhouse_password)
            .with_database(clickhouse_db);

        Self { clickhouse }
    }
}
