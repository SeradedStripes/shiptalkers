use ship_talkers::db::normalize_clickhouse_url;

#[test]
fn converts_coolify_internal_url() {
    let (url, user, password, db) =
        normalize_clickhouse_url("clickhouse://ship_talkers:ship_talkers@dbhost:9000/default");
    assert_eq!(url, "http://dbhost:8123");
    assert_eq!(user.as_deref(), Some("ship_talkers"));
    assert_eq!(password.as_deref(), Some("ship_talkers"));
    assert_eq!(db.as_deref(), Some("default"));
}

#[test]
fn converts_without_credentials() {
    let (url, user, password, db) = normalize_clickhouse_url("clickhouse://dbhost:9000");
    assert_eq!(url, "http://dbhost:8123");
    assert_eq!(user, None);
    assert_eq!(password, None);
    assert_eq!(db, None);
}

#[test]
fn converts_without_database() {
    let (url, _, _, db) = normalize_clickhouse_url("clickhouse://user:pass@dbhost:9000");
    assert_eq!(url, "http://dbhost:8123");
    assert_eq!(db, None);
}

#[test]
fn leaves_http_urls_untouched() {
    let (url, user, password, db) = normalize_clickhouse_url("http://localhost:8123");
    assert_eq!(url, "http://localhost:8123");
    assert_eq!(user, None);
    assert_eq!(password, None);
    assert_eq!(db, None);
}

#[test]
fn leaves_https_urls_untouched() {
    let (url, _, _, _) = normalize_clickhouse_url("https://db.example.com:8443");
    assert_eq!(url, "https://db.example.com:8443");
}

#[test]
fn strips_trailing_slashes_from_database() {
    let (url, _, _, db) = normalize_clickhouse_url("clickhouse://user:pass@dbhost:9000/db/");
    assert_eq!(url, "http://dbhost:8123");
    assert_eq!(db.as_deref(), Some("db"));
}

#[test]
fn drops_empty_credentials() {
    let (url, user, password, db) = normalize_clickhouse_url("clickhouse://:@dbhost:9000/db");
    assert_eq!(url, "http://dbhost:8123");
    assert_eq!(user, None);
    assert_eq!(password, None);
    assert_eq!(db.as_deref(), Some("db"));
}
