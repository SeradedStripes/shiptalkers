use ship_talkers::db::sqlite::AuthDb;
use ship_talkers::settings::RuntimeSettings;
use ship_talkers::website::router;

use axum::body::Body;
use axum::http::Request;
use clickhouse::Client;
use tower::ServiceExt;

fn app() -> axum::Router {
    router(
        Client::default()
            .with_url("http://localhost:8123")
            .with_user("ship_talkers")
            .with_password("ship_talkers")
            .with_database("ship_talkers"),
        RuntimeSettings::load(),
        std::sync::Arc::new(AuthDb::open(":memory:").expect("open in-memory auth db")),
    )
}

#[tokio::test]
async fn stats_routes_match() {
    let app = app();
    for uri in [
        "/stats/U01MPHKFZ7S",
        "/stats/C0123456789",
        "/search",
        "/leaderboard/channels",
        "/leaderboard/words",
    ] {
        let res = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_ne!(
            res.status(),
            axum::http::StatusCode::NOT_FOUND,
            "route must match: {uri}"
        );
    }
}
