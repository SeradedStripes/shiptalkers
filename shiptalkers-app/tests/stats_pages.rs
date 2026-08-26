use ship_talkers::db::postgres_db::AuthDb;
use ship_talkers::settings::RuntimeSettings;
use ship_talkers::website::router;

use axum::body::Body;
use axum::http::Request;
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

fn app() -> axum::Router {
    router(
        PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy("postgres://ship_talkers:ship_talkers@localhost:5432/ship_talkers")
            .expect("lazy pool"),
        RuntimeSettings::load(),
        std::sync::Arc::new(AuthDb::new(
            PgPoolOptions::new()
                .max_connections(1)
                .connect_lazy("postgres://ship_talkers:ship_talkers@localhost:5432/ship_talkers")
                .expect("lazy pool"),
        )),
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
        "/leaderboard/talkers?q=1234",
        "/leaderboard/talkers?q=ZachLatta",
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
