use ship_talkers::db::postgres_db::AuthDb;
use ship_talkers::settings::RuntimeSettings;
use ship_talkers::website::router;

use axum::body::Body;
use axum::http::Request;
use ship_talkers_lib::sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

fn dsn() -> &'static str {
    "postgres://ship_talkers:ship_talkers@localhost:5432/ship_talkers"
}

/// These routes run real queries against Postgres, so a lazy pool that never
/// establishes a connection makes every DB-backed request block for the
/// connect timeout (tens of seconds each). Probe reachability up front with a
/// short timeout and skip the test when no Postgres is around, so `cargo test`
/// stays fast on machines without a database.
async fn db_available() -> bool {
    let Ok(pool) = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(3))
        .connect(dsn())
        .await
    else {
        return false;
    };
    let ok = pool.acquire().await.is_ok();
    pool.close().await;
    ok
}

#[tokio::test]
async fn stats_routes_match() {
    if !db_available().await {
        eprintln!("Postgres not reachable, skipping stats route test");
        return;
    }

    let app = router(
        PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy(dsn())
            .expect("lazy pool"),
        RuntimeSettings::load(),
        std::sync::Arc::new(AuthDb::new(
            PgPoolOptions::new()
                .max_connections(1)
                .connect_lazy(dsn())
                .expect("lazy pool"),
        )),
    );
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
