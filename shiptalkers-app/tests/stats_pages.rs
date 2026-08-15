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

#[test]
fn daily_chart_svg_renders_bars() {
    let x = vec!["Sat 14".to_string(), "Sun 15".to_string()];
    let chart = ship_talkers::website::daily_chart(
        &[
            ("2026-08-14".to_string(), 60, "1h 0m".to_string()),
            ("2026-08-15".to_string(), 120, "2h 0m".to_string()),
        ],
        &x,
    );
    assert!(chart.svg.starts_with("<svg"));
    assert!(chart.svg.contains("<title>2026-08-15: 2h 0m</title>"));
    assert!(chart.svg.contains("<line"));
    assert!(chart.svg.ends_with("</svg>"));
    assert_eq!(chart.axis.len(), 5);
    assert_eq!(chart.axis[4], "0s");
    assert_eq!(chart.x, x);
    let empty = ship_talkers::website::daily_chart(&[], &[]);
    assert!(empty.svg.is_empty());
    assert!(empty.axis.is_empty());
    assert!(empty.x.is_empty());
}

#[test]
fn fmt_axis_secs_compact() {
    let f = ship_talkers::website::fmt_axis_secs;
    assert_eq!(f(0), "0s");
    assert_eq!(f(45), "45s");
    assert_eq!(f(120), "2m");
    assert_eq!(f(3600), "1.0h");
    assert_eq!(f(10800), "3.0h");
    assert_eq!(f(86400), "24h");
    assert_eq!(f(2 * 86400), "2d");
}
