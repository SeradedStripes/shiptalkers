use axum::{
    Router,
    routing::get,
    Json,
    response::Html,
    extract::State,
};
use clickhouse::Client;
use serde::Serialize;
use tower_http::services::ServeDir;

#[derive(Clone)]
pub struct AppState {
    pub clickhouse: Client,
}

#[derive(Serialize)]
pub struct Stats {
    pub total_messages: u64,
    pub active_users: u64,
    pub channels_tracked: u64,
    pub total_channels: u64,
    pub archived_channels: u64,
    pub coding_minutes: u64,
    pub leaderboard: Vec<UserStats>,
}

#[derive(Serialize)]
pub struct UserStats {
    pub user_id: String,
    pub messages: u64,
    pub coding_minutes: u64,
}

pub fn router(clickhouse: Client) -> Router {
    let state = AppState { clickhouse };

    let api_routes = Router::new()
        .route("/stats", get(get_stats));

    Router::new()
        .route("/", get(|| async { Html(include_str!("static/index.html")) }))
        .route("/link", get(|| async { Html(include_str!("static/link.html")) }))
        .route("/stats", get(|| async { Html(include_str!("static/stats.html")) }))
        .nest("/api", api_routes)
        .fallback_service(ServeDir::new("static"))
        .with_state(state)
}

async fn get_stats(State(state): State<AppState>) -> Json<Stats> {
    let ch = &state.clickhouse;

    let total_messages: u64 = ch
        .query("SELECT count() FROM slack_messages")
        .fetch_one()
        .await
        .unwrap_or(0);

    let active_users: u64 = ch
        .query("SELECT uniqExact(user_id) FROM slack_messages")
        .fetch_one()
        .await
        .unwrap_or(0);

    let channels_tracked: u64 = ch
        .query("SELECT uniqExact(channel_id) FROM slack_messages")
        .fetch_one()
        .await
        .unwrap_or(0);

    let total_channels: u64 = ch
        .query("SELECT count() FROM slack_channels")
        .fetch_one()
        .await
        .unwrap_or(0);

    let archived_channels: u64 = crate::db::clickhouse_db::get_metric(ch, "archived_channels")
        .await
        .unwrap_or(0);

    let coding_minutes: u64 = ch
        .query("SELECT sum(minutes) FROM coding_activity")
        .fetch_one()
        .await
        .unwrap_or(0);

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct LeaderboardRow {
        user_id: String,
        messages: u64,
    }

    let leaderboard: Vec<UserStats> = ch
        .query(
            "SELECT user_id, count() as messages
             FROM slack_messages
             GROUP BY user_id
             ORDER BY messages DESC
             LIMIT 20"
        )
        .fetch_all::<LeaderboardRow>()
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|r| UserStats {
                    user_id: r.user_id,
                    messages: r.messages,
                    coding_minutes: 0,
                })
                .collect()
        })
        .unwrap_or_default();

    Json(Stats {
        total_messages,
        active_users,
        channels_tracked,
        total_channels,
        archived_channels,
        coding_minutes,
        leaderboard,
    })
}
