use axum::{
    Router,
    routing::get,
    Json,
    response::Html,
    extract::State,
};
use clickhouse::Client;
use serde::Serialize;
use std::collections::HashMap;
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
    pub coding_minutes: u64,
    pub db_size_gib: f64,
    pub leaderboard: Vec<UserStats>,
    pub shiptalkers: Vec<UserStats>,
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
        .query("SELECT count() FROM slack_messages FINAL")
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
        .query("SELECT count() FROM slack_channels FINAL")
        .fetch_one()
        .await
        .unwrap_or(0);

    let coding_minutes: u64 = ch
        .query("SELECT sum(minutes) FROM coding_activity")
        .fetch_one()
        .await
        .unwrap_or(0);

    let db_size_bytes: u64 = ch
        .query("SELECT sum(bytes_on_disk) as bytes FROM system.parts WHERE database = currentDatabase() AND active")
        .fetch_one()
        .await
        .unwrap_or(0);

    let db_size_gib = db_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct LeaderboardRow {
        user_id: String,
        messages: u64,
    }

    let leaderboard: Vec<UserStats> = ch
        .query(
            "SELECT user_id, count() as messages
             FROM slack_messages FINAL
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

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct UserMsgRow {
        user_id: String,
        messages: u64,
    }

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct UserMinRow {
        user_id: String,
        minutes: i64,
    }

    let msg_rows: Vec<UserMsgRow> = ch
        .query("SELECT user_id, count() as messages FROM slack_messages FINAL GROUP BY user_id")
        .fetch_all()
        .await
        .unwrap_or_default();

    let min_rows: Vec<UserMinRow> = ch
        .query("SELECT user_id, sum(minutes) as minutes FROM coding_activity GROUP BY user_id")
        .fetch_all()
        .await
        .unwrap_or_default();

    let mut combined: HashMap<String, (u64, u64)> = HashMap::new();
    for r in msg_rows {
        combined.entry(r.user_id).or_insert((0, 0)).0 = r.messages;
    }
    for r in min_rows {
        combined.entry(r.user_id).or_insert((0, 0)).1 = r.minutes.max(0) as u64;
    }

    let n = combined.len().max(1) as f64;
    let mut sum_m = 0.0;
    let mut sum_c = 0.0;
    for &(m, c) in combined.values() {
        sum_m += m as f64;
        sum_c += c as f64;
    }
    let mean_m = sum_m / n;
    let mean_c = sum_c / n;

    let mut ss_m = 0.0;
    let mut ss_c = 0.0;
    for &(m, c) in combined.values() {
        ss_m += (m as f64 - mean_m).powi(2);
        ss_c += (c as f64 - mean_c).powi(2);
    }
    let std_m = (ss_m / n).sqrt();
    let std_c = (ss_c / n).sqrt();

    let mut scored: Vec<(String, u64, u64, f64)> = combined
        .into_iter()
        .map(|(uid, (m, c))| {
            let z_m = if std_m > 0.0 { (m as f64 - mean_m) / std_m } else { 0.0 };
            let z_c = if std_c > 0.0 { (c as f64 - mean_c) / std_c } else { 0.0 };
            (uid, m, c, z_m + z_c)
        })
        .collect();
    scored.sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap_or(std::cmp::Ordering::Equal));

    let shiptalkers: Vec<UserStats> = scored
        .into_iter()
        .take(20)
        .map(|(uid, m, c, _)| UserStats {
            user_id: uid,
            messages: m,
            coding_minutes: c,
        })
        .collect();

    Json(Stats {
        total_messages,
        active_users,
        channels_tracked,
        total_channels,
        coding_minutes,
        db_size_gib,
        leaderboard,
        shiptalkers,
    })
}
