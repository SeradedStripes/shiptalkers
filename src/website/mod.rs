use askama::Template;
use axum::{Router, extract::State, http::StatusCode, response::Html, routing::get};
use clickhouse::Client;
use std::collections::HashMap;
use tower_http::services::ServeDir;

#[derive(Clone)]
pub struct AppState {
    pub clickhouse: Client,
}

#[derive(Template)]
#[template(path = "stats.html")]
pub struct Stats {
    pub total_messages: String,
    pub active_users: String,
    pub channels_tracked: String,
    pub total_channels: String,
    pub coding_minutes: String,
    pub db_size_label: String,
    pub leaderboard: Vec<UserStats>,
    pub shiptalkers: Vec<UserStats>,
}

pub struct UserStats {
    pub user_id: String,
    pub messages: String,
    pub coding_minutes: String,
}

pub fn router(clickhouse: Client) -> Router {
    let state = AppState { clickhouse };

    Router::new()
        .route(
            "/",
            get(|| async { Html(include_str!("static/index.html")) }),
        )
        .route(
            "/link",
            get(|| async { Html(include_str!("static/link.html")) }),
        )
        .route("/stats", get(get_stats_page))
        .route(
            "/style.css",
            get(|| async {
                axum::response::Response::builder()
                    .header(axum::http::header::CONTENT_TYPE, "text/css")
                    .body(axum::body::Body::from(include_str!("static/style.css")))
                    .unwrap()
            }),
        )
        .fallback_service(ServeDir::new("static"))
        .with_state(state)
}

fn fmt_thousands(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

async fn get_stats_page(State(state): State<AppState>) -> Result<Html<String>, StatusCode> {
    let stats = load_stats(&state.clickhouse).await;
    let html = stats
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

async fn load_stats(ch: &Client) -> Stats {
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
    let db_size_label = format!(
        "{:.prec$} GiB",
        db_size_gib,
        prec = if db_size_gib < 1.0 { 5 } else { 2 }
    );

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
             LIMIT 20",
        )
        .fetch_all::<LeaderboardRow>()
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|r| UserStats {
                    user_id: r.user_id,
                    messages: fmt_thousands(r.messages),
                    coding_minutes: String::new(),
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
            let z_m = if std_m > 0.0 {
                (m as f64 - mean_m) / std_m
            } else {
                0.0
            };
            let z_c = if std_c > 0.0 {
                (c as f64 - mean_c) / std_c
            } else {
                0.0
            };
            (uid, m, c, z_m + z_c)
        })
        .collect();
    scored.sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap_or(std::cmp::Ordering::Equal));

    let shiptalkers: Vec<UserStats> = scored
        .into_iter()
        .take(20)
        .map(|(uid, m, c, _)| UserStats {
            user_id: uid,
            messages: fmt_thousands(m),
            coding_minutes: fmt_thousands(c),
        })
        .collect();

    Stats {
        total_messages: fmt_thousands(total_messages),
        active_users: fmt_thousands(active_users),
        channels_tracked: fmt_thousands(channels_tracked),
        total_channels: fmt_thousands(total_channels),
        coding_minutes: fmt_thousands(coding_minutes),
        db_size_label,
        leaderboard,
        shiptalkers,
    }
}
