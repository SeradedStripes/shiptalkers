use askama::Template;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::get;
use clickhouse::Client;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tower_http::services::ServeDir;

use crate::settings::RuntimeSettings;

const EXCLUDE_BOTS_DELETED: &str =
    "user_id NOT IN (SELECT user_id FROM users FINAL WHERE is_bot = 1 OR is_deleted = 1)";

pub mod auth;

struct TtlValue<T> {
    value: T,
    expires_at: Instant,
}

struct TtlCache<T> {
    inner: Mutex<Option<TtlValue<T>>>,
    ttl: Duration,
}

impl<T> TtlCache<T> {
    fn new(ttl: Duration) -> Self {
        Self {
            inner: Mutex::new(None),
            ttl,
        }
    }

    async fn get_or<F>(&self, compute: F) -> T
    where
        T: Clone,
        F: Future<Output = T>,
    {
        let mut guard = self.inner.lock().await;
        if let Some(cached) = guard.as_ref()
            && cached.expires_at > Instant::now()
        {
            return cached.value.clone();
        }
        let value = compute.await;
        *guard = Some(TtlValue {
            value: value.clone(),
            expires_at: Instant::now() + self.ttl,
        });
        value
    }
}

#[derive(Clone, Debug)]
struct RankedRow {
    id: String,
    value: i64,
    extra: Option<i64>,
    rank: u64,
}

#[derive(Clone)]
pub struct AppCache {
    stats: Arc<TtlCache<StatsSnapshot>>,
    words: Arc<TtlCache<Vec<RankedRow>>>,
}

impl AppCache {
    fn new() -> Self {
        Self {
            stats: Arc::new(TtlCache::new(Duration::from_secs(30))),
            words: Arc::new(TtlCache::new(Duration::from_secs(600))),
        }
    }
}

#[derive(Clone)]
struct StatsSnapshot {
    total_messages: u64,
    active_users: u64,
    channels_tracked: u64,
    total_channels: u64,
    total_users: u64,
    coding_minutes: u64,
    db_size_bytes: u64,
}

#[derive(Clone)]
pub struct AppState {
    pub clickhouse: Client,
    pub http: reqwest::Client,
    pub auth_db: std::sync::Arc<crate::db::sqlite::AuthDb>,
    pub cache: AppCache,
    pub settings: RuntimeSettings,
}

#[derive(Template)]
#[template(path = "index.html")]
pub struct IndexTemplate {
    pub signed_in: bool,
    pub page_load_ms: String,
}

#[derive(Template)]
#[template(path = "stats.html")]
pub struct Stats {
    pub total_messages: String,
    pub active_users: String,
    pub channels_tracked: String,
    pub total_channels: String,
    pub total_users: String,
    pub coding_hours: String,
    pub db_size_label: String,
    pub signed_in: bool,
    pub page_load_ms: String,
}

#[derive(Template)]
#[template(path = "user.html")]
pub struct UserTemplate {
    pub display_name: String,
    pub pfp: String,
    pub slack_id: String,
    pub total_messages: String,
    pub coding_hours: String,
    pub channels: String,
    pub slack_time_total: String,
    pub slack_time_avg: String,
    pub slack_time_longest: String,
    pub slack_time_per_day: String,
    pub leaderboard_rank: String,
    pub active_hour: String,
    pub top_channels: Vec<ChannelStats>,
    pub signed_in: bool,
    pub found: bool,
    pub page_load_ms: String,
}

pub struct ChannelStats {
    pub user_id: String,
    pub channel_name: String,
    pub messages: String,
}

#[derive(Template)]
#[template(path = "channel.html")]
pub struct ChannelTemplate {
    pub channel_name: String,
    pub channel_id: String,
    pub total_messages: String,
    pub active_users: String,
    pub first_msg: String,
    pub last_msg: String,
    pub top_posters: Vec<UserStats>,
    pub signed_in: bool,
    pub found: bool,
    pub page_load_ms: String,
}

#[derive(Template)]
#[template(path = "search.html")]
pub struct SearchTemplate {
    pub query: String,
    pub results: Vec<SearchResult>,
    pub channels: Vec<SearchResult>,
    pub signed_in: bool,
    pub page_load_ms: String,
}

#[derive(Template)]
#[template(path = "leaderboard.html")]
pub struct LeaderboardTemplate {
    pub signed_in: bool,
    pub page_load_ms: String,
}

#[derive(Template)]
#[template(path = "leaderboard_category.html")]
pub struct LeaderboardCategoryTemplate {
    pub title: String,
    pub entity: String,
    pub unit: String,
    pub extra_unit: Option<String>,
    pub rows: Vec<LeaderboardEntry>,
    pub coming_soon: bool,
    pub category: String,
    pub query: String,
    pub notice: Option<String>,
    pub signed_in: bool,
    pub page_load_ms: String,
}

pub struct LeaderboardEntry {
    pub user_id: String,
    pub display_name: String,
    pub pfp: String,
    pub value: String,
    pub extra: String,
    pub linked: bool,
    pub rank: u64,
}

pub struct SearchResult {
    pub display_name: String,
    pub pfp: String,
    pub user_id: String,
}

pub struct UserStats {
    pub display_name: String,
    pub pfp: String,
    pub user_id: String,
    pub messages: String,
}

pub fn router(
    clickhouse: Client,
    settings: RuntimeSettings,
    auth_db: std::sync::Arc<crate::db::sqlite::AuthDb>,
) -> Router {
    let state = AppState {
        clickhouse,
        http: reqwest::Client::new(),
        auth_db,
        cache: AppCache::new(),
        settings,
    };

    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/", get(get_index))
        .route("/link", get(auth::get_link))
        .route("/stats", get(get_stats_page))
        .route("/stats/{id}", get(get_stats_for_id))
        .route("/leaderboard", get(get_leaderboard))
        .route("/leaderboard/{category}", get(get_leaderboard_category))
        .route("/search", get(get_search))
        .route("/pfp/{id}", get(get_pfp))
        .route("/auth/hackclub/login", get(auth::auth_hackclub_login))
        .route("/auth/hackclub/callback", get(auth::auth_hackclub_callback))
        .route("/auth/hackatime/login", get(auth::auth_hackatime_login))
        .route(
            "/auth/hackatime/callback",
            get(auth::auth_hackatime_callback),
        )
        .route("/auth/logout", get(auth::auth_logout))
        .route(
            "/auth/hackatime/disconnect",
            get(auth::auth_hackatime_disconnect),
        )
        .route(
            "/style.css",
            get(|| async {
                axum::response::Response::builder()
                    .header(axum::http::header::CONTENT_TYPE, "text/css")
                    .body(axum::body::Body::from(include_str!("static/style.css")))
                    .unwrap()
            }),
        )
        .route(
            "/time.js",
            get(|| async {
                axum::response::Response::builder()
                    .header(axum::http::header::CONTENT_TYPE, "application/javascript")
                    .body(axum::body::Body::from(include_str!("static/time.js")))
                    .unwrap()
            }),
        )
        .fallback_service(ServeDir::new("static"))
        .with_state(state)
}

fn local_pfp(user_id: &str, pfp_url: &str) -> String {
    if pfp_url.is_empty() {
        String::new()
    } else {
        format!("/pfp/{}", user_id)
    }
}

fn signed_in(state: &AppState, headers: &HeaderMap) -> bool {
    auth::session_from_request(headers, &state.settings.auth_config()).is_some()
}

async fn get_pfp(State(state): State<AppState>, Path(user_id): Path<String>) -> Response {
    let url: String = state
        .clickhouse
        .query("SELECT pfp FROM users FINAL WHERE user_id = ?")
        .bind(&user_id)
        .fetch_one()
        .await
        .unwrap_or_default();
    if url.is_empty() {
        return StatusCode::NOT_FOUND.into_response();
    }
    Redirect::temporary(&url).into_response()
}

pub fn fmt_thousands(n: u64) -> String {
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

async fn get_index(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Html<String>, StatusCode> {
    let started = Instant::now();
    let signed_in = signed_in(&state, &headers);
    let template = IndexTemplate {
        signed_in,
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

async fn get_stats_page(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Html<String>, StatusCode> {
    let started = Instant::now();
    let stats = Stats {
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
        ..load_stats(&state, &headers).await
    };
    let html = stats
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

async fn get_search(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Html<String>, StatusCode> {
    let started = Instant::now();
    let signed_in = signed_in(&state, &headers);
    let query = params.get("q").cloned().unwrap_or_default();

    let results = if query.trim().is_empty() {
        Vec::new()
    } else {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct SearchRow {
            user_id: String,
            display_name: String,
            pfp: String,
            is_deleted: u8,
        }
        let pattern = format!("%{}%", query.trim());
        state
            .clickhouse
            .query(
                "SELECT user_id, display_name, pfp, is_deleted FROM users FINAL
                 WHERE display_name ILIKE ? OR user_id ILIKE ?
                 ORDER BY (display_name ILIKE ?) DESC, display_name
                 LIMIT 25",
            )
            .bind(&pattern)
            .bind(&pattern)
            .bind(&pattern)
            .fetch_all::<SearchRow>()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|r| SearchResult {
                display_name: if r.is_deleted == 1 {
                    "Deleted account".to_string()
                } else {
                    r.display_name
                },
                pfp: local_pfp(&r.user_id, &r.pfp),
                user_id: r.user_id,
            })
            .collect()
    };

    let channels = if query.trim().is_empty() {
        Vec::new()
    } else {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct ChannelRow {
            channel_id: String,
            name: String,
        }
        let pattern = format!("%{}%", query.trim());
        state
            .clickhouse
            .query(
                "SELECT channel_id, name FROM slack_channels FINAL
                 WHERE name ILIKE ?
                 ORDER BY name
                 LIMIT 25",
            )
            .bind(&pattern)
            .fetch_all::<ChannelRow>()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|r| SearchResult {
                display_name: r.name,
                pfp: String::new(),
                user_id: r.channel_id,
            })
            .collect()
    };

    let template = SearchTemplate {
        query,
        results,
        channels,
        signed_in,
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

async fn get_stats_for_id(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Html<String>, StatusCode> {
    if id.starts_with('C') {
        get_channel_stats(&state, &headers, &id).await
    } else {
        get_user_stats(&state, &headers, &id).await
    }
}

async fn get_leaderboard(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Html<String>, StatusCode> {
    let started = Instant::now();
    let signed_in = signed_in(&state, &headers);
    let template = LeaderboardTemplate {
        signed_in,
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

const RANK_WINDOW: u64 = 10;

fn sql_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

async fn resolve_id(ch: &Client, sql: &str) -> Option<String> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct IdRow {
        id: String,
    }
    ch.query(sql)
        .fetch_optional::<IdRow>()
        .await
        .ok()
        .flatten()
        .map(|r| r.id)
}

async fn fetch_rank_of(ch: &Client, inner: &str, id: &str) -> Option<u64> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct RankRow {
        rank: u64,
    }
    ch.query(&format!("SELECT rank FROM ({inner}) WHERE id = '{}'", id))
        .fetch_optional::<RankRow>()
        .await
        .ok()
        .flatten()
        .map(|r| r.rank)
}

async fn fetch_rank_window(ch: &Client, inner: &str, lo: u64, hi: u64) -> Vec<RankedRow> {
    #[derive(clickhouse::Row, serde::Deserialize)]
    struct Row {
        id: String,
        value: i64,
        extra: Option<i64>,
        rank: u64,
    }
    ch.query(&format!(
        "SELECT id, value, extra, rank FROM ({inner}) WHERE rank BETWEEN {lo} AND {hi} ORDER BY rank"
    ))
    .fetch_all::<Row>()
    .await
    .unwrap_or_default()
    .into_iter()
    .map(|r| RankedRow {
        id: r.id,
        value: r.value,
        extra: r.extra,
        rank: r.rank,
    })
    .collect()
}

/// Fetches a ranked window for a leaderboard. No query returns the top 100; a
/// numeric query jumps to that rank; anything else resolves an entity (user,
/// channel, word) and jumps to its rank. Returns the rows plus an optional
/// notice (e.g. no match found).
async fn ranked_window(
    ch: &Client,
    inner: &str,
    q: &str,
    parsed_rank: Option<u64>,
    resolve_sql: Option<&str>,
) -> (Vec<RankedRow>, Option<String>) {
    if q.is_empty() {
        return (fetch_rank_window(ch, inner, 1, 100).await, None);
    }
    if let Some(n) = parsed_rank
        && n >= 1
    {
        let lo = n.saturating_sub(RANK_WINDOW);
        let hi = n + RANK_WINDOW;
        return (fetch_rank_window(ch, inner, lo, hi).await, None);
    }
    let id = match resolve_sql {
        Some(sql) => resolve_id(ch, sql).await,
        None => None,
    };
    let id = match id {
        Some(id) => id,
        None => return (Vec::new(), Some(format!("No matches for '{}'", q))),
    };
    match fetch_rank_of(ch, inner, &id).await {
        Some(rank) => {
            let lo = rank.saturating_sub(RANK_WINDOW);
            let hi = rank + RANK_WINDOW;
            (fetch_rank_window(ch, inner, lo, hi).await, None)
        }
        None => (
            Vec::new(),
            Some(format!("'{}' is not on this leaderboard", q)),
        ),
    }
}

fn resolve_user_sql(inner: &str, q: &str) -> String {
    // LIKE is case-sensitive in ClickHouse, so lowercase the query to match the
    // lower(display_name) comparison case-insensitively. Only users already on
    // the leaderboard are candidates, so a similarly-named user without scores
    // can't shadow the one that's actually ranked.
    let eq = sql_escape(&q.to_lowercase());
    format!(
        "SELECT u.user_id AS id FROM users FINAL u \
         JOIN ({inner}) lb ON u.user_id = lb.id \
         WHERE lower(u.display_name) LIKE '%{}%' \
         ORDER BY (lower(u.display_name) = '{}') DESC, lower(u.display_name) \
         LIMIT 1",
        eq, eq
    )
}

async fn get_leaderboard_category(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(category): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Html<String>, StatusCode> {
    let started = Instant::now();
    let ch = &state.clickhouse;
    let signed_in = signed_in(&state, &headers);
    let query = params.get("q").cloned().unwrap_or_default();
    let q = query.trim();
    let parsed_rank: Option<u64> = q.parse().ok();

    let (title, unit, extra_unit, coming_soon, rows, notice): (
        String,
        String,
        Option<String>,
        bool,
        Vec<LeaderboardEntry>,
        Option<String>,
    ) = match category.as_str() {
        "talkers" => {
            let inner = format!(
                "SELECT user_id AS id, score AS value, toNullable(toInt64(messages)) AS extra, \
                 row_number() OVER (ORDER BY score DESC) AS rank \
                 FROM user_scores FINAL \
                 WHERE {EXCLUDE_BOTS_DELETED}"
            );
            let (ranked, notice) = ranked_window(
                ch,
                &inner,
                q,
                parsed_rank,
                Some(&resolve_user_sql(&inner, q)),
            )
            .await;
            let rows = leaderboard_entries(
                ch,
                ranked,
                LeaderboardSource::Users,
                fmt_duration,
                Some(fmt_thousands),
            )
            .await;
            (
                "Top Talkers".into(),
                "Slack Time".into(),
                Some("Messages".into()),
                false,
                rows,
                notice,
            )
        }
        "coders" => {
            let inner = format!(
                "SELECT user_id AS id, value, CAST(NULL AS Nullable(Int64)) AS extra, rank \
                 FROM ( \
                     SELECT user_id, sum(m) AS value, \
                            row_number() OVER (ORDER BY sum(m) DESC) AS rank \
                     FROM ( \
                         SELECT user_id, date, max(minutes) AS m \
                         FROM coding_activity \
                         GROUP BY user_id, date \
                     ) \
                     WHERE {EXCLUDE_BOTS_DELETED} \
                     GROUP BY user_id \
                 )"
            );
            let (ranked, notice) = ranked_window(
                ch,
                &inner,
                q,
                parsed_rank,
                Some(&resolve_user_sql(&inner, q)),
            )
            .await;
            let rows =
                leaderboard_entries(ch, ranked, LeaderboardSource::Users, fmt_minutes, None).await;
            (
                "Top Coders".into(),
                "Coding Time".into(),
                None,
                false,
                rows,
                notice,
            )
        }
        "channels" => {
            let inner = "SELECT channel_id AS id, toInt64(total_time) AS value, \
                 toNullable(toInt64(messages)) AS extra, \
                 row_number() OVER (ORDER BY total_time DESC) AS rank \
                 FROM channel_scores FINAL";
            let eq = sql_escape(&q.to_lowercase());
            let resolve = format!(
                "SELECT c.channel_id AS id FROM slack_channels FINAL c \
                 JOIN ({inner}) lb ON c.channel_id = lb.id \
                 WHERE lower(c.name) LIKE '%{}%' \
                 ORDER BY (lower(c.name) = '{}') DESC, lower(c.name) \
                 LIMIT 1",
                eq, eq
            );
            let (ranked, notice) = ranked_window(ch, inner, q, parsed_rank, Some(&resolve)).await;
            let rows = leaderboard_entries(
                ch,
                ranked,
                LeaderboardSource::Channels,
                fmt_duration,
                Some(fmt_thousands),
            )
            .await;
            (
                "Top Channels".into(),
                "Slack Time".into(),
                Some("Messages".into()),
                false,
                rows,
                notice,
            )
        }
        "combined" => (
            "Top Combined".into(),
            String::new(),
            None,
            true,
            Vec::new(),
            None,
        ),
        "words" => {
            let inner = format!(
                "SELECT word AS id, toInt64(cnt) AS value, \
                 CAST(NULL AS Nullable(Int64)) AS extra, rank \
                 FROM ( \
                     SELECT word, sum(count) AS cnt, \
                            row_number() OVER (ORDER BY sum(count) DESC) AS rank \
                     FROM word_counts FINAL \
                     WHERE {EXCLUDE_BOTS_DELETED} \
                     GROUP BY word \
                 )"
            );
            let (ranked, notice) = if q.is_empty() {
                let cached = state
                    .cache
                    .words
                    .get_or(async { fetch_rank_window(ch, &inner, 1, 100).await })
                    .await;
                (cached, None)
            } else {
                // Words are stored lowercase, so match the query case-insensitively.
                let eq = sql_escape(&q.to_lowercase());
                let resolve = format!(
                    "SELECT id FROM ({inner}) WHERE id = '{}' OR id LIKE '{}%' \
                     ORDER BY (id = '{}') DESC LIMIT 1",
                    eq, eq, eq
                );
                ranked_window(ch, &inner, q, parsed_rank, Some(&resolve)).await
            };
            let entries = ranked
                .into_iter()
                .map(|r| LeaderboardEntry {
                    user_id: r.id.clone(),
                    display_name: r.id,
                    pfp: String::new(),
                    value: fmt_thousands(r.value.max(0) as u64),
                    extra: String::new(),
                    linked: false,
                    rank: r.rank,
                })
                .collect();
            (
                "Top Words".into(),
                "Uses".into(),
                None,
                false,
                entries,
                notice,
            )
        }
        _ => {
            return Err(StatusCode::NOT_FOUND);
        }
    };

    let notice = notice.or_else(|| {
        if rows.is_empty() && !q.is_empty() {
            Some(format!("No results for '{}'", q))
        } else {
            None
        }
    });

    let template = LeaderboardCategoryTemplate {
        title,
        entity: match category.as_str() {
            "channels" => "Channel",
            "words" => "Word",
            _ => "User",
        }
        .into(),
        unit,
        extra_unit,
        rows,
        coming_soon,
        category,
        query,
        notice,
        signed_in,
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

enum LeaderboardSource {
    Users,
    Channels,
}

async fn leaderboard_entries(
    ch: &Client,
    rows: Vec<RankedRow>,
    source: LeaderboardSource,
    format_value: impl Fn(u64) -> String,
    format_extra: Option<fn(u64) -> String>,
) -> Vec<LeaderboardEntry> {
    let mut name_ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
    name_ids.sort();
    name_ids.dedup();

    let names: std::collections::HashMap<String, (String, String)> = if name_ids.is_empty() {
        std::collections::HashMap::new()
    } else {
        let in_list = name_ids.join("', '");
        match source {
            LeaderboardSource::Users => {
                #[derive(clickhouse::Row, serde::Deserialize)]
                struct NameRow {
                    user_id: String,
                    display_name: String,
                    pfp: String,
                }

                ch.query(&format!(
                    "SELECT user_id, display_name, pfp FROM users FINAL WHERE user_id IN ('{}')",
                    in_list
                ))
                .fetch_all::<NameRow>()
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|r| (r.user_id, (r.display_name, r.pfp)))
                .collect()
            }
            LeaderboardSource::Channels => {
                #[derive(clickhouse::Row, serde::Deserialize)]
                struct NameRow {
                    channel_id: String,
                    name: String,
                }

                ch.query(&format!(
                    "SELECT channel_id, name FROM slack_channels FINAL WHERE channel_id IN ('{}')",
                    in_list
                ))
                .fetch_all::<NameRow>()
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|r| (r.channel_id, (r.name, String::new())))
                .collect()
            }
        }
    };

    rows.into_iter()
        .map(|r| {
            let value = r.value.max(0) as u64;
            let (display_name, pfp) = names.get(&r.id).cloned().unwrap_or_default();
            LeaderboardEntry {
                user_id: r.id.clone(),
                display_name: if display_name.is_empty() {
                    r.id.clone()
                } else {
                    display_name
                },
                pfp: local_pfp(&r.id, &pfp),
                value: format_value(value),
                extra: r
                    .extra
                    .map(|v| v.max(0) as u64)
                    .and_then(|v| format_extra.map(|f| f(v)))
                    .unwrap_or_default(),
                linked: true,
                rank: r.rank,
            }
        })
        .collect()
}

pub fn fmt_duration(secs: u64) -> String {
    let hours = secs / 3600;
    let mins = (secs % 3600) / 60;
    if hours > 0 {
        format!("{}h {}m", hours, mins)
    } else {
        format!("{}m", mins)
    }
}

pub fn fmt_minutes(minutes: u64) -> String {
    format!("{}hrs {}min", minutes / 60, minutes % 60)
}

pub fn fmt_hour(hour: u8) -> String {
    let ampm = if hour < 12 { "AM" } else { "PM" };
    let mut hour = hour % 12;
    if hour == 0 {
        hour = 12;
    }
    format!("{} {}", hour, ampm)
}

async fn get_user_stats(
    state: &AppState,
    headers: &HeaderMap,
    slack_id: &str,
) -> Result<Html<String>, StatusCode> {
    let started = Instant::now();
    let ch = &state.clickhouse;
    let signed_in = signed_in(state, headers);

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct UserInfoRow {
        display_name: String,
        pfp: String,
        is_bot: u8,
        is_deleted: u8,
    }
    let info: Option<UserInfoRow> = ch
        .query("SELECT display_name, pfp, is_bot, is_deleted FROM users FINAL WHERE user_id = ?")
        .bind(slack_id)
        .fetch_optional()
        .await
        .unwrap_or(None);
    let is_bot = info.as_ref().map(|i| i.is_bot == 1).unwrap_or(false);
    let is_deleted = info.as_ref().map(|i| i.is_deleted == 1).unwrap_or(false);
    let display_name = info
        .as_ref()
        .map(|i| i.display_name.clone())
        .unwrap_or_default();
    let pfp_url = info.as_ref().map(|i| i.pfp.clone()).unwrap_or_default();
    let pfp = local_pfp(slack_id, &pfp_url);

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ScoreRow {
        score: i64,
        total_time: u64,
        messages: u64,
        sessions: u64,
        longest: u64,
        days: u64,
        channels: u64,
        active_hour: u8,
    }

    let scores: Option<ScoreRow> = ch
        .query(
            "SELECT score, total_time, messages, sessions, longest,
                    days, channels, active_hour
             FROM user_scores FINAL WHERE user_id = ?",
        )
        .bind(slack_id)
        .fetch_optional()
        .await
        .unwrap_or(None);

    let coding_minutes: i64 = ch
        .query(
            "SELECT sum(minutes) FROM (
                 SELECT max(minutes) AS minutes
                 FROM coding_activity
                 WHERE user_id = ?
                 GROUP BY date
             )",
        )
        .bind(slack_id)
        .fetch_one()
        .await
        .unwrap_or(0);

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct ChannelCount {
        channel_id: String,
        messages: u64,
    }

    let counts: Vec<ChannelCount> = ch
        .query(
            "SELECT channel_id, count() as messages
             FROM slack_messages_by_user
             WHERE user_id = ?
             GROUP BY channel_id
             ORDER BY messages DESC
             LIMIT 10",
        )
        .bind(slack_id)
        .fetch_all()
        .await
        .unwrap_or_default();

    let channel_names: std::collections::HashMap<String, String> = if counts.is_empty() {
        std::collections::HashMap::new()
    } else {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct ChannelName {
            channel_id: String,
            name: String,
        }
        let placeholders = counts.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let mut query = ch.query(&format!(
            "SELECT channel_id, name FROM slack_channels FINAL WHERE channel_id IN ({})",
            placeholders
        ));
        for c in &counts {
            query = query.bind(&c.channel_id);
        }
        query
            .fetch_all::<ChannelName>()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|r| (r.channel_id, r.name))
            .collect()
    };

    let top_channels: Vec<ChannelStats> = counts
        .into_iter()
        .map(|c| ChannelStats {
            user_id: c.channel_id.clone(),
            channel_name: channel_names
                .get(&c.channel_id)
                .cloned()
                .unwrap_or_else(|| c.channel_id.clone()),
            messages: fmt_thousands(c.messages),
        })
        .collect();

    let total_messages = scores.as_ref().map(|s| s.messages).unwrap_or(0);
    let found = total_messages > 0 || coding_minutes > 0 || !display_name.is_empty();

    let (slack_time_total, slack_time_avg, slack_time_longest, slack_time_per_day, active_hour) =
        match scores.as_ref() {
            Some(s) if s.messages > 0 => {
                let total = s.total_time;
                let avg_session = total.checked_div(s.sessions).unwrap_or(0);
                let per_day = s.sessions as f64 / s.days.max(1) as f64;
                (
                    fmt_duration(total),
                    fmt_duration(avg_session),
                    fmt_duration(s.longest),
                    format!("{:.1} / day", per_day),
                    fmt_hour(s.active_hour),
                )
            }
            _ => (
                "0m".into(),
                "0m".into(),
                "0m".into(),
                "0 / day".into(),
                String::new(),
            ),
        };

    let leaderboard_rank: String = {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct RankRow {
            rank: u64,
        }

        if is_bot || is_deleted {
            String::new()
        } else {
            match scores.as_ref() {
                Some(s) => ch
                    .query(&format!(
                        "SELECT count() AS rank
                         FROM (
                             SELECT user_id FROM user_scores FINAL
                             WHERE {EXCLUDE_BOTS_DELETED} AND score > ?
                         )"
                    ))
                    .bind(s.score)
                    .fetch_one::<RankRow>()
                    .await
                    .map(|r| format!("#{}", fmt_thousands(r.rank + 1)))
                    .unwrap_or_default(),
                None => String::new(),
            }
        }
    };

    let template = UserTemplate {
        display_name: if is_deleted {
            "Deleted account".to_string()
        } else if display_name.is_empty() {
            slack_id.to_string()
        } else {
            display_name
        },
        pfp,
        slack_id: slack_id.to_string(),
        total_messages: fmt_thousands(total_messages),
        coding_hours: fmt_minutes(coding_minutes.max(0) as u64),
        channels: fmt_thousands(scores.as_ref().map(|s| s.channels).unwrap_or(0)),
        slack_time_total,
        slack_time_avg,
        slack_time_longest,
        slack_time_per_day,
        leaderboard_rank,
        active_hour,
        top_channels,
        signed_in,
        found,
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

async fn get_channel_stats(
    state: &AppState,
    headers: &HeaderMap,
    channel_id: &str,
) -> Result<Html<String>, StatusCode> {
    let started = Instant::now();
    let ch = &state.clickhouse;
    let signed_in = signed_in(state, headers);

    let channel_name: String = ch
        .query("SELECT name FROM slack_channels FINAL WHERE channel_id = ?")
        .bind(channel_id)
        .fetch_one()
        .await
        .unwrap_or_default();

    let total_messages: u64 = ch
        .query("SELECT count() FROM slack_messages WHERE channel_id = ?")
        .bind(channel_id)
        .fetch_one()
        .await
        .unwrap_or(0);

    let active_users: u64 = ch
        .query(&format!(
            "SELECT uniqExact(user_id) FROM slack_messages
             WHERE channel_id = ? AND {EXCLUDE_BOTS_DELETED}"
        ))
        .bind(channel_id)
        .fetch_one()
        .await
        .unwrap_or(0);

    let last_ts: u64 = ch
        .query("SELECT max(message_ts) FROM slack_messages WHERE channel_id = ?")
        .bind(channel_id)
        .fetch_one()
        .await
        .unwrap_or(0);

    let first_ts: u64 = ch
        .query("SELECT min(message_ts) FROM slack_messages WHERE channel_id = ?")
        .bind(channel_id)
        .fetch_one()
        .await
        .unwrap_or(0);

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct PosterRow {
        user_id: String,
        messages: u64,
    }

    let posters: Vec<PosterRow> = ch
        .query(&format!(
            "SELECT user_id, count() as messages
             FROM slack_messages
             WHERE channel_id = ? AND {EXCLUDE_BOTS_DELETED}
             GROUP BY user_id
             ORDER BY messages DESC
             LIMIT 10"
        ))
        .bind(channel_id)
        .fetch_all()
        .await
        .unwrap_or_default();

    let name_ids: Vec<String> = posters.iter().map(|p| p.user_id.clone()).collect();

    #[derive(clickhouse::Row, serde::Deserialize)]
    struct PosterNameRow {
        user_id: String,
        display_name: String,
        pfp: String,
        is_deleted: u8,
    }

    let poster_names: std::collections::HashMap<String, (String, String)> = if name_ids.is_empty() {
        std::collections::HashMap::new()
    } else {
        let in_list = name_ids.join("', '");
        ch.query(&format!(
            "SELECT user_id, display_name, pfp, is_deleted FROM users FINAL WHERE user_id IN ('{}')",
            in_list
        ))
        .fetch_all::<PosterNameRow>()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|r| {
            let label = if r.is_deleted == 1 {
                "Deleted account".to_string()
            } else {
                r.display_name
            };
            (r.user_id, (label, r.pfp))
        })
        .collect()
    };

    let top_posters: Vec<UserStats> = posters
        .into_iter()
        .map(|p| {
            let (display_name, pfp) = poster_names.get(&p.user_id).cloned().unwrap_or_default();
            UserStats {
                user_id: p.user_id.clone(),
                display_name: if display_name.is_empty() {
                    p.user_id.clone()
                } else {
                    display_name
                },
                pfp: local_pfp(&p.user_id, &pfp),
                messages: fmt_thousands(p.messages),
            }
        })
        .collect();

    let found = total_messages > 0 || !channel_name.is_empty();

    let template = ChannelTemplate {
        channel_name: if channel_name.is_empty() {
            channel_id.to_string()
        } else {
            channel_name
        },
        channel_id: channel_id.to_string(),
        total_messages: fmt_thousands(total_messages),
        active_users: fmt_thousands(active_users),
        first_msg: fmt_ts_local(first_ts),
        last_msg: fmt_ts_local(last_ts),
        top_posters,
        signed_in,
        found,
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

async fn load_stats(state: &AppState, headers: &HeaderMap) -> Stats {
    let snapshot = state
        .cache
        .stats
        .get_or(async { compute_stats(state).await })
        .await;

    let db_size_gib = snapshot.db_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
    let db_size_label = format!(
        "{:.prec$} GiB",
        db_size_gib,
        prec = if db_size_gib < 10.0 { 5 } else { 3 }
    );

    Stats {
        total_messages: fmt_thousands(snapshot.total_messages),
        active_users: fmt_thousands(snapshot.active_users),
        channels_tracked: fmt_thousands(snapshot.channels_tracked),
        total_channels: fmt_thousands(snapshot.total_channels),
        total_users: fmt_thousands(snapshot.total_users),
        coding_hours: fmt_minutes(snapshot.coding_minutes),
        db_size_label,
        signed_in: signed_in(state, headers),
        page_load_ms: String::new(),
    }
}

async fn compute_stats(state: &AppState) -> StatsSnapshot {
    let ch = &state.clickhouse;
    let total_messages: u64 = ch
        .query("SELECT count() FROM slack_messages")
        .fetch_one()
        .await
        .unwrap_or(0);

    let active_users: u64 = ch
        .query(&format!(
            "SELECT count() FROM user_scores FINAL WHERE {EXCLUDE_BOTS_DELETED}"
        ))
        .fetch_one()
        .await
        .unwrap_or(0);

    let channels_tracked: u64 = ch
        .query("SELECT count() FROM scraped_channels FINAL")
        .fetch_one()
        .await
        .unwrap_or(0);

    let total_channels: u64 = ch
        .query("SELECT count() FROM slack_channels FINAL")
        .fetch_one()
        .await
        .unwrap_or(0);

    let total_users: u64 = ch
        .query("SELECT count() FROM users FINAL WHERE is_bot = 0 AND is_deleted = 0")
        .fetch_one()
        .await
        .unwrap_or(0);

    let coding_minutes: u64 = ch
        .query(
            "SELECT sum(toUInt64(minutes)) FROM (
                 SELECT max(minutes) AS minutes
                 FROM coding_activity
                 GROUP BY user_id, date
             )",
        )
        .fetch_one()
        .await
        .unwrap_or(0);

    let db_size_bytes: u64 = ch
        .query("SELECT sum(bytes_on_disk) as bytes FROM system.parts WHERE database = currentDatabase() AND active")
        .fetch_one()
        .await
        .unwrap_or(0);

    StatsSnapshot {
        total_messages,
        active_users,
        channels_tracked,
        total_channels,
        total_users,
        coding_minutes,
        db_size_bytes,
    }
}

pub fn parse_ts(micros: u64) -> Option<(u32, u32, u32, u32, u32)> {
    let secs = micros / 1_000_000;
    if secs == 0 {
        return None;
    }
    let (year, month, day) = crate::auth::civil_from_days((secs / 86400) as i64);
    let hour = ((secs % 86400) / 3600) as u32;
    let minute = ((secs % 3600) / 60) as u32;
    Some((year, month, day, hour, minute))
}

fn fmt_ts_local(micros: u64) -> String {
    match parse_ts(micros) {
        Some((year, month, day, hour, minute)) => format!(
            "<time datetime=\"{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:00Z\">\
             {year:04}-{month:02}-{day:02} {hour:02}:{minute:02} UTC</time>"
        ),
        None => String::new(),
    }
}
