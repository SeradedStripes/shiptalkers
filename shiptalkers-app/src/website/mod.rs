use crate::sqlx;
use crate::sqlx::PgPool;
use askama::Template;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Html;
use axum::routing::{delete, get, post};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

use crate::settings::RuntimeSettings;

const EXCLUDE_BOTS_DELETED: &str = "NOT EXISTS (SELECT 1 FROM slack_identities bi JOIN users bu ON bu.ship_talkers_id = bi.ship_talkers_id WHERE bi.internal_id = m.identity_id AND (bu.is_bot = 1 OR bu.is_deleted = 1))";
const EXCLUDE_BOTS_DELETED_SLACK_ID: &str =
    "slack_id NOT IN (SELECT user_id FROM users WHERE is_bot = 1 OR is_deleted = 1)";
const EXCLUDE_BOTS_DELETED_SCORE: &str =
    "user_id NOT IN (SELECT user_id FROM users WHERE is_bot = 1 OR is_deleted = 1)";

pub mod api;
pub mod auth;
mod boards;
mod channels;
mod main_page;
mod search;
mod stats;
mod users;

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
    highlight: bool,
}

#[derive(Clone)]
pub struct AppCache {
    stats: Arc<TtlCache<stats::StatsSnapshot>>,
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
pub struct AppState {
    pub pool: Option<PgPool>,
    pub http: reqwest::Client,
    pub auth_db: Option<std::sync::Arc<crate::db::postgres_db::AuthDb>>,
    pub cache: AppCache,
    pub settings: RuntimeSettings,
}

impl AppState {
    /// The shared Postgres pool, or 503 when the server was started without a database (DATABASE_URL unset) so static pages still work in dev.
    pub fn pool(&self) -> Result<&PgPool, StatusCode> {
        self.pool.as_ref().ok_or(StatusCode::SERVICE_UNAVAILABLE)
    }

    fn auth_db(&self) -> Result<&std::sync::Arc<crate::db::postgres_db::AuthDb>, StatusCode> {
        self.auth_db.as_ref().ok_or(StatusCode::SERVICE_UNAVAILABLE)
    }
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
    pub total_channels: String,
    pub archived_channels: String,
    pub total_users: String,
    pub hackatime_users: String,
    pub private_hackatime_users: String,
    pub no_hackatime_account_users: String,
    pub coding_hours: String,
    pub coding_time: String,
    pub slack_hours: String,
    pub slack_time: String,
    pub combined_time: String,
    pub db_size_label: String,
    pub signed_in: bool,
    pub page_load_ms: String,
}

#[derive(Template)]
#[template(path = "user.html")]
pub struct UserTemplate {
    pub merged_name: String,
    pub display_name: String,
    pub real_name: String,
    pub username: String,
    pub email: String,
    pub pfp: String,
    pub slack_id: String,
    pub shiptalkers_id: String,
    pub deactivated: bool,
    pub total_messages: String,
    pub coding_hours: String,
    pub channels: String,
    pub slack_time: String,
    pub top_channels: Vec<ChannelStats>,
    pub show_coding_prompt: bool,
    pub signed_in: bool,
    pub found: bool,
    pub page_load_ms: String,
}

pub struct ChannelStats {
    pub user_id: String,
    pub url_id: String,
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
#[template(path = "boards.html")]
pub struct BoardsTemplate {
    pub signed_in: bool,
    pub page_load_ms: String,
}

#[derive(Template)]
#[template(path = "docs/overview.html")]
pub struct ApiDocsOverview {
    pub signed_in: bool,
    pub page_load_ms: String,
    pub base_url: String,
    pub current: &'static str,
}

#[derive(Template)]
#[template(path = "docs/stats.html")]
pub struct ApiDocsStats {
    pub signed_in: bool,
    pub page_load_ms: String,
    pub base_url: String,
    pub current: &'static str,
}

#[derive(Template)]
#[template(path = "docs/channels.html")]
pub struct ApiDocsChannels {
    pub signed_in: bool,
    pub page_load_ms: String,
    pub base_url: String,
    pub current: &'static str,
}

#[derive(Template)]
#[template(path = "docs/daily_stats.html")]
pub struct ApiDocsDailyStats {
    pub signed_in: bool,
    pub page_load_ms: String,
    pub base_url: String,
    pub current: &'static str,
}

#[derive(Template)]
#[template(path = "docs/search.html")]
pub struct ApiDocsSearch {
    pub signed_in: bool,
    pub page_load_ms: String,
    pub base_url: String,
    pub current: &'static str,
}

#[derive(Template)]
#[template(path = "docs/account.html")]
pub struct ApiDocsAccount {
    pub signed_in: bool,
    pub page_load_ms: String,
    pub base_url: String,
    pub current: &'static str,
}

#[derive(Template)]
#[template(path = "docs/grants.html")]
pub struct ApiDocsGrants {
    pub signed_in: bool,
    pub page_load_ms: String,
    pub base_url: String,
    pub current: &'static str,
}

#[derive(Template)]
#[template(path = "board_category.html")]
pub struct BoardCategoryTemplate {
    pub title: String,
    pub entity: String,
    pub unit: String,
    pub extra_unit: Option<String>,
    pub rows: Vec<BoardEntry>,
    pub coming_soon: bool,
    pub category: String,
    pub query: String,
    pub notice: Option<String>,
    pub numbered: bool,
    pub show_pfp: bool,
    pub has_previous: bool,
    pub has_next: bool,
    pub page: u64,
    pub page_count: u64,
    pub directory_path: String,
    pub signed_in: bool,
    pub page_load_ms: String,
}

pub struct BoardEntry {
    pub user_id: String,
    pub url_id: String,
    pub merged_name: String,
    pub pfp: String,
    pub value: String,
    pub extra: String,
    pub linked: bool,
    pub rank: u64,
    pub label: String,
    pub highlight: bool,
}

pub struct SearchResult {
    pub merged_name: String,
    pub pfp: String,
    pub user_id: String,
    pub url_id: String,
    pub deactivated: bool,
}

pub struct UserStats {
    pub merged_name: String,
    pub pfp: String,
    pub user_id: String,
    pub url_id: String,
    pub messages: String,
}

pub fn router(
    pool: Option<PgPool>,
    settings: RuntimeSettings,
    auth_db: Option<std::sync::Arc<crate::db::postgres_db::AuthDb>>,
) -> Router {
    crate::init_tls();
    let state = AppState {
        pool,
        http: reqwest::Client::new(),
        auth_db,
        cache: AppCache::new(),
        settings,
    };

    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/", get(main_page::get_index))
        .route("/link", get(auth::get_link))
        .route("/stats", get(stats::get_stats_page))
        .route("/stats/{id}", get(stats::get_stats_for_id))
        .route("/boards", get(boards::get_boards))
        .route("/boards/users", get(users::get_users_board))
        .route("/boards/users/", get(users::get_users_board))
        .route("/boards/channels/", get(channels::get_channels_board))
        .route("/boards/{category}", get(boards::get_board_category))
        .route("/api/docs", get(main_page::get_api_docs))
        .route("/api/docs/{topic}", get(main_page::get_api_docs))
        .route("/api/v1/me", get(api::get_me))
        .route("/api/v1/stats", get(api::get_stats))
        .route("/api/v1/boards/{category}", get(api::get_boards))
        .route("/api/v1/users/{slack_id}", get(api::get_user))
        .route("/api/v1/channels/{channel_id}", get(api::get_channel))
        .route("/api/v1/daily-stats", get(api::get_daily_stats))
        .route("/api/v1/search", get(api::get_search))
        .route(
            "/api/v1/keys",
            get(api::list_api_keys).post(api::create_api_key),
        )
        .route("/api/v1/keys/{key_id}", delete(api::revoke_api_key))
        .route(
            "/api/v1/grants",
            get(api::list_grants).post(api::create_grant),
        )
        .route("/api/v1/grants/{id}", get(api::get_granted_stats))
        .route("/api/v1/grants/{id}", delete(api::revoke_grant))
        .route("/link/api-keys", post(auth::link_create_api_key))
        .route(
            "/link/api-keys/{key_id}/revoke",
            post(auth::link_revoke_api_key),
        )
        .route("/link/grants", post(auth::link_create_grant))
        .route("/link/consent", post(auth::link_grant_consent))
        .route("/link/consent/revoke", post(auth::link_revoke_consent))
        .route(
            "/link/grants/{key_id}/revoke",
            post(auth::link_revoke_grant),
        )
        .route("/search", get(search::get_search))
        .route("/pfp/{id}", get(main_page::get_pfp))
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

const RANK_WINDOW: u64 = 3;
const DIRECTORY_PAGE_SIZE: i64 = 100;

async fn directory_target(
    ch: &PgPool,
    table: &str,
    name_column: &str,
    id_column: &str,
    q: &str,
) -> Option<(u64, String)> {
    let numeric_rank = q.parse::<u64>().ok().filter(|rank| *rank > 0);
    if let Some(rank) = numeric_rank {
        return Some((rank, String::new()));
    }
    if q.is_empty() {
        return None;
    }
    let pattern = format!("%{}%", q);
    let target_sql = format!(
        "SELECT {id_column}, COALESCE(ship_talkers_id, {id_column}) \
         FROM {table} \
         WHERE {name_column} ILIKE $1 OR {id_column} ILIKE $1 OR COALESCE(ship_talkers_id, {id_column}) ILIKE $1 \
         ORDER BY ({name_column} ILIKE $1) DESC, COALESCE(ship_talkers_id, {id_column}), {id_column} \
         LIMIT 1"
    );
    let target: Option<(String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(target_sql))
        .bind(&pattern)
        .fetch_optional(ch)
        .await
        .ok()
        .flatten();
    let (id, ship_talkers_id) = target?;
    let rank_sql = format!(
        "SELECT count(*) + 1 FROM {table} \
         WHERE COALESCE(ship_talkers_id, {id_column}) < $1 \
            OR (COALESCE(ship_talkers_id, {id_column}) = $1 AND {id_column} < $2)"
    );
    let rank: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(rank_sql))
        .bind(&ship_talkers_id)
        .bind(&id)
        .fetch_one(ch)
        .await
        .ok()?;
    Some((rank.max(1) as u64, id))
}

fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

async fn resolve_id(ch: &PgPool, sql: &str) -> Option<String> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(sql.to_string()))
        .fetch_optional(ch)
        .await
        .ok()
        .flatten()
}

async fn fetch_rank_of(ch: &PgPool, inner: &str, id: &str) -> Option<u64> {
    let sql = format!("SELECT rank FROM ({inner}) ranked WHERE ranked.id = $1");
    let row: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.as_str()))
        .bind(id)
        .fetch_optional(ch)
        .await
        .ok()
        .flatten();
    row.map(|rank| rank.max(0) as u64)
}

async fn fetch_rank_window(ch: &PgPool, inner: &str, lo: u64, hi: u64) -> Vec<RankedRow> {
    let sql = format!(
        "SELECT id, value, extra, rank FROM ({inner}) ranked WHERE rank BETWEEN {lo} AND {hi} ORDER BY rank"
    );
    let rows: Vec<(String, i64, Option<i64>, i64)> =
        sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
            .fetch_all(ch)
            .await
            .unwrap_or_default();
    rows.into_iter()
        .map(|(id, value, extra, rank)| RankedRow {
            id,
            value,
            extra,
            rank: rank.max(0) as u64,
            highlight: false,
        })
        .collect()
}

/// Fetches a ranked window for a board. No query returns the top 100; a
/// numeric query jumps to that rank; anything else resolves an entity (user,
/// channel, word) and jumps to its rank. Returns the rows plus an optional
/// notice (e.g. no match found).
#[allow(dead_code)]
async fn ranked_window(
    ch: &PgPool,
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
        let mut rows = fetch_rank_window(ch, inner, lo, hi).await;
        if let Some(r) = rows.iter_mut().find(|r| r.rank == n) {
            r.highlight = true;
        }
        return (rows, None);
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
            let mut rows = fetch_rank_window(ch, inner, lo, hi).await;
            if let Some(r) = rows.iter_mut().find(|r| r.id == id) {
                r.highlight = true;
            }
            (rows, None)
        }
        None => (Vec::new(), Some(format!("'{}' is not on this board", q))),
    }
}

fn resolve_user_sql(inner: &str, q: &str) -> String {
    // LIKE is case-sensitive in PostgreSQL for lower() comparisons against a
    // lowercased query it still matches case-insensitively in effect. Only
    // users already on the board are candidates, so a similarly-named
    // user without scores can't shadow the one that's actually ranked. Ties
    // (duplicate display names) break by rank, so the highest-ranked match wins.
    let eq = sql_escape(&q.to_lowercase());
    format!(
        "SELECT u.user_id AS id FROM users AS u \
         JOIN ({inner}) lb ON u.user_id = lb.id \
         WHERE lower(u.merged_name) LIKE '%{eq}%' \
         ORDER BY (lower(u.merged_name) = '{eq}') DESC, lb.rank, lower(u.merged_name) \
         LIMIT 1"
    )
}

#[allow(dead_code)]
async fn legacy_board_category(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(category): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Html<String>, StatusCode> {
    let started = Instant::now();
    let ch = state.pool()?;
    let signed_in = signed_in(&state, &headers);
    let query = params.get("q").cloned().unwrap_or_default();
    let q = query.trim();
    let parsed_rank: Option<u64> = q.parse().ok();

    let (title, unit, extra_unit, coming_soon, rows, notice): (
        String,
        String,
        Option<String>,
        bool,
        Vec<BoardEntry>,
        Option<String>,
    ) = match category.as_str() {
        "talkers" => {
            let inner = format!(
                "SELECT user_id AS id, score AS value, messages::bigint AS extra, \
                 row_number() OVER (ORDER BY score DESC) AS rank \
                 FROM user_scores \
                 WHERE {EXCLUDE_BOTS_DELETED_SCORE}"
            );
            let (ranked, notice) = ranked_window(
                ch,
                &inner,
                q,
                parsed_rank,
                Some(&resolve_user_sql(&inner, q)),
            )
            .await;
            let rows = board_entries(
                ch,
                ranked,
                BoardSource::Users,
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
                "SELECT user_id AS id, value, CAST(NULL AS BIGINT) AS extra, rank \
                 FROM ( \
                     SELECT slack_id AS user_id, total_minutes::bigint AS value, \
                            row_number() OVER (ORDER BY total_minutes DESC) AS rank \
                     FROM hackatime_connections \
                     WHERE {EXCLUDE_BOTS_DELETED_SLACK_ID} \
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
            let rows = board_entries(ch, ranked, BoardSource::Users, fmt_minutes, None).await;
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
            let inner = "SELECT channel_id AS id, total_time::bigint AS value, \
                 messages::bigint AS extra, \
                 row_number() OVER (ORDER BY total_time DESC) AS rank \
                 FROM channel_scores";
            let eq = sql_escape(&q.to_lowercase());
            let resolve = format!(
                "SELECT c.channel_id AS id FROM slack_channels AS c FINAL \
                 JOIN ({inner}) lb ON c.channel_id = lb.id \
                 WHERE lower(c.name) LIKE '%{}%' \
                 ORDER BY (lower(c.name) = '{}') DESC, lb.rank, lower(c.name) \
                 LIMIT 1",
                eq, eq
            );
            let (ranked, notice) = ranked_window(ch, inner, q, parsed_rank, Some(&resolve)).await;
            let rows = board_entries(
                ch,
                ranked,
                BoardSource::Channels,
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
        "combined" => {
            // Slack Time seconds plus total Coding Time (minutes) converted to
            // seconds, summed per user and ranked. Bots/deleted users are
            // excluded before ranking so ranks stay gap-free.
            let inner = format!(
                "SELECT user_id AS id, value, CAST(NULL AS BIGINT) AS extra, rank \
                 FROM ( \
                     SELECT user_id, value, row_number() OVER (ORDER BY value DESC) AS rank \
                     FROM ( \
                         SELECT user_id, sum(v)::bigint AS value \
                         FROM ( \
                             SELECT user_id, total_time::bigint AS v \
                             FROM user_scores \
                             UNION ALL \
                             SELECT slack_id AS user_id, (total_minutes * 60)::bigint AS v \
                             FROM hackatime_connections \
                         ) \
                         GROUP BY user_id \
                     ) \
                      WHERE {EXCLUDE_BOTS_DELETED_SCORE} \
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
            let rows = board_entries(ch, ranked, BoardSource::Users, fmt_duration, None).await;
            (
                "Top Combined".into(),
                "Combined Time".into(),
                None,
                false,
                rows,
                notice,
            )
        }
        "words" => {
            let inner = "SELECT word AS id, cnt::bigint AS value, \
                 CAST(NULL AS BIGINT) AS extra, rank \
                 FROM ( \
                     SELECT word, cnt, \
                            row_number() OVER (ORDER BY cnt DESC) AS rank \
                     FROM word_totals \
                 )";
            let (ranked, notice) = if q.is_empty() {
                let cached = state
                    .cache
                    .words
                    .get_or(async { fetch_rank_window(ch, inner, 1, 100).await })
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
                ranked_window(ch, inner, q, parsed_rank, Some(&resolve)).await
            };
            let entries = ranked
                .into_iter()
                .map(|r| BoardEntry {
                    user_id: r.id.clone(),
                    url_id: r.id.clone(),
                    merged_name: r.id,
                    pfp: String::new(),
                    value: fmt_thousands(r.value.max(0) as u64),
                    extra: String::new(),
                    linked: false,
                    rank: r.rank,
                    label: r.rank.to_string(),
                    highlight: r.highlight,
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

    let template = BoardCategoryTemplate {
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
        numbered: true,
        show_pfp: true,
        has_previous: false,
        has_next: false,
        page: 1,
        page_count: 1,
        directory_path: String::new(),
        signed_in,
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

#[allow(dead_code)]
async fn legacy_users_board(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Html<String>, StatusCode> {
    let started = Instant::now();
    let ch = state.pool()?;
    let requested_page = params
        .get("page")
        .and_then(|page| page.parse::<u64>().ok())
        .filter(|page| *page > 0)
        .unwrap_or(1);
    let query = params.get("q").cloned().unwrap_or_default();
    let target = directory_target(ch, "users", "merged_name", "user_id", query.trim()).await;
    let page = target
        .as_ref()
        .map(|(rank, _)| rank.saturating_sub(1) / DIRECTORY_PAGE_SIZE as u64 + 1)
        .unwrap_or(requested_page);
    let search_mode = !query.trim().is_empty() && target.is_some();
    let row_offset = target
        .as_ref()
        .filter(|_| search_mode)
        .map(|(rank, _)| rank.saturating_sub(4))
        .unwrap_or(page.saturating_sub(1) * DIRECTORY_PAGE_SIZE as u64);
    let total: i64 = sqlx::query_scalar("SELECT count(*) FROM users")
        .fetch_one(ch)
        .await
        .unwrap_or(0);
    let page_count = ((total.max(0) as u64).saturating_add(DIRECTORY_PAGE_SIZE as u64 - 1)
        / DIRECTORY_PAGE_SIZE as u64)
        .max(1);
    let mut records: Vec<(String, String, String, String)> = sqlx::query_as(
        "SELECT user_id, COALESCE(ship_talkers_id, user_id), merged_name, pfp \
         FROM users \
         ORDER BY COALESCE(ship_talkers_id, user_id), user_id \
         LIMIT $1 OFFSET $2",
    )
    .bind(if search_mode {
        6
    } else {
        DIRECTORY_PAGE_SIZE + 1
    })
    .bind(row_offset.min(i64::MAX as u64) as i64)
    .fetch_all(ch)
    .await
    .unwrap_or_default();
    if !query.trim().is_empty() && target.is_none() {
        records.clear();
    }
    let has_next = !search_mode && records.len() > DIRECTORY_PAGE_SIZE as usize;
    records.truncate(if search_mode {
        6
    } else {
        DIRECTORY_PAGE_SIZE as usize
    });
    let rows: Vec<BoardEntry> = records
        .into_iter()
        .enumerate()
        .map(|(index, (user_id, ship_talkers_id, merged_name, pfp))| {
            let pfp = local_pfp(&ship_talkers_id, &pfp);
            let highlight = target.as_ref().is_some_and(|(_, id)| id == &user_id);
            BoardEntry {
                user_id,
                url_id: ship_talkers_id.clone(),
                merged_name: if merged_name.is_empty() {
                    ship_talkers_id.clone()
                } else {
                    merged_name
                },
                pfp,
                value: String::new(),
                extra: String::new(),
                linked: true,
                rank: row_offset + index as u64 + 1,
                label: ship_talkers_id,
                highlight,
            }
        })
        .collect();
    let template = BoardCategoryTemplate {
        title: "All Users".into(),
        entity: "User".into(),
        unit: String::new(),
        extra_unit: None,
        rows,
        coming_soon: false,
        category: "users".into(),
        query,
        notice: None,
        numbered: false,
        show_pfp: true,
        has_previous: page > 1,
        has_next,
        page,
        page_count,
        directory_path: "/boards/users/".into(),
        signed_in: signed_in(&state, &headers),
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

#[allow(dead_code)]
async fn legacy_channels_board(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Html<String>, StatusCode> {
    let started = Instant::now();
    let ch = state.pool()?;
    let requested_page = params
        .get("page")
        .and_then(|page| page.parse::<u64>().ok())
        .filter(|page| *page > 0)
        .unwrap_or(1);
    let query = params.get("q").cloned().unwrap_or_default();
    let target = directory_target(ch, "slack_channels", "name", "channel_id", query.trim()).await;
    let page = target
        .as_ref()
        .map(|(rank, _)| rank.saturating_sub(1) / DIRECTORY_PAGE_SIZE as u64 + 1)
        .unwrap_or(requested_page);
    let search_mode = !query.trim().is_empty() && target.is_some();
    let row_offset = target
        .as_ref()
        .filter(|_| search_mode)
        .map(|(rank, _)| rank.saturating_sub(4))
        .unwrap_or(page.saturating_sub(1) * DIRECTORY_PAGE_SIZE as u64);
    let total: i64 = sqlx::query_scalar("SELECT count(*) FROM slack_channels")
        .fetch_one(ch)
        .await
        .unwrap_or(0);
    let page_count = ((total.max(0) as u64).saturating_add(DIRECTORY_PAGE_SIZE as u64 - 1)
        / DIRECTORY_PAGE_SIZE as u64)
        .max(1);
    let mut records: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT channel_id, COALESCE(ship_talkers_id, channel_id), name \
         FROM slack_channels \
          ORDER BY COALESCE(ship_talkers_id, channel_id), channel_id \
         LIMIT $1 OFFSET $2",
    )
    .bind(if search_mode {
        6
    } else {
        DIRECTORY_PAGE_SIZE + 1
    })
    .bind(row_offset.min(i64::MAX as u64) as i64)
    .fetch_all(ch)
    .await
    .unwrap_or_default();
    if !query.trim().is_empty() && target.is_none() {
        records.clear();
    }
    let has_next = !search_mode && records.len() > DIRECTORY_PAGE_SIZE as usize;
    records.truncate(if search_mode {
        6
    } else {
        DIRECTORY_PAGE_SIZE as usize
    });
    let rows: Vec<BoardEntry> = records
        .into_iter()
        .enumerate()
        .map(|(index, (channel_id, ship_talkers_id, name))| {
            let highlight = target.as_ref().is_some_and(|(_, id)| id == &channel_id);
            BoardEntry {
                user_id: channel_id,
                url_id: ship_talkers_id.clone(),
                merged_name: if name.is_empty() {
                    ship_talkers_id.clone()
                } else {
                    name
                },
                pfp: String::new(),
                value: String::new(),
                extra: String::new(),
                linked: true,
                rank: row_offset + index as u64 + 1,
                label: ship_talkers_id,
                highlight,
            }
        })
        .collect();
    let template = BoardCategoryTemplate {
        title: "All Channels".into(),
        entity: "Channel".into(),
        unit: String::new(),
        extra_unit: None,
        rows,
        coming_soon: false,
        category: "channels".into(),
        query,
        notice: None,
        numbered: false,
        show_pfp: false,
        has_previous: page > 1,
        has_next,
        page,
        page_count,
        directory_path: "/boards/channels/".into(),
        signed_in: signed_in(&state, &headers),
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

#[allow(dead_code)]
enum BoardSource {
    Users,
    Channels,
}

#[allow(dead_code)]
async fn board_entries(
    ch: &PgPool,
    rows: Vec<RankedRow>,
    source: BoardSource,
    format_value: impl Fn(u64) -> String,
    format_extra: Option<fn(u64) -> String>,
) -> Vec<BoardEntry> {
    let mut name_ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
    name_ids.sort();
    name_ids.dedup();

    let names: std::collections::HashMap<String, (String, String, String)> = if name_ids.is_empty()
    {
        std::collections::HashMap::new()
    } else {
        match source {
            BoardSource::Users => sqlx::query_as::<_, (String, String, String, String)>(
                "SELECT user_id, merged_name, pfp, COALESCE(ship_talkers_id, user_id) FROM users WHERE user_id = ANY($1)",
            )
            .bind(&name_ids)
            .fetch_all(ch)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|(user_id, merged_name, pfp, ship_talkers_id)| {
                (user_id, (merged_name, pfp, ship_talkers_id))
            })
            .collect(),
            BoardSource::Channels => sqlx::query_as::<_, (String, String, String)>(
                "SELECT channel_id, name, COALESCE(ship_talkers_id, channel_id) FROM slack_channels WHERE channel_id = ANY($1)",
            )
            .bind(&name_ids)
            .fetch_all(ch)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|(channel_id, name, ship_talkers_id)| {
                (channel_id, (name, String::new(), ship_talkers_id))
            })
            .collect(),
        }
    };

    rows.into_iter()
        .map(|r| {
            let value = r.value.max(0) as u64;
            let (merged_name, pfp, url_id) = names.get(&r.id).cloned().unwrap_or_default();
            let pfp = local_pfp(&url_id, &pfp);
            BoardEntry {
                user_id: r.id.clone(),
                url_id: if url_id.is_empty() {
                    r.id.clone()
                } else {
                    url_id
                },
                merged_name: if merged_name.is_empty() {
                    r.id.clone()
                } else {
                    merged_name
                },
                pfp,
                value: format_value(value),
                extra: r
                    .extra
                    .map(|v| v.max(0) as u64)
                    .and_then(|v| format_extra.map(|f| f(v)))
                    .unwrap_or_default(),
                linked: true,
                rank: r.rank,
                label: r.rank.to_string(),
                highlight: r.highlight,
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

pub fn fmt_total_time(secs: u64) -> String {
    let minute = 60;
    let hour = 60 * minute;
    let day = 24 * hour;
    let month = 30 * day;
    let year = 365 * day;

    let years = secs / year;
    let months = (secs % year) / month;
    let days = ((secs % year) % month) / day;
    let hours = (secs % day) / hour;
    let minutes = (secs % hour) / minute;

    let mut parts = Vec::new();
    if years > 0 {
        parts.push(format!("{}y", years));
    }
    if months > 0 {
        parts.push(format!("{}mo", months));
    }
    if days > 0 {
        parts.push(format!("{}d", days));
    }
    if hours > 0 {
        parts.push(format!("{}h", hours));
    }
    if minutes > 0 {
        parts.push(format!("{}min", minutes));
    }
    if parts.is_empty() {
        return "0min".to_string();
    }
    parts.join(" ")
}

pub fn fmt_hour(hour: u8) -> String {
    let ampm = if hour < 12 { "AM" } else { "PM" };
    let mut hour = hour % 12;
    if hour == 0 {
        hour = 12;
    }
    format!("{} {}", hour, ampm)
}

pub use stats::parse_ts;
