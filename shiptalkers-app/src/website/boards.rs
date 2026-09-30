use super::{
    AppState, BlacklistedChannelRow, BlacklistedChannelsTemplate, BlacklistedUserRow,
    BlacklistedUsersTemplate, BoardCategoryTemplate, BoardEntry, BoardsTemplate,
    EXCLUDE_BOTS_DELETED_SCORE, EXCLUDE_BOTS_DELETED_SLACK_ID, LinkedBoardRow,
    LinkedBoardsTemplate, PgPool, PrivateChannelRow, PrivateChannelToken, PrivateChannelsTemplate,
    RankedRow, State, StatusCode, fmt_duration, fmt_minutes, fmt_thousands, signed_in, sql_escape,
};
use askama::Template;
use axum::extract::{Form, Path, Query};
use axum::http::HeaderMap;
use axum::response::{Html, Redirect};
use std::collections::HashMap;
use std::time::Instant;

type LinkedBoardRecord = (
    String,
    bool,
    Option<String>,
    bool,
    Option<String>,
    Option<i64>,
);

pub(super) async fn get_boards(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Html<String>, StatusCode> {
    let started = Instant::now();
    let template = BoardsTemplate {
        signed_in: signed_in(&state, &headers),
        is_admin: super::shiptalkers_admin_signed_in(&state, &headers),
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

pub(super) async fn get_linked_boards(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Html<String>, StatusCode> {
    if !super::shiptalkers_admin_signed_in(&state, &headers) {
        return Err(StatusCode::FORBIDDEN);
    }
    let started = Instant::now();
    let pool = state.pool()?;
    let filter = match params.get("filter").map(String::as_str) {
        Some("slack") => "slack",
        Some("hackatime") => "hackatime",
        _ => "all",
    }
    .to_string();
    let query = params.get("q").cloned().unwrap_or_default();
    let pattern = format!("%{}%", query.trim());
    let requested_page = params
        .get("page")
        .and_then(|page| page.parse::<u64>().ok())
        .filter(|page| *page > 0)
        .unwrap_or(1);
    let total: i64 = super::sqlx::query_scalar(
        "SELECT count(*) FROM users u
         LEFT JOIN hackatime_connections h ON h.slack_id = u.user_id
           LEFT JOIN slack_oauth_tokens s ON s.slack_id = u.user_id AND s.disabled_at IS NULL
           WHERE (($1 = 'all' AND (h.access_token != '' OR s.slack_id IS NOT NULL))
              OR ($1 = 'slack' AND s.slack_id IS NOT NULL)
            OR ($1 = 'hackatime' AND h.access_token != ''))
           AND (COALESCE(u.ship_talkers_id, u.user_id) ILIKE $2
                OR u.user_id ILIKE $2 OR COALESCE(u.merged_name, '') ILIKE $2)",
    )
    .bind(&filter)
    .bind(&pattern)
    .fetch_one(pool)
    .await
    .unwrap_or(0);
    let page_count = (total.max(0) as u64)
        .div_ceil(super::DIRECTORY_PAGE_SIZE as u64)
        .max(1);
    let page = requested_page.min(page_count);
    let records: Vec<LinkedBoardRecord> = super::sqlx::query_as(
        "SELECT COALESCE(u.ship_talkers_id, u.user_id),
                COALESCE(h.access_token != '', false),
                to_char(h.connected_at AT TIME ZONE 'UTC', 'YYYY-MM-DD'),
                s.slack_id IS NOT NULL,
                to_char(s.connected_at AT TIME ZONE 'UTC', 'YYYY-MM-DD'),
                 s.token_no
         FROM users u
         LEFT JOIN hackatime_connections h ON h.slack_id = u.user_id
         LEFT JOIN (
             SELECT slack_id, connected_at,
                    row_number() OVER (ORDER BY slack_id) - 1 AS token_no
             FROM slack_oauth_tokens
             WHERE disabled_at IS NULL
          ) s ON s.slack_id = u.user_id
           WHERE (($1 = 'all' AND (h.access_token != '' OR s.slack_id IS NOT NULL))
              OR ($1 = 'slack' AND s.slack_id IS NOT NULL)
            OR ($1 = 'hackatime' AND h.access_token != ''))
         AND (COALESCE(u.ship_talkers_id, u.user_id) ILIKE $2
              OR u.user_id ILIKE $2 OR COALESCE(u.merged_name, '') ILIKE $2)
         GROUP BY COALESCE(u.ship_talkers_id, u.user_id), u.user_id,
                    h.access_token, h.connected_at, s.slack_id, s.connected_at, s.token_no
         ORDER BY COALESCE(u.ship_talkers_id, u.user_id), u.user_id
         LIMIT $3 OFFSET $4",
    )
    .bind(&filter)
    .bind(&pattern)
    .bind(super::DIRECTORY_PAGE_SIZE)
    .bind((page - 1) as i64 * super::DIRECTORY_PAGE_SIZE)
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    let rows = records
        .into_iter()
        .enumerate()
        .map(
            |(
                index,
                (shiptalkers_id, hackatime, hackatime_date, slack, slack_date, slack_token_no),
            )| {
                LinkedBoardRow {
                    rank: (page - 1) * super::DIRECTORY_PAGE_SIZE as u64 + index as u64 + 1,
                    shiptalkers_id,
                    hackatime,
                    hackatime_date: if hackatime {
                        hackatime_date.unwrap_or_else(|| "-".into())
                    } else {
                        "-".into()
                    },
                    slack,
                    slack_date: if slack {
                        slack_date.unwrap_or_else(|| "-".into())
                    } else {
                        "-".into()
                    },
                    slack_token_no: if slack {
                        slack_token_no
                            .map(|token_no| token_no.to_string())
                            .unwrap_or_else(|| "-".into())
                    } else {
                        "-".into()
                    },
                }
            },
        )
        .collect();
    let template = LinkedBoardsTemplate {
        rows,
        filter,
        query,
        has_previous: page > 1,
        has_next: page < page_count,
        page,
        page_count,
        signed_in: signed_in(&state, &headers),
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

pub(super) async fn get_blacklisted_channels(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Html<String>, StatusCode> {
    if !super::shiptalkers_admin_signed_in(&state, &headers) {
        return Err(StatusCode::FORBIDDEN);
    }
    let started = Instant::now();
    let pool = state.pool()?;
    let rows: Vec<BlacklistedChannelRow> = super::sqlx::query_as::<_, (String, String, String)>(
        "SELECT ship_talkers_id, slack_channel_id, channel_name
         FROM blacklisted_channels ORDER BY slack_channel_id",
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default()
    .into_iter()
    .map(
        |(ship_talkers_id, slack_channel_id, channel_name)| BlacklistedChannelRow {
            ship_talkers_id,
            slack_channel_id,
            channel_name,
        },
    )
    .collect();
    let template = BlacklistedChannelsTemplate {
        rows,
        signed_in: signed_in(&state, &headers),
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    Ok(Html(
        template
            .render()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    ))
}

pub(super) async fn get_blacklisted_users(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Html<String>, StatusCode> {
    if !super::shiptalkers_admin_signed_in(&state, &headers) {
        return Err(StatusCode::FORBIDDEN);
    }
    let started = Instant::now();
    let rows = state
        .auth_db()?
        .list_blacklisted_slack_users()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .map(|(slack_user_id, name, consent_status)| BlacklistedUserRow {
            slack_user_id,
            name,
            blacklisted: true,
            consent_status,
        })
        .collect();
    let csrf_token =
        super::auth::csrf_token_for(&headers, &state.settings.auth_config()).unwrap_or_default();
    let template = BlacklistedUsersTemplate {
        rows,
        csrf_token,
        signed_in: true,
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    Ok(Html(
        template
            .render()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    ))
}

async fn check_blacklist_form(
    state: &AppState,
    headers: &HeaderMap,
    csrf: Option<&str>,
) -> Result<(), StatusCode> {
    if !super::shiptalkers_admin_signed_in(state, headers) {
        return Err(StatusCode::FORBIDDEN);
    }
    if !super::auth::csrf_matches(headers, &state.settings.auth_config(), csrf) {
        return Err(StatusCode::FORBIDDEN);
    }
    Ok(())
}

pub(super) async fn add_blacklisted_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(params): Form<HashMap<String, String>>,
) -> Result<Redirect, StatusCode> {
    check_blacklist_form(&state, &headers, params.get("csrf").map(String::as_str)).await?;
    let slack_id = params
        .get("slack_user_id")
        .map(|id| id.trim())
        .filter(|id| !id.is_empty())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let disable_consent = params.contains_key("disable_consent");
    state
        .auth_db()?
        .blacklist_slack_user(slack_id, disable_consent)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Redirect::to("/boards/blacklisted-users"))
}

pub(super) async fn remove_blacklisted_user(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(slack_id): Path<String>,
    Form(params): Form<HashMap<String, String>>,
) -> Result<Redirect, StatusCode> {
    check_blacklist_form(&state, &headers, params.get("csrf").map(String::as_str)).await?;
    state
        .auth_db()?
        .unblacklist_slack_user(&slack_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Redirect::to("/boards/blacklisted-users"))
}

pub(super) async fn get_private_channels_board(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Html<String>, StatusCode> {
    if !super::shiptalkers_admin_signed_in(&state, &headers) {
        return Err(StatusCode::FORBIDDEN);
    }
    let started = Instant::now();
    let pool = state.pool()?;
    let query = params.get("q").cloned().unwrap_or_default();
    let token_filter = params.get("token").cloned().unwrap_or_default();
    let token_rows: Vec<(String, i64)> = super::sqlx::query_as(
        "SELECT slack_id, row_number() OVER (ORDER BY slack_id) - 1
         FROM slack_oauth_tokens WHERE disabled_at IS NULL ORDER BY slack_id",
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    let token_key = if token_filter.is_empty() {
        String::new()
    } else {
        token_filter
            .parse::<i64>()
            .ok()
            .and_then(|id| {
                token_rows
                    .iter()
                    .find(|(_, token_no)| *token_no == id)
                    .map(|(slack_id, _)| slack_id.clone())
            })
            .unwrap_or_else(|| "__invalid_token__".into())
    };
    let pattern = format!("%{}%", query.trim());
    let requested_page = params
        .get("page")
        .and_then(|page| page.parse::<u64>().ok())
        .filter(|page| *page > 0)
        .unwrap_or(1);
    let total: i64 = super::sqlx::query_scalar(
        "WITH token_numbers AS (
             SELECT slack_id, row_number() OVER (ORDER BY slack_id) - 1 AS token_no
             FROM slack_oauth_tokens WHERE disabled_at IS NULL
         )
         SELECT count(*) FROM slack_channels c
         WHERE c.is_private = 1 AND (c.name ILIKE $1 OR c.channel_id ILIKE $1)
           AND ($2 = '' OR EXISTS (
               SELECT 1 FROM slack_oauth_channel_access a
               JOIN token_numbers t ON t.slack_id = a.slack_id
               WHERE a.channel_id = c.channel_id AND t.slack_id = $2
           ))",
    )
    .bind(&pattern)
    .bind(&token_key)
    .fetch_one(pool)
    .await
    .unwrap_or(0);
    let page_count = (total.max(0) as u64)
        .div_ceil(super::DIRECTORY_PAGE_SIZE as u64)
        .max(1);
    let page = requested_page.min(page_count);
    let records: Vec<(String, String, String, Vec<String>)> = super::sqlx::query_as(
        "WITH token_numbers AS (
             SELECT slack_id, row_number() OVER (ORDER BY slack_id) - 1 AS token_no
             FROM slack_oauth_tokens WHERE disabled_at IS NULL
         )
         SELECT c.name, COALESCE(c.ship_talkers_id, c.channel_id), c.channel_id,
                COALESCE(array_agg(
                    t.token_no::text || ':' || COALESCE(u.ship_talkers_id, u.user_id)
                    ORDER BY t.token_no
                ) FILTER (WHERE t.slack_id IS NOT NULL), ARRAY[]::text[])
         FROM slack_channels c
         LEFT JOIN slack_oauth_channel_access a ON a.channel_id = c.channel_id
         LEFT JOIN token_numbers t ON t.slack_id = a.slack_id
         LEFT JOIN users u ON u.user_id = t.slack_id
         WHERE c.is_private = 1 AND (c.name ILIKE $1 OR c.channel_id ILIKE $1)
           AND ($2 = '' OR EXISTS (
               SELECT 1 FROM slack_oauth_channel_access a2
               JOIN token_numbers t2 ON t2.slack_id = a2.slack_id
               WHERE a2.channel_id = c.channel_id AND t2.slack_id = $2
           ))
          GROUP BY c.name, c.ship_talkers_id, c.channel_id
         ORDER BY c.name, c.channel_id
          LIMIT $3 OFFSET $4",
    )
    .bind(&pattern)
    .bind(&token_key)
    .bind(super::DIRECTORY_PAGE_SIZE)
    .bind((page - 1) as i64 * super::DIRECTORY_PAGE_SIZE)
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    let rows = records
        .into_iter()
        .enumerate()
        .map(
            |(index, (name, channel_url_id, channel_id, token_ids))| PrivateChannelRow {
                rank: (page - 1) * super::DIRECTORY_PAGE_SIZE as u64 + index as u64 + 1,
                name,
                channel_url_id,
                channel_id,
                tokens: token_ids
                    .into_iter()
                    .filter_map(|token| {
                        let (token_id, user_url_id) = token.split_once(':')?;
                        Some(PrivateChannelToken {
                            token_id: token_id.to_string(),
                            user_url_id: user_url_id.to_string(),
                        })
                    })
                    .collect(),
            },
        )
        .collect();
    let template = PrivateChannelsTemplate {
        rows,
        query,
        token_filter,
        token_options: token_rows
            .into_iter()
            .map(|(_, id)| id.to_string())
            .collect(),
        has_previous: page > 1,
        has_next: page < page_count,
        page,
        page_count,
        directory_path: "/boards/private-channels".into(),
        signed_in: true,
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    Ok(Html(
        template
            .render()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    ))
}

pub(super) async fn get_board_category(
    state: State<AppState>,
    headers: super::HeaderMap,
    category: Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Html<String>, StatusCode> {
    render_board_category(state, headers, category, Query(params)).await
}

const RANK_WINDOW: u64 = 3;

async fn resolve_id(ch: &PgPool, sql: &str) -> Option<String> {
    super::sqlx::query_scalar(super::sqlx::AssertSqlSafe(sql.to_string()))
        .fetch_optional(ch)
        .await
        .ok()
        .flatten()
}

async fn fetch_rank_of(ch: &PgPool, inner: &str, id: &str) -> Option<u64> {
    let sql = format!("SELECT rank FROM ({inner}) ranked WHERE ranked.id = $1");
    let row: Option<i64> = super::sqlx::query_scalar(super::sqlx::AssertSqlSafe(sql.as_str()))
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
        super::sqlx::query_as(super::sqlx::AssertSqlSafe(sql.as_str()))
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

async fn ranked_page_count(ch: &PgPool, inner: &str) -> u64 {
    let sql = format!("SELECT count(*) FROM ({inner}) ranked");
    let total: i64 = super::sqlx::query_scalar(super::sqlx::AssertSqlSafe(sql.as_str()))
        .fetch_one(ch)
        .await
        .unwrap_or(0);
    (total.max(0) as u64).div_ceil(100).max(1)
}

async fn ranked_window(
    ch: &PgPool,
    inner: &str,
    q: &str,
    parsed_rank: Option<u64>,
    resolve_sql: Option<&str>,
    requested_page: u64,
) -> (Vec<RankedRow>, Option<String>, u64, u64) {
    if q.is_empty() {
        let page_count = ranked_page_count(ch, inner).await;
        let page = requested_page.min(page_count).max(1);
        let lo = (page - 1) * 100 + 1;
        let hi = page * 100;
        return (
            fetch_rank_window(ch, inner, lo, hi).await,
            None,
            page,
            page_count,
        );
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
        return (rows, None, 1, 1);
    }
    let id = match resolve_sql {
        Some(sql) => resolve_id(ch, sql).await,
        None => None,
    };
    let id = match id {
        Some(id) => id,
        None => return (Vec::new(), Some(format!("No matches for '{}'", q)), 1, 1),
    };
    match fetch_rank_of(ch, inner, &id).await {
        Some(rank) => {
            let lo = rank.saturating_sub(RANK_WINDOW);
            let hi = rank + RANK_WINDOW;
            let mut rows = fetch_rank_window(ch, inner, lo, hi).await;
            if let Some(r) = rows.iter_mut().find(|r| r.id == id) {
                r.highlight = true;
            }
            (rows, None, 1, 1)
        }
        None => (
            Vec::new(),
            Some(format!("'{}' is not on this board", q)),
            1,
            1,
        ),
    }
}

fn resolve_user_sql(inner: &str, q: &str) -> String {
    let eq = sql_escape(&q.to_lowercase());
    format!(
        "SELECT u.user_id AS id FROM users AS u JOIN ({inner}) lb ON u.user_id = lb.id WHERE lower(u.merged_name) LIKE '%{eq}%' ORDER BY (lower(u.merged_name) = '{eq}') DESC, lb.rank, lower(u.merged_name) LIMIT 1"
    )
}

async fn render_board_category(
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
    let requested_page = params
        .get("page")
        .and_then(|page| page.parse::<u64>().ok())
        .filter(|page| *page > 0)
        .unwrap_or(1);
    let (title, unit, extra_unit, rows, notice, page, page_count): (
        String,
        String,
        Option<String>,
        Vec<BoardEntry>,
        Option<String>,
        u64,
        u64,
    ) = match category.as_str() {
        "talkers" => {
            let inner = format!(
                "SELECT user_id AS id, score AS value, messages::bigint AS extra, row_number() OVER (ORDER BY score DESC) AS rank FROM user_scores WHERE {EXCLUDE_BOTS_DELETED_SCORE}"
            );
            let (ranked, notice, page, page_count) = ranked_window(
                ch,
                &inner,
                q,
                parsed_rank,
                Some(&resolve_user_sql(&inner, q)),
                requested_page,
            )
            .await;
            (
                "Top Talkers".into(),
                "Slack Time".into(),
                Some("Messages".into()),
                board_entries(
                    ch,
                    ranked,
                    BoardSource::Users,
                    fmt_duration,
                    Some(fmt_thousands),
                )
                .await,
                notice,
                page,
                page_count,
            )
        }
        "coders" => {
            let inner = format!(
                "SELECT user_id AS id, value, CAST(NULL AS BIGINT) AS extra, rank FROM (SELECT slack_id AS user_id, total_minutes::bigint AS value, row_number() OVER (ORDER BY total_minutes DESC) AS rank FROM hackatime_connections WHERE {EXCLUDE_BOTS_DELETED_SLACK_ID})"
            );
            let (ranked, notice, page, page_count) = ranked_window(
                ch,
                &inner,
                q,
                parsed_rank,
                Some(&resolve_user_sql(&inner, q)),
                requested_page,
            )
            .await;
            (
                "Top Coders".into(),
                "Coding Time".into(),
                None,
                board_entries(ch, ranked, BoardSource::Users, fmt_minutes, None).await,
                notice,
                page,
                page_count,
            )
        }
        "channels" => {
            let inner = "SELECT s.channel_id AS id, s.total_time::bigint AS value, s.messages::bigint AS extra, row_number() OVER (ORDER BY s.total_time DESC) AS rank FROM channel_scores s WHERE EXISTS (SELECT 1 FROM slack_channels c JOIN slack_messages m ON m.channel_id = c.internal_id JOIN slack_identities i ON i.internal_id = m.identity_id JOIN slack_user_consents consent ON consent.ship_talkers_id = i.ship_talkers_id WHERE c.channel_id = s.channel_id AND consent.revoked_at IS NULL)";
            let eq = sql_escape(&q.to_lowercase());
            let resolve = format!(
                "SELECT c.channel_id AS id FROM slack_channels AS c FINAL JOIN ({inner}) lb ON c.channel_id = lb.id WHERE lower(c.name) LIKE '%{eq}%' ORDER BY (lower(c.name) = '{eq}') DESC, lb.rank, lower(c.name) LIMIT 1"
            );
            let (ranked, notice, page, page_count) =
                ranked_window(ch, inner, q, parsed_rank, Some(&resolve), requested_page).await;
            (
                "Top Channels".into(),
                "Slack Time".into(),
                Some("Messages".into()),
                board_entries(
                    ch,
                    ranked,
                    BoardSource::Channels,
                    fmt_duration,
                    Some(fmt_thousands),
                )
                .await,
                notice,
                page,
                page_count,
            )
        }
        "combined" => {
            let inner = format!(
                "SELECT user_id AS id, value, CAST(NULL AS BIGINT) AS extra, rank FROM (SELECT user_id, value, row_number() OVER (ORDER BY value DESC) AS rank FROM (SELECT user_id, sum(v)::bigint AS value FROM (SELECT user_id, total_time::bigint AS v FROM user_scores UNION ALL SELECT slack_id AS user_id, (total_minutes * 60)::bigint AS v FROM hackatime_connections) GROUP BY user_id) WHERE {EXCLUDE_BOTS_DELETED_SCORE})"
            );
            let (ranked, notice, page, page_count) = ranked_window(
                ch,
                &inner,
                q,
                parsed_rank,
                Some(&resolve_user_sql(&inner, q)),
                requested_page,
            )
            .await;
            (
                "Top Combined".into(),
                "Combined Time".into(),
                None,
                board_entries(ch, ranked, BoardSource::Users, fmt_duration, None).await,
                notice,
                page,
                page_count,
            )
        }
        _ => return Err(StatusCode::NOT_FOUND),
    };
    let notice = notice.or_else(|| {
        (!rows.is_empty() || q.is_empty())
            .then_some(())
            .and(None)
            .or_else(|| Some(format!("No results for '{}'", q)))
    });
    let template = BoardCategoryTemplate {
        title,
        entity: if category == "channels" {
            "Channel"
        } else {
            "User"
        }
        .into(),
        unit,
        extra_unit,
        rows,
        coming_soon: false,
        category: category.clone(),
        query,
        notice,
        numbered: true,
        show_pfp: true,
        has_previous: page > 1,
        has_next: page < page_count,
        page,
        page_count,
        directory_path: format!("/boards/{category}"),
        signed_in,
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    Ok(Html(
        template
            .render()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    ))
}

enum BoardSource {
    Users,
    Channels,
}

async fn board_entries(
    ch: &PgPool,
    rows: Vec<RankedRow>,
    source: BoardSource,
    format_value: impl Fn(u64) -> String,
    format_extra: Option<fn(u64) -> String>,
) -> Vec<BoardEntry> {
    let ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
    let names: HashMap<String, (String, String, String, String)> = match source {
        BoardSource::Users => super::sqlx::query_as::<_, (String, String, String, String)>("SELECT user_id, merged_name, pfp, COALESCE(ship_talkers_id, user_id) FROM users WHERE user_id = ANY($1)").bind(&ids).fetch_all(ch).await.unwrap_or_default().into_iter().map(|(id, name, pfp, url)| (id, (name, pfp, url, String::new()))).collect(),
        BoardSource::Channels => super::sqlx::query_as::<_, (String, String, String, i16, i16)>("SELECT channel_id, name, COALESCE(ship_talkers_id, channel_id), is_private, is_archived FROM slack_channels WHERE channel_id = ANY($1)").bind(&ids).fetch_all(ch).await.unwrap_or_default().into_iter().map(|(id, name, url, is_private, is_archived)| (id, (name, String::new(), url, super::channel_status(is_private, is_archived)))).collect(),
    };
    rows.into_iter()
        .map(|r| {
            let (name, pfp, url, status) = names.get(&r.id).cloned().unwrap_or_default();
            BoardEntry {
                user_id: r.id.clone(),
                url_id: if url.is_empty() {
                    r.id.clone()
                } else {
                    url.clone()
                },
                merged_name: if name.is_empty() { r.id.clone() } else { name },
                pfp: super::local_pfp(&url, &pfp),
                value: format_value(r.value.max(0) as u64),
                extra: r
                    .extra
                    .map(|v| v.max(0) as u64)
                    .and_then(|v| format_extra.map(|f| f(v)))
                    .unwrap_or_default(),
                linked: true,
                rank: r.rank,
                label: r.rank.to_string(),
                highlight: r.highlight,
                status,
            }
        })
        .collect()
}
