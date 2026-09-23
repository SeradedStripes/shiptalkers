use super::{
    AppState, BoardCategoryTemplate, BoardEntry, BoardsTemplate, EXCLUDE_BOTS_DELETED_SCORE,
    EXCLUDE_BOTS_DELETED_SLACK_ID, PgPool, RankedRow, State, StatusCode, fmt_duration, fmt_minutes,
    fmt_thousands, signed_in, sql_escape,
};
use askama::Template;
use axum::extract::{Path, Query};
use axum::http::HeaderMap;
use axum::response::Html;
use std::collections::HashMap;
use std::time::Instant;

pub(super) async fn get_boards(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Html<String>, StatusCode> {
    let started = Instant::now();
    let template = BoardsTemplate {
        signed_in: signed_in(&state, &headers),
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
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
            let inner = "SELECT channel_id AS id, total_time::bigint AS value, messages::bigint AS extra, row_number() OVER (ORDER BY total_time DESC) AS rank FROM channel_scores";
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
    let names: HashMap<String, (String, String, String)> = match source {
        BoardSource::Users => super::sqlx::query_as::<_, (String, String, String, String)>("SELECT user_id, merged_name, pfp, COALESCE(ship_talkers_id, user_id) FROM users WHERE user_id = ANY($1)").bind(&ids).fetch_all(ch).await.unwrap_or_default().into_iter().map(|(id, name, pfp, url)| (id, (name, pfp, url))).collect(),
        BoardSource::Channels => super::sqlx::query_as::<_, (String, String, String, i16, i16)>("SELECT channel_id, name, COALESCE(ship_talkers_id, channel_id), is_private, is_archived FROM slack_channels WHERE channel_id = ANY($1)").bind(&ids).fetch_all(ch).await.unwrap_or_default().into_iter().map(|(id, name, url, is_private, is_archived)| (id, (super::channel_display_name(&name, is_private, is_archived), String::new(), url))).collect(),
    };
    rows.into_iter()
        .map(|r| {
            let (name, pfp, url) = names.get(&r.id).cloned().unwrap_or_default();
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
            }
        })
        .collect()
}
