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
    let (title, unit, extra_unit, rows, notice): (
        String,
        String,
        Option<String>,
        Vec<BoardEntry>,
        Option<String>,
    ) = match category.as_str() {
        "talkers" => {
            let inner = format!(
                "SELECT user_id AS id, score AS value, messages::bigint AS extra, row_number() OVER (ORDER BY score DESC) AS rank FROM user_scores WHERE {EXCLUDE_BOTS_DELETED_SCORE}"
            );
            let (ranked, notice) = ranked_window(
                ch,
                &inner,
                q,
                parsed_rank,
                Some(&resolve_user_sql(&inner, q)),
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
            )
        }
        "coders" => {
            let inner = format!(
                "SELECT user_id AS id, value, CAST(NULL AS BIGINT) AS extra, rank FROM (SELECT slack_id AS user_id, total_minutes::bigint AS value, row_number() OVER (ORDER BY total_minutes DESC) AS rank FROM hackatime_connections WHERE {EXCLUDE_BOTS_DELETED_SLACK_ID})"
            );
            let (ranked, notice) = ranked_window(
                ch,
                &inner,
                q,
                parsed_rank,
                Some(&resolve_user_sql(&inner, q)),
            )
            .await;
            (
                "Top Coders".into(),
                "Coding Time".into(),
                None,
                board_entries(ch, ranked, BoardSource::Users, fmt_minutes, None).await,
                notice,
            )
        }
        "channels" => {
            let inner = "SELECT channel_id AS id, total_time::bigint AS value, messages::bigint AS extra, row_number() OVER (ORDER BY total_time DESC) AS rank FROM channel_scores";
            let eq = sql_escape(&q.to_lowercase());
            let resolve = format!(
                "SELECT c.channel_id AS id FROM slack_channels AS c FINAL JOIN ({inner}) lb ON c.channel_id = lb.id WHERE lower(c.name) LIKE '%{eq}%' ORDER BY (lower(c.name) = '{eq}') DESC, lb.rank, lower(c.name) LIMIT 1"
            );
            let (ranked, notice) = ranked_window(ch, inner, q, parsed_rank, Some(&resolve)).await;
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
            )
        }
        "combined" => {
            let inner = format!(
                "SELECT user_id AS id, value, CAST(NULL AS BIGINT) AS extra, rank FROM (SELECT user_id, value, row_number() OVER (ORDER BY value DESC) AS rank FROM (SELECT user_id, sum(v)::bigint AS value FROM (SELECT user_id, total_time::bigint AS v FROM user_scores UNION ALL SELECT slack_id AS user_id, (total_minutes * 60)::bigint AS v FROM hackatime_connections) GROUP BY user_id) WHERE {EXCLUDE_BOTS_DELETED_SCORE})"
            );
            let (ranked, notice) = ranked_window(
                ch,
                &inner,
                q,
                parsed_rank,
                Some(&resolve_user_sql(&inner, q)),
            )
            .await;
            (
                "Top Combined".into(),
                "Combined Time".into(),
                None,
                board_entries(ch, ranked, BoardSource::Users, fmt_duration, None).await,
                notice,
            )
        }
        "words" => {
            let inner = "SELECT word AS id, cnt::bigint AS value, CAST(NULL AS BIGINT) AS extra, rank FROM (SELECT word, cnt, row_number() OVER (ORDER BY cnt DESC) AS rank FROM word_totals)";
            let (ranked, notice) = if q.is_empty() {
                (
                    state
                        .cache
                        .words
                        .get_or(async { fetch_rank_window(ch, inner, 1, 100).await })
                        .await,
                    None,
                )
            } else {
                let eq = sql_escape(&q.to_lowercase());
                let resolve = format!(
                    "SELECT id FROM ({inner}) WHERE id = '{eq}' OR id LIKE '{eq}%' ORDER BY (id = '{eq}') DESC LIMIT 1"
                );
                ranked_window(ch, inner, q, parsed_rank, Some(&resolve)).await
            };
            let rows = ranked
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
            ("Top Words".into(), "Uses".into(), None, rows, notice)
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
        } else if category == "words" {
            "Word"
        } else {
            "User"
        }
        .into(),
        unit,
        extra_unit,
        rows,
        coming_soon: false,
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
        BoardSource::Channels => super::sqlx::query_as::<_, (String, String, String)>("SELECT channel_id, name, COALESCE(ship_talkers_id, channel_id) FROM slack_channels WHERE channel_id = ANY($1)").bind(&ids).fetch_all(ch).await.unwrap_or_default().into_iter().map(|(id, name, url)| (id, (name, String::new(), url))).collect(),
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
