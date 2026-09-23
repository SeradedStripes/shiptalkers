use super::{AppState, ChannelStats, EXCLUDE_BOTS_DELETED, Stats};
use askama::Template;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Html;
use std::time::Instant;

#[derive(Clone)]
pub(super) struct StatsSnapshot {
    pub(super) total_messages: u64,
    pub(super) total_channels: u64,
    pub(super) archived_channels: u64,
    pub(super) total_users: u64,
    pub(super) hackatime_users: u64,
    pub(super) private_hackatime_users: u64,
    pub(super) no_hackatime_account_users: u64,
    pub(super) coding_minutes: u64,
    pub(super) slack_time_secs: u64,
    pub(super) db_size_bytes: u64,
    pub(super) updated: u64,
}

type StatsMetaRow = (i64, i64, i64, i64, i64, i64, i64, i64, i64, i64, i64);

pub(super) async fn get_stats_page(
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

pub(super) async fn get_stats_for_id(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Html<String>, StatusCode> {
    let pool = state.pool()?;
    let channel_id: Option<String> = super::sqlx::query_scalar(
        "SELECT channel_id FROM slack_channels WHERE channel_id = $1 OR ship_talkers_id = $1 LIMIT 1",
    )
    .bind(&id)
    .fetch_optional(pool)
    .await
    .unwrap_or(None);
    if let Some(channel_id) = channel_id {
        return render_channel_stats(&state, &headers, &channel_id).await;
    }
    let user_id: Option<String> = super::sqlx::query_scalar(
        "SELECT user_id FROM users WHERE user_id = $1 OR ship_talkers_id = $1 LIMIT 1",
    )
    .bind(&id)
    .fetch_optional(pool)
    .await
    .unwrap_or(None);
    render_user_stats(&state, &headers, user_id.as_deref().unwrap_or(&id)).await
}

async fn render_user_stats(
    state: &super::AppState,
    headers: &HeaderMap,
    slack_id: &str,
) -> Result<Html<String>, StatusCode> {
    let started = Instant::now();
    let ch = state.pool()?;
    let signed_in = super::signed_in(state, headers);
    let ship_talkers_id = ship_talkers_lib::base36::encode(slack_id.as_bytes());

    #[derive(Debug)]
    struct UserInfo {
        ship_talkers_id: Option<String>,
        merged_name: String,
        display_name: String,
        real_name: String,
        username: String,
        email: String,
        pfp: String,
        is_bot: bool,
        is_deleted: bool,
    }
    let info: Option<UserInfo> =
        super::sqlx::query_as::<_, (Option<String>, String, String, String, String, String, String, i16, i16)>(
            "SELECT ship_talkers_id, merged_name, display_name, real_name, username, email, pfp, is_bot, is_deleted FROM users WHERE user_id = $1",
        )
        .bind(slack_id)
        .fetch_optional(ch)
        .await
        .unwrap_or(None)
        .map(
            |(
                ship_talkers_id,
                merged_name,
                display_name,
                real_name,
                username,
                email,
                pfp,
                is_bot,
                is_deleted,
            )| {
                UserInfo {
                    ship_talkers_id,
                    merged_name,
                    display_name,
                    real_name,
                    username,
                    email,
                    pfp,
                    is_bot: is_bot == 1,
                    is_deleted: is_deleted == 1,
                }
            },
        );
    let is_bot = info.as_ref().map(|i| i.is_bot).unwrap_or(false);
    let is_deleted = info.as_ref().map(|i| i.is_deleted).unwrap_or(false);
    let merged_name = info
        .as_ref()
        .map(|i| i.merged_name.clone())
        .unwrap_or_default();
    let display_name = info
        .as_ref()
        .map(|i| i.display_name.clone())
        .unwrap_or_default();
    let real_name = info
        .as_ref()
        .map(|i| i.real_name.clone())
        .unwrap_or_default();
    let username = info
        .as_ref()
        .map(|i| i.username.clone())
        .unwrap_or_default();
    let email = info.as_ref().map(|i| i.email.clone()).unwrap_or_default();
    let shiptalkers_id = info
        .as_ref()
        .and_then(|i| i.ship_talkers_id.clone())
        .unwrap_or_default();
    let pfp_url = info.as_ref().map(|i| i.pfp.clone()).unwrap_or_default();
    let pfp = super::local_pfp(
        if shiptalkers_id.is_empty() {
            slack_id
        } else {
            &shiptalkers_id
        },
        &pfp_url,
    );

    #[derive(Debug)]
    struct ScoreRow {
        total_time: u64,
        messages: u64,
        channels: u64,
    }

    let scores: Option<ScoreRow> = super::sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT total_time, messages, channels
         FROM user_scores WHERE user_id = $1",
    )
    .bind(slack_id)
    .fetch_optional(ch)
    .await
    .unwrap_or(None)
    .map(|(total_time, messages, channels)| ScoreRow {
        total_time: total_time.max(0) as u64,
        messages: messages.max(0) as u64,
        channels: channels.max(0) as u64,
    });

    let coding_minutes: u64 = super::sqlx::query_scalar::<_, i64>(
        "SELECT total_minutes FROM hackatime_connections WHERE slack_id = $1",
    )
    .bind(slack_id)
    .fetch_optional(ch)
    .await
    .unwrap_or(None)
    .unwrap_or(0)
    .max(0) as u64;

    let counts: Vec<(String, i64)> = super::sqlx::query_as(
        "SELECT c.channel_id, count(*) as messages
         FROM slack_messages m
         JOIN slack_identities i ON i.internal_id = m.identity_id
         JOIN slack_channels c ON c.internal_id = m.channel_id
         WHERE i.ship_talkers_id = $1
         GROUP BY c.channel_id
         ORDER BY messages DESC
         LIMIT 5",
    )
    .bind(ship_talkers_id)
    .fetch_all(ch)
    .await
    .unwrap_or_default();

    let name_ids: Vec<String> = counts.iter().map(|(id, _)| id.clone()).collect();
    let channel_names: std::collections::HashMap<String, (String, String)> = if name_ids.is_empty()
    {
        std::collections::HashMap::new()
    } else {
        super::sqlx::query_as::<_, (String, String, String)>(
            "SELECT channel_id, name, COALESCE(ship_talkers_id, channel_id) FROM slack_channels WHERE channel_id = ANY($1)",
        )
        .bind(&name_ids)
        .fetch_all(ch)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(channel_id, name, ship_talkers_id)| (channel_id, (name, ship_talkers_id)))
        .collect()
    };

    let top_channels: Vec<ChannelStats> = counts
        .into_iter()
        .map(|(channel_id, messages)| ChannelStats {
            user_id: channel_id.clone(),
            url_id: channel_names
                .get(&channel_id)
                .map(|(_, id)| id.clone())
                .unwrap_or_else(|| channel_id.clone()),
            channel_name: channel_names
                .get(&channel_id)
                .map(|(name, _)| name.clone())
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| channel_id.clone()),
            messages: super::fmt_thousands(messages.max(0) as u64),
        })
        .collect();

    let total_messages = scores.as_ref().map(|s| s.messages).unwrap_or(0);
    let found = total_messages > 0 || coding_minutes > 0 || !merged_name.is_empty();

    let slack_time = match scores.as_ref() {
        Some(s) if s.messages > 0 => super::fmt_minutes(s.total_time / 60),
        _ => "0hrs 0min".into(),
    };

    let template = super::UserTemplate {
        merged_name: if merged_name.is_empty() {
            slack_id.to_string()
        } else {
            merged_name
        },
        display_name,
        real_name,
        username,
        email,
        pfp,
        slack_id: slack_id.to_string(),
        shiptalkers_id,
        deactivated: is_deleted,
        total_messages: super::fmt_thousands(total_messages),
        coding_hours: super::fmt_minutes(coding_minutes),
        channels: super::fmt_thousands(scores.as_ref().map(|s| s.channels).unwrap_or(0)),
        slack_time,
        top_channels,
        show_coding_prompt: !is_bot && !is_deleted && coding_minutes == 0,
        signed_in,
        found,
        page_load_ms: format!("{}ms", started.elapsed().as_millis()),
    };
    let html = template
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Html(html))
}

async fn render_channel_stats(
    state: &super::AppState,
    headers: &HeaderMap,
    channel_id: &str,
) -> Result<Html<String>, StatusCode> {
    let started = Instant::now();
    let ch = state.pool()?;
    let signed_in = super::signed_in(state, headers);

    let channel_meta: Option<(String, i16, i16)> = super::sqlx::query_as::<_, (String, i16, i16)>(
        "SELECT name, is_private, is_archived FROM slack_channels WHERE channel_id = $1",
    )
    .bind(channel_id)
    .fetch_optional(ch)
    .await
    .ok()
    .flatten();

    let (channel_name, channel_status) = channel_meta
        .map(|(name, is_private, is_archived)| {
            (name, super::channel_status(is_private, is_archived))
        })
        .unwrap_or_else(|| (String::new(), String::new()));

    let total_messages: u64 =
        super::sqlx::query_scalar::<_, i64>("SELECT count(*) FROM slack_messages m JOIN slack_channels c ON c.internal_id = m.channel_id WHERE c.channel_id = $1")
            .bind(channel_id)
            .fetch_one(ch)
            .await
            .unwrap_or(0)
            .max(0) as u64;

    let active_users: u64 = super::sqlx::query_scalar::<_, i64>(super::sqlx::AssertSqlSafe(format!(
        "SELECT count(DISTINCT m.identity_id) FROM slack_messages m
         WHERE m.channel_id = (SELECT internal_id FROM slack_channels WHERE channel_id = $1) AND {EXCLUDE_BOTS_DELETED}"
    )))
    .bind(channel_id)
    .fetch_one(ch)
    .await
    .unwrap_or(0)
    .max(0) as u64;

    let (channel_created_at, slack_time_secs): (i64, i64) = super::sqlx::query_as(
        "SELECT c.created_at, COALESCE(s.total_time, 0)
         FROM slack_channels c
         LEFT JOIN channel_scores s ON s.channel_id = c.channel_id
         WHERE c.channel_id = $1",
    )
    .bind(channel_id)
    .fetch_optional(ch)
    .await
    .ok()
    .flatten()
    .unwrap_or((0, 0));

    let posters: Vec<(String, String, i64)> = super::sqlx::query_as(super::sqlx::AssertSqlSafe(format!(
        "SELECT u.user_id, COALESCE(u.ship_talkers_id, u.user_id), count(*) as messages
         FROM slack_messages m
         JOIN slack_identities i ON i.internal_id = m.identity_id
          JOIN users u ON u.ship_talkers_id = i.ship_talkers_id
          WHERE m.channel_id = (SELECT internal_id FROM slack_channels WHERE channel_id = $1) AND {EXCLUDE_BOTS_DELETED}
         GROUP BY u.user_id
         ORDER BY messages DESC
         LIMIT 10"
    )))
    .bind(channel_id)
    .fetch_all(ch)
    .await
    .unwrap_or_default();

    let name_ids: Vec<String> = posters.iter().map(|(id, _, _)| id.clone()).collect();

    let poster_names: std::collections::HashMap<String, (String, String)> = if name_ids.is_empty() {
        std::collections::HashMap::new()
    } else {
        super::sqlx::query_as::<_, (String, String, String, i16)>(
            "SELECT user_id, merged_name, pfp, is_deleted FROM users WHERE user_id = ANY($1)",
        )
        .bind(&name_ids)
        .fetch_all(ch)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|(user_id, merged_name, pfp, _)| {
            let label = if merged_name.is_empty() {
                user_id.clone()
            } else {
                merged_name
            };
            (user_id, (label, pfp))
        })
        .collect()
    };

    let top_posters: Vec<super::UserStats> = posters
        .into_iter()
        .map(|(user_id, ship_talkers_id, messages)| {
            let (merged_name, pfp) = poster_names.get(&user_id).cloned().unwrap_or_default();
            super::UserStats {
                user_id: user_id.clone(),
                url_id: ship_talkers_id.clone(),
                merged_name: if merged_name.is_empty() {
                    user_id.clone()
                } else {
                    merged_name
                },
                pfp: super::local_pfp(&ship_talkers_id, &pfp),
                messages: super::fmt_thousands(messages.max(0) as u64),
            }
        })
        .collect();

    let found = total_messages > 0 || !channel_name.is_empty();

    let template = super::ChannelTemplate {
        channel_name: if channel_name.is_empty() {
            channel_id.to_string()
        } else {
            channel_name
        },
        channel_status,
        channel_id: channel_id.to_string(),
        total_messages: super::fmt_thousands(total_messages),
        active_users: super::fmt_thousands(active_users),
        slack_time: super::fmt_minutes(slack_time_secs.max(0) as u64 / 60),
        creation_date: fmt_date(channel_created_at.max(0) as u64),
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

async fn load_stats(state: &super::AppState, headers: &HeaderMap) -> super::Stats {
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

    super::Stats {
        total_messages: super::fmt_thousands(snapshot.total_messages),
        total_channels: super::fmt_thousands(snapshot.total_channels),
        archived_channels: super::fmt_thousands(snapshot.archived_channels),
        total_users: super::fmt_thousands(snapshot.total_users),
        hackatime_users: super::fmt_thousands(snapshot.hackatime_users),
        private_hackatime_users: super::fmt_thousands(snapshot.private_hackatime_users),
        no_hackatime_account_users: super::fmt_thousands(snapshot.no_hackatime_account_users),
        coding_hours: super::fmt_minutes(snapshot.coding_minutes),
        coding_time: super::fmt_total_time(snapshot.coding_minutes * 60),
        slack_hours: super::fmt_minutes(snapshot.slack_time_secs / 60),
        slack_time: super::fmt_total_time(snapshot.slack_time_secs),
        combined_hours: super::fmt_minutes(
            (snapshot.slack_time_secs + snapshot.coding_minutes * 60) / 60,
        ),
        combined_time: super::fmt_total_time(
            snapshot.slack_time_secs + snapshot.coding_minutes * 60,
        ),
        db_size_label,
        signed_in: super::signed_in(state, headers),
        page_load_ms: String::new(),
    }
}

pub(super) async fn compute_stats(state: &super::AppState) -> StatsSnapshot {
    let Ok(ch) = state.pool() else {
        return StatsSnapshot {
            total_messages: 0,
            total_channels: 0,
            archived_channels: 0,
            total_users: 0,
            hackatime_users: 0,
            private_hackatime_users: 0,
            no_hackatime_account_users: 0,
            coding_minutes: 0,
            slack_time_secs: 0,
            db_size_bytes: 0,
            updated: 0,
        };
    };

    let cached: Option<StatsMetaRow> = super::sqlx::query_as(
        "SELECT total_messages, total_channels, archived_channels, total_users, hackatime_users, private_hackatime_users, no_hackatime_account_users, coding_minutes, slack_time_secs, db_size_bytes, updated
         FROM stats_meta WHERE id = 1",
    )
    .fetch_optional(ch)
    .await
    .ok()
    .flatten();

    match cached {
        Some((
            total_messages,
            total_channels,
            archived_channels,
            total_users,
            hackatime_users,
            private_hackatime_users,
            no_hackatime_account_users,
            coding_minutes,
            slack_time_secs,
            db_size_bytes,
            updated,
        )) => StatsSnapshot {
            total_messages: total_messages.max(0) as u64,
            total_channels: total_channels.max(0) as u64,
            archived_channels: archived_channels.max(0) as u64,
            total_users: total_users.max(0) as u64,
            hackatime_users: hackatime_users.max(0) as u64,
            private_hackatime_users: private_hackatime_users.max(0) as u64,
            no_hackatime_account_users: no_hackatime_account_users.max(0) as u64,
            coding_minutes: coding_minutes.max(0) as u64,
            slack_time_secs: slack_time_secs.max(0) as u64,
            db_size_bytes: db_size_bytes.max(0) as u64,
            updated: updated.max(0) as u64,
        },
        None => {
            // First run before the background refresh has written a row.
            let total_messages: i64 =
                super::sqlx::query_scalar("SELECT total FROM message_count WHERE id = 1")
                    .fetch_optional(ch)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(0)
                    .max(0);
            let total_channels: i64 =
                super::sqlx::query_scalar("SELECT count(*) FROM slack_channels")
                    .fetch_one(ch)
                    .await
                    .unwrap_or(0)
                    .max(0);
            let archived_channels: i64 = super::sqlx::query_scalar(
                "SELECT count(*) FROM slack_channels WHERE is_archived = 1",
            )
            .fetch_one(ch)
            .await
            .unwrap_or(0)
            .max(0);
            let total_users: i64 = super::sqlx::query_scalar(
                "SELECT count(*) FROM users WHERE is_bot = 0 AND is_deleted = 0",
            )
            .fetch_one(ch)
            .await
            .unwrap_or(0)
            .max(0);
            let no_hackatime_account_users: i64 = super::sqlx::query_scalar(
                "SELECT count(*) FROM hackatime_connections WHERE status = 'no_account'",
            )
            .fetch_one(ch)
            .await
            .unwrap_or(0)
            .max(0);
            let hackatime_users: i64 = super::sqlx::query_scalar(
                "SELECT count(*) FROM hackatime_connections WHERE status != 'no_account'",
            )
            .fetch_one(ch)
            .await
            .unwrap_or(0)
            .max(0);
            let private_hackatime_users: i64 = super::sqlx::query_scalar(
                "SELECT count(*) FROM hackatime_connections WHERE status = 'private'",
            )
            .fetch_one(ch)
            .await
            .unwrap_or(0)
            .max(0);
            let coding_minutes: i64 = super::sqlx::query_scalar::<_, Option<i64>>(
                "SELECT sum(total_minutes)::bigint FROM hackatime_connections",
            )
            .fetch_one(ch)
            .await
            .ok()
            .flatten()
            .unwrap_or(0)
            .max(0);
            let slack_time_secs: i64 =
                super::sqlx::query_scalar::<_, Option<i64>>(super::sqlx::AssertSqlSafe(format!(
                    "SELECT sum(total_time)::bigint FROM user_scores WHERE {EXCLUDE_BOTS_DELETED}"
                )))
                .fetch_one(ch)
                .await
                .ok()
                .flatten()
                .unwrap_or(0)
                .max(0);
            let db_size_bytes: i64 =
                super::sqlx::query_scalar::<_, i64>("SELECT pg_database_size(current_database())")
                    .fetch_one(ch)
                    .await
                    .unwrap_or(0)
                    .max(0);

            StatsSnapshot {
                total_messages: total_messages as u64,
                total_channels: total_channels as u64,
                archived_channels: archived_channels as u64,
                total_users: total_users as u64,
                hackatime_users: hackatime_users as u64,
                private_hackatime_users: private_hackatime_users as u64,
                no_hackatime_account_users: no_hackatime_account_users as u64,
                coding_minutes: coding_minutes as u64,
                slack_time_secs: slack_time_secs as u64,
                db_size_bytes: db_size_bytes as u64,
                updated: 0,
            }
        }
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

fn fmt_date(secs: u64) -> String {
    match parse_ts(secs.saturating_mul(1_000_000)) {
        Some((year, month, day, _, _)) => format!(
            "<time datetime=\"{year:04}-{month:02}-{day:02}\">{year:04}-{month:02}-{day:02}</time>"
        ),
        None => String::new(),
    }
}
