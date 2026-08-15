use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct SlackMessageRow {
    pub user_id: String,
    pub channel_id: String,
    pub message_ts: u64,
    pub text: String,
    pub thread_ts: Option<String>,
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct SlackReactionRow {
    pub channel_id: String,
    pub message_ts: u64,
    pub emoji: String,
    pub user_id: String,
}

/// Converts a Slack timestamp string ("seconds.microseconds") to microseconds.
pub fn slack_ts_to_micros(ts: &str) -> u64 {
    let (secs, frac) = match ts.split_once('.') {
        Some((s, f)) => (s, f),
        None => (ts, "0"),
    };
    let secs: u64 = secs.parse().unwrap_or(0);
    let micros: u64 = frac.parse().unwrap_or(0);
    secs.saturating_mul(1_000_000).saturating_add(micros)
}

/// Formats microseconds as a Slack timestamp string ("seconds.microseconds").
pub fn micros_to_slack_ts(micros: u64) -> String {
    format!("{}.{:06}", micros / 1_000_000, micros % 1_000_000)
}

/// Parses an ISO "YYYY-MM-DD" date string into a `time::Date`.
pub fn parse_date(s: &str) -> Option<time::Date> {
    let mut parts = s.split('-');
    let year: i32 = parts.next()?.parse().ok()?;
    let month: u8 = parts.next()?.parse().ok()?;
    let day: u8 = parts.next()?.parse().ok()?;
    time::Date::from_calendar_date(year, time::Month::try_from(month).ok()?, day).ok()
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct SlackChannelRow {
    pub channel_id: String,
    pub name: String,
}

#[derive(Debug, Row, Deserialize)]
pub struct ChannelIdRow {
    pub channel_id: String,
}

pub async fn init_tables(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    let admin_client = client.clone().with_database("");

    admin_client
        .query("CREATE DATABASE IF NOT EXISTS ship_talkers")
        .execute()
        .await?;

    // A broken slack_messages_by_user (e.g. too many empty broken parts after a
    // power loss) poisons slack_messages too: slack_messages's startup load job
    // waits on the materialized view, which waits on this table, so the first
    // query touching slack_messages below would fail. Probe this table first and
    // drop it when it cannot load. It is derived from slack_messages, so the
    // startup backfill rebuilds it and the service comes back up on its own.
    if let Err(e) = client
        .query("SELECT count() FROM slack_messages_by_user")
        .execute()
        .await
    {
        tracing::warn!(
            "slack_messages_by_user cannot load ({}), dropping it for recreate",
            e
        );
        client
            .query("DROP TABLE IF EXISTS slack_messages_by_user_mv")
            .execute()
            .await?;
        client
            .query("DROP TABLE IF EXISTS slack_messages_by_user")
            .execute()
            .await?;
    }

    // Migrate existing MergeTree to ReplacingMergeTree if needed
    migrate_slack_messages(client).await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS slack_messages (
                user_id String,
                channel_id String,
                message_ts UInt64,
                text String,
                thread_ts Nullable(String)
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (channel_id, message_ts)",
        )
        .execute()
        .await?;

    // Add thread_ts column if it doesn't exist (migration for existing tables)
    client
        .query("ALTER TABLE slack_messages ADD COLUMN IF NOT EXISTS thread_ts Nullable(String)")
        .execute()
        .await
        .ok();

    // Migrate message_ts from String ("seconds.microseconds") to UInt64 microseconds
    migrate_slack_messages_ts(client).await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS slack_channels (
                channel_id String,
                name String
            ) ENGINE = ReplacingMergeTree()
            ORDER BY channel_id",
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS users (
                user_id String,
                display_name String,
                pfp String DEFAULT '',
                updated UInt64 DEFAULT 0,
                is_bot UInt8 DEFAULT 0,
                is_deleted UInt8 DEFAULT 0
            ) ENGINE = ReplacingMergeTree()
            ORDER BY user_id",
        )
        .execute()
        .await?;

    // Add columns if they don't exist (migrations for existing tables)
    client
        .query("ALTER TABLE users ADD COLUMN IF NOT EXISTS updated UInt64 DEFAULT 0")
        .execute()
        .await
        .ok();
    client
        .query("ALTER TABLE users ADD COLUMN IF NOT EXISTS pfp String DEFAULT ''")
        .execute()
        .await
        .ok();
    client
        .query("ALTER TABLE users ADD COLUMN IF NOT EXISTS is_bot UInt8 DEFAULT 0")
        .execute()
        .await
        .ok();
    client
        .query("ALTER TABLE users ADD COLUMN IF NOT EXISTS is_deleted UInt8 DEFAULT 0")
        .execute()
        .await
        .ok();

    client
        .query(
            "CREATE TABLE IF NOT EXISTS coding_activity (
                user_id String,
                date Date,
                minutes Int64,
                language Nullable(String)
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (user_id, date)",
        )
        .execute()
        .await?;

    // Migrate date from String ("YYYY-MM-DD") to Date
    migrate_coding_activity_date(client).await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS scrape_checkpoints (
                channel_id String,
                fully_scraped UInt8
            ) ENGINE = ReplacingMergeTree()
            ORDER BY channel_id",
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS thread_checkpoints (
                channel_id String,
                thread_ts String,
                fully_scraped UInt8
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (channel_id, thread_ts)",
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS scraped_channels (
                channel_id String,
                scraped_at DateTime('UTC') DEFAULT now()
            ) ENGINE = ReplacingMergeTree()
            ORDER BY channel_id",
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS user_scores (
                user_id String,
                score Int64,
                total_time UInt64,
                messages UInt64,
                sessions UInt64,
                longest UInt64,
                days UInt64,
                channels UInt64,
                first_ts UInt64,
                last_ts UInt64,
                active_hour UInt8,
                updated UInt64
            ) ENGINE = ReplacingMergeTree(updated)
            ORDER BY user_id",
        )
        .execute()
        .await?;

    // Migration for existing deployments: the columns below were added later, so
    // they arrive as zeros; backfill_stale_user_scores recomputes any user with
    // longest = 0 (real users always have at least one session worth more than
    // zero, since every session earns its first message's production time).
    for column in [
        "longest UInt64",
        "days UInt64",
        "channels UInt64",
        "first_ts UInt64",
        "last_ts UInt64",
        "active_hour UInt8",
    ] {
        client
            .query(&format!(
                "ALTER TABLE user_scores ADD COLUMN IF NOT EXISTS {column}"
            ))
            .execute()
            .await
            .ok();
    }

    // Fresh deployments created `total_chars` (it was in the CREATE TABLE from
    // an older ranking system) but the sessionizer no longer writes it, and the
    // insert struct lacks the field, so every recompute failed with a schema
    // mismatch on a fresh DB. Drop it for any DB that still has it.
    client
        .query("ALTER TABLE user_scores DROP COLUMN IF EXISTS total_chars")
        .execute()
        .await
        .ok();

    client
        .query(
            "CREATE TABLE IF NOT EXISTS hackatime_connections (
                slack_id String,
                access_token String,
                last_synced_date Nullable(String),
                connected_at DateTime('UTC') DEFAULT now()
            ) ENGINE = ReplacingMergeTree()
            ORDER BY slack_id",
        )
        .execute()
        .await?;

    // Add last_synced_date column if it doesn't exist (migration for existing tables)
    client
        .query(
            "ALTER TABLE hackatime_connections ADD COLUMN IF NOT EXISTS last_synced_date Nullable(String)",
        )
        .execute()
        .await
        .ok();

    // Secondary copy of slack_messages sorted by user so per-user reads (stats
    // pages and score recompute) only touch that user's granules instead of
    // scanning the whole table. Kept in sync by the materialized view plus a
    // one-time backfill at startup. It tolerates a higher broken-parts limit:
    // after an unclean shutdown ClickHouse refuses to attach a table with more
    // than 100 broken parts, and since this table is rebuilt from slack_messages
    // anyway, letting it sweep empty broken parts keeps a power loss from taking
    // the whole service down. slack_messages keeps the default guard.
    let create_by_user_table = "CREATE TABLE IF NOT EXISTS slack_messages_by_user (
            user_id String,
            channel_id String,
            message_ts UInt64,
            text String,
            thread_ts Nullable(String)
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (user_id, message_ts)
        SETTINGS max_suspicious_broken_parts = 1000";
    let create_by_user_mv = "CREATE MATERIALIZED VIEW IF NOT EXISTS slack_messages_by_user_mv
             TO slack_messages_by_user
             AS SELECT user_id, channel_id, message_ts, text, thread_ts FROM slack_messages";
    let ensure_slack_messages_by_user = || async {
        client.query(create_by_user_table).execute().await?;
        client.query(create_by_user_mv).execute().await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    };
    if let Err(e) = ensure_slack_messages_by_user().await {
        // Either create can fail when the table is stuck in a failed attach
        // (e.g. a power loss beat the setting's sweep). Recreate on the spot:
        // it is derived data, so the startup backfill rebuilds it and the
        // service comes back up on its own.
        tracing::warn!(
            "slack_messages_by_user failed to attach ({}), dropping and recreating",
            e
        );
        client
            .query("DROP TABLE IF EXISTS slack_messages_by_user_mv")
            .execute()
            .await?;
        client
            .query("DROP TABLE IF EXISTS slack_messages_by_user")
            .execute()
            .await?;
        ensure_slack_messages_by_user().await?;
    }

    // Per-message reactions (emoji name + reacting user) as fetched from
    // conversations.history / conversations.replies. A raw snapshot per message;
    // re-fetches replace the rows for that message.
    client
        .query(
            "CREATE TABLE IF NOT EXISTS slack_reactions (
                channel_id String,
                message_ts UInt64,
                emoji String,
                user_id String
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (channel_id, message_ts, emoji, user_id)",
        )
        .execute()
        .await?;

    // Per-message word frequencies driving the Top Words leaderboard. Rows are
    // written by the scraper for each newly inserted message (one per distinct
    // lowercase word, count = occurrences in that message) plus a one-time
    // backfill; reads use FINAL so re-scans never double-count. `inserted_at` is
    // when the row was written (not `message_ts`), so thread re-scans that add
    // replies to old threads still show up as dirty words for the incremental
    // `word_totals` refresh.
    client
        .query(
            "CREATE TABLE IF NOT EXISTS word_counts (
                word String,
                user_id String,
                channel_id String,
                message_ts UInt64,
                count UInt64,
                inserted_at UInt64 DEFAULT 0
            ) ENGINE = ReplacingMergeTree()
            ORDER BY (word, channel_id, message_ts)",
        )
        .execute()
        .await?;

    // Add inserted_at column if it doesn't exist (migration for existing tables)
    client
        .query("ALTER TABLE word_counts ADD COLUMN IF NOT EXISTS inserted_at UInt64 DEFAULT 0")
        .execute()
        .await
        .ok();

    // Materialized per-word totals (bots/deleted excluded) so the Top Words
    // leaderboard reads a small table instead of scanning every word_counts
    // row. Maintained by `refresh_word_totals`.
    client
        .query(
            "CREATE TABLE IF NOT EXISTS word_totals (
                word String,
                cnt UInt64,
                updated UInt64
            ) ENGINE = ReplacingMergeTree(updated)
            ORDER BY word",
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS channel_scores (
                channel_id String,
                total_time UInt64,
                messages UInt64,
                updated UInt64
            ) ENGINE = ReplacingMergeTree(updated)
            ORDER BY channel_id",
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS score_meta (
                id UInt8,
                formula String
            ) ENGINE = ReplacingMergeTree()
            ORDER BY id",
        )
        .execute()
        .await?;

    // Records which one-time backfills have completed (currently `word_counts`),
    // so a restart never re-runs a full-table scan that already finished.
    client
        .query(
            "CREATE TABLE IF NOT EXISTS backfill_meta (
                name String,
                done UInt8
            ) ENGINE = ReplacingMergeTree()
            ORDER BY name",
        )
        .execute()
        .await?;

    // Watermarks for the incremental `word_totals` refresh: `watermark` is the
    // `word_counts.inserted_at` cutoff already folded in (pre-existing rows fold
    // at the first full rebuild), and `last_full` is when the last full rebuild
    // ran, so a daily safety-net rebuild catches anything a watermark missed.
    client
        .query(
            "CREATE TABLE IF NOT EXISTS word_refresh_meta (
                id UInt8,
                watermark UInt64,
                last_full UInt64
            ) ENGINE = ReplacingMergeTree()
            ORDER BY id",
        )
        .execute()
        .await?;

    Ok(())
}

pub async fn optimize_slack_messages(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    client
        .query("OPTIMIZE TABLE slack_messages")
        .execute()
        .await?;
    client
        .query("OPTIMIZE TABLE slack_messages_by_user")
        .execute()
        .await?;
    Ok(())
}

/// Copies existing rows from slack_messages into slack_messages_by_user once.
/// The materialized view keeps new inserts in sync, so this only needs to run
/// when the secondary table is empty (first deploy or after a recreate).
pub async fn backfill_slack_messages_by_user(
    client: &Client,
) -> Result<(), Box<dyn std::error::Error>> {
    let count: u64 = client
        .query("SELECT count() FROM slack_messages_by_user")
        .fetch_one()
        .await
        .unwrap_or(0);
    if count > 0 {
        return Ok(());
    }
    tracing::info!("Backfilling slack_messages_by_user from slack_messages...");
    client
        .query(
            "INSERT INTO slack_messages_by_user
             SELECT user_id, channel_id, message_ts, text, thread_ts FROM slack_messages",
        )
        .execute()
        .await?;
    tracing::info!("slack_messages_by_user backfill complete");
    Ok(())
}

/// Builds word_counts for every existing slack_messages row once, so the Top
/// Words leaderboard is all-time on first deploy. New inserts keep it in sync
/// from then on. Completion is recorded in `backfill_meta`, so the full-table
/// scan runs exactly once and never again on later restarts; a non-empty
/// `word_counts` (e.g. deployments that predate the marker) counts as done too.
pub async fn backfill_word_counts(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    if word_counts_backfilled(client).await {
        return Ok(());
    }
    let count: u64 = client
        .query("SELECT count() FROM word_counts")
        .fetch_one()
        .await
        .unwrap_or(0);
    if count > 0 {
        mark_word_counts_backfilled(client).await?;
        return Ok(());
    }
    tracing::info!("Backfilling word_counts from slack_messages...");
    client
        .query(
            "INSERT INTO word_counts (word, user_id, channel_id, message_ts, count)
             SELECT word, user_id, channel_id, message_ts, count()
             FROM (
                 SELECT arrayJoin(extractAll(lower(text), '[a-z]+')) AS word,
                        user_id, channel_id, message_ts
                 FROM slack_messages
             )
             WHERE length(word) > 1
             GROUP BY word, user_id, channel_id, message_ts",
        )
        .execute()
        .await?;
    mark_word_counts_backfilled(client).await?;
    tracing::info!("word_counts backfill complete");
    Ok(())
}

/// Whether the word_counts one-time backfill has already completed, read from
/// `backfill_meta`. A transient DB failure falls back to false, which only
/// re-attempts the backfill.
async fn word_counts_backfilled(client: &Client) -> bool {
    #[derive(Debug, Row, Deserialize)]
    struct BackfillMetaRead {
        done: u8,
    }
    client
        .query("SELECT done FROM backfill_meta FINAL WHERE name = 'word_counts'")
        .fetch_optional()
        .await
        .ok()
        .flatten()
        .map(|r: BackfillMetaRead| r.done == 1)
        .unwrap_or(false)
}

/// Records that the word_counts backfill is complete.
async fn mark_word_counts_backfilled(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Serialize)]
    struct BackfillMetaRow {
        name: String,
        done: u8,
    }
    let mut insert = client.insert::<BackfillMetaRow>("backfill_meta").await?;
    insert
        .write(&BackfillMetaRow {
            name: "word_counts".to_string(),
            done: 1,
        })
        .await?;
    insert.end().await?;
    Ok(())
}

/// Recomputes scores only for users who are missing from `user_scores` or have
/// messages newer than their last recompute, so a restart doesn't trigger a full
/// recompute over every user (hours of full-table scans on slow hardware). A full
/// recompute still runs when the sessionizer changed since the last one, and once
/// after a migration to fill in columns added to `user_scores` (marked by
/// `longest = 0`, since real users always have a session worth more than zero).
/// Reads `score_meta` once so both startup backfills can decide a full recompute
/// up front; the user backfill writes the row after it finishes, so the check must
/// happen before either backfill runs.
pub async fn sessionizer_changed(client: &Client) -> Result<bool, Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct ScoreMetaRead {
        formula: String,
    }

    let stored: Option<ScoreMetaRead> = client
        .query("SELECT formula FROM score_meta FINAL WHERE id = 1")
        .fetch_optional()
        .await?;
    Ok(stored.as_ref().map(|m| m.formula.clone()) != Some(sessionizer_fingerprint()))
}

/// Fingerprint of the sessionizer parameters, stored in `score_meta.formula`.
/// Changing any constant in `src/sessionize.rs` flips this and triggers one full
/// recompute of all user and channel scores on the next restart.
fn sessionizer_fingerprint() -> String {
    format!(
        "sessionize:{}:{}:{}:{}",
        crate::sessionize::SESSION_GAP_BOUNDARY_SECS,
        crate::sessionize::MESSAGE_TYPING_CHARS_PER_SEC,
        crate::sessionize::MESSAGE_READ_OVERHEAD_SECS,
        crate::sessionize::SESSION_MAX_SECS,
    )
}

pub async fn backfill_stale_user_scores(
    client: &Client,
    force_full: bool,
) -> Result<usize, Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct UserRow {
        user_id: String,
    }

    #[derive(Debug, Row, Serialize)]
    struct ScoreMetaRow {
        id: u8,
        formula: String,
    }

    if force_full {
        tracing::info!("Sessionizer changed, recomputing scores for all users");
        let ids: Vec<String> = client
            .query("SELECT DISTINCT user_id FROM slack_messages_by_user")
            .fetch_all::<UserRow>()
            .await?
            .into_iter()
            .map(|r| r.user_id)
            .collect();
        recompute_user_scores(client, &ids).await?;
        let mut insert = client.insert::<ScoreMetaRow>("score_meta").await?;
        insert
            .write(&ScoreMetaRow {
                id: 1,
                formula: sessionizer_fingerprint(),
            })
            .await?;
        insert.end().await?;
        return Ok(ids.len());
    }

    let ids: Vec<String> = client
        .query(
            "SELECT msg.user_id FROM (
                 SELECT user_id, max(message_ts) AS last_ts
                 FROM slack_messages_by_user
                 GROUP BY user_id
             ) msg
             LEFT JOIN (SELECT user_id, updated, longest FROM user_scores FINAL) sc
               ON msg.user_id = sc.user_id
             WHERE sc.user_id IS NULL OR toUInt64(msg.last_ts / 1000000) > sc.updated OR sc.longest = 0",
        )
        .fetch_all::<UserRow>()
        .await?
        .into_iter()
        .map(|r| r.user_id)
        .collect();

    if ids.is_empty() {
        tracing::info!("All users already have fresh Slack Time scores, skipping backfill");
        return Ok(0);
    }
    tracing::info!(
        "Backfilling Slack Time scores for {} stale/missing users",
        ids.len()
    );
    recompute_user_scores(client, &ids).await?;
    Ok(ids.len())
}

pub async fn backfill_stale_channel_scores(
    client: &Client,
    force_full: bool,
) -> Result<usize, Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct ChannelRow {
        channel_id: String,
    }

    if force_full {
        tracing::info!("Sessionizer changed, recomputing channel scores for all channels");
        return recompute_channel_scores(client, &[]).await;
    }

    let ids: Vec<String> = client
        .query(
            "SELECT msg.channel_id FROM (
                 SELECT channel_id, max(message_ts) AS last_ts
                 FROM slack_messages_by_user
                 GROUP BY channel_id
             ) msg
             LEFT JOIN (SELECT channel_id, updated FROM channel_scores FINAL) sc
               ON msg.channel_id = sc.channel_id
             WHERE sc.channel_id IS NULL OR toUInt64(msg.last_ts / 1000000) > sc.updated",
        )
        .fetch_all::<ChannelRow>()
        .await?
        .into_iter()
        .map(|r| r.channel_id)
        .collect();

    if ids.is_empty() {
        return Ok(0);
    }
    tracing::info!(
        "Backfilling Slack Time scores for {} stale/missing channels",
        ids.len()
    );
    recompute_channel_scores(client, &ids).await
}

async fn migrate_slack_messages(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct CountRow {
        count: u64,
    }

    // Check if table is already ReplacingMergeTree
    let existing: Option<CountRow> = client
        .query("SELECT count() as count FROM system.tables WHERE database = currentDatabase() AND name = 'slack_messages' AND engine = 'ReplacingMergeTree'")
        .fetch_optional()
        .await
        .unwrap_or(None);

    if existing.map(|r| r.count > 0).unwrap_or(false) {
        return Ok(());
    }

    let pre_count: u64 = client
        .query("SELECT count() FROM slack_messages")
        .fetch_one()
        .await
        .unwrap_or(0);

    if pre_count == 0 {
        return Ok(());
    }

    tracing::info!(
        "Migrating slack_messages to ReplacingMergeTree ({} rows)...",
        pre_count
    );

    client
        .query(
            "CREATE TABLE IF NOT EXISTS slack_messages_new (
            user_id String,
            channel_id String,
            message_ts String,
            text String,
            thread_ts Nullable(String)
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (channel_id, message_ts)",
        )
        .execute()
        .await?;

    client
        .query("INSERT INTO slack_messages_new SELECT * FROM slack_messages")
        .execute()
        .await?;

    client
        .query("DROP TABLE IF EXISTS slack_messages")
        .execute()
        .await?;

    client
        .query("RENAME TABLE slack_messages_new TO slack_messages")
        .execute()
        .await?;

    client
        .query("OPTIMIZE TABLE slack_messages FINAL")
        .execute()
        .await?;

    let dedupd: u64 = client
        .query("SELECT count() FROM slack_messages")
        .fetch_one()
        .await
        .unwrap_or(0);

    tracing::info!(
        "Migration complete: {} rows -> {} rows after dedup",
        pre_count,
        dedupd
    );
    Ok(())
}

async fn migrate_slack_messages_ts(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct TypeRow {
        type_: String,
    }

    let current: Option<TypeRow> = client
        .query(
            "SELECT type AS type_ FROM system.columns
             WHERE database = currentDatabase() AND table = 'slack_messages' AND name = 'message_ts'",
        )
        .fetch_optional()
        .await?;

    if current
        .as_ref()
        .map(|r| r.type_.as_str() == "UInt64")
        .unwrap_or(false)
    {
        return Ok(());
    }
    if current.is_none() {
        return Ok(());
    }

    tracing::info!("Migrating slack_messages.message_ts to UInt64...");

    client
        .query("DROP TABLE IF EXISTS slack_messages_new")
        .execute()
        .await?;
    client
        .query(
            "CREATE TABLE IF NOT EXISTS slack_messages_new (
            user_id String,
            channel_id String,
            message_ts UInt64,
            text String,
            thread_ts Nullable(String)
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (channel_id, message_ts)",
        )
        .execute()
        .await?;

    client
        .query(
            "INSERT INTO slack_messages_new
             SELECT user_id,
                    channel_id,
                    toUInt64OrZero(splitByChar('.', message_ts)[1]) * 1000000
                        + toUInt64OrZero(splitByChar('.', message_ts)[2]) AS message_ts,
                    text,
                    thread_ts
             FROM slack_messages",
        )
        .execute()
        .await?;

    client
        .query("DROP TABLE IF EXISTS slack_messages_old")
        .execute()
        .await?;
    client
        .query("RENAME TABLE slack_messages TO slack_messages_old, slack_messages_new TO slack_messages")
        .execute()
        .await?;
    client
        .query("DROP TABLE IF EXISTS slack_messages_old")
        .execute()
        .await?;
    client
        .query("OPTIMIZE TABLE slack_messages FINAL")
        .execute()
        .await?;

    let count: u64 = client
        .query("SELECT count() FROM slack_messages")
        .fetch_one()
        .await
        .unwrap_or(0);
    tracing::info!(
        "slack_messages.message_ts migration complete ({} rows)",
        count
    );
    Ok(())
}

async fn migrate_coding_activity_date(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct TypeRow {
        type_: String,
    }

    let current: Option<TypeRow> = client
        .query(
            "SELECT type AS type_ FROM system.columns
             WHERE database = currentDatabase() AND table = 'coding_activity' AND name = 'date'",
        )
        .fetch_optional()
        .await?;

    if current
        .as_ref()
        .map(|r| r.type_.as_str() == "Date")
        .unwrap_or(false)
    {
        return Ok(());
    }
    if current.is_none() {
        return Ok(());
    }

    tracing::info!("Migrating coding_activity.date to Date...");

    client
        .query("DROP TABLE IF EXISTS coding_activity_new")
        .execute()
        .await?;
    client
        .query(
            "CREATE TABLE IF NOT EXISTS coding_activity_new (
            user_id String,
            date Date,
            minutes Int64,
            language Nullable(String)
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (user_id, date)",
        )
        .execute()
        .await?;

    client
        .query(
            "INSERT INTO coding_activity_new
             SELECT user_id, toDateOrZero(date), minutes, language FROM coding_activity",
        )
        .execute()
        .await?;

    client
        .query("DROP TABLE IF EXISTS coding_activity_old")
        .execute()
        .await?;
    client
        .query("RENAME TABLE coding_activity TO coding_activity_old, coding_activity_new TO coding_activity")
        .execute()
        .await?;
    client
        .query("DROP TABLE IF EXISTS coding_activity_old")
        .execute()
        .await?;
    client
        .query("OPTIMIZE TABLE coding_activity FINAL")
        .execute()
        .await?;

    let count: u64 = client
        .query("SELECT count() FROM coding_activity")
        .fetch_one()
        .await
        .unwrap_or(0);
    tracing::info!("coding_activity.date migration complete ({} rows)", count);
    Ok(())
}

pub async fn insert_messages(
    client: &Client,
    messages: &[SlackMessageRow],
) -> Result<u64, Box<dyn std::error::Error>> {
    if messages.is_empty() {
        return Ok(0);
    }
    let count = messages.len() as u64;
    let mut insert = client.insert::<SlackMessageRow>("slack_messages").await?;
    for msg in messages {
        insert.write(msg).await?;
    }
    insert.end().await?;
    Ok(count)
}

/// Stores a fresh reactions snapshot for the touched messages. Reactions are
/// whatever the fetch returned at that moment, so rows for each touched message
/// are cleared first (chunked, mutations_sync = 2) and then re-inserted, keeping
/// counts accurate when reactions are added or removed between passes.
pub async fn insert_reactions(
    client: &Client,
    rows: &[SlackReactionRow],
) -> Result<u64, Box<dyn std::error::Error>> {
    if rows.is_empty() {
        return Ok(0);
    }
    let mut touched: Vec<(String, u64)> = rows
        .iter()
        .map(|r| (r.channel_id.clone(), r.message_ts))
        .collect();
    touched.sort();
    touched.dedup();
    for chunk in touched.chunks(500) {
        let tuples: Vec<String> = chunk
            .iter()
            .map(|(channel_id, ts)| format!("('{}', {})", channel_id, ts))
            .collect();
        client
            .query(&format!(
                "ALTER TABLE slack_reactions DELETE WHERE (channel_id, message_ts) IN ({}) SETTINGS mutations_sync = 2",
                tuples.join(", ")
            ))
            .execute()
            .await?;
    }
    let count = rows.len() as u64;
    let mut insert = client.insert::<SlackReactionRow>("slack_reactions").await?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(count)
}

#[derive(Debug, Clone, Row, Serialize)]
pub struct WordCountRow {
    pub word: String,
    pub user_id: String,
    pub channel_id: String,
    pub message_ts: u64,
    pub count: u64,
    pub inserted_at: u64,
}

/// Stores per-message word frequencies for newly inserted messages. Reads use
/// FINAL on the ReplacingMergeTree, so the same (word, channel, message) key
/// written twice never double-counts. `inserted_at` is stamped here, so the
/// incremental `word_totals` refresh can find rows regardless of `message_ts`.
pub async fn insert_word_counts(
    client: &Client,
    rows: &[WordCountRow],
) -> Result<u64, Box<dyn std::error::Error>> {
    if rows.is_empty() {
        return Ok(0);
    }
    let now = now_secs();
    let count = rows.len() as u64;
    let mut insert = client.insert::<WordCountRow>("word_counts").await?;
    for row in rows {
        let mut row = row.clone();
        row.inserted_at = now;
        insert.write(&row).await?;
    }
    insert.end().await?;
    Ok(count)
}

/// Seconds since the UNIX epoch, used for the `word_counts.inserted_at` stamp
/// and the `word_totals.updated` version.
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// How often the `word_totals` refresh falls back to a full rebuild. The
/// incremental pass recomputes only words with rows inserted since the last
/// fold, which misses nothing for new data but would let a row inserted in the
/// same instant as a fold slip through (and cannot undo a word whose author
/// was just flagged bot/deleted), so a daily full rebuild is the safety net.
const WORD_FULL_REBUILD_SECS: u64 = 24 * 3600;

/// Rebuilds the materialized `word_totals` summary from `word_counts`, so the
/// Top Words leaderboard reads one small row per word instead of re-aggregating
/// every word row on every page load. Only words whose count changed are
/// re-inserted (as a new ReplacingMergeTree version), then the table is
/// compacted back to one version per word. Runs in the background on a schedule:
/// each pass folds just the words touched since the last fold (tracked in
/// `word_refresh_meta`), with a daily full rebuild as the safety net.
pub async fn refresh_word_totals(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    let now = now_secs();
    let (watermark, last_full) = read_word_refresh_meta(client).await;

    // First run, or the safety-net rebuild is due: recompute the whole table.
    // The watermark is set to now rather than max(inserted_at), because rows
    // backfilled before the `inserted_at` column existed all carry 0 and must
    // count as folded, not as dirty on every pass.
    if watermark == 0 || now.saturating_sub(last_full) >= WORD_FULL_REBUILD_SECS {
        refresh_word_totals_full(client, now).await?;
        write_word_refresh_meta(client, now, now).await?;
        return Ok(());
    }

    let words = dirty_words(client, watermark).await?;
    if words.is_empty() {
        // Nothing new since the last fold; advance the watermark so the scan
        // does not re-read the same rows next pass.
        write_word_refresh_meta(client, now, last_full).await?;
        return Ok(());
    }
    refresh_word_totals_for_words(client, &words, now).await?;
    write_word_refresh_meta(client, now, last_full).await?;
    Ok(())
}

/// The `word_refresh_meta` row's watermark and last full rebuild, or (0, 0)
/// when no fold has happened yet.
async fn read_word_refresh_meta(client: &Client) -> (u64, u64) {
    #[derive(Debug, Row, Deserialize)]
    struct MetaRow {
        watermark: u64,
        last_full: u64,
    }
    client
        .query("SELECT watermark, last_full FROM word_refresh_meta FINAL WHERE id = 1")
        .fetch_optional()
        .await
        .ok()
        .flatten()
        .map(|r: MetaRow| (r.watermark, r.last_full))
        .unwrap_or((0, 0))
}

/// Records the refresh progress. `last_full` is kept from the last full rebuild
/// (passed back in on incremental passes) so it does not get bumped every 30m.
async fn write_word_refresh_meta(
    client: &Client,
    watermark: u64,
    last_full: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Serialize)]
    struct MetaRow {
        id: u8,
        watermark: u64,
        last_full: u64,
    }
    let mut insert = client.insert::<MetaRow>("word_refresh_meta").await?;
    insert
        .write(&MetaRow {
            id: 1,
            watermark,
            last_full,
        })
        .await?;
    insert.end().await?;
    Ok(())
}

/// Words with any `word_counts` row inserted after the watermark.
async fn dirty_words(
    client: &Client,
    watermark: u64,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct WordRow {
        word: String,
    }
    Ok(client
        .query(&format!(
            "SELECT DISTINCT word FROM word_counts WHERE inserted_at > {}",
            watermark
        ))
        .fetch_all::<WordRow>()
        .await?
        .into_iter()
        .map(|r| r.word)
        .collect())
}

/// Recomputes the totals for the given words (the ones touched since the last
/// fold) and compacts `word_totals`. The `word IN (...)` filter runs on the
/// table's primary key, so this is a few granules, not a full-table scan.
async fn refresh_word_totals_for_words(
    client: &Client,
    words: &[String],
    updated: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let in_list = words
        .iter()
        .map(|w| format!("'{}'", w.replace('\'', "\\'")))
        .collect::<Vec<_>>()
        .join(", ");
    client
        .query(&format!(
            "INSERT INTO word_totals (word, cnt, updated)
             SELECT w.word, w.cnt, {updated}
             FROM (
                 SELECT word, sum(count) AS cnt
                 FROM word_counts FINAL
                 WHERE word IN ({in_list})
                   AND user_id NOT IN (SELECT user_id FROM users FINAL
                                       WHERE is_bot = 1 OR is_deleted = 1)
                 GROUP BY word
             ) AS w
             LEFT JOIN (SELECT word, cnt FROM word_totals FINAL) AS t
               ON w.word = t.word
             WHERE t.word IS NULL OR t.cnt != w.cnt"
        ))
        .execute()
        .await?;
    client
        .query("OPTIMIZE TABLE word_totals FINAL")
        .execute()
        .await
        .ok();
    Ok(())
}

/// Recomputes the totals for every word from scratch.
async fn refresh_word_totals_full(
    client: &Client,
    updated: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    client
        .query(&format!(
            "INSERT INTO word_totals (word, cnt, updated)
             SELECT w.word, w.cnt, {updated}
             FROM (
                 SELECT word, sum(count) AS cnt
                 FROM word_counts FINAL
                 WHERE user_id NOT IN (SELECT user_id FROM users FINAL
                                       WHERE is_bot = 1 OR is_deleted = 1)
                 GROUP BY word
             ) AS w
             LEFT JOIN (SELECT word, cnt FROM word_totals FINAL) AS t
               ON w.word = t.word
             WHERE t.word IS NULL OR t.cnt != w.cnt"
        ))
        .execute()
        .await?;
    client
        .query("OPTIMIZE TABLE word_totals FINAL")
        .execute()
        .await
        .ok();
    Ok(())
}

/// Recomputes Slack Time scores for the given users (or every user when `user_ids`
/// is empty) and upserts them into `user_scores`. The leaderboard reads top 100
/// from `user_scores`, so scores only need to be refreshed when new messages arrive.
const SCORE_RECOMPUTE_CHUNK: usize = 50;

pub async fn recompute_user_scores(
    client: &Client,
    user_ids: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let full = user_ids.is_empty();
    let ids: Vec<String> = if full {
        #[derive(Debug, Row, Deserialize)]
        struct UserRow {
            user_id: String,
        }

        let ids = client
            .query("SELECT DISTINCT user_id FROM slack_messages_by_user")
            .fetch_all::<UserRow>()
            .await?
            .into_iter()
            .map(|r| r.user_id)
            .collect::<Vec<String>>();
        tracing::info!("Backfilling Slack Time scores for all {} users", ids.len());
        ids
    } else {
        let mut ids: Vec<String> = user_ids.to_vec();
        ids.sort();
        ids.dedup();
        ids
    };

    let start = std::time::Instant::now();
    let mut done = 0usize;
    for chunk in ids.chunks(SCORE_RECOMPUTE_CHUNK) {
        done += recompute_user_scores_chunk(client, chunk).await?;
        if full {
            tracing::debug!("Backfill progress: {}/{} users recomputed", done, ids.len());
        }
    }
    if done > 0 {
        tracing::info!(
            "Recomputed Slack Time scores for {} users in {:.1}s",
            done,
            start.elapsed().as_secs_f64()
        );
    }
    Ok(())
}

async fn recompute_user_scores_chunk(
    client: &Client,
    ids: &[String],
) -> Result<usize, Box<dyn std::error::Error>> {
    let where_clause = format!("WHERE user_id IN ('{}')", ids.join("', '"));
    let boundary = crate::sessionize::SESSION_GAP_BOUNDARY_SECS;
    let rate = crate::sessionize::MESSAGE_TYPING_CHARS_PER_SEC;
    let overhead = crate::sessionize::MESSAGE_READ_OVERHEAD_SECS;
    let max_secs = crate::sessionize::SESSION_MAX_SECS;

    #[derive(Debug, Row, Deserialize)]
    struct MetricsRow {
        user_id: String,
        total_time: u64,
        longest: u64,
        sessions: u64,
        days: u64,
    }

    let metrics: Vec<MetricsRow> = client
        .query(&format!(
            "WITH
             msg AS (
                 SELECT user_id, toInt64(message_ts / 1000000) AS ts,
                        sum(char_length(text)) AS chars,
                        count() AS msgs
                 FROM slack_messages_by_user
                 {}
                 GROUP BY user_id, ts
             ),
             flagged AS (
                 SELECT user_id, ts, chars, msgs,
                     if(ts - lag(ts) OVER (PARTITION BY user_id ORDER BY ts) > {boundary}, 1, 0) AS boundary
                 FROM msg
             ),
             sess AS (
                 SELECT user_id, ts, chars, msgs,
                     sum(boundary) OVER (PARTITION BY user_id ORDER BY ts) AS sid
                 FROM flagged
             ),
             sessions AS (
                 SELECT user_id, sid, min(ts) AS start_ts, max(ts) AS end_ts,
                        argMin(chars, ts) AS first_chars,
                        argMin(msgs, ts) AS first_msgs
                 FROM sess
                 GROUP BY user_id, sid
             )
             SELECT user_id,
                    sum(toUInt64(least(end_ts - start_ts + (first_chars + {rate} - 1) / {rate} + first_msgs * {overhead}, {max_secs}))) AS total_time,
                    max(toUInt64(least(end_ts - start_ts + (first_chars + {rate} - 1) / {rate} + first_msgs * {overhead}, {max_secs}))) AS longest,
                    count() AS sessions,
                    greatest(toUInt64(dateDiff('day', toDateTime(min(start_ts)), toDateTime(max(start_ts))) + 1), 1) AS days
             FROM sessions
             GROUP BY user_id
             SETTINGS max_bytes_before_external_sort = 268435456",
            where_clause
        ))
        .fetch_all()
        .await?;

    #[derive(Debug, Clone, Row, Deserialize)]
    struct CountRow {
        user_id: String,
        messages: u64,
        channels: u64,
        first_ts: u64,
        last_ts: u64,
    }

    let counts: Vec<CountRow> = client
        .query(&format!(
            "SELECT user_id, count() AS messages,
                    uniqExact(channel_id) AS channels,
                    min(message_ts) AS first_ts,
                    max(message_ts) AS last_ts
             FROM slack_messages_by_user
             {}
             GROUP BY user_id",
            where_clause
        ))
        .fetch_all()
        .await?;

    let count_map: HashMap<String, CountRow> =
        counts.into_iter().map(|r| (r.user_id.clone(), r)).collect();

    #[derive(Debug, Row, Deserialize)]
    struct HourRow {
        user_id: String,
        active_hour: u8,
    }

    let hours: Vec<HourRow> = client
        .query(&format!(
            "SELECT user_id, argMax(hour, cnt) AS active_hour
             FROM (
                 SELECT user_id, toHour(toDateTime(message_ts / 1000000)) AS hour,
                        count() AS cnt
                 FROM slack_messages_by_user
                 {}
                 GROUP BY user_id, hour
             )
             GROUP BY user_id",
            where_clause
        ))
        .fetch_all()
        .await?;

    let hour_map: HashMap<String, u8> = hours
        .into_iter()
        .map(|r| (r.user_id, r.active_hour))
        .collect();

    let updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    #[derive(Debug, Row, Serialize)]
    struct ScoreRow {
        user_id: String,
        score: i64,
        total_time: u64,
        messages: u64,
        sessions: u64,
        longest: u64,
        days: u64,
        channels: u64,
        first_ts: u64,
        last_ts: u64,
        active_hour: u8,
        updated: u64,
    }

    let rows: Vec<ScoreRow> = metrics
        .into_iter()
        .map(|m| {
            let count = count_map.get(&m.user_id).cloned().unwrap_or(CountRow {
                user_id: String::new(),
                messages: 0,
                channels: 0,
                first_ts: 0,
                last_ts: 0,
            });
            let active_hour = hour_map.get(&m.user_id).copied().unwrap_or(0);
            ScoreRow {
                user_id: m.user_id,
                score: m.total_time as i64,
                total_time: m.total_time,
                messages: count.messages,
                sessions: m.sessions,
                longest: m.longest,
                days: m.days,
                channels: count.channels,
                first_ts: count.first_ts,
                last_ts: count.last_ts,
                active_hour,
                updated,
            }
        })
        .collect();

    if rows.is_empty() {
        return Ok(0);
    }

    let mut insert = client.insert::<ScoreRow>("user_scores").await?;
    for r in &rows {
        insert.write(r).await?;
    }
    insert.end().await?;
    Ok(rows.len())
}

pub async fn recompute_channel_scores(
    client: &Client,
    channel_ids: &[String],
) -> Result<usize, Box<dyn std::error::Error>> {
    let full = channel_ids.is_empty();
    let ids: Vec<String> = if full {
        #[derive(Debug, Row, Deserialize)]
        struct ChannelRow {
            channel_id: String,
        }

        let ids = client
            .query("SELECT DISTINCT channel_id FROM slack_messages_by_user")
            .fetch_all::<ChannelRow>()
            .await?
            .into_iter()
            .map(|r| r.channel_id)
            .collect::<Vec<String>>();
        tracing::info!(
            "Backfilling Slack Time scores for all {} channels",
            ids.len()
        );
        ids
    } else {
        let mut ids: Vec<String> = channel_ids.to_vec();
        ids.sort();
        ids.dedup();
        ids
    };

    let start = std::time::Instant::now();
    let mut done = 0usize;
    for chunk in ids.chunks(SCORE_RECOMPUTE_CHUNK) {
        done += recompute_channel_scores_chunk(client, chunk).await?;
        if full {
            tracing::debug!(
                "Channel score backfill progress: {}/{} channels",
                done,
                ids.len()
            );
        }
    }
    if done > 0 {
        tracing::info!(
            "Recomputed Slack Time scores for {} channels in {:.1}s",
            done,
            start.elapsed().as_secs_f64()
        );
    }
    Ok(done)
}

async fn recompute_channel_scores_chunk(
    client: &Client,
    ids: &[String],
) -> Result<usize, Box<dyn std::error::Error>> {
    if ids.is_empty() {
        return Ok(0);
    }
    let in_list = format!("'{}'", ids.join("', '"));
    let scope = format!("channel_id IN ({})", in_list);
    let exclude_bots_deleted =
        "user_id NOT IN (SELECT user_id FROM users FINAL WHERE is_bot = 1 OR is_deleted = 1)";
    let boundary = crate::sessionize::SESSION_GAP_BOUNDARY_SECS;
    let rate = crate::sessionize::MESSAGE_TYPING_CHARS_PER_SEC;
    let overhead = crate::sessionize::MESSAGE_READ_OVERHEAD_SECS;
    let max_secs = crate::sessionize::SESSION_MAX_SECS;

    #[derive(Debug, Row, Deserialize)]
    struct SessionRow {
        channel_id: String,
        total_time: u64,
    }

    #[derive(Debug, Row, Deserialize)]
    struct CountRow {
        channel_id: String,
        messages: u64,
    }

    let sessions: Vec<SessionRow> = client
        .query(&format!(
            "WITH
             msg AS (
                 SELECT channel_id, toInt64(message_ts / 1000000) AS ts,
                        sum(char_length(text)) AS chars,
                        count() AS msgs
                 FROM slack_messages_by_user
                 WHERE {scope} AND {exclude_bots_deleted}
                 GROUP BY channel_id, ts
             ),
             flagged AS (
                 SELECT channel_id, ts, chars, msgs,
                        if(ts - lag(ts) OVER (PARTITION BY channel_id ORDER BY ts) > {boundary}, 1, 0) AS boundary
                 FROM msg
             ),
             sess AS (
                 SELECT channel_id, ts, chars, msgs,
                        sum(boundary) OVER (PARTITION BY channel_id ORDER BY ts) AS sid
                 FROM flagged
             ),
             sessions AS (
                 SELECT channel_id, sid, min(ts) AS start_ts, max(ts) AS end_ts,
                        argMin(chars, ts) AS first_chars,
                        argMin(msgs, ts) AS first_msgs
                 FROM sess
                 GROUP BY channel_id, sid
             )
             SELECT channel_id,
                    sum(toUInt64(least(end_ts - start_ts + (first_chars + {rate} - 1) / {rate} + first_msgs * {overhead}, {max_secs}))) AS total_time
             FROM sessions
             GROUP BY channel_id
             SETTINGS max_bytes_before_external_sort = 268435456"
        ))
        .fetch_all()
        .await?;

    let counts: Vec<CountRow> = client
        .query(&format!(
            "SELECT channel_id, count() AS messages
             FROM slack_messages_by_user
             WHERE {scope} AND {exclude_bots_deleted}
             GROUP BY channel_id"
        ))
        .fetch_all()
        .await?;
    let count_map: std::collections::HashMap<String, u64> = counts
        .into_iter()
        .map(|r| (r.channel_id, r.messages))
        .collect();

    let updated = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    #[derive(Debug, Row, Serialize)]
    struct ChannelScoreRow {
        channel_id: String,
        total_time: u64,
        messages: u64,
        updated: u64,
    }

    let rows: Vec<ChannelScoreRow> = sessions
        .into_iter()
        .map(|r| {
            let channel_id = r.channel_id;
            ChannelScoreRow {
                channel_id: channel_id.clone(),
                total_time: r.total_time,
                messages: count_map.get(&channel_id).copied().unwrap_or(0),
                updated,
            }
        })
        .collect();

    if rows.is_empty() {
        return Ok(0);
    }

    let mut insert = client.insert::<ChannelScoreRow>("channel_scores").await?;
    for r in &rows {
        insert.write(r).await?;
    }
    insert.end().await?;
    Ok(rows.len())
}

pub async fn get_known_channel_ids(
    client: &Client,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let rows: Vec<ChannelIdRow> = client
        .query("SELECT channel_id FROM slack_channels FINAL")
        .fetch_all()
        .await?;

    Ok(rows.into_iter().map(|r| r.channel_id).collect())
}

pub async fn insert_new_channels(
    client: &Client,
    channels: &[SlackChannelRow],
) -> Result<u64, Box<dyn std::error::Error>> {
    let known = get_known_channel_ids(client).await?;
    let new_channels: Vec<&SlackChannelRow> = channels
        .iter()
        .filter(|ch| !known.contains(&ch.channel_id))
        .collect();

    if new_channels.is_empty() {
        return Ok(0);
    }

    let count = new_channels.len() as u64;
    let mut insert = client.insert::<SlackChannelRow>("slack_channels").await?;
    for ch in new_channels {
        insert.write(ch).await?;
    }
    insert.end().await?;

    tracing::info!("Inserted {} new channels into ClickHouse", count);
    Ok(count)
}

#[derive(Debug, Row, Serialize)]
pub struct SlackUserRow {
    pub user_id: String,
    pub display_name: String,
    pub pfp: String,
    pub updated: u64,
    pub is_bot: u8,
    pub is_deleted: u8,
}

pub async fn upsert_users(
    client: &Client,
    users: &[SlackUserRow],
) -> Result<(), Box<dyn std::error::Error>> {
    if users.is_empty() {
        return Ok(());
    }
    let mut insert = client.insert::<SlackUserRow>("users").await?;
    for u in users {
        insert.write(u).await?;
    }
    insert.end().await?;
    Ok(())
}

pub async fn get_user_updates(
    client: &Client,
) -> Result<HashMap<String, u64>, Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct UserUpdateRow {
        user_id: String,
        updated: u64,
    }
    let rows: Vec<UserUpdateRow> = client
        .query("SELECT user_id, updated FROM users FINAL")
        .fetch_all()
        .await?;
    Ok(rows.into_iter().map(|r| (r.user_id, r.updated)).collect())
}

pub async fn get_user_ids_without_pfp(
    client: &Client,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct PfpRow {
        user_id: String,
    }
    let rows: Vec<PfpRow> = client
        .query("SELECT user_id FROM users FINAL WHERE pfp = ''")
        .fetch_all()
        .await?;
    Ok(rows.into_iter().map(|r| r.user_id).collect())
}

pub async fn get_max_message_ts(
    client: &Client,
    channel_id: &str,
) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    let row: Option<u64> = client
        .query(&format!(
            "SELECT max(message_ts) FROM slack_messages WHERE channel_id = '{}'",
            channel_id
        ))
        .fetch_optional()
        .await?;

    Ok(row)
}

pub async fn get_scraped_channel_ids(
    client: &Client,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let rows: Vec<ChannelIdRow> = client
        .query("SELECT channel_id FROM scraped_channels")
        .fetch_all()
        .await?;

    Ok(rows.into_iter().map(|r| r.channel_id).collect())
}

pub async fn mark_channels_scraped(
    client: &Client,
    channel_ids: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    if channel_ids.is_empty() {
        return Ok(());
    }

    #[derive(Debug, Row, Serialize)]
    struct ScrapedRow {
        channel_id: String,
    }

    let mut insert = client.insert::<ScrapedRow>("scraped_channels").await?;
    for id in channel_ids {
        insert
            .write(&ScrapedRow {
                channel_id: id.clone(),
            })
            .await?;
    }
    insert.end().await?;

    tracing::info!("Recorded {} channels as scraped", channel_ids.len());
    Ok(())
}

pub async fn mark_channel_scraped(
    client: &Client,
    channel_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Serialize)]
    struct ScrapedRow {
        channel_id: String,
    }

    let mut insert = client.insert::<ScrapedRow>("scraped_channels").await?;
    insert
        .write(&ScrapedRow {
            channel_id: channel_id.to_string(),
        })
        .await?;
    insert.end().await?;
    Ok(())
}

pub async fn backfill_scraped_channels(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    let rows: Vec<ChannelIdRow> = client
        .query(
            "SELECT channel_id FROM (
                SELECT channel_id FROM scrape_checkpoints FINAL WHERE fully_scraped = 1
                UNION DISTINCT
                SELECT DISTINCT channel_id FROM slack_messages
            )
            WHERE channel_id NOT IN (SELECT channel_id FROM scraped_channels)",
        )
        .fetch_all()
        .await?;

    if rows.is_empty() {
        return Ok(());
    }

    let ids: Vec<String> = rows.into_iter().map(|r| r.channel_id).collect();
    tracing::info!(
        "Backfilling {} previously-scraped channels into scraped_channels",
        ids.len()
    );
    mark_channels_scraped(client, &ids).await?;
    Ok(())
}

pub async fn is_fully_scraped(
    client: &Client,
    channel_id: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let count: u64 = client
        .query(&format!(
            "SELECT count() FROM scrape_checkpoints FINAL WHERE channel_id = '{}' AND fully_scraped = 1",
            channel_id
        ))
        .fetch_one()
        .await?;

    Ok(count > 0)
}

pub async fn mark_fully_scraped(
    client: &Client,
    channel_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Serialize)]
    struct CheckpointRow {
        channel_id: String,
        fully_scraped: u8,
    }

    let mut insert = client.insert::<CheckpointRow>("scrape_checkpoints").await?;
    insert
        .write(&CheckpointRow {
            channel_id: channel_id.to_string(),
            fully_scraped: 1,
        })
        .await?;
    insert.end().await?;
    Ok(())
}

pub async fn is_thread_fully_scraped(
    client: &Client,
    channel_id: &str,
    thread_ts: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let count: u64 = client
        .query(&format!(
            "SELECT count() FROM thread_checkpoints FINAL WHERE channel_id = '{}' AND thread_ts = '{}' AND fully_scraped = 1",
            channel_id, thread_ts
        ))
        .fetch_one()
        .await?;
    Ok(count > 0)
}

pub async fn mark_thread_fully_scraped(
    client: &Client,
    channel_id: &str,
    thread_ts: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Serialize)]
    struct ThreadCheckpointRow {
        channel_id: String,
        thread_ts: String,
        fully_scraped: u8,
    }

    let mut insert = client
        .insert::<ThreadCheckpointRow>("thread_checkpoints")
        .await?;
    insert
        .write(&ThreadCheckpointRow {
            channel_id: channel_id.to_string(),
            thread_ts: thread_ts.to_string(),
            fully_scraped: 1,
        })
        .await?;
    insert.end().await?;
    Ok(())
}

pub async fn get_max_thread_reply_ts(
    client: &Client,
    channel_id: &str,
    thread_ts: &str,
) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    let row: Option<u64> = client
        .query(&format!(
            "SELECT max(message_ts) FROM slack_messages WHERE channel_id = '{}' AND thread_ts = '{}'",
            channel_id, thread_ts
        ))
        .fetch_optional()
        .await?;

    Ok(row)
}

/// All distinct thread root timestamps stored for a channel. Used by the
/// thread-reply recovery pass to re-fetch threads whose first-scrape thread
/// phase was interrupted, since their roots are older than the rescan window.
pub async fn get_thread_roots(
    client: &Client,
    channel_id: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct ThreadRootRow {
        thread_ts: String,
    }

    let rows: Vec<ThreadRootRow> = client
        .query(&format!(
            "SELECT DISTINCT thread_ts FROM slack_messages \
             WHERE channel_id = '{}' AND thread_ts IS NOT NULL AND thread_ts != ''",
            channel_id
        ))
        .fetch_all()
        .await?;

    Ok(rows.into_iter().map(|r| r.thread_ts).collect())
}

#[derive(Debug, Row, Serialize)]
pub struct HackatimeConnectionRow {
    pub slack_id: String,
    pub access_token: String,
    pub last_synced_date: Option<String>,
}

#[derive(Debug, Row, Deserialize)]
pub struct HackatimeConnectionReadRow {
    pub slack_id: String,
    pub access_token: String,
    pub last_synced_date: Option<String>,
}

pub async fn upsert_hackatime_connection(
    client: &Client,
    slack_id: &str,
    access_token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut insert = client
        .insert::<HackatimeConnectionRow>("hackatime_connections")
        .await?;
    insert
        .write(&HackatimeConnectionRow {
            slack_id: slack_id.to_string(),
            access_token: access_token.to_string(),
            last_synced_date: None,
        })
        .await?;
    insert.end().await?;
    Ok(())
}

pub async fn update_hackatime_connection(
    client: &Client,
    row: &HackatimeConnectionRow,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut insert = client
        .insert::<HackatimeConnectionRow>("hackatime_connections")
        .await?;
    insert.write(row).await?;
    insert.end().await?;
    Ok(())
}

pub async fn get_hackatime_connections(
    client: &Client,
) -> Result<Vec<HackatimeConnectionReadRow>, Box<dyn std::error::Error>> {
    let rows: Vec<HackatimeConnectionReadRow> = client
        .query("SELECT slack_id, access_token, last_synced_date FROM hackatime_connections FINAL")
        .fetch_all()
        .await?;
    Ok(rows)
}

pub async fn delete_hackatime_connection(
    client: &Client,
    slack_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    client
        .query(&format!(
            "ALTER TABLE hackatime_connections DELETE WHERE slack_id = '{}'",
            slack_id
        ))
        .execute()
        .await?;
    Ok(())
}

pub async fn is_hackatime_connected(
    client: &Client,
    slack_id: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct CountRow {
        count: u64,
    }
    let row: Option<CountRow> = client
        .query(&format!(
            "SELECT count() as count FROM hackatime_connections FINAL WHERE slack_id = '{}'",
            slack_id
        ))
        .fetch_optional()
        .await?;
    Ok(row.map(|r| r.count > 0).unwrap_or(false))
}

pub async fn clear_coding_activity_from(
    client: &Client,
    slack_id: &str,
    from_date: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    client
        .query(&format!(
            "ALTER TABLE coding_activity DELETE WHERE user_id = '{}' AND date >= '{}' SETTINGS mutations_sync = 2",
            slack_id, from_date
        ))
        .execute()
        .await?;
    Ok(())
}

pub async fn insert_coding_activity(
    client: &Client,
    rows: &[CodingActivityRow],
) -> Result<(), Box<dyn std::error::Error>> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = client
        .insert::<CodingActivityRow>("coding_activity")
        .await?;
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}

#[derive(Debug, Row, Serialize)]
pub struct CodingActivityRow {
    pub user_id: String,
    #[serde(with = "clickhouse::serde::time::date")]
    pub date: time::Date,
    pub minutes: i64,
    pub language: Option<String>,
}
