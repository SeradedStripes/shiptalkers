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

    // Sync state for users without an OAuth connection: '' when synced via the
    // public API, 'private' when the profile hides public stats (no token to
    // fall back on), 'no_account' when hackatime has no user for the Slack UID.
    client
        .query(
            "ALTER TABLE hackatime_connections ADD COLUMN IF NOT EXISTS status String DEFAULT ''",
        )
        .execute()
        .await
        .ok();

    // Per-user total coding minutes fetched from hackatime (public API or
    // OAuth). There is no per-day coding data anymore, just this total.
    client
        .query(
            "ALTER TABLE hackatime_connections ADD COLUMN IF NOT EXISTS total_minutes UInt64 DEFAULT 0",
        )
        .execute()
        .await
        .ok();

    // Per-user coding spans from hackatime, so the stats bot can scope coding
    // time to the requested range (Slack time is range-scoped already).
    // Fetched via the public spans API with exact timestamps; spans are
    // block-shaped (end - start == duration), so the card sums each span's
    // exact overlap with the range. ReplacingMergeTree(updated) makes
    // re-fetched spans idempotent. `total_minutes` in hackatime_connections is
    // derived from this table.
    client
        .query(
            "CREATE TABLE IF NOT EXISTS hackatime_spans (
                slack_id String,
                start_ts UInt64,
                duration UInt64,
                updated UInt64
            ) ENGINE = ReplacingMergeTree(updated)
            ORDER BY (slack_id, start_ts)",
        )
        .execute()
        .await?;

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

    // Per-day Slack Time seconds for the stats page chart. Refreshed on a
    // background schedule by `refresh_daily_stats` so the sessionizer never runs
    // on a page load. Coding time stopped being per-day, so the old
    // `coding_minutes` column is dropped on existing deployments.
    client
        .query(
            "CREATE TABLE IF NOT EXISTS daily_stats (
                date Date,
                slack_secs UInt64
            ) ENGINE = ReplacingMergeTree()
            ORDER BY date",
        )
        .execute()
        .await?;
    client
        .query("ALTER TABLE daily_stats DROP COLUMN IF EXISTS coding_minutes")
        .execute()
        .await
        .ok();

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

async fn migrate_slack_messages(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct CountRow {
        count: u64,
    }

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

pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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
    known: &mut std::collections::HashSet<String>,
) -> Result<u64, Box<dyn std::error::Error>> {
    let new_channels: Vec<&SlackChannelRow> = channels
        .iter()
        .filter(|ch| known.insert(ch.channel_id.clone()))
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

pub async fn insert_new_channels_rows(
    client: &Client,
    channels: &[SlackChannelRow],
) -> Result<u64, Box<dyn std::error::Error>> {
    if channels.is_empty() {
        return Ok(0);
    }
    let count = channels.len() as u64;
    let mut insert = client.insert::<SlackChannelRow>("slack_channels").await?;
    for ch in channels {
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
