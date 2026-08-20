use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

const WORD_FULL_REBUILD_SECS: u64 = 24 * 3600;

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

pub async fn refresh_daily_stats(client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Debug, Row, Deserialize)]
    struct SlackDayRow {
        #[serde(with = "clickhouse::serde::time::date")]
        date: time::Date,
        total_time: u64,
    }
    let boundary = crate::sessionize::SESSION_GAP_BOUNDARY_SECS;
    let rate = crate::sessionize::MESSAGE_TYPING_CHARS_PER_SEC;
    let overhead = crate::sessionize::MESSAGE_READ_OVERHEAD_SECS;
    let max_secs = crate::sessionize::SESSION_MAX_SECS;
    let slack: Vec<SlackDayRow> = client
        .query(&format!(
            "WITH
             msg AS (
                 SELECT user_id, toInt64(message_ts / 1000000) AS ts,
                        sum(char_length(text)) AS chars,
                        count() AS msgs
                 FROM slack_messages_by_user
                 WHERE user_id NOT IN (SELECT user_id FROM users FINAL
                                       WHERE is_bot = 1 OR is_deleted = 1)
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
             SELECT toDate(toDateTime(start_ts)) AS date,
                    sum(toUInt64(least(end_ts - start_ts + (first_chars + {rate} - 1) / {rate} + first_msgs * {overhead}, {max_secs}))) AS total_time
             FROM sessions
             GROUP BY date
             SETTINGS max_bytes_before_external_group_by = 268435456, max_bytes_before_external_sort = 268435456"
        ))
        .fetch_all()
        .await?;

    #[derive(Debug, Row, Serialize)]
    struct DailyStatsRow {
        #[serde(with = "clickhouse::serde::time::date")]
        date: time::Date,
        slack_secs: u64,
    }

    client
        .query("DELETE FROM daily_stats WHERE 1 SETTINGS mutations_sync = 2")
        .execute()
        .await?;
    let mut insert = client.insert::<DailyStatsRow>("daily_stats").await?;
    for r in &slack {
        insert
            .write(&DailyStatsRow {
                date: r.date,
                slack_secs: r.total_time,
            })
            .await?;
    }
    insert.end().await?;
    Ok(())
}
