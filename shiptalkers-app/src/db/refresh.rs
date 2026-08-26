use sqlx::PgPool;

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

const WORD_FULL_REBUILD_SECS: u64 = 24 * 3600;

const EXCLUDE_BOTS_DELETED: &str =
    "user_id NOT IN (SELECT user_id FROM users WHERE is_bot = 1 OR is_deleted = 1)";

pub async fn refresh_word_totals(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let now = now_secs();
    let (watermark, last_full) = read_word_refresh_meta(pool).await;

    // First run, or the safety-net rebuild is due: recompute the whole table.
    // The watermark is set to now rather than max(inserted_at), because rows
    // backfilled before the `inserted_at` column existed all carry 0 and must
    // count as folded, not as dirty on every pass.
    if watermark == 0 || now.saturating_sub(last_full) >= WORD_FULL_REBUILD_SECS {
        refresh_word_totals_full(pool, now).await?;
        write_word_refresh_meta(pool, now, now).await?;
        return Ok(());
    }

    let words = dirty_words(pool, watermark).await?;
    if words.is_empty() {
        // Nothing new since the last fold; advance the watermark so the scan
        // does not re-read the same rows next pass.
        write_word_refresh_meta(pool, now, last_full).await?;
        return Ok(());
    }
    refresh_word_totals_for_words(pool, &words, now).await?;
    write_word_refresh_meta(pool, now, last_full).await?;
    Ok(())
}

async fn read_word_refresh_meta(pool: &PgPool) -> (u64, u64) {
    sqlx::query_as::<_, (i64, i64)>(
        "SELECT watermark, last_full FROM word_refresh_meta WHERE id = 1",
    )
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
    .map(|(watermark, last_full)| (watermark.max(0) as u64, last_full.max(0) as u64))
    .unwrap_or((0, 0))
}

async fn write_word_refresh_meta(
    pool: &PgPool,
    watermark: u64,
    last_full: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    sqlx::query(
        "INSERT INTO word_refresh_meta (id, watermark, last_full) VALUES (1, $1, $2)
         ON CONFLICT (id) DO UPDATE SET watermark = EXCLUDED.watermark, last_full = EXCLUDED.last_full",
    )
    .bind(watermark as i64)
    .bind(last_full as i64)
    .execute(pool)
    .await?;
    Ok(())
}

async fn dirty_words(
    pool: &PgPool,
    watermark: u64,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let rows: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT word FROM word_counts WHERE inserted_at > $1")
            .bind(watermark as i64)
            .fetch_all(pool)
            .await?;
    Ok(rows)
}

/// Recomputes totals for the given words and writes only the rows whose count actually changed, mirroring the old FINAL + LEFT JOIN guard.
async fn refresh_word_totals_upsert(
    pool: &PgPool,
    words: Option<&[String]>,
    updated: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let (sql, has_word_bind) = match words {
        Some(_) => (
            format!(
                "WITH agg AS (
                     SELECT word, sum(count) AS cnt
                     FROM word_counts
                     WHERE word = ANY($2)
                       AND {EXCLUDE_BOTS_DELETED}
                     GROUP BY word
                 )
                 INSERT INTO word_totals (word, cnt, updated)
                 SELECT a.word, a.cnt, $1
                 FROM agg a
                 LEFT JOIN word_totals t ON t.word = a.word
                 WHERE t.word IS NULL OR t.cnt != a.cnt
                 ON CONFLICT (word) DO UPDATE SET cnt = EXCLUDED.cnt, updated = EXCLUDED.updated"
            ),
            true,
        ),
        None => (
            format!(
                "WITH agg AS (
                     SELECT word, sum(count) AS cnt
                     FROM word_counts
                     WHERE {EXCLUDE_BOTS_DELETED}
                     GROUP BY word
                 )
                 INSERT INTO word_totals (word, cnt, updated)
                 SELECT a.word, a.cnt, $1
                 FROM agg a
                 LEFT JOIN word_totals t ON t.word = a.word
                 WHERE t.word IS NULL OR t.cnt != a.cnt
                 ON CONFLICT (word) DO UPDATE SET cnt = EXCLUDED.cnt, updated = EXCLUDED.updated"
            ),
            false,
        ),
    };
    let mut q = sqlx::query(&sql).bind(updated as i64);
    if let Some(words) = words.filter(|_| has_word_bind) {
        q = q.bind(words);
    }
    q.execute(pool).await?;
    Ok(())
}

async fn refresh_word_totals_for_words(
    pool: &PgPool,
    words: &[String],
    updated: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    refresh_word_totals_upsert(pool, Some(words), updated).await
}

async fn refresh_word_totals_full(
    pool: &PgPool,
    updated: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    refresh_word_totals_upsert(pool, None, updated).await
}

pub async fn refresh_daily_stats(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let boundary = crate::sessionize::SESSION_GAP_BOUNDARY_SECS;
    let rate = crate::sessionize::MESSAGE_TYPING_CHARS_PER_SEC;
    let overhead = crate::sessionize::MESSAGE_READ_OVERHEAD_SECS;
    let max_secs = crate::sessionize::SESSION_MAX_SECS;
    let slack: Vec<(time::Date, i64)> = sqlx::query_as(&format!(
        "WITH
         msg AS (
             SELECT user_id, message_ts / 1000000 AS ts,
                    sum(char_length(text)) AS chars,
                    count(*) AS msgs
             FROM slack_messages_by_user
             WHERE {EXCLUDE_BOTS_DELETED}
             GROUP BY user_id, ts
         ),
         flagged AS (
             SELECT user_id, ts, chars, msgs,
                 CASE WHEN ts - lag(ts) OVER (PARTITION BY user_id ORDER BY ts) > {boundary} THEN 1 ELSE 0 END AS boundary
             FROM msg
         ),
         sess AS (
             SELECT user_id, ts, chars, msgs,
                 sum(boundary) OVER (PARTITION BY user_id ORDER BY ts) AS sid
             FROM flagged
         ),
         sessions AS (
             SELECT user_id, sid, min(ts) AS start_ts, max(ts) AS end_ts,
                    (array_agg(chars ORDER BY ts))[1] AS first_chars,
                    (array_agg(msgs ORDER BY ts))[1] AS first_msgs
             FROM sess
             GROUP BY user_id, sid
         )
         SELECT to_timestamp(start_ts) AT TIME ZONE 'UTC' AS date,
                sum(least(end_ts - start_ts + (first_chars + {rate} - 1) / {rate} + first_msgs * {overhead}, {max_secs})) AS total_time
         FROM sessions
         GROUP BY date"
    ))
    .fetch_all(pool)
    .await?;

    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM daily_stats")
        .execute(&mut *tx)
        .await?;
    for chunk in slack.chunks(super::postgres_db::INSERT_CHUNK) {
        let mut sql = String::from("INSERT INTO daily_stats (date, slack_secs) VALUES ");
        sql.push_str(&super::postgres_db::placeholders(chunk.len(), 2));
        let mut q = sqlx::query(&sql);
        for (date, slack_secs) in chunk {
            q = q.bind(date).bind(*slack_secs);
        }
        q.execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}
