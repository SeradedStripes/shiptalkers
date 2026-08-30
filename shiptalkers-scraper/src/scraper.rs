use crate::db;
use crate::settings;
use crate::slack;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

pub fn insert_page(
    pool: sqlx::PgPool,
    page: Vec<slack::SlackChannel>,
    known_channels: Arc<Mutex<std::collections::HashSet<String>>>,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move {
        let rows: Vec<db::postgres_db::SlackChannelRow> = page
            .iter()
            .map(|ch| db::postgres_db::SlackChannelRow {
                channel_id: ch.id.clone(),
                name: ch.name.clone(),
            })
            .collect();

        let new_rows: Vec<_> = {
            let mut guard = known_channels.lock().unwrap();
            rows.into_iter()
                .filter(|ch| guard.insert(ch.channel_id.clone()))
                .collect()
        };

        if new_rows.is_empty() {
            return;
        }

        match tokio::time::timeout(
            Duration::from_secs(120),
            db::postgres_db::insert_new_channels_rows(&pool, &new_rows),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => tracing::error!("Failed to insert channels: {}", e),
            Err(_) => tracing::error!("Failed to insert channels: timed out after 2m"),
        }
    })
}

pub async fn sync_users(slack_pool: &slack::SlackClientPool, pool: &sqlx::PgPool) -> bool {
    let existing = match db::postgres_db::get_user_updates(pool).await {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!("Failed to get stored user updates: {}", e);
            return false;
        }
    };
    let missing_pfps: std::collections::HashSet<String> =
        match db::postgres_db::get_user_ids_without_pfp(pool).await {
            Ok(ids) => ids.into_iter().collect(),
            Err(e) => {
                tracing::warn!("Failed to get users missing pfps: {}", e);
                std::collections::HashSet::new()
            }
        };
    tracing::info!(
        "Syncing users from Slack ({} already stored, {} missing pfps)",
        existing.len(),
        missing_pfps.len()
    );
    let existing = Arc::new(existing);
    let missing_pfps = Arc::new(missing_pfps);
    let changed_total = Arc::new(AtomicU64::new(0));
    let result = slack_pool
        .fetch_users(|batch| {
            let pool = pool.clone();
            let existing = existing.clone();
            let missing_pfps = missing_pfps.clone();
            let changed_total = changed_total.clone();
            Box::pin(async move {
                let changed: Vec<db::postgres_db::SlackUserRow> = batch
                    .into_iter()
                    .filter(|u| {
                        u.is_deleted
                            || match existing.get(&u.id) {
                                Some(prev) => *prev < u.updated || missing_pfps.contains(&u.id),
                                None => true,
                            }
                    })
                    .map(|u| db::postgres_db::SlackUserRow {
                        user_id: u.id,
                        display_name: u.display_name,
                        pfp: u.pfp,
                        updated: u.updated,
                        is_bot: u.is_bot as u8,
                        is_deleted: u.is_deleted as u8,
                    })
                    .collect();
                if changed.is_empty() {
                    return;
                }
                match db::postgres_db::upsert_users(&pool, &changed).await {
                    Ok(()) => {
                        changed_total.fetch_add(changed.len() as u64, Ordering::Relaxed);
                    }
                    Err(e) => tracing::warn!("Failed to upsert users: {}", e),
                }
            })
        })
        .await;
    match result {
        Ok(total) => {
            if total == 0 {
                tracing::warn!("users.list returned no members");
                return false;
            }
            if changed_total.load(Ordering::Relaxed) == 0 {
                tracing::info!("No user changes since last sync, skipping upsert");
            } else {
                tracing::info!(
                    "Synced {} users from Slack ({} upserted)",
                    total,
                    changed_total.load(Ordering::Relaxed)
                );
            }
            true
        }
        Err(e) => {
            tracing::warn!("Failed to fetch users: {}", e);
            false
        }
    }
}

pub async fn run_scraper(
    pool: sqlx::PgPool,
    settings: settings::RuntimeSettings,
) -> Result<(), String> {
    if let Err(e) = db::postgres_db::backfill_slack_messages_by_user(&pool).await {
        tracing::warn!("Failed to backfill slack_messages_by_user: {}", e);
    }
    if let Err(e) = db::postgres_db::backfill_word_counts(&pool).await {
        tracing::warn!("Failed to backfill word_counts: {}", e);
    }
    let sessionizer_changed = db::scores::sessionizer_changed(&pool)
        .await
        .unwrap_or(false);
    let (channels, users) = tokio::join!(
        async {
            db::scores::backfill_stale_channel_scores(&pool, sessionizer_changed)
                .await
                .map_err(|e| e.to_string())
        },
        async {
            db::scores::backfill_stale_user_scores(&pool, sessionizer_changed)
                .await
                .map_err(|e| e.to_string())
        },
    );
    match channels {
        Ok(n) => tracing::info!("Startup channel score backfill done ({} channels)", n),
        Err(e) => tracing::warn!("Failed to backfill channel scores: {}", e),
    }
    match users {
        Ok(n) => tracing::info!("Startup Slack Time score backfill done ({} users)", n),
        Err(e) => tracing::warn!("Failed to backfill user scores: {}", e),
    }

    let cycle = Duration::from_secs(30 * 60);

    loop {
        let cycle_start = std::time::Instant::now();
        let request_delay = Duration::from_millis(settings.get_u64("SLACK_REQUEST_DELAY_MS"));
        let max_inflight = settings.get_u64("SLACK_MAX_INFLIGHT") as usize;
        let bot_tokens = settings.get_list("SLACK_BOT_TOKENS");
        let user_tokens = settings.get_list("SLACK_USER_TOKENS");
        let bot_pool = slack::SlackClientPool::new(bot_tokens, request_delay, max_inflight);

        if let Err(e) = full_fetch(&bot_pool, &pool).await {
            tracing::warn!("Failed to fetch channel list: {}", e);
        }

        if !user_tokens.is_empty() {
            scrape_all_messages(&settings, &pool).await;
        }

        let elapsed = cycle_start.elapsed();
        if elapsed < cycle {
            let wait = cycle.saturating_sub(elapsed);
            tracing::info!(
                "Scrape cycle done in {:.0}s, sleeping {:.0}s until next cycle",
                elapsed.as_secs_f64(),
                wait.as_secs_f64()
            );
            tokio::time::sleep(wait).await;
        } else {
            tracing::info!(
                "Scrape cycle took {:.0}s (longer than 30m), starting next cycle immediately",
                elapsed.as_secs_f64()
            );
        }
    }
}

async fn scrape_all_messages(settings: &settings::RuntimeSettings, pool: &sqlx::PgPool) {
    let channels = match db::postgres_db::get_known_channel_ids(pool).await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("Failed to get channel IDs: {}", e);
            return;
        }
    };

    if let Err(e) = db::postgres_db::backfill_scraped_channels(pool).await {
        tracing::warn!("Failed to backfill scraped channels: {}", e);
    }

    let scraped = match db::postgres_db::get_scraped_channel_ids(pool).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Failed to get scraped channel IDs: {}", e);
            return;
        }
    };
    let scraped_set: std::collections::HashSet<&String> = scraped.iter().collect();

    let new_channels: Vec<String> = channels
        .iter()
        .filter(|c| !scraped_set.contains(c))
        .cloned()
        .collect();
    let check_channels: Vec<String> = channels
        .iter()
        .filter(|c| scraped_set.contains(c))
        .cloned()
        .collect();

    tracing::info!(
        "{} known channels: {} new to full-scrape, {} already-scraped to check for new messages",
        channels.len(),
        new_channels.len(),
        check_channels.len()
    );

    let touched_users = Arc::new(Mutex::new(std::collections::HashSet::new()));
    let touched_channels = Arc::new(Mutex::new(std::collections::HashSet::new()));

    if !new_channels.is_empty() {
        tracing::info!("Full-scraping {} new channels...", new_channels.len());
        scrape_channel_list(
            settings,
            pool,
            &new_channels,
            touched_users.clone(),
            touched_channels.clone(),
        )
        .await;
    }

    if !check_channels.is_empty() {
        tracing::info!(
            "Checking {} already-scraped channels for new messages...",
            check_channels.len()
        );
        scrape_channel_list(
            settings,
            pool,
            &check_channels,
            touched_users.clone(),
            touched_channels.clone(),
        )
        .await;
    }

    let users: Vec<String> = touched_users.lock().unwrap().iter().cloned().collect();
    if !users.is_empty()
        && let Err(e) = db::scores::recompute_user_scores(pool, &users).await
    {
        tracing::warn!(
            "Failed to recompute scores for {} users this pass: {}",
            users.len(),
            e
        );
    }

    let channels: Vec<String> = touched_channels.lock().unwrap().iter().cloned().collect();
    if !channels.is_empty()
        && let Err(e) = db::scores::recompute_channel_scores(pool, &channels).await
    {
        tracing::warn!(
            "Failed to recompute scores for {} channels this pass: {}",
            channels.len(),
            e
        );
    }

    tracing::info!("Message scrape pass complete");
}

async fn scrape_channel_list(
    settings: &settings::RuntimeSettings,
    pool: &sqlx::PgPool,
    channels: &[String],
    touched_users: Arc<Mutex<std::collections::HashSet<String>>>,
    touched_channels: Arc<Mutex<std::collections::HashSet<String>>>,
) {
    let request_delay = Duration::from_millis(settings.get_u64("SLACK_REQUEST_DELAY_MS"));
    let max_inflight = settings.get_u64("SLACK_MAX_INFLIGHT") as usize;
    let channel_concurrency = settings.get_u64("SLACK_CHANNEL_CONCURRENCY") as usize;
    let thread_rescan_window_hours = settings.get_u64("SLACK_THREAD_RESCAN_HOURS");
    let thread_rescan_interval_hours = settings.get_u64("SLACK_THREAD_RESCAN_INTERVAL_HOURS");
    let user_tokens = settings.get_list("SLACK_USER_TOKENS");
    tracing::info!(
        "Scraping {} channels with {} token(s)...",
        channels.len(),
        user_tokens.len()
    );
    let total = Arc::new(AtomicU64::new(0));
    let processed = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let num_channels = channels.len();

    {
        let total = total.clone();
        let processed = processed.clone();
        let done = done.clone();
        tokio::spawn(async move {
            let mut last_msgs = 0u64;
            let mut last_report = std::time::Instant::now();
            while !done.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(15)).await;
                if done.load(Ordering::Relaxed) {
                    break;
                }
                let p = processed.load(Ordering::Relaxed);
                let m = total.load(Ordering::Relaxed);
                if m > last_msgs {
                    let dt = last_report.elapsed().as_secs_f64().max(0.001);
                    let rate = (m - last_msgs) as f64 / dt;
                    let pct = p as f64 / num_channels as f64 * 100.0;
                    tracing::info!(
                        "Progress: {}/{} channels ({:.1}%), {} msgs inserted this run ({:.0} msg/s)",
                        p,
                        num_channels,
                        pct,
                        m,
                        rate
                    );
                    last_msgs = m;
                    last_report = std::time::Instant::now();
                }
            }
        });
    }

    let mut workers = Vec::new();
    let next = Arc::new(AtomicUsize::new(0));
    for (token_idx, token) in user_tokens.iter().enumerate() {
        let client = slack::SlackClient::new(token.clone(), request_delay, max_inflight);

        let (tx, rx) = tokio::sync::mpsc::channel::<usize>(512);
        let ctx = ShardCtx {
            token_idx,
            total_channels: channels.len(),
            max_inflight,
            channel_concurrency,
            thread_rescan_window_hours,
            thread_rescan_interval_hours,
            total: total.clone(),
            processed: processed.clone(),
            tx,
            touched_users: touched_users.clone(),
            touched_channels: touched_channels.clone(),
        };
        let pool = pool.clone();
        let next = next.clone();
        let channels = Arc::new(channels.to_vec());
        workers.push(tokio::spawn(async move {
            scrape_shard(&client, &pool, channels, next, ctx, rx).await;
        }));
    }

    for worker in workers {
        let _ = worker.await;
    }

    done.store(true, Ordering::Relaxed);
    tracing::info!(
        "Pass complete! {} new messages inserted",
        total.load(Ordering::Relaxed)
    );
}

#[derive(Clone)]
struct ShardCtx {
    token_idx: usize,
    total_channels: usize,
    max_inflight: usize,
    channel_concurrency: usize,
    thread_rescan_window_hours: u64,
    thread_rescan_interval_hours: u64,
    total: Arc<AtomicU64>,
    processed: Arc<AtomicU64>,
    tx: tokio::sync::mpsc::Sender<usize>,
    touched_users: Arc<Mutex<std::collections::HashSet<String>>>,
    touched_channels: Arc<Mutex<std::collections::HashSet<String>>>,
}

async fn scrape_shard(
    user_client: &slack::SlackClient,
    pool: &sqlx::PgPool,
    channels: Arc<Vec<String>>,
    next: Arc<AtomicUsize>,
    ctx: ShardCtx,
    rx: tokio::sync::mpsc::Receiver<usize>,
) {
    if channels.is_empty() {
        tracing::info!("[token {}] No channels to scrape", ctx.token_idx);
        return;
    }
    tracing::info!(
        "[token {}] Scraping {} channels from shared queue",
        ctx.token_idx,
        channels.len()
    );

    let channel_concurrency = ctx.channel_concurrency.max(1);

    let tx = ctx.tx.clone();
    let token_idx = ctx.token_idx;
    let mut rx = rx;
    let reporter = tokio::spawn(async move {
        let mut batch: Vec<String> = Vec::new();
        let mut tick = tokio::time::interval(Duration::from_secs(4));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    if !batch.is_empty() {
                        tracing::info!("[token {}]-[{}]", token_idx, batch.join(", "));
                        batch.clear();
                    }
                }
                got = rx.recv() => match got {
                    Some(i) => {
                        batch.push(i.to_string());
                        if batch.len() >= 40 {
                            tracing::info!("[token {}]-[{}]", token_idx, batch.join(", "));
                            batch.clear();
                        }
                    }
                    None => break,
                },
            }
        }
        if !batch.is_empty() {
            tracing::info!("[token {}]-[{}]", token_idx, batch.join(", "));
        }
    });

    let mut handles = Vec::with_capacity(channel_concurrency);
    for _ in 0..channel_concurrency {
        let client = user_client.clone();
        let pool = pool.clone();
        let channels = channels.clone();
        let next = next.clone();
        let ctx = ctx.clone();
        handles.push(tokio::spawn(async move {
            loop {
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= channels.len() {
                    break;
                }
                let channel_id = channels[idx].clone();
                scrape_one_channel(&client, &pool, channel_id, idx + 1, &ctx).await;
            }
        }));
    }
    drop(tx);
    for handle in handles {
        let _ = handle.await;
    }
    drop(ctx);
    let _ = reporter.await;
}

fn reaction_rows_from(
    messages: &[slack::SlackMessage],
    channel_id: &str,
) -> Vec<db::postgres_db::SlackReactionRow> {
    let mut rows = Vec::new();
    for m in messages {
        let message_ts = db::postgres_db::slack_ts_to_micros(&m.ts);
        for reaction in &m.reactions {
            for user_id in &reaction.users {
                rows.push(db::postgres_db::SlackReactionRow {
                    channel_id: channel_id.to_string(),
                    message_ts,
                    emoji: reaction.name.clone(),
                    user_id: user_id.clone(),
                });
            }
        }
    }
    rows
}

fn word_count_rows_from(
    messages: &[slack::SlackMessage],
    channel_id: &str,
) -> Vec<db::postgres_db::WordCountRow> {
    let mut rows = Vec::new();
    for m in messages {
        let message_ts = db::postgres_db::slack_ts_to_micros(&m.ts);
        let lower = m.text.to_lowercase();
        let mut counts: std::collections::HashMap<&str, u64> = std::collections::HashMap::new();
        for word in lower
            .split(|c: char| !c.is_ascii_lowercase())
            .filter(|w| w.len() > 1)
        {
            *counts.entry(word).or_insert(0) += 1;
        }
        for (word, count) in counts {
            rows.push(db::postgres_db::WordCountRow {
                word: word.to_string(),
                user_id: m.user.clone(),
                channel_id: channel_id.to_string(),
                message_ts,
                count,
                inserted_at: 0,
            });
        }
    }
    rows
}

async fn upsert_bot_users(pool: &sqlx::PgPool, messages: &[slack::SlackMessage]) {
    let mut seen = std::collections::HashSet::new();
    let bots: Vec<db::postgres_db::SlackUserRow> = messages
        .iter()
        .filter(|m| m.user.starts_with('B') && seen.insert(m.user.clone()))
        .map(|m| db::postgres_db::SlackUserRow {
            user_id: m.user.clone(),
            display_name: m.bot_name.clone().unwrap_or_else(|| m.user.clone()),
            pfp: String::new(),
            updated: 0,
            is_bot: 1,
            is_deleted: 0,
        })
        .collect();
    if bots.is_empty() {
        return;
    }
    if let Err(e) = db::postgres_db::upsert_users(pool, &bots).await {
        tracing::warn!("Failed to upsert bot users: {}", e);
    }
}

#[derive(Default)]
struct ChannelPageAccum {
    inserted: u64,
    filtered_out: u64,
    thread_roots: std::collections::HashSet<String>,
}

#[derive(Default)]
struct ThreadPageAccum {
    inserted: u64,
    reply_users: Vec<String>,
}

async fn process_channel_page(
    pool: &sqlx::PgPool,
    channel_id: &str,
    oldest: Option<&str>,
    page: Vec<slack::SlackMessage>,
    accum: &std::sync::Mutex<ChannelPageAccum>,
    total: &AtomicU64,
    touched_users: &std::sync::Mutex<std::collections::HashSet<String>>,
) {
    let raw = page.len() as u64;
    let page: Vec<_> = if let Some(o) = oldest {
        page.into_iter().filter(|m| m.ts.as_str() > o).collect()
    } else {
        page
    };
    let filtered = raw.saturating_sub(page.len() as u64);

    {
        let mut a = accum.lock().unwrap();
        a.filtered_out += filtered;
        for m in &page {
            if let Some(ref t) = m.thread_ts
                && t == &m.ts
            {
                a.thread_roots.insert(t.clone());
            }
        }
    }
    for m in &page {
        touched_users.lock().unwrap().insert(m.user.clone());
    }

    let rows: Vec<db::postgres_db::SlackMessageRow> = page
        .iter()
        .map(|m| db::postgres_db::SlackMessageRow {
            user_id: m.user.clone(),
            channel_id: m.channel.clone(),
            message_ts: db::postgres_db::slack_ts_to_micros(&m.ts),
            text: m.text.clone(),
            thread_ts: m.thread_ts.clone(),
        })
        .collect();

    let inserted = if rows.is_empty() {
        0
    } else {
        db::postgres_db::insert_messages(pool, &rows)
            .await
            .unwrap_or(0)
    };
    total.fetch_add(inserted, Ordering::Relaxed);
    accum.lock().unwrap().inserted += inserted;

    let reaction_rows = reaction_rows_from(&page, channel_id);
    if !reaction_rows.is_empty()
        && let Err(e) = db::postgres_db::insert_reactions(pool, &reaction_rows).await
    {
        tracing::warn!("Failed to insert reactions for {}: {}", channel_id, e);
    }

    let word_rows = word_count_rows_from(&page, channel_id);
    if !word_rows.is_empty()
        && let Err(e) = db::postgres_db::insert_word_counts(pool, &word_rows).await
    {
        tracing::warn!("Failed to insert word counts for {}: {}", channel_id, e);
    }

    upsert_bot_users(pool, &page).await;
}

async fn process_thread_page(
    pool: &sqlx::PgPool,
    channel_id: &str,
    thread_ts: &str,
    thread_oldest: Option<&str>,
    page: Vec<slack::SlackMessage>,
    accum: &std::sync::Mutex<ThreadPageAccum>,
    total: &AtomicU64,
) {
    let page: Vec<_> = page
        .into_iter()
        .filter(|m| m.ts != thread_ts)
        .filter(|m| match thread_oldest {
            Some(o) => m.ts.as_str() > o,
            None => true,
        })
        .collect();

    let mut inserted = 0u64;
    if !page.is_empty() {
        let rows: Vec<db::postgres_db::SlackMessageRow> = page
            .iter()
            .map(|m| db::postgres_db::SlackMessageRow {
                user_id: m.user.clone(),
                channel_id: m.channel.clone(),
                message_ts: db::postgres_db::slack_ts_to_micros(&m.ts),
                text: m.text.clone(),
                thread_ts: m.thread_ts.clone(),
            })
            .collect();
        inserted = db::postgres_db::insert_messages(pool, &rows)
            .await
            .unwrap_or(0);
        total.fetch_add(inserted, Ordering::Relaxed);
    }

    {
        let mut a = accum.lock().unwrap();
        a.inserted += inserted;
        for m in &page {
            a.reply_users.push(m.user.clone());
        }
    }

    if inserted > 0 {
        tracing::debug!(
            "Inserted {} thread replies from thread {} in {}",
            inserted,
            thread_ts,
            channel_id
        );
    }

    let reply_reactions = reaction_rows_from(&page, channel_id);
    if !reply_reactions.is_empty()
        && let Err(e) = db::postgres_db::insert_reactions(pool, &reply_reactions).await
    {
        tracing::warn!(
            "Failed to insert reactions for thread {} in {}: {}",
            thread_ts,
            channel_id,
            e
        );
    }

    let reply_words = word_count_rows_from(&page, channel_id);
    if !reply_words.is_empty()
        && let Err(e) = db::postgres_db::insert_word_counts(pool, &reply_words).await
    {
        tracing::warn!(
            "Failed to insert word counts for thread {} in {}: {}",
            thread_ts,
            channel_id,
            e
        );
    }

    upsert_bot_users(pool, &page).await;
}

async fn scrape_one_channel(
    user_client: &slack::SlackClient,
    pool: &sqlx::PgPool,
    channel_id: String,
    idx: usize,
    ctx: &ShardCtx,
) {
    let token_idx = ctx.token_idx;
    let total_channels = ctx.total_channels;
    let max_inflight = ctx.max_inflight;
    let total = ctx.total.clone();
    let processed = ctx.processed.clone();
    let tx = ctx.tx.clone();
    let start = std::time::Instant::now();
    let fully_scraped = db::postgres_db::is_fully_scraped(pool, &channel_id)
        .await
        .unwrap_or(false);

    let oldest = match db::postgres_db::get_max_message_ts(pool, &channel_id).await {
        Ok(Some(ts)) if ts > 0 => {
            let ts = db::postgres_db::micros_to_slack_ts(ts);
            tracing::debug!(
                "[token {}][{}/{}] Scraping channel {} (mode={}, oldest={})",
                token_idx,
                idx,
                total_channels,
                channel_id,
                if fully_scraped {
                    "incremental"
                } else {
                    "incremental(partial)"
                },
                ts
            );
            Some(ts)
        }
        Ok(_) => {
            tracing::debug!(
                "[token {}][{}/{}] Scraping channel {} (mode=full, no data yet)",
                token_idx,
                idx,
                total_channels,
                channel_id
            );
            None
        }
        Err(e) => {
            tracing::warn!(
                "[token {}][{}/{}] Failed to get max ts for {}: {}, doing full scrape",
                token_idx,
                idx,
                total_channels,
                channel_id,
                e
            );
            None
        }
    };

    let accum = std::sync::Arc::new(std::sync::Mutex::new(ChannelPageAccum {
        inserted: 0,
        filtered_out: 0,
        thread_roots: std::collections::HashSet::new(),
    }));
    let pool_for_stream = pool.clone();
    let channel_id_for_stream = channel_id.clone();
    let oldest_for_stream = oldest.clone();
    let total_for_stream = total.clone();
    let touched_users_for_stream = ctx.touched_users.clone();
    let accum_for_stream = accum.clone();

    let raw_count = match user_client
        .stream_channel_history(&channel_id, oldest.as_deref(), move |page| {
            let accum = accum_for_stream.clone();
            let pool = pool_for_stream.clone();
            let channel_id = channel_id_for_stream.clone();
            let oldest = oldest_for_stream.clone();
            let total = total_for_stream.clone();
            let touched_users = touched_users_for_stream.clone();
            Box::pin(async move {
                process_channel_page(
                    &pool,
                    &channel_id,
                    oldest.as_deref(),
                    page,
                    &accum,
                    &total,
                    &touched_users,
                )
                .await;
            })
        })
        .await
    {
        Ok(n) => n,
        Err(e) => {
            if e.to_string().contains("channel_not_found") {
                tracing::warn!(
                    "[token {}][{}/{}] Channel {} no longer exists, skipping",
                    token_idx,
                    idx,
                    total_channels,
                    channel_id
                );
                if let Err(err) = db::postgres_db::mark_channel_scraped(pool, &channel_id).await {
                    tracing::warn!(
                        "[token {}][{}/{}] Failed to record {} as scraped: {}",
                        token_idx,
                        idx,
                        total_channels,
                        channel_id,
                        err
                    );
                }
                processed.fetch_add(1, Ordering::Relaxed);
                let _ = tx.send(idx).await;
                return;
            }
            tracing::warn!(
                "[token {}][{}/{}] Failed to scrape {}: {}",
                token_idx,
                idx,
                total_channels,
                channel_id,
                e
            );
            return;
        }
    };

    let acc = Arc::try_unwrap(accum)
        .map(|m| m.into_inner().unwrap_or_default())
        .unwrap_or_default();
    let inserted = acc.inserted;
    let filtered_out = acc.filtered_out;
    let thread_roots = std::sync::Arc::new(std::sync::Mutex::new(acc.thread_roots));

    tracing::info!(
        "[token {}][{}/{}] Inserted {} new messages from {} (fetched {}, dupes filtered {})",
        token_idx,
        idx,
        total_channels,
        inserted,
        channel_id,
        raw_count,
        filtered_out
    );

    if let Err(e) = db::postgres_db::mark_channel_scraped(pool, &channel_id).await {
        tracing::warn!(
            "[token {}][{}/{}] Failed to record {} as scraped: {}",
            token_idx,
            idx,
            total_channels,
            channel_id,
            e
        );
    }

    if !fully_scraped {
        let stored_roots = db::postgres_db::get_thread_roots(pool, &channel_id)
            .await
            .unwrap_or_default();
        let mut added = 0usize;
        {
            let mut set = thread_roots.lock().unwrap();
            for root in stored_roots {
                if set.insert(root) {
                    added += 1;
                }
            }
        }
        if added > 0 {
            tracing::info!(
                "[token {}][{}/{}] Recovering {} stored thread root(s) in {}",
                token_idx,
                idx,
                total_channels,
                added,
                channel_id
            );
        }
    }

    if oldest.is_some()
        && thread_rescan_due(
            &channel_id,
            Duration::from_secs(ctx.thread_rescan_interval_hours * 3600),
        )
    {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let window_ts = now
            .saturating_sub(ctx.thread_rescan_window_hours * 3600)
            .to_string();
        record_thread_rescan(&channel_id);
        let thread_roots = thread_roots.clone();
        let pool_for_stream = pool.clone();
        let channel_id_for_rescan = channel_id.clone();
        match user_client
            .stream_channel_history(&channel_id, Some(&window_ts), move |page| {
                let thread_roots = thread_roots.clone();
                let pool = pool_for_stream.clone();
                let channel_id = channel_id_for_rescan.clone();
                Box::pin(async move {
                    let mut found = 0usize;
                    {
                        let mut set = thread_roots.lock().unwrap();
                        for msg in &page {
                            if let Some(ref t) = msg.thread_ts
                                && t == &msg.ts
                                && set.insert(t.clone())
                            {
                                found += 1;
                            }
                        }
                    }
                    if found > 0 {
                        tracing::info!(
                            "Thread re-scan of {} found {} new thread root(s)",
                            channel_id,
                            found
                        );
                    }
                    let extra_reactions = reaction_rows_from(&page, &channel_id);
                    if !extra_reactions.is_empty()
                        && let Err(e) =
                            db::postgres_db::insert_reactions(&pool, &extra_reactions).await
                    {
                        tracing::warn!(
                            "Failed to insert reactions from thread re-scan of {}: {}",
                            channel_id,
                            e
                        );
                    }
                })
            })
            .await
        {
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    "[token {}][{}/{}] Failed to re-scan recent history for threads in {}: {}",
                    token_idx,
                    idx,
                    total_channels,
                    channel_id,
                    e
                );
            }
        }
    }

    let thread_parents: Vec<String> = thread_roots.lock().unwrap().iter().cloned().collect();

    let mut threads_found = 0usize;
    let mut thread_replies = 0u64;
    let mut threads_skipped = 0usize;
    let mut thread_users: Vec<String> = Vec::new();
    if !thread_parents.is_empty() {
        threads_found = thread_parents.len();
        let sem = Arc::new(tokio::sync::Semaphore::new(max_inflight));
        let mut handles = Vec::with_capacity(thread_parents.len());
        for thread_ts in &thread_parents {
            let permit = sem.clone();
            let client = user_client.clone();
            let pool = pool.clone();
            let channel_id = channel_id.clone();
            let thread_ts = thread_ts.clone();
            let total = total.clone();
            handles.push(tokio::spawn(async move {
                let _permit = permit.acquire().await;
                scrape_thread(
                    &client,
                    &pool,
                    channel_id,
                    thread_ts,
                    token_idx,
                    idx,
                    total_channels,
                    total,
                )
                .await
            }));
        }
        for (skipped, inserted_replies, users) in futures_util::future::join_all(handles)
            .await
            .into_iter()
            .flatten()
        {
            threads_skipped += skipped;
            thread_replies += inserted_replies;
            thread_users.extend(users);
        }
    }

    {
        let mut set = ctx.touched_users.lock().unwrap();
        for u in thread_users {
            set.insert(u);
        }
        if inserted > 0 || thread_replies > 0 {
            ctx.touched_channels
                .lock()
                .unwrap()
                .insert(channel_id.clone());
        }
    }

    if !fully_scraped
        && threads_skipped == 0
        && let Err(e) = db::postgres_db::mark_fully_scraped(pool, &channel_id).await
    {
        tracing::warn!(
            "[token {}][{}/{}] Failed to mark {} as fully scraped: {}",
            token_idx,
            idx,
            total_channels,
            channel_id,
            e
        );
    }

    processed.fetch_add(1, Ordering::Relaxed);
    let _ = tx.send(idx).await;

    let elapsed = start.elapsed().as_secs_f64();
    let mut summary = Vec::new();
    if inserted > 0 {
        summary.push(format!("{} msgs", inserted));
    }
    if thread_replies > 0 {
        summary.push(format!("{} thread replies", thread_replies));
    }
    if threads_found > 0 && thread_replies == 0 {
        summary.push(format!("{} threads, no replies", threads_found));
    }
    if threads_skipped > 0 {
        summary.push(format!("{} threads skipped", threads_skipped));
    }

    if summary.is_empty() {
        tracing::debug!(
            "[token {}][{}/{}] Channel {} done in {:.1}s (nothing new)",
            token_idx,
            idx,
            total_channels,
            channel_id,
            elapsed
        );
    } else {
        tracing::debug!(
            "[token {}][{}/{}] Channel {} done in {:.1}s: {}",
            token_idx,
            idx,
            total_channels,
            channel_id,
            elapsed,
            summary.join(", ")
        );
    }
}

static THREAD_RESCAN_LAST: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();

fn thread_rescan_due(channel_id: &str, interval: Duration) -> bool {
    let last = THREAD_RESCAN_LAST.get_or_init(|| Mutex::new(HashMap::new()));
    let map = last.lock().unwrap();
    match map.get(channel_id) {
        Some(prev) => prev.elapsed() >= interval,
        None => true,
    }
}

fn record_thread_rescan(channel_id: &str) {
    let last = THREAD_RESCAN_LAST.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = last.lock().unwrap();
    map.insert(channel_id.to_string(), Instant::now());
}

async fn scrape_thread(
    user_client: &slack::SlackClient,
    pool: &sqlx::PgPool,
    channel_id: String,
    thread_ts: String,
    token_idx: usize,
    idx: usize,
    total_channels: usize,
    total: Arc<AtomicU64>,
) -> (usize, u64, Vec<String>) {
    let thread_fully = db::postgres_db::is_thread_fully_scraped(pool, &channel_id, &thread_ts)
        .await
        .unwrap_or(false);
    let thread_oldest = if thread_fully {
        db::postgres_db::get_max_thread_reply_ts(pool, &channel_id, &thread_ts)
            .await
            .ok()
            .flatten()
            .filter(|&ts| ts > 0)
            .map(db::postgres_db::micros_to_slack_ts)
    } else {
        None
    };

    let accum = std::sync::Arc::new(std::sync::Mutex::new(ThreadPageAccum::default()));
    let pool_for_stream = pool.clone();
    let channel_id_for_stream = channel_id.clone();
    let thread_ts_for_stream = thread_ts.clone();
    let thread_oldest_for_stream = thread_oldest.clone();
    let total_for_stream = total.clone();
    let accum_for_stream = accum.clone();

    match user_client
        .stream_thread_replies(
            &channel_id,
            &thread_ts,
            thread_oldest.as_deref(),
            move |page| {
                let accum = accum_for_stream.clone();
                let pool = pool_for_stream.clone();
                let channel_id = channel_id_for_stream.clone();
                let thread_ts = thread_ts_for_stream.clone();
                let thread_oldest = thread_oldest_for_stream.clone();
                let total = total_for_stream.clone();
                Box::pin(async move {
                    process_thread_page(
                        &pool,
                        &channel_id,
                        &thread_ts,
                        thread_oldest.as_deref(),
                        page,
                        &accum,
                        &total,
                    )
                    .await;
                })
            },
        )
        .await
    {
        Ok(_) => {
            let acc = Arc::try_unwrap(accum)
                .map(|m| m.into_inner().unwrap_or_default())
                .unwrap_or_default();
            let inserted = acc.inserted;
            let reply_users = acc.reply_users;

            if inserted > 0 {
                tracing::debug!(
                    "[token {}][{}/{}] Inserted {} thread replies from thread {} in {}",
                    token_idx,
                    idx,
                    total_channels,
                    inserted,
                    thread_ts,
                    channel_id
                );
            }

            if thread_oldest.is_none()
                && let Err(e) =
                    db::postgres_db::mark_thread_fully_scraped(pool, &channel_id, &thread_ts).await
            {
                tracing::warn!(
                    "[token {}][{}/{}] Failed to mark thread {} as scraped: {}",
                    token_idx,
                    idx,
                    total_channels,
                    thread_ts,
                    e
                );
            }

            (0, inserted, reply_users)
        }
        Err(e) => {
            tracing::warn!(
                "[token {}][{}/{}] Failed to scrape thread {} in {}: {}",
                token_idx,
                idx,
                total_channels,
                thread_ts,
                channel_id,
                e
            );
            (1, 0, Vec::new())
        }
    }
}

async fn full_fetch(
    slack_pool: &slack::SlackClientPool,
    pool: &sqlx::PgPool,
) -> Result<(), String> {
    let known = match db::postgres_db::get_known_channel_ids(pool).await {
        Ok(ids) => ids.into_iter().collect(),
        Err(e) => {
            tracing::warn!("Failed to pre-fetch known channel IDs: {}", e);
            std::collections::HashSet::new()
        }
    };
    let known_channels = Arc::new(Mutex::new(known));
    let pool_for_fetch = pool.clone();
    let kc = known_channels;
    let total = slack_pool
        .fetch_channels_paginated(
            move |page| insert_page(pool_for_fetch.clone(), page, kc.clone()),
            None,
        )
        .await
        .map_err(|e| e.to_string())?;

    tracing::info!("Full rescan done! {} total channels", total);
    Ok(())
}
