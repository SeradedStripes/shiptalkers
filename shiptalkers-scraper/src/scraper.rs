use crate::db;
use crate::settings;
use crate::slack;
use crate::sqlx;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const SCORE_RECOMPUTE_INTERVAL: Duration = Duration::from_secs(60 * 60);
static MESSAGE_SCRAPE_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> =
    std::sync::OnceLock::new();

async fn recompute_stale_scores(pool: &sqlx::PgPool, force_full: bool, reason: &str) {
    let (channels, users) = tokio::join!(
        async {
            db::scores::backfill_stale_channel_scores(pool, force_full)
                .await
                .map_err(|e| e.to_string())
        },
        async {
            db::scores::backfill_stale_user_scores(pool, force_full)
                .await
                .map_err(|e| e.to_string())
        },
    );
    match channels {
        Ok(n) => tracing::info!(
            "{} channel score recomputation done ({} channels)",
            reason,
            n
        ),
        Err(e) => tracing::warn!("{} channel score recomputation failed: {}", reason, e),
    }
    match users {
        Ok(n) => tracing::info!("{} user score recomputation done ({} users)", reason, n),
        Err(e) => tracing::warn!("{} user score recomputation failed: {}", reason, e),
    }
}

pub fn insert_page(
    pool: sqlx::PgPool,
    page: Vec<slack::SlackChannel>,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move {
        let rows: Vec<db::postgres_db::SlackChannelRow> = page
            .iter()
            .map(|ch| db::postgres_db::SlackChannelRow {
                channel_id: ch.id.clone(),
                name: ch.name.clone(),
                is_archived: u8::from(ch.is_archived),
                num_members: ch.num_members,
                created_at: ch.created_at,
            })
            .collect();
        if rows.is_empty() {
            return;
        }

        let ids: Vec<String> = rows.iter().map(|r| r.channel_id.clone()).collect();
        let previously_archived = match db::postgres_db::get_archived_channel_ids(&pool, &ids).await
        {
            Ok(ids) => Some(ids),
            Err(_) => {
                tracing::warn!("Failed to read channel archive state");
                None
            }
        };

        match tokio::time::timeout(
            Duration::from_secs(120),
            db::postgres_db::insert_new_channels_rows(&pool, &rows),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => tracing::error!("Failed to insert channels: {}", e),
            Err(_) => tracing::error!("Failed to insert channels: timed out after 2m"),
        }

        // An archived channel that came back needs a full re-scrape, not an incremental one.
        let Some(previously_archived) = previously_archived else {
            return;
        };
        for row in &rows {
            if row.is_archived == 0
                && previously_archived.contains(&row.channel_id)
                && let Err(e) = db::postgres_db::clear_fully_scraped(&pool, &row.channel_id).await
            {
                tracing::warn!(
                    "Failed to clear fully-scraped for unarchived channel {}: {}",
                    row.channel_id,
                    e
                );
            }
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
    let missing_profiles: std::collections::HashSet<String> =
        match db::postgres_db::get_user_ids_missing_profile(pool).await {
            Ok(ids) => ids.into_iter().collect(),
            Err(e) => {
                tracing::warn!("Failed to get users missing profiles: {}", e);
                std::collections::HashSet::new()
            }
        };
    tracing::info!(
        "Syncing users from Slack ({} already stored, {} missing pfps, {} missing profiles)",
        existing.len(),
        missing_pfps.len(),
        missing_profiles.len()
    );
    let existing = Arc::new(existing);
    let missing_pfps = Arc::new(missing_pfps);
    let missing_profiles = Arc::new(missing_profiles);
    let changed_total = Arc::new(AtomicU64::new(0));
    let result = slack_pool
        .fetch_users(|batch| {
            let pool = pool.clone();
            let existing = existing.clone();
            let missing_pfps = missing_pfps.clone();
            let missing_profiles = missing_profiles.clone();
            let changed_total = changed_total.clone();
            Box::pin(async move {
                let changed: Vec<db::postgres_db::SlackUserRow> = batch
                    .into_iter()
                    .filter(|u| {
                        u.is_deleted
                            || match existing.get(&u.id) {
                                Some(prev) => {
                                    *prev < u.updated
                                        || missing_pfps.contains(&u.id)
                                        || missing_profiles.contains(&u.id)
                                }
                                None => true,
                            }
                    })
                    .map(|u| db::postgres_db::SlackUserRow {
                        user_id: u.id,
                        merged_name: if !u.display_name.is_empty() {
                            u.display_name.clone()
                        } else if !u.real_name.is_empty() {
                            u.real_name.clone()
                        } else {
                            u.username.clone()
                        },
                        display_name: u.display_name.clone(),
                        real_name: u.real_name,
                        username: u.username,
                        email: u.email,
                        title: u.title,
                        status_text: u.status_text,
                        status_emoji: u.status_emoji,
                        tz: u.tz,
                        tz_label: u.tz_label,
                        locale: u.locale,
                        pfp: u.pfp,
                        updated: u.updated,
                        is_bot: u.is_bot as u8,
                        is_deleted: u.is_deleted as u8,
                        is_admin: u.is_admin as u8,
                        is_owner: u.is_owner as u8,
                        is_restricted: u.is_restricted as u8,
                        is_app_user: u.is_app_user as u8,
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
    db::postgres_db::load_locator_caches(&pool)
        .await
        .map_err(|e| format!("failed to load locator caches: {e}"))?;
    db::postgres_db::load_consent_cache(&pool)
        .await
        .map_err(|e| format!("failed to load consent cache: {e}"))?;
    if let Err(e) = db::postgres_db::seed_message_count(&pool).await {
        tracing::warn!("Failed to seed message count: {}", e);
    }
    if let Err(e) = db::postgres_db::backfill_word_counts(&pool).await {
        tracing::warn!("Failed to backfill word_counts: {}", e);
    }
    let sessionizer_changed = db::scores::sessionizer_changed(&pool)
        .await
        .unwrap_or(false);
    recompute_stale_scores(&pool, sessionizer_changed, "Startup").await;

    let pool_for_scores = pool.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(SCORE_RECOMPUTE_INTERVAL).await;
            recompute_stale_scores(&pool_for_scores, false, "Hourly").await;
        }
    });

    let cycle = Duration::from_secs(30 * 60);

    loop {
        let cycle_start = std::time::Instant::now();
        let request_delay = Duration::from_millis(settings.get_u64("SLACK_REQUEST_DELAY_MS"));
        let max_inflight = settings.get_u64("SLACK_MAX_INFLIGHT") as usize;
        let bot_tokens = settings.get_list("SLACK_BOT_TOKENS");
        let user_tokens = settings.get_list("SLACK_USER_TOKENS");
        // lists accept any token, so fall back to user tokens for the archive sweep
        let list_tokens = if bot_tokens.is_empty() {
            user_tokens.clone()
        } else {
            bot_tokens
        };
        let list_pool = slack::SlackClientPool::new(list_tokens, request_delay, max_inflight);

        // List and message passes have separate rate budgets, so run them in parallel
        let (list_result, _) = tokio::join!(full_fetch(&list_pool, &pool), async {
            if !user_tokens.is_empty() {
                scrape_all_messages(&settings, &pool).await;
                backfill_consented_content(&settings, &pool).await;
            }
        });
        if let Err(e) = list_result {
            tracing::warn!("Failed to fetch channel list: {}", e);
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

async fn backfill_consented_content(settings: &settings::RuntimeSettings, pool: &sqlx::PgPool) {
    let pending = match db::postgres_db::pending_consent_channels(pool).await {
        Ok(pending) => pending,
        Err(e) => {
            tracing::warn!("Failed to load consent content backfills: {}", e);
            return;
        }
    };
    if pending.is_empty() {
        return;
    }
    let tokens = settings.get_list("SLACK_USER_TOKENS");
    let Some(token) = tokens.first() else {
        return;
    };
    let client = slack::SlackClient::new(
        token.clone(),
        Duration::from_millis(settings.get_u64("SLACK_REQUEST_DELAY_MS")),
        settings.get_u64("SLACK_MAX_INFLIGHT") as usize,
    );
    let mut channels: HashMap<String, std::collections::HashSet<i32>> = HashMap::new();
    let mut resume_at: HashMap<String, u64> = HashMap::new();
    let mut complete: HashMap<i32, bool> = HashMap::new();
    for (identity_id, channel_id, latest_message_ts) in pending {
        channels
            .entry(channel_id.clone())
            .or_default()
            .insert(identity_id);
        resume_at
            .entry(channel_id)
            .and_modify(|current| *current = (*current).min(latest_message_ts))
            .or_insert(latest_message_ts);
        complete.entry(identity_id).or_insert(true);
    }

    for (channel_id, identities) in channels {
        let pool_for_page = pool.clone();
        let channel_for_page = channel_id.clone();
        let identities_for_page: Vec<i32> = identities.iter().copied().collect();
        let identities_for_callback = identities_for_page.clone();
        let failed = Arc::new(AtomicBool::new(false));
        let failed_for_page = failed.clone();
        let latest = resume_at
            .get(&channel_id)
            .copied()
            .filter(|&timestamp| timestamp > 0)
            .map(db::postgres_db::micros_to_slack_ts);
        let result = client
            .stream_channel_history_before(&channel_id, None, latest.as_deref(), move |page| {
                let pool = pool_for_page.clone();
                let channel_id = channel_for_page.clone();
                let identities = identities_for_callback.clone();
                let failed = failed_for_page.clone();
                Box::pin(async move {
                    if failed.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                    let oldest = page
                        .iter()
                        .map(|message| db::postgres_db::slack_ts_to_micros(&message.ts))
                        .min();
                    let rows: Vec<db::postgres_db::SlackMessageRow> = page
                        .iter()
                        .map(|message| db::postgres_db::SlackMessageRow {
                            user_id: message.user.clone(),
                            channel_id: message.channel.clone(),
                            message_ts: db::postgres_db::slack_ts_to_micros(&message.ts),
                            char_count: message.text.chars().count() as i32,
                            thread_ts: message
                                .thread_ts
                                .as_deref()
                                .map(db::postgres_db::slack_ts_to_micros),
                            text: message.text.clone(),
                        })
                        .collect();
                    db::postgres_db::insert_messages(&pool, &rows)
                        .await
                        .map_err(|e| e.to_string())?;
                    if let Some(oldest) = oldest {
                        db::postgres_db::save_consent_backfill_progress(
                            &pool,
                            &identities,
                            &channel_id,
                            oldest,
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                    }
                    Ok(())
                })
            })
            .await;
        if result.is_err() || failed.load(Ordering::Relaxed) {
            for identity_id in &identities_for_page {
                complete.insert(*identity_id, false);
            }
            continue;
        }
        if let Err(e) =
            db::postgres_db::complete_consent_backfill(pool, &identities_for_page, &channel_id)
                .await
        {
            tracing::warn!(
                "Failed to complete consent backfill in {}: {}",
                channel_id,
                e
            );
            for identity_id in &identities_for_page {
                complete.insert(*identity_id, false);
            }
        }
    }
    for (identity_id, succeeded) in complete {
        if succeeded
            && let Err(e) = db::postgres_db::mark_content_backfilled(pool, identity_id).await
        {
            tracing::warn!(
                "Failed to mark content backfill complete for {}: {}",
                identity_id,
                e
            );
        }
    }
}

pub async fn scrape_incremental_messages(
    settings: &settings::RuntimeSettings,
    pool: &sqlx::PgPool,
) {
    scrape_messages(settings, pool, true).await;
}

async fn scrape_all_messages(settings: &settings::RuntimeSettings, pool: &sqlx::PgPool) {
    scrape_messages(settings, pool, false).await;
}

async fn scrape_messages(
    settings: &settings::RuntimeSettings,
    pool: &sqlx::PgPool,
    incremental_only: bool,
) {
    let _guard = MESSAGE_SCRAPE_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
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

    // Archive+fully-scraped channels are skipped: their history is frozen.
    let skip_archived: std::collections::HashSet<String> =
        match db::postgres_db::get_archived_scraped_channel_ids(pool).await {
            Ok(ids) => ids.into_iter().collect(),
            Err(e) => {
                tracing::warn!("Failed to get archived scraped channel IDs: {}", e);
                std::collections::HashSet::new()
            }
        };

    let new_channels: Vec<String> = channels
        .iter()
        .filter(|c| !scraped_set.contains(c))
        .cloned()
        .collect();
    let check_channels: Vec<String> = channels
        .iter()
        .filter(|c| scraped_set.contains(c) && !skip_archived.contains(*c))
        .cloned()
        .collect();

    tracing::info!(
        "{} known channels: {} new to full-scrape, {} already-scraped to check for new messages (skipping {} archived fully-scraped)",
        channels.len(),
        new_channels.len(),
        check_channels.len(),
        skip_archived.len()
    );

    let check_channels = if incremental_only {
        check_channels
    } else {
        Vec::new()
    };

    let touched_users = Arc::new(Mutex::new(std::collections::HashSet::new()));
    let touched_channels = Arc::new(Mutex::new(std::collections::HashSet::new()));

    if !check_channels.is_empty() {
        let resume = db::postgres_db::get_sweep_resume(pool)
            .await
            .unwrap_or(None);
        let start = resume
            .as_ref()
            .and_then(|c| check_channels.iter().position(|x| x == c).map(|p| p + 1))
            .unwrap_or(0);
        tracing::info!(
            "Checking {} already-scraped channels for new messages (resuming at {}: {})",
            check_channels.len(),
            start,
            if start > 0 {
                check_channels[start - 1].as_str()
            } else {
                "start"
            }
        );
        let sweep = ScrapeSweep::new(pool.clone(), &check_channels, start);
        scrape_channel_list(
            settings,
            pool,
            &check_channels,
            touched_users.clone(),
            touched_channels.clone(),
            start,
            Some(sweep.clone()),
            Some(Duration::from_secs(
                settings.get_u64("SLACK_INCREMENTAL_BUDGET_SECS"),
            )),
        )
        .await;
        sweep.finish().await;
    }

    if incremental_only {
        tracing::info!("Incremental message check budget complete");
        return;
    }

    if !new_channels.is_empty() {
        tracing::info!("Full-scraping {} new channels...", new_channels.len());
        scrape_channel_list(
            settings,
            pool,
            &new_channels,
            touched_users.clone(),
            touched_channels.clone(),
            0,
            None,
            None,
        )
        .await;
    }

    tracing::info!("Message scrape pass complete");
}

async fn scrape_channel_list(
    settings: &settings::RuntimeSettings,
    pool: &sqlx::PgPool,
    channels: &[String],
    touched_users: Arc<Mutex<std::collections::HashSet<String>>>,
    touched_channels: Arc<Mutex<std::collections::HashSet<String>>>,
    resume_from: usize,
    sweep: Option<ScrapeSweep>,
    budget: Option<Duration>,
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
    let deadline = budget.map(|duration| Instant::now() + duration);

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
    let next = Arc::new(AtomicUsize::new(resume_from));
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
            sweep: sweep.clone(),
            tx,
            deadline,
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
    sweep: Option<ScrapeSweep>,
    deadline: Option<Instant>,
    tx: tokio::sync::mpsc::Sender<usize>,
    touched_users: Arc<Mutex<std::collections::HashSet<String>>>,
    touched_channels: Arc<Mutex<std::collections::HashSet<String>>>,
}

/// Tracks the position of the incremental channel sweep across passes so the
/// next pass continues where the last one left off instead of restarting at the
/// start of the channel list. The resume point is persisted to `scrape_sweep`;
/// when a full pass reaches the end the cursor is cleared so the next pass
/// starts fresh.
#[derive(Clone)]
struct ScrapeSweep {
    pool: sqlx::PgPool,
    channels: Arc<Vec<String>>,
    start: usize,
    expected: Arc<AtomicUsize>,
    done: Arc<Mutex<std::collections::HashSet<usize>>>,
}

impl ScrapeSweep {
    fn new(pool: sqlx::PgPool, channels: &[String], start: usize) -> Self {
        Self {
            pool,
            channels: Arc::new(channels.to_vec()),
            start,
            expected: Arc::new(AtomicUsize::new(start)),
            done: Arc::new(Mutex::new(std::collections::HashSet::new())),
        }
    }

    /// Records a completed channel index and advances the contiguous resume point,
    /// Only advances past channels that have actually finished so a slow channel is never skipped.
    async fn note_done(&self, idx: usize) {
        {
            let mut done = self.done.lock().unwrap();
            done.insert(idx);
            let mut next = self.expected.load(Ordering::Relaxed);
            while done.remove(&next) {
                next += 1;
            }
            self.expected.store(next, Ordering::Relaxed);
        }
        let next = self.expected.load(Ordering::Relaxed);
        if next >= self.channels.len() || (next > self.start && next.is_multiple_of(250)) {
            self.persist().await;
        }
    }

    async fn persist(&self) {
        let next = self.expected.load(Ordering::Relaxed);
        let res = if next >= self.channels.len() {
            db::postgres_db::clear_sweep_resume(&self.pool).await
        } else if next > 0 {
            db::postgres_db::set_sweep_resume(&self.pool, &self.channels[next - 1]).await
        } else {
            return;
        };
        if let Err(e) = res {
            tracing::warn!("Failed to persist sweep resume: {}", e);
        }
    }

    /// Ensures a completed pass clears the cursor
    async fn finish(&self) {
        if self.expected.load(Ordering::Relaxed) >= self.channels.len()
            && let Err(e) = db::postgres_db::clear_sweep_resume(&self.pool).await
        {
            tracing::warn!("Failed to clear sweep resume: {}", e);
        }
    }
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
                if ctx
                    .deadline
                    .is_some_and(|deadline| Instant::now() >= deadline)
                {
                    break;
                }
                let idx = next.fetch_add(1, Ordering::Relaxed);
                if idx >= channels.len() {
                    break;
                }
                let channel_id = channels[idx].clone();
                scrape_one_channel(&client, &pool, channel_id, idx + 1, &ctx).await;
                if let Some(sweep) = &ctx.sweep {
                    sweep.note_done(idx).await;
                }
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

async fn upsert_bot_users(
    pool: &sqlx::PgPool,
    messages: &[slack::SlackMessage],
) -> Result<(), String> {
    let mut seen = std::collections::HashSet::new();
    let bots: Vec<db::postgres_db::SlackUserRow> = messages
        .iter()
        .filter(|m| m.user.starts_with('B') && seen.insert(m.user.clone()))
        .map(|m| db::postgres_db::SlackUserRow {
            user_id: m.user.clone(),
            merged_name: m.bot_name.clone().unwrap_or_else(|| m.user.clone()),
            display_name: String::new(),
            real_name: String::new(),
            username: String::new(),
            email: String::new(),
            title: String::new(),
            status_text: String::new(),
            status_emoji: String::new(),
            tz: String::new(),
            tz_label: String::new(),
            locale: String::new(),
            pfp: String::new(),
            updated: 0,
            is_bot: 1,
            is_deleted: 0,
            is_admin: 0,
            is_owner: 0,
            is_restricted: 0,
            is_app_user: 0,
        })
        .collect();
    if bots.is_empty() {
        return Ok(());
    }
    db::postgres_db::upsert_users(pool, &bots)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

#[derive(Default)]
struct ChannelPageAccum {
    inserted: u64,
    filtered_out: u64,
    thread_roots: std::collections::HashSet<String>,
    thread_activity: HashMap<String, (i64, u64)>,
}

#[derive(Default)]
struct ThreadPageAccum {
    inserted: u64,
    reply_users: Vec<String>,
    latest_reply_ts: u64,
}

async fn process_channel_page(
    pool: &sqlx::PgPool,
    channel_id: &str,
    oldest: Option<&str>,
    page: Vec<slack::SlackMessage>,
    accum: &std::sync::Mutex<ChannelPageAccum>,
    total: &AtomicU64,
    touched_users: &std::sync::Mutex<std::collections::HashSet<String>>,
) -> Result<(), String> {
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
                if m.reply_count.is_some() || m.latest_reply.is_some() {
                    a.thread_activity.insert(
                        t.clone(),
                        (
                            m.reply_count.map(|count| count as i64).unwrap_or(-1),
                            m.latest_reply
                                .as_deref()
                                .map(db::postgres_db::slack_ts_to_micros)
                                .unwrap_or(0),
                        ),
                    );
                }
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
            char_count: m.text.chars().count() as i32,
            thread_ts: m
                .thread_ts
                .as_deref()
                .map(db::postgres_db::slack_ts_to_micros),
            text: m.text.clone(),
        })
        .collect();

    let inserted = if rows.is_empty() {
        0
    } else {
        db::postgres_db::insert_messages(pool, &rows)
            .await
            .map_err(|e| e.to_string())?
    };
    total.fetch_add(inserted, Ordering::Relaxed);
    accum.lock().unwrap().inserted += inserted;

    let reaction_rows = reaction_rows_from(&page, channel_id);
    if !reaction_rows.is_empty() {
        db::postgres_db::insert_reactions(pool, &reaction_rows)
            .await
            .map_err(|e| e.to_string())?;
    }

    let word_rows = word_count_rows_from(&page, channel_id);
    if !word_rows.is_empty() {
        db::postgres_db::insert_word_counts(pool, &word_rows)
            .await
            .map_err(|e| e.to_string())?;
    }

    upsert_bot_users(pool, &page).await?;
    Ok(())
}

async fn process_thread_page(
    pool: &sqlx::PgPool,
    channel_id: &str,
    thread_ts: &str,
    thread_oldest: Option<&str>,
    page: Vec<slack::SlackMessage>,
    accum: &std::sync::Mutex<ThreadPageAccum>,
    total: &AtomicU64,
) -> Result<(), String> {
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
                char_count: m.text.chars().count() as i32,
                thread_ts: m
                    .thread_ts
                    .as_deref()
                    .map(db::postgres_db::slack_ts_to_micros),
                text: m.text.clone(),
            })
            .collect();
        inserted = db::postgres_db::insert_messages(pool, &rows)
            .await
            .map_err(|e| e.to_string())?;
        total.fetch_add(inserted, Ordering::Relaxed);
    }

    {
        let mut a = accum.lock().unwrap();
        a.inserted += inserted;
        a.latest_reply_ts = a.latest_reply_ts.max(
            page.iter()
                .map(|m| db::postgres_db::slack_ts_to_micros(&m.ts))
                .max()
                .unwrap_or(0),
        );
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
    if !reply_reactions.is_empty() {
        db::postgres_db::insert_reactions(pool, &reply_reactions)
            .await
            .map_err(|e| e.to_string())?;
    }

    let reply_words = word_count_rows_from(&page, channel_id);
    if !reply_words.is_empty() {
        db::postgres_db::insert_word_counts(pool, &reply_words)
            .await
            .map_err(|e| e.to_string())?;
    }

    upsert_bot_users(pool, &page).await?;
    Ok(())
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
        thread_activity: HashMap::new(),
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
                .await
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
    let thread_activity = std::sync::Arc::new(std::sync::Mutex::new(acc.thread_activity));

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

    let mut rescan_failed = false;
    if oldest.is_some()
        && db::postgres_db::get_thread_rescan_at(pool, &channel_id)
            .await
            .map(|last| {
                let now = db::postgres_db::now_secs();
                now.saturating_sub(last) >= ctx.thread_rescan_interval_hours.saturating_mul(3600)
            })
            .unwrap_or(true)
    {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let window_ts = now
            .saturating_sub(ctx.thread_rescan_window_hours * 3600)
            .to_string();
        if let Err(e) =
            db::postgres_db::mark_thread_rescan(pool, &channel_id, db::postgres_db::now_secs())
                .await
        {
            tracing::warn!(
                "Failed to persist thread rescan time for {}: {}",
                channel_id,
                e
            );
            rescan_failed = true;
        }
        let thread_roots = thread_roots.clone();
        let thread_activity = thread_activity.clone();
        let thread_activity_for_page = thread_activity.clone();
        let pool_for_stream = pool.clone();
        let channel_id_for_rescan = channel_id.clone();
        match user_client
            .stream_channel_history(&channel_id, Some(&window_ts), move |page| {
                let thread_roots = thread_roots.clone();
                let thread_activity = thread_activity_for_page.clone();
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
                            if let Some(ref t) = msg.thread_ts
                                && t == &msg.ts
                                && (msg.reply_count.is_some() || msg.latest_reply.is_some())
                            {
                                thread_activity.lock().unwrap().insert(
                                    t.clone(),
                                    (
                                        msg.reply_count.map(|count| count as i64).unwrap_or(-1),
                                        msg.latest_reply
                                            .as_deref()
                                            .map(db::postgres_db::slack_ts_to_micros)
                                            .unwrap_or(0),
                                    ),
                                );
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
                    if !extra_reactions.is_empty() {
                        db::postgres_db::insert_reactions(&pool, &extra_reactions)
                            .await
                            .map_err(|e| e.to_string())?;
                    }
                    Ok(())
                })
            })
            .await
        {
            Ok(_) => {}
            Err(e) => {
                rescan_failed = true;
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

    let all_thread_parents: Vec<String> = thread_roots.lock().unwrap().iter().cloned().collect();
    let current_activity = thread_activity.lock().unwrap().clone();
    let stored_activity = match db::postgres_db::get_thread_activities(
        pool,
        &channel_id,
        &all_thread_parents,
    )
    .await
    {
        Ok(activity) => activity,
        Err(e) => {
            tracing::warn!("Failed to read thread activity for {}: {}", channel_id, e);
            return;
        }
    };
    let mut thread_activity_rows = Vec::with_capacity(current_activity.len());
    for (thread_ts, (reply_count, latest_reply_ts)) in &current_activity {
        thread_activity_rows.push((thread_ts.clone(), *reply_count, *latest_reply_ts));
    }
    if let Err(e) =
        db::postgres_db::upsert_thread_activities(pool, &channel_id, &thread_activity_rows).await
    {
        tracing::warn!("Failed to save thread activity for {}: {}", channel_id, e);
        return;
    }
    let thread_parents: Vec<String> = all_thread_parents
        .into_iter()
        .filter(|thread_ts| {
            if matches!(current_activity.get(thread_ts), Some((reply_count, _)) if *reply_count == 0)
                || matches!(stored_activity.get(thread_ts), Some((_, reply_count, _)) if *reply_count == 0)
            {
                return false;
            }
            match stored_activity.get(thread_ts) {
                Some((fully_scraped, reply_count, latest_reply_ts)) if *fully_scraped => {
                    match current_activity.get(thread_ts) {
                        Some((current_count, current_latest)) => {
                            *reply_count != *current_count || *latest_reply_ts != *current_latest
                        }
                        None => false,
                    }
                }
                _ => true,
            }
        })
        .collect();

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
        && !rescan_failed
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
        db::postgres_db::get_thread_high_water_mark(pool, &channel_id, &thread_ts)
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
                    .await
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
            let latest_reply_ts = acc.latest_reply_ts.max(
                thread_oldest
                    .as_deref()
                    .map(db::postgres_db::slack_ts_to_micros)
                    .unwrap_or(0),
            );

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

            if let Err(e) = db::postgres_db::mark_thread_fully_scraped(
                pool,
                &channel_id,
                &thread_ts,
                latest_reply_ts,
            )
            .await
            {
                tracing::warn!(
                    "[token {}][{}/{}] Failed to mark thread {} as scraped: {}",
                    token_idx,
                    idx,
                    total_channels,
                    thread_ts,
                    e
                );
                return (1, inserted, reply_users);
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
    let pool_for_fetch = pool.clone();
    let total = slack_pool
        .fetch_channels_paginated(move |page| insert_page(pool_for_fetch.clone(), page), None)
        .await
        .map_err(|e| e.to_string())?;

    tracing::info!("Full rescan done! {} total channels", total);
    Ok(())
}
