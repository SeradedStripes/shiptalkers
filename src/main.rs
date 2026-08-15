use ship_talkers::{db, settings, slack, website};

use dotenvy::dotenv;
use std::collections::HashMap;
use std::env;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let auth_db_path = env::var("SQLITE_DB_PATH").unwrap_or_else(|_| "data/auth.db".into());
    let auth_db = std::sync::Arc::new(
        db::sqlite::AuthDb::open(&auth_db_path)
            .map_err(|e| format!("Failed to open auth DB {}: {}", auth_db_path, e))?,
    );
    tracing::info!("Auth DB at {}", auth_db_path);

    let settings = settings::RuntimeSettings::load();

    let (clickhouse_url, url_user, url_password, url_db) =
        db::normalize_clickhouse_url(&settings.get("CLICKHOUSE_URL"));
    let clickhouse_user = if settings.was_set("CLICKHOUSE_USER") {
        settings.get("CLICKHOUSE_USER")
    } else {
        url_user.unwrap_or_else(|| settings.get("CLICKHOUSE_USER"))
    };
    let clickhouse_password = if settings.was_set("CLICKHOUSE_PASSWORD") {
        settings.get("CLICKHOUSE_PASSWORD")
    } else {
        url_password.unwrap_or_else(|| settings.get("CLICKHOUSE_PASSWORD"))
    };
    let clickhouse_db = if settings.was_set("CLICKHOUSE_DB") {
        settings.get("CLICKHOUSE_DB")
    } else {
        url_db.unwrap_or_else(|| settings.get("CLICKHOUSE_DB"))
    };

    let database = db::Database::new(
        &clickhouse_url,
        &clickhouse_user,
        &clickhouse_password,
        &clickhouse_db,
    );

    tracing::info!("Initializing ClickHouse tables...");
    db::clickhouse_db::init_tables(&database.clickhouse).await?;

    let has_bot_tokens = !settings.get_list("SLACK_BOT_TOKENS").is_empty();
    let has_user_tokens = !settings.get_list("SLACK_USER_TOKENS").is_empty();
    let has_app_tokens = !settings.get_list("SLACK_APP_TOKENS").is_empty();

    {
        let clickhouse_for_words = database.clickhouse.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = db::clickhouse_db::refresh_word_totals(&clickhouse_for_words).await
                {
                    tracing::warn!("Failed to refresh word totals: {}", e);
                }
                tokio::time::sleep(std::time::Duration::from_secs(30 * 60)).await;
            }
        });
    }

    if has_bot_tokens || has_user_tokens || has_app_tokens {
        let clickhouse_for_scraper = database.clickhouse.clone();
        let settings_for_scraper = settings.clone();

        tokio::spawn(async move {
            if let Err(e) = run_scraper(clickhouse_for_scraper, settings_for_scraper).await {
                tracing::error!("Scraper error: {}", e);
            }
        });

        if has_bot_tokens {
            let clickhouse_for_users = database.clickhouse.clone();
            let settings_for_users = settings.clone();
            tokio::spawn(async move {
                loop {
                    let pool = slack::SlackClientPool::new(
                        settings_for_users.get_list("SLACK_BOT_TOKENS"),
                        Duration::from_millis(
                            settings_for_users.get_u64("SLACK_USER_SYNC_DELAY_MS"),
                        ),
                        settings_for_users.get_u64("SLACK_MAX_INFLIGHT") as usize,
                    );
                    let ok = sync_users(&pool, &clickhouse_for_users).await;
                    let wait = if ok { 7200 } else { 300 };
                    tokio::time::sleep(std::time::Duration::from_secs(wait)).await;
                }
            });
        } else {
            tracing::warn!("SLACK_BOT_TOKENS not set, user sync disabled");
        }
    } else {
        tracing::warn!(
            "No Slack tokens set (SLACK_BOT_TOKENS/SLACK_USER_TOKENS/SLACK_APP_TOKENS), \
             skipping Slack API entirely and serving existing ClickHouse data"
        );
    }

    if has_app_tokens {
        let socket_config = slack::SocketConfig::new(settings.get_list("SLACK_APP_TOKENS"));
        let clickhouse_for_socket = database.clickhouse.clone();
        let auth_db_for_socket = auth_db.clone();
        let settings_for_socket = settings.clone();
        tokio::spawn(async move {
            if let Err(e) = slack::start_socket_mode(
                socket_config,
                clickhouse_for_socket,
                auth_db_for_socket,
                settings_for_socket,
            )
            .await
            {
                tracing::error!("Socket Mode error: {}", e);
            }
        });
    } else {
        tracing::warn!("SLACK_APP_TOKENS not set, Socket Mode disabled");
    }

    let addr = format!("{}:{}", settings.get("HOST"), settings.get("PORT"));

    {
        let clickhouse_for_resync = database.clickhouse.clone();
        let http_for_resync = reqwest::Client::new();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                website::auth::resync_all(&clickhouse_for_resync, &http_for_resync).await;
            }
        });
    }

    tracing::info!("Starting web server on {}", addr);
    let listener = TcpListener::bind(&addr).await?;
    axum::serve(
        listener,
        website::router(database.clickhouse, settings, auth_db),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("Received shutdown signal, stopping");
}

fn insert_page(
    clickhouse: clickhouse::Client,
    page: Vec<slack::SlackChannel>,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move {
        let rows: Vec<db::clickhouse_db::SlackChannelRow> = page
            .iter()
            .map(|ch| db::clickhouse_db::SlackChannelRow {
                channel_id: ch.id.clone(),
                name: ch.name.clone(),
            })
            .collect();

        match tokio::time::timeout(
            Duration::from_secs(120),
            db::clickhouse_db::insert_new_channels(&clickhouse, &rows),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => tracing::error!("Failed to insert channels: {}", e),
            Err(_) => tracing::error!("Failed to insert channels: timed out after 2m"),
        }
    })
}

async fn sync_users(slack_pool: &slack::SlackClientPool, clickhouse: &clickhouse::Client) -> bool {
    let existing = match db::clickhouse_db::get_user_updates(clickhouse).await {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!("Failed to get stored user updates: {}", e);
            return false;
        }
    };
    let missing_pfps: std::collections::HashSet<String> =
        match db::clickhouse_db::get_user_ids_without_pfp(clickhouse).await {
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
            let clickhouse = clickhouse.clone();
            let existing = existing.clone();
            let missing_pfps = missing_pfps.clone();
            let changed_total = changed_total.clone();
            Box::pin(async move {
                let changed: Vec<db::clickhouse_db::SlackUserRow> = batch
                    .into_iter()
                    .filter(|u| {
                        u.is_deleted
                            || match existing.get(&u.id) {
                                Some(prev) => *prev < u.updated || missing_pfps.contains(&u.id),
                                None => true,
                            }
                    })
                    .map(|u| db::clickhouse_db::SlackUserRow {
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
                match db::clickhouse_db::upsert_users(&clickhouse, &changed).await {
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

async fn run_scraper(
    clickhouse: clickhouse::Client,
    settings: settings::RuntimeSettings,
) -> Result<(), String> {
    if let Err(e) = db::clickhouse_db::backfill_slack_messages_by_user(&clickhouse).await {
        tracing::warn!("Failed to backfill slack_messages_by_user: {}", e);
    }
    if let Err(e) = db::clickhouse_db::backfill_word_counts(&clickhouse).await {
        tracing::warn!("Failed to backfill word_counts: {}", e);
    }
    // Resolve the sessionizer-change flag once so the backfills can run in parallel:
    // the user backfill writes score_meta after it finishes, so checking it first
    // means the channel backfill can't see a stale row mid-recompute.
    let sessionizer_changed = db::clickhouse_db::sessionizer_changed(&clickhouse)
        .await
        .unwrap_or(false);
    let (channels, users) = tokio::join!(
        async {
            db::clickhouse_db::backfill_stale_channel_scores(&clickhouse, sessionizer_changed)
                .await
                .map_err(|e| e.to_string())
        },
        async {
            db::clickhouse_db::backfill_stale_user_scores(&clickhouse, sessionizer_changed)
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
    let mut last_optimize = std::time::Instant::now();

    loop {
        let cycle_start = std::time::Instant::now();
        let request_delay = Duration::from_millis(settings.get_u64("SLACK_REQUEST_DELAY_MS"));
        let max_inflight = settings.get_u64("SLACK_MAX_INFLIGHT") as usize;
        let bot_tokens = settings.get_list("SLACK_BOT_TOKENS");
        let user_tokens = settings.get_list("SLACK_USER_TOKENS");
        let bot_pool = slack::SlackClientPool::new(bot_tokens, request_delay, max_inflight);

        if let Err(e) = full_fetch(&bot_pool, &clickhouse).await {
            tracing::warn!("Failed to fetch channel list: {}", e);
        }

        if !user_tokens.is_empty() {
            scrape_all_messages(&settings, &clickhouse).await;
        }

        if last_optimize.elapsed() >= Duration::from_secs(86400) {
            if let Err(e) = db::clickhouse_db::optimize_slack_messages(&clickhouse).await {
                tracing::warn!("Failed to optimize slack_messages: {}", e);
            }
            last_optimize = std::time::Instant::now();
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

async fn scrape_all_messages(
    settings: &settings::RuntimeSettings,
    clickhouse: &clickhouse::Client,
) {
    let channels = match db::clickhouse_db::get_known_channel_ids(clickhouse).await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("Failed to get channel IDs: {}", e);
            return;
        }
    };

    if let Err(e) = db::clickhouse_db::backfill_scraped_channels(clickhouse).await {
        tracing::warn!("Failed to backfill scraped channels: {}", e);
    }

    let scraped = match db::clickhouse_db::get_scraped_channel_ids(clickhouse).await {
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
            clickhouse,
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
            clickhouse,
            &check_channels,
            touched_users.clone(),
            touched_channels.clone(),
        )
        .await;
    }

    // Recompute scores once per pass for every user whose messages changed,
    // instead of after each channel.
    let users: Vec<String> = touched_users.lock().unwrap().iter().cloned().collect();
    if !users.is_empty()
        && let Err(e) = db::clickhouse_db::recompute_user_scores(clickhouse, &users).await
    {
        tracing::warn!(
            "Failed to recompute scores for {} users this pass: {}",
            users.len(),
            e
        );
    }

    let channels: Vec<String> = touched_channels.lock().unwrap().iter().cloned().collect();
    if !channels.is_empty()
        && let Err(e) = db::clickhouse_db::recompute_channel_scores(clickhouse, &channels).await
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
    clickhouse: &clickhouse::Client,
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
    // Shared work queue: each token's workers pull the next channel when a slot
    // frees up, so a token that finishes a slow channel immediately grabs the
    // next one instead of idling on a pre-computed shard.
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
        let clickhouse = clickhouse.clone();
        let next = next.clone();
        let channels = channels.to_vec();
        workers.push(tokio::spawn(async move {
            scrape_shard(&client, &clickhouse, channels, next, ctx, rx).await;
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
    clickhouse: &clickhouse::Client,
    channels: Vec<String>,
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
        let clickhouse = clickhouse.clone();
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
                scrape_one_channel(&client, &clickhouse, channel_id, idx + 1, &ctx).await;
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
) -> Vec<db::clickhouse_db::SlackReactionRow> {
    let mut rows = Vec::new();
    for m in messages {
        let message_ts = db::clickhouse_db::slack_ts_to_micros(&m.ts);
        for reaction in &m.reactions {
            for user_id in &reaction.users {
                rows.push(db::clickhouse_db::SlackReactionRow {
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
) -> Vec<db::clickhouse_db::WordCountRow> {
    let mut rows = Vec::new();
    for m in messages {
        let message_ts = db::clickhouse_db::slack_ts_to_micros(&m.ts);
        let lower = m.text.to_lowercase();
        let mut counts: std::collections::HashMap<&str, u64> = std::collections::HashMap::new();
        for word in lower
            .split(|c: char| !c.is_ascii_lowercase())
            .filter(|w| w.len() > 1)
        {
            *counts.entry(word).or_insert(0) += 1;
        }
        for (word, count) in counts {
            rows.push(db::clickhouse_db::WordCountRow {
                word: word.to_string(),
                user_id: m.user.clone(),
                channel_id: channel_id.to_string(),
                message_ts,
                count,
            });
        }
    }
    rows
}

async fn scrape_one_channel(
    user_client: &slack::SlackClient,
    clickhouse: &clickhouse::Client,
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
    let fully_scraped = db::clickhouse_db::is_fully_scraped(clickhouse, &channel_id)
        .await
        .unwrap_or(false);

    let oldest = match db::clickhouse_db::get_max_message_ts(clickhouse, &channel_id).await {
        Ok(Some(ts)) if ts > 0 => {
            let ts = db::clickhouse_db::micros_to_slack_ts(ts);
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

    let raw_count;
    let messages = match user_client
        .get_channel_history(&channel_id, oldest.as_deref())
        .await
    {
        Ok(m) => {
            raw_count = m.len();
            m
        }
        Err(e) => {
            if e.to_string().contains("channel_not_found") {
                tracing::warn!(
                    "[token {}][{}/{}] Channel {} no longer exists, skipping",
                    token_idx,
                    idx,
                    total_channels,
                    channel_id
                );
                if let Err(err) =
                    db::clickhouse_db::mark_channel_scraped(clickhouse, &channel_id).await
                {
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

    // Filter out messages at or before the oldest timestamp to avoid duplicates
    let messages: Vec<_> = if let Some(ref oldest_ts) = oldest {
        messages.into_iter().filter(|m| m.ts > *oldest_ts).collect()
    } else {
        messages
    };
    let filtered_out = raw_count.saturating_sub(messages.len());

    // Insert main channel messages first, before thread replies
    let rows: Vec<db::clickhouse_db::SlackMessageRow> = messages
        .iter()
        .map(|m| db::clickhouse_db::SlackMessageRow {
            user_id: m.user.clone(),
            channel_id: m.channel.clone(),
            message_ts: db::clickhouse_db::slack_ts_to_micros(&m.ts),
            text: m.text.clone(),
            thread_ts: m.thread_ts.clone(),
        })
        .collect();

    let mut inserted = 0u64;
    if !rows.is_empty() {
        inserted = db::clickhouse_db::insert_messages(clickhouse, &rows)
            .await
            .unwrap_or(0);
        total.fetch_add(inserted, Ordering::Relaxed);
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
    }

    let reaction_rows = reaction_rows_from(&messages, &channel_id);
    if !reaction_rows.is_empty()
        && let Err(e) = db::clickhouse_db::insert_reactions(clickhouse, &reaction_rows).await
    {
        tracing::warn!(
            "[token {}][{}/{}] Failed to insert reactions for {}: {}",
            token_idx,
            idx,
            total_channels,
            channel_id,
            e
        );
    }

    let word_rows = word_count_rows_from(&messages, &channel_id);
    if !word_rows.is_empty()
        && let Err(e) = db::clickhouse_db::insert_word_counts(clickhouse, &word_rows).await
    {
        tracing::warn!(
            "[token {}][{}/{}] Failed to insert word counts for {}: {}",
            token_idx,
            idx,
            total_channels,
            channel_id,
            e
        );
    }

    // Mark the channel as scraped as soon as its messages are stored, so a timeout
    // later in the thread phase doesn't force a full re-scrape next pass.
    if let Err(e) = db::clickhouse_db::mark_channel_scraped(clickhouse, &channel_id).await {
        tracing::warn!(
            "[token {}][{}/{}] Failed to record {} as scraped: {}",
            token_idx,
            idx,
            total_channels,
            channel_id,
            e
        );
    }

    // Collect unique thread parents from replies
    let mut thread_parents: Vec<String> = Vec::new();
    for msg in &messages {
        if let Some(ref t) = msg.thread_ts
            && t == &msg.ts
            && !thread_parents.contains(t)
        {
            thread_parents.push(t.clone());
        }
    }

    // If the channel's first scrape's thread phase was interrupted (messages
    // stored but threads never all fetched), pull every stored thread root so
    // old threads outside the rescan window still get their replies recovered.
    // The channel is marked fully scraped below only once every thread fetched
    // cleanly, so a channel with transient failures retries the recovery.
    if !fully_scraped {
        let stored_roots = db::clickhouse_db::get_thread_roots(clickhouse, &channel_id)
            .await
            .unwrap_or_default();
        let mut added = 0usize;
        for root in stored_roots {
            if !thread_parents.contains(&root) {
                thread_parents.push(root);
                added += 1;
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

    // For channels already being checked incrementally, periodically re-scan a
    // recent window of history so messages that gained a thread since they were
    // first scraped (their root is older than this pass's new messages) get their
    // threads fetched too. Throttled per channel.
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
        // Record the attempt up front so a rescan that hangs or fails retries at
        // most every interval instead of on every pass.
        record_thread_rescan(&channel_id);
        match user_client
            .get_channel_history(&channel_id, Some(&window_ts))
            .await
        {
            Ok(extra) => {
                let mut found = 0usize;
                for msg in &extra {
                    if let Some(ref t) = msg.thread_ts
                        && t == &msg.ts
                        && !thread_parents.contains(t)
                    {
                        thread_parents.push(t.clone());
                        found += 1;
                    }
                }
                if found > 0 {
                    tracing::info!(
                        "[token {}][{}/{}] Thread re-scan of {} found {} new thread roots",
                        token_idx,
                        idx,
                        total_channels,
                        channel_id,
                        found
                    );
                }
                let extra_reactions = reaction_rows_from(&extra, &channel_id);
                if !extra_reactions.is_empty()
                    && let Err(e) =
                        db::clickhouse_db::insert_reactions(clickhouse, &extra_reactions).await
                {
                    tracing::warn!(
                        "[token {}][{}/{}] Failed to insert reactions from thread re-scan of {}: {}",
                        token_idx,
                        idx,
                        total_channels,
                        channel_id,
                        e
                    );
                }
            }
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
            let clickhouse = clickhouse.clone();
            let channel_id = channel_id.clone();
            let thread_ts = thread_ts.clone();
            let total = total.clone();
            handles.push(tokio::spawn(async move {
                let _permit = permit.acquire().await;
                scrape_thread(
                    &client,
                    &clickhouse,
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
        for (skipped, inserted_replies, users) in futures::future::join_all(handles)
            .await
            .into_iter()
            .flatten()
        {
            threads_skipped += skipped;
            thread_replies += inserted_replies;
            thread_users.extend(users);
        }
    }

    // Collect everyone who posted so scores can be recomputed once at the end of
    // the pass instead of per channel.
    {
        let mut set = ctx.touched_users.lock().unwrap();
        for m in &messages {
            set.insert(m.user.clone());
        }
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
        && let Err(e) = db::clickhouse_db::mark_fully_scraped(clickhouse, &channel_id).await
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
    clickhouse: &clickhouse::Client,
    channel_id: String,
    thread_ts: String,
    token_idx: usize,
    idx: usize,
    total_channels: usize,
    total: Arc<AtomicU64>,
) -> (usize, u64, Vec<String>) {
    let thread_fully =
        db::clickhouse_db::is_thread_fully_scraped(clickhouse, &channel_id, &thread_ts)
            .await
            .unwrap_or(false);
    let thread_oldest = if thread_fully {
        db::clickhouse_db::get_max_thread_reply_ts(clickhouse, &channel_id, &thread_ts)
            .await
            .ok()
            .flatten()
            .filter(|&ts| ts > 0)
            .map(db::clickhouse_db::micros_to_slack_ts)
    } else {
        None
    };

    match user_client
        .fetch_thread_replies(&channel_id, &thread_ts, thread_oldest.as_deref())
        .await
    {
        Ok(replies) => {
            let replies: Vec<_> = replies
                .into_iter()
                .filter(|m| m.ts != thread_ts)
                .filter(|m| match &thread_oldest {
                    Some(o) => m.ts > *o,
                    None => true,
                })
                .collect();

            let mut inserted = 0u64;
            let reply_users: Vec<String> = replies.iter().map(|m| m.user.clone()).collect();
            if !replies.is_empty() {
                let rows: Vec<db::clickhouse_db::SlackMessageRow> = replies
                    .iter()
                    .map(|m| db::clickhouse_db::SlackMessageRow {
                        user_id: m.user.clone(),
                        channel_id: m.channel.clone(),
                        message_ts: db::clickhouse_db::slack_ts_to_micros(&m.ts),
                        text: m.text.clone(),
                        thread_ts: m.thread_ts.clone(),
                    })
                    .collect();

                inserted = db::clickhouse_db::insert_messages(clickhouse, &rows)
                    .await
                    .unwrap_or(0);
                total.fetch_add(inserted, Ordering::Relaxed);
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

            let reply_reactions = reaction_rows_from(&replies, &channel_id);
            if !reply_reactions.is_empty()
                && let Err(e) =
                    db::clickhouse_db::insert_reactions(clickhouse, &reply_reactions).await
            {
                tracing::warn!(
                    "[token {}][{}/{}] Failed to insert reactions for thread {} in {}: {}",
                    token_idx,
                    idx,
                    total_channels,
                    thread_ts,
                    channel_id,
                    e
                );
            }

            let reply_words = word_count_rows_from(&replies, &channel_id);
            if !reply_words.is_empty()
                && let Err(e) =
                    db::clickhouse_db::insert_word_counts(clickhouse, &reply_words).await
            {
                tracing::warn!(
                    "[token {}][{}/{}] Failed to insert word counts for thread {} in {}: {}",
                    token_idx,
                    idx,
                    total_channels,
                    thread_ts,
                    channel_id,
                    e
                );
            }

            if thread_oldest.is_none()
                && let Err(e) = db::clickhouse_db::mark_thread_fully_scraped(
                    clickhouse,
                    &channel_id,
                    &thread_ts,
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
    clickhouse: &clickhouse::Client,
) -> Result<(), String> {
    let ch = clickhouse.clone();
    let total = slack_pool
        .fetch_channels_paginated(move |page| insert_page(ch.clone(), page), None)
        .await
        .map_err(|e| e.to_string())?;

    tracing::info!("Full rescan done! {} total channels", total);
    Ok(())
}
