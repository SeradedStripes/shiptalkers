mod auth;
mod bot_image;
mod db;
mod formula;
mod slack;
mod website;

use dotenvy::dotenv;
use std::env;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let slack_bot_tokens = parse_token_list("SLACK_BOT_TOKENS");
    if slack_bot_tokens.is_empty() {
        return Err("SLACK_BOT_TOKENS must be set".into());
    }
    let slack_user_tokens = parse_token_list("SLACK_USER_TOKENS");
    let slack_app_tokens = parse_token_list("SLACK_APP_TOKENS");
    let slack_request_delay_ms = env::var("SLACK_REQUEST_DELAY_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1200);
    let slack_max_inflight = env::var("SLACK_MAX_INFLIGHT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    let slack_channel_concurrency = env::var("SLACK_CHANNEL_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    let clickhouse_url =
        env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".into());
    let clickhouse_user = env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".into());
    let clickhouse_password = env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();
    let clickhouse_db = env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "default".into());

    let slack_time = formula::Formula::parse(formula::SLACK_TIME_CALCULATION_FORMULA)
        .expect("SLACK_TIME_CALCULATION_FORMULA must be a valid formula");
    tracing::info!("Slack time formula: {}", slack_time.source());

    let database = db::Database::new(
        &clickhouse_url,
        &clickhouse_user,
        &clickhouse_password,
        &clickhouse_db,
    );

    let auth_db_path = env::var("SQLITE_DB_PATH").unwrap_or_else(|_| "data/auth.db".into());
    let auth_db = std::sync::Arc::new(
        db::sqlite::AuthDb::open(&auth_db_path)
            .map_err(|e| format!("Failed to open auth DB {}: {}", auth_db_path, e))?,
    );
    tracing::info!("Auth DB at {}", auth_db_path);

    tracing::info!("Initializing ClickHouse tables...");
    db::clickhouse_db::init_tables(&database.clickhouse).await?;

    let clickhouse_for_scraper = database.clickhouse.clone();
    let slack_bot_tokens_for_scraper = slack_bot_tokens.clone();
    let slack_user_tokens_for_scraper = slack_user_tokens.clone();

    tokio::spawn(async move {
        if let Err(e) = run_scraper(
            clickhouse_for_scraper,
            slack_bot_tokens_for_scraper,
            slack_user_tokens_for_scraper,
            Duration::from_millis(slack_request_delay_ms),
            slack_max_inflight,
            slack_channel_concurrency,
        )
        .await
        {
            tracing::error!("Scraper error: {}", e);
        }
    });

    {
        let clickhouse_for_users = database.clickhouse.clone();
        let slack_bot_tokens_for_users = slack_bot_tokens.clone();
        let user_sync_delay = env::var("SLACK_USER_SYNC_DELAY_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3000);
        tokio::spawn(async move {
            let pool = slack::SlackClientPool::new(
                slack_bot_tokens_for_users,
                Duration::from_millis(user_sync_delay),
                slack_max_inflight,
            );
            loop {
                let ok = sync_users(&pool, &clickhouse_for_users).await;
                let wait = if ok { 86400 } else { 300 };
                tokio::time::sleep(std::time::Duration::from_secs(wait)).await;
            }
        });
    }

    if !slack_app_tokens.is_empty() {
        let base_url = env::var("BASE_URL").unwrap_or_else(|_| "http://localhost:3000".into());
        let socket_config = slack::SocketConfig::new(
            slack_app_tokens,
            slack_bot_tokens.clone(),
            env::var("SLACK_MAIN_CHANNEL")
                .ok()
                .filter(|v| !v.trim().is_empty()),
            base_url,
        );
        let clickhouse_for_socket = database.clickhouse.clone();
        let auth_db_for_socket = auth_db.clone();
        tokio::spawn(async move {
            if let Err(e) =
                slack::start_socket_mode(socket_config, clickhouse_for_socket, auth_db_for_socket)
                    .await
            {
                tracing::error!("Socket Mode error: {}", e);
            }
        });
    } else {
        tracing::warn!("SLACK_APP_TOKENS not set, Socket Mode disabled");
    }

    let host = env::var("HOST").unwrap_or_else(|_| "0.0.0.0".into());
    let port = env::var("PORT").unwrap_or_else(|_| "3000".into());
    let addr = format!("{}:{}", host, port);

    let auth_config = auth::AuthConfig {
        hca_client_id: env::var("HCA_CLIENT_ID")?,
        hca_client_secret: env::var("HCA_CLIENT_SECRET")?,
        hackatime_client_id: env::var("HACKATIME_CLIENT_ID")?,
        hackatime_client_secret: env::var("HACKATIME_CLIENT_SECRET")?,
        base_url: env::var("BASE_URL").unwrap_or_else(|_| "http://localhost:3000".into()),
        session_secret: env::var("SESSION_SECRET")?,
    };

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
        website::router(database.clickhouse, auth_config, slack_time, auth_db),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    Ok(())
}

fn parse_token_list(var: &str) -> Vec<String> {
    env::var(var)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(|v| {
            v.split(',')
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect()
        })
        .unwrap_or_default()
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

        if let Err(e) = db::clickhouse_db::insert_new_channels(&clickhouse, &rows).await {
            tracing::error!("Failed to insert channels: {}", e);
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
    tracing::info!(
        "Syncing users from Slack ({} already stored)",
        existing.len()
    );
    let existing = Arc::new(existing);
    let changed_total = Arc::new(AtomicU64::new(0));
    let result = slack_pool
        .fetch_users(|page| {
            let clickhouse = clickhouse.clone();
            let existing = existing.clone();
            let changed_total = changed_total.clone();
            Box::pin(async move {
                let changed: Vec<db::clickhouse_db::SlackUserRow> = page
                    .into_iter()
                    .filter(|u| match existing.get(&u.id) {
                        Some(prev) => *prev < u.updated,
                        None => true,
                    })
                    .map(|u| db::clickhouse_db::SlackUserRow {
                        user_id: u.id,
                        display_name: u.display_name,
                        updated: u.updated,
                    })
                    .collect();
                if changed.is_empty() {
                    return;
                }
                match db::clickhouse_db::upsert_users(&clickhouse, &changed).await {
                    Ok(()) => {
                        changed_total.fetch_add(changed.len() as u64, Ordering::Relaxed);
                        tracing::info!("Upserted {} users so far", changed.len());
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
            }
            tracing::info!("Synced {} users from Slack", total);
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
    bot_tokens: Vec<String>,
    slack_user_tokens: Vec<String>,
    request_delay: Duration,
    max_inflight: usize,
    channel_concurrency: usize,
) -> Result<(), String> {
    let bot_pool = slack::SlackClientPool::new(bot_tokens, request_delay, max_inflight);

    let cycle = Duration::from_secs(30 * 60);
    let mut last_optimize = std::time::Instant::now();

    if slack_user_tokens.is_empty() {
        tracing::warn!("No SLACK_USER_TOKENS set, message scraping disabled");
    }

    loop {
        let cycle_start = std::time::Instant::now();

        if let Err(e) = full_fetch(&bot_pool, &clickhouse).await {
            tracing::warn!("Failed to fetch channel list: {}", e);
        }

        if !slack_user_tokens.is_empty() {
            scrape_all_messages(
                &slack_user_tokens,
                &clickhouse,
                request_delay,
                max_inflight,
                channel_concurrency,
            )
            .await;
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
    user_tokens: &[String],
    clickhouse: &clickhouse::Client,
    request_delay: Duration,
    max_inflight: usize,
    channel_concurrency: usize,
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

    if !new_channels.is_empty() {
        tracing::info!("Full-scraping {} new channels...", new_channels.len());
        scrape_channel_list(
            user_tokens,
            clickhouse,
            &new_channels,
            request_delay,
            max_inflight,
            channel_concurrency,
        )
        .await;
    }

    if !check_channels.is_empty() {
        tracing::info!(
            "Checking {} already-scraped channels for new messages...",
            check_channels.len()
        );
        scrape_channel_list(
            user_tokens,
            clickhouse,
            &check_channels,
            request_delay,
            max_inflight,
            channel_concurrency,
        )
        .await;
    }

    tracing::info!("Message scrape pass complete");
}

async fn scrape_channel_list(
    user_tokens: &[String],
    clickhouse: &clickhouse::Client,
    channels: &[String],
    request_delay: Duration,
    max_inflight: usize,
    channel_concurrency: usize,
) {
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
        });
    }

    let mut workers = Vec::new();
    for (token_idx, token) in user_tokens.iter().enumerate() {
        let client = slack::SlackClient::new(token.clone(), request_delay, max_inflight);
        let shard: Vec<String> = channels
            .iter()
            .enumerate()
            .filter(|(i, _)| i % user_tokens.len() == token_idx)
            .map(|(_, c)| c.clone())
            .collect();

        let clickhouse = clickhouse.clone();
        let total = total.clone();
        let processed = processed.clone();
        workers.push(tokio::spawn(async move {
            scrape_shard(
                &client,
                &clickhouse,
                &shard,
                token_idx,
                max_inflight,
                channel_concurrency,
                total,
                processed,
            )
            .await;
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
    total: Arc<AtomicU64>,
    processed: Arc<AtomicU64>,
    tx: tokio::sync::mpsc::Sender<usize>,
}

async fn scrape_shard(
    user_client: &slack::SlackClient,
    clickhouse: &clickhouse::Client,
    channels: &[String],
    token_idx: usize,
    max_inflight: usize,
    channel_concurrency: usize,
    total: Arc<AtomicU64>,
    processed: Arc<AtomicU64>,
) {
    if channels.is_empty() {
        tracing::info!("[token {}] No channels assigned", token_idx);
        return;
    }
    tracing::info!("[token {}] Scraping {} channels", token_idx, channels.len());

    let channel_concurrency = channel_concurrency.max(1);
    let total_channels = channels.len();
    let sem = Arc::new(tokio::sync::Semaphore::new(channel_concurrency));

    let (tx, mut rx) = tokio::sync::mpsc::channel::<usize>(512);
    let ctx = ShardCtx {
        token_idx,
        total_channels,
        max_inflight,
        total: total.clone(),
        processed: processed.clone(),
        tx: tx.clone(),
    };
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

    let mut handles = Vec::with_capacity(channels.len());
    for (i, channel_id) in channels.iter().enumerate() {
        let permit = sem.clone();
        let client = user_client.clone();
        let clickhouse = clickhouse.clone();
        let channel_id = channel_id.clone();
        let ctx = ctx.clone();
        handles.push(tokio::spawn(async move {
            let _permit = permit.acquire().await;
            scrape_one_channel(&client, &clickhouse, channel_id, i + 1, ctx).await;
        }));
    }
    drop(tx);
    for handle in handles {
        let _ = handle.await;
    }
    let _ = reporter.await;
}

async fn scrape_one_channel(
    user_client: &slack::SlackClient,
    clickhouse: &clickhouse::Client,
    channel_id: String,
    idx: usize,
    ctx: ShardCtx,
) {
    let token_idx = ctx.token_idx;
    let total_channels = ctx.total_channels;
    let max_inflight = ctx.max_inflight;
    let total = ctx.total;
    let processed = ctx.processed;
    let tx = ctx.tx;
    let start = std::time::Instant::now();
    let fully_scraped = db::clickhouse_db::is_fully_scraped(clickhouse, &channel_id)
        .await
        .unwrap_or(false);

    let oldest = match db::clickhouse_db::get_max_message_ts(clickhouse, &channel_id).await {
        Ok(Some(ts)) if !ts.is_empty() => {
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
            message_ts: m.ts.clone(),
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

    // Collect unique thread parents from replies
    let mut thread_parents: Vec<String> = Vec::new();
    for msg in &messages {
        if let Some(ref t) = msg.thread_ts
            && t != &msg.ts
            && !thread_parents.contains(t)
        {
            thread_parents.push(t.clone());
        }
    }

    let mut threads_found = 0usize;
    let mut thread_replies = 0u64;
    let mut threads_skipped = 0usize;
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
        for (skipped, inserted_replies) in futures::future::join_all(handles)
            .await
            .into_iter()
            .flatten()
        {
            threads_skipped += skipped;
            thread_replies += inserted_replies;
        }
    }

    if !fully_scraped
        && oldest.is_none()
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
    clickhouse: &clickhouse::Client,
    channel_id: String,
    thread_ts: String,
    token_idx: usize,
    idx: usize,
    total_channels: usize,
    total: Arc<AtomicU64>,
) -> (usize, u64) {
    let thread_fully =
        db::clickhouse_db::is_thread_fully_scraped(clickhouse, &channel_id, &thread_ts)
            .await
            .unwrap_or(false);
    let thread_oldest = if thread_fully {
        db::clickhouse_db::get_max_thread_reply_ts(clickhouse, &channel_id, &thread_ts)
            .await
            .ok()
            .flatten()
    } else {
        None
    };

    if thread_fully {
        return (1, 0);
    }

    match user_client
        .fetch_thread_replies(&channel_id, &thread_ts, thread_oldest.as_deref())
        .await
    {
        Ok(replies) => {
            let replies: Vec<_> = if let Some(ref o) = thread_oldest {
                replies.into_iter().filter(|m| m.ts > *o).collect()
            } else {
                replies
            };

            let mut inserted = 0u64;
            if !replies.is_empty() {
                let rows: Vec<db::clickhouse_db::SlackMessageRow> = replies
                    .iter()
                    .map(|m| db::clickhouse_db::SlackMessageRow {
                        user_id: m.user.clone(),
                        channel_id: m.channel.clone(),
                        message_ts: m.ts.clone(),
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

            (0, inserted)
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
            (0, 0)
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
