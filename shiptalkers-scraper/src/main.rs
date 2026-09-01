use ship_talkers_scraper::{db, hackatime, scraper, settings, slack};

use dotenvy::dotenv;
use std::time::Duration;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    ship_talkers_scraper::init_tls();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let settings = settings::RuntimeSettings::load();

    let database_url = settings.get("DATABASE_URL");
    if database_url.is_empty() {
        return Err("DATABASE_URL is not set. Point it at your Postgres instance.".into());
    }
    tracing::info!("Connecting to Postgres...");
    let pool = db::postgres_db::connect(&database_url).await?;
    db::postgres_db::init_tables(&pool).await?;

    let pool_for_compact = pool.clone();
    tokio::spawn(async move {
        if let Err(e) = db::postgres_db::compact_toast_once(&pool_for_compact).await {
            tracing::warn!("Failed to compact tables: {}", e);
        }
    });

    let pool_for_hackatime = pool.clone();
    let http_for_hackatime = reqwest::Client::new();
    tokio::spawn(async move {
        loop {
            hackatime::resync_all(&pool_for_hackatime, &http_for_hackatime).await;
            tokio::time::sleep(Duration::from_secs(1800)).await;
        }
    });

    let has_bot_tokens = !settings.get_list("SLACK_BOT_TOKENS").is_empty();
    let has_user_tokens = !settings.get_list("SLACK_USER_TOKENS").is_empty();

    if has_bot_tokens || has_user_tokens {
        let pool_for_scraper = pool.clone();
        let settings_for_scraper = settings.clone();
        tokio::spawn(async move {
            if let Err(e) = scraper::run_scraper(pool_for_scraper, settings_for_scraper).await {
                tracing::error!("Scraper error: {}", e);
            }
        });

        if has_bot_tokens {
            let pool_for_users = pool.clone();
            let settings_for_users = settings.clone();
            tokio::spawn(async move {
                loop {
                    let slack_pool = slack::SlackClientPool::new(
                        settings_for_users.get_list("SLACK_BOT_TOKENS"),
                        Duration::from_millis(
                            settings_for_users.get_u64("SLACK_USER_SYNC_DELAY_MS"),
                        ),
                        settings_for_users.get_u64("SLACK_MAX_INFLIGHT") as usize,
                    );
                    let ok = scraper::sync_users(&slack_pool, &pool_for_users).await;
                    let wait = if ok { 7200 } else { 300 };
                    tokio::time::sleep(Duration::from_secs(wait)).await;
                }
            });
        } else {
            tracing::warn!("SLACK_BOT_TOKENS not set, user sync disabled");
        }
    } else {
        tracing::warn!(
            "No Slack tokens set (SLACK_BOT_TOKENS/SLACK_USER_TOKENS), \
             skipping Slack API entirely"
        );
    }

    shutdown_signal().await;
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
