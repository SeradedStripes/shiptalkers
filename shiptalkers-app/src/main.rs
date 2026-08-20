mod scraper;

use ship_talkers::{db, settings, slack, website};

use dotenvy::dotenv;
use std::env;
use std::time::Duration;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    ship_talkers::init_tls();

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
                if let Err(e) = db::refresh::refresh_word_totals(&clickhouse_for_words).await {
                    tracing::warn!("Failed to refresh word totals: {}", e);
                }
                tokio::time::sleep(std::time::Duration::from_secs(30 * 60)).await;
            }
        });
    }

    {
        let clickhouse_for_daily = database.clickhouse.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = db::refresh::refresh_daily_stats(&clickhouse_for_daily).await {
                    tracing::warn!("Failed to refresh daily stats: {}", e);
                }
                tokio::time::sleep(std::time::Duration::from_secs(30 * 60)).await;
            }
        });
    }

    if has_bot_tokens || has_user_tokens || has_app_tokens {
        let clickhouse_for_scraper = database.clickhouse.clone();
        let settings_for_scraper = settings.clone();

        tokio::spawn(async move {
            if let Err(e) = scraper::run_scraper(clickhouse_for_scraper, settings_for_scraper).await
            {
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
                    let ok = scraper::sync_users(&pool, &clickhouse_for_users).await;
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
        let settings_for_socket = settings.clone();
        tokio::spawn(async move {
            if let Err(e) =
                slack::start_socket_mode(socket_config, clickhouse_for_socket, settings_for_socket)
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
                website::auth::resync_all(&clickhouse_for_resync, &http_for_resync).await;
                tokio::time::sleep(std::time::Duration::from_secs(1800)).await;
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
