use reqwest::Client;
use serde::Deserialize;

use crate::db::postgres_db;
use crate::settings::RuntimeSettings;

const HELP: &str = r#"Available commands:
@shiptalkers blacklist <channel_id>
@shiptalkers whitelist <channel_id>
@shiptalkers help"#;
const NOT_ADMIN: &str = "You're not an admin silly <3";

#[derive(Debug, Deserialize)]
struct PostMessageResponse {
    ok: bool,
    error: Option<String>,
}

pub async fn handle_message(
    client: &Client,
    pool: &crate::sqlx::PgPool,
    settings: &RuntimeSettings,
    channel: &str,
    thread_ts: &str,
    sender: &str,
    text: &str,
) -> bool {
    let Some(command) = parse_command(text) else {
        return false;
    };

    if !settings.is_shiptalkers_admin(sender) {
        if let Err(error) =
            post_thread_message(client, settings, channel, thread_ts, NOT_ADMIN).await
        {
            tracing::error!(error = %error, "Failed to reply to unauthorized Slack admin command");
        }
        return true;
    }

    let response = match command {
        AdminCommand::Help => Ok(HELP.to_string()),
        AdminCommand::Blacklist(channel_id) => postgres_db::blacklist_channel(pool, &channel_id)
            .await
            .map(|_| format!("Blacklisted channel {channel_id}")),
        AdminCommand::Whitelist(channel_id) => postgres_db::unblacklist_channel(pool, &channel_id)
            .await
            .map(|_| format!("Whitelisted channel {channel_id}")),
    };

    let message = match response {
        Ok(message) => message,
        Err(error) => {
            tracing::error!(sender, error = %error, "Slack admin command failed");
            format!("Command failed: {error}")
        }
    };
    if let Err(error) = post_thread_message(client, settings, channel, thread_ts, &message).await {
        tracing::error!(error = %error, "Failed to reply to Slack admin command");
    }
    true
}

enum AdminCommand {
    Help,
    Blacklist(String),
    Whitelist(String),
}

fn parse_command(text: &str) -> Option<AdminCommand> {
    let mut tokens = text.split_whitespace();
    let mention = tokens.next()?;
    if !mention.starts_with("<@") || !mention.ends_with('>') {
        return None;
    }
    match tokens.next()?.to_ascii_lowercase().as_str() {
        "help" => Some(AdminCommand::Help),
        "blacklist" => Some(AdminCommand::Blacklist(tokens.next()?.to_string())),
        "whitelist" => Some(AdminCommand::Whitelist(tokens.next()?.to_string())),
        _ => None,
    }
}

async fn post_thread_message(
    client: &Client,
    settings: &RuntimeSettings,
    channel: &str,
    thread_ts: &str,
    text: &str,
) -> Result<(), String> {
    let Some(bot_token) = settings.get_list("SLACK_BOT_TOKENS").first().cloned() else {
        return Err("no bot token configured".into());
    };
    let response = client
        .post("https://slack.com/api/chat.postMessage")
        .header("Authorization", format!("Bearer {bot_token}"))
        .form(&[
            ("channel", channel),
            ("thread_ts", thread_ts),
            ("text", text),
        ])
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let parsed: PostMessageResponse = serde_json::from_str(&body).map_err(|error| {
        format!("chat.postMessage returned bad JSON ({status}, {body:?}): {error}")
    })?;
    if !parsed.ok {
        return Err(format!(
            "Slack API error: {} ({status})",
            parsed.error.unwrap_or_default()
        ));
    }
    Ok(())
}
