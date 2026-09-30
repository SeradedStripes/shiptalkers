use reqwest::Client;
use serde::Deserialize;

use crate::db::postgres_db;
use crate::settings::RuntimeSettings;

const HELP: &str = r#"Available commands:
======================
blacklist <channel_id> [--silent]
whitelist <channel_id>
----------------------
Multiple IDs may be comma-separated: blacklist C123, C456
======================"#;
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
        AdminCommand::Blacklist {
            channel_ids,
            silent,
        } => blacklist_channels(pool, &channel_ids, silent).await,
        AdminCommand::Whitelist(channel_ids) => whitelist_channels(pool, &channel_ids).await,
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
    Blacklist {
        channel_ids: Vec<String>,
        silent: bool,
    },
    Whitelist(Vec<String>),
}

fn parse_command(text: &str) -> Option<AdminCommand> {
    let mut tokens = text.split_whitespace();
    let mention = tokens.next()?;
    if !mention.starts_with("<@") || !mention.ends_with('>') {
        return None;
    }
    match tokens.next()?.to_ascii_lowercase().as_str() {
        "help" => Some(AdminCommand::Help),
        "blacklist" => {
            let (ids, silent) = channel_ids_and_silent(tokens);
            (!ids.is_empty()).then_some(AdminCommand::Blacklist {
                channel_ids: ids,
                silent,
            })
        }
        "whitelist" => {
            let ids = channel_ids(tokens);
            (!ids.is_empty()).then_some(AdminCommand::Whitelist(ids))
        }
        _ => None,
    }
}

fn channel_ids<'a>(tokens: impl Iterator<Item = &'a str>) -> Vec<String> {
    tokens
        .flat_map(|token| token.split(','))
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_string)
        .collect()
}

fn channel_ids_and_silent<'a>(tokens: impl Iterator<Item = &'a str>) -> (Vec<String>, bool) {
    let mut silent = false;
    let ids = tokens
        .filter(|token| {
            if *token == "--silent" {
                silent = true;
                false
            } else {
                true
            }
        })
        .flat_map(|token| token.split(','))
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_string)
        .collect();
    (ids, silent)
}

async fn blacklist_channels(
    pool: &crate::sqlx::PgPool,
    channel_ids: &[String],
    silent: bool,
) -> Result<String, String> {
    let mut responses = Vec::with_capacity(channel_ids.len());
    for channel_id in channel_ids {
        let channel_name = channel_name(pool, channel_id).await;
        postgres_db::blacklist_channel(pool, channel_id)
            .await
            .map_err(|error| format!("Command failed for {channel_id}: {error}"))?;
        responses.push(format!(
            "Blacklisted channel: {} - {channel_id}",
            if silent {
                blur_channel_name(&channel_name)
            } else {
                channel_name
            }
        ));
    }
    Ok(responses.join("\n"))
}

fn blur_channel_name(name: &str) -> String {
    name.split('-')
        .map(|part| {
            let chars: Vec<char> = part.chars().collect();
            match chars.len() {
                0..=2 => part.to_string(),
                len => format!("{}{}{}", chars[0], "*".repeat(len - 2), chars[len - 1]),
            }
        })
        .collect::<Vec<_>>()
        .join("-")
}

async fn whitelist_channels(
    pool: &crate::sqlx::PgPool,
    channel_ids: &[String],
) -> Result<String, String> {
    let mut responses = Vec::with_capacity(channel_ids.len());
    for channel_id in channel_ids {
        let channel_name = channel_name(pool, channel_id).await;
        postgres_db::unblacklist_channel(pool, channel_id)
            .await
            .map_err(|error| format!("Command failed for {channel_id}: {error}"))?;
        responses.push(format!(
            "Whitelisted channel: {channel_name} - {channel_id}"
        ));
    }
    Ok(responses.join("\n"))
}

async fn channel_name(pool: &crate::sqlx::PgPool, channel_id: &str) -> String {
    crate::sqlx::query_scalar::<_, Option<String>>(
        "SELECT COALESCE(
             (SELECT channel_name FROM blacklisted_channels WHERE slack_channel_id = $1),
             (SELECT name FROM slack_channels WHERE channel_id = $1),
             ''
         )",
    )
    .bind(channel_id)
    .fetch_one(pool)
    .await
    .unwrap_or(None)
    .unwrap_or_default()
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
