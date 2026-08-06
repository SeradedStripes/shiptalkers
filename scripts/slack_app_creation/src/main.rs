use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, exit};
use std::time::Duration;

use rand::Rng;
use rand::distributions::Alphanumeric;
use reqwest::Client;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const API: &str = "https://slack.com/api";

fn usage() -> String {
    "Usage: slack_app_creation [manifest.json|manifest.yml]

Environment:
  SLACK_CONFIG_TOKEN          App configuration token (xoxe...), required.
  SLACK_CONFIG_REFRESH_TOKEN  Optional config refresh token; rotates the
                              config token first (config tokens expire after
                              12 hours).
  SLACK_INSTALL_PORT          Local callback port (default 8099).

Uses manifest.yml (in the same directory as .env.example) unless another
manifest file is passed as an argument. Prints the app credentials plus the
bot and user tokens after the OAuth install finishes. The app-level token
(xapp...) is created separately under the app's App-Level Tokens page; the
manifest enables Socket Mode.
"
    .to_string()
}

fn default_manifest_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("manifest.yml")
}

fn load_manifest(path: &Path) -> Result<Value, String> {
    let text = fs::read_to_string(path)
        .map_err(|e| format!("manifest file not found: {}: {e}", path.display()))?;
    let value = serde_json::from_str(&text)
        .or_else(|_| serde_yaml::from_str::<Value>(&text))
        .map_err(|e| {
            format!(
                "manifest is not valid JSON or YAML ({}): {e}",
                path.display()
            )
        })?;
    if !value.is_object() {
        return Err(format!(
            "manifest must be a mapping ({}): {value}",
            path.display()
        ));
    }
    Ok(value)
}

fn inject_redirect(manifest: &mut Value, redirect_uri: &str) {
    let mut redirects = extract_strings(&manifest["oauth_config"]["redirect_urls"]);
    if !redirects.iter().any(|r| r == redirect_uri) {
        redirects.push(redirect_uri.to_string());
    }
    manifest["oauth_config"]["redirect_urls"] = json!(redirects);
}

fn var(name: &str) -> Option<String> {
    env::var(name).ok().filter(|v| !v.is_empty())
}

fn extract_strings(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    if let Err(e) = run().await {
        eprintln!("error: {e}");
        exit(1);
    }
}

async fn run() -> Result<(), String> {
    let manifest_arg = env::args().nth(1);
    if matches!(manifest_arg.as_deref(), Some("-h") | Some("--help")) {
        print!("{}", usage());
        return Ok(());
    }

    let config_token = var("SLACK_CONFIG_TOKEN").ok_or_else(|| {
        "SLACK_CONFIG_TOKEN is required (app configuration token, xoxe...); generate one \
         at https://api.slack.com/apps -> your app -> App Configuration Tokens"
            .to_string()
    })?;
    if !config_token.starts_with("xoxe") {
        eprintln!("warning: SLACK_CONFIG_TOKEN does not look like a config token (xoxe...)");
    }

    let port: u16 = match var("SLACK_INSTALL_PORT") {
        Some(p) => p
            .parse()
            .map_err(|_| format!("SLACK_INSTALL_PORT must be a number, got: {p}"))?,
        None => 8099,
    };
    let redirect_uri = format!("http://localhost:{port}/callback");
    let state: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(24)
        .map(char::from)
        .collect();

    let client = Client::new();

    let config_token = if let Some(refresh) = var("SLACK_CONFIG_REFRESH_TOKEN") {
        println!("Rotating config token...");
        let resp: Value = client
            .post(format!("{API}/tooling.tokens.rotate"))
            .form(&[("refresh_token", refresh.as_str())])
            .send()
            .await
            .map_err(|e| e.to_string())?
            .json()
            .await
            .map_err(|e| e.to_string())?;
        if resp["ok"] != true {
            return Err(format!("token rotation failed: {}", resp["error"]));
        }
        resp["token"].as_str().unwrap_or("").to_string()
    } else {
        config_token
    };

    let manifest_path = manifest_arg
        .map(PathBuf::from)
        .unwrap_or_else(default_manifest_path);
    let mut manifest = load_manifest(&manifest_path)?;
    inject_redirect(&mut manifest, &redirect_uri);

    println!("Creating app from manifest...");
    let resp: Value = client
        .post(format!("{API}/apps.manifest.create"))
        .bearer_auth(&config_token)
        .json(&json!({ "manifest": manifest.to_string() }))
        .send()
        .await
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    if resp["ok"] != true {
        return Err(format!("apps.manifest.create failed: {}", resp["error"]));
    }
    let app_id = resp["app_id"].as_str().unwrap_or("").to_string();
    let client_id = resp["credentials"]["client_id"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let client_secret = resp["credentials"]["client_secret"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let signing_secret = resp["credentials"]["signing_secret"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let app_name = manifest["display_information"]["name"]
        .as_str()
        .unwrap_or("")
        .to_string();
    println!("App {app_id} created ({app_name})");

    let listener = TcpListener::bind(("127.0.0.1", port))
        .await
        .map_err(|e| e.to_string())?;

    let bot_scope = extract_strings(&manifest["oauth_config"]["scopes"]["bot"]).join(",");
    let user_scope = extract_strings(&manifest["oauth_config"]["scopes"]["user"]).join(",");
    let auth_url = reqwest::Url::parse_with_params(
        "https://slack.com/oauth/v2/authorize",
        &[
            ("client_id", client_id.as_str()),
            ("scope", &bot_scope),
            ("user_scope", &user_scope),
            ("redirect_uri", redirect_uri.as_str()),
            ("state", state.as_str()),
        ],
    )
    .map_err(|e| e.to_string())?;

    println!("\nAuthorize the app here:");
    println!("  {auth_url}\n");

    if Command::new("xdg-open")
        .arg(auth_url.as_str())
        .spawn()
        .is_err()
    {
        let _ = Command::new("open").arg(auth_url.as_str()).spawn();
    }

    println!("Waiting for authorization (the code expires after 10 minutes)...");
    let (mut socket, _) =
        match tokio::time::timeout(Duration::from_secs(600), listener.accept()).await {
            Err(_) => return Err("timed out waiting for authorization".to_string()),
            Ok(Err(e)) => return Err(format!("listener error: {e}")),
            Ok(Ok(conn)) => conn,
        };

    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        match socket.read(&mut byte).await {
            Ok(0) => break,
            Ok(_) => {
                if byte[0] == b'\n' {
                    break;
                }
                buf.push(byte[0]);
            }
            Err(e) => return Err(format!("callback read error: {e}")),
        }
    }
    let line = String::from_utf8_lossy(&buf);
    let path = line.split(' ').nth(1).unwrap_or("");
    let query = path.split('?').nth(1).unwrap_or("");
    let params: HashMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();
    let cb_code = params.get("code").cloned().unwrap_or_default();
    let cb_state = params.get("state").cloned().unwrap_or_default();
    let cb_error = params.get("error").cloned().unwrap_or_default();

    let body = if cb_error.is_empty() {
        "<h1>Authorized! You can close this window.</h1>"
    } else {
        "<h1>Authorization failed. You can close this window.</h1>"
    };
    let _ = socket
        .write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await;

    if !cb_error.is_empty() {
        return Err(format!("authorization failed: {cb_error}"));
    }
    if cb_code.is_empty() {
        return Err("no code received in callback".to_string());
    }
    if cb_state != state {
        return Err("state mismatch, aborting (possible CSRF)".to_string());
    }

    println!("Exchanging code for tokens...");
    let resp: Value = client
        .post(format!("{API}/oauth.v2.access"))
        .form(&[
            ("client_id", client_id.as_str()),
            ("client_secret", client_secret.as_str()),
            ("code", cb_code.as_str()),
            ("redirect_uri", redirect_uri.as_str()),
        ])
        .send()
        .await
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    if resp["ok"] != true {
        return Err(format!("oauth.v2.access failed: {}", resp["error"]));
    }

    let bot_token = resp["access_token"].as_str().unwrap_or("").to_string();
    let bot_user_id = resp["bot_user_id"].as_str().unwrap_or("").to_string();
    let user_token = resp["authed_user"]["access_token"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let user_id = resp["authed_user"]["id"].as_str().unwrap_or("").to_string();
    let team_id = resp["team"]["id"].as_str().unwrap_or("").to_string();
    let team_name = resp["team"]["name"].as_str().unwrap_or("").to_string();

    println!();
    println!("======================");
    println!(" App installed");
    println!("======================");
    println!("App name:       {app_name}");
    println!("App ID:         {app_id}");
    println!("Manage app:     https://api.slack.com/apps/{app_id}");
    println!("Workspace:      {team_name} ({team_id})");
    println!();
    println!("Bot token (SLACK_BOT_TOKENS):   {bot_token}");
    println!("Bot user ID:                    {bot_user_id}");
    println!("User token (SLACK_USER_TOKENS): {user_token}");
    println!("User ID:                        {user_id}");
    println!("App token (SLACK_APP_TOKENS):   create under App-Level Tokens:");
    println!("                                https://api.slack.com/apps/{app_id}");
    println!();
    println!("OAuth credentials (only needed for the app page):");
    println!("  client_id:     {client_id}");
    println!("  client_secret: {client_secret}");
    println!("  signing_secret:{signing_secret}");
    println!();
    println!("Notes:");
    println!("  - The app token (xapp-...) cannot be generated via the API; the manifest");
    println!("    enables Socket Mode, so create one under the app's App-Level Tokens page.");
    let cfg = &config_token[..12.min(config_token.len())];
    println!("  - The config token ({cfg}...) expires 12 hours after it is generated;");
    println!("    rotate it with SLACK_CONFIG_REFRESH_TOKEN when needed.");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_default_manifest() {
        let manifest = load_manifest(&default_manifest_path()).unwrap();
        assert_eq!(manifest["display_information"]["name"], "Ship Talkers");
        assert!(!extract_strings(&manifest["oauth_config"]["scopes"]["bot"]).is_empty());
    }

    #[test]
    fn injects_redirect_url() {
        let mut manifest = load_manifest(&default_manifest_path()).unwrap();
        inject_redirect(&mut manifest, "http://localhost:8099/callback");
        let urls = extract_strings(&manifest["oauth_config"]["redirect_urls"]);
        assert!(urls.contains(&"http://localhost:8099/callback".to_string()));
        inject_redirect(&mut manifest, "http://localhost:8099/callback");
        assert_eq!(
            extract_strings(&manifest["oauth_config"]["redirect_urls"]).len(),
            1
        );
    }
}
