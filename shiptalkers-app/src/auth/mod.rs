use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, KeyInit, Mac};
use rand::Rng;
use rand::rng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const HCA_AUTHORIZE_URL: &str = "https://auth.hackclub.com/oauth/authorize";
const HCA_TOKEN_URL: &str = "https://auth.hackclub.com/oauth/token";
const HCA_ME_URL: &str = "https://auth.hackclub.com/api/v1/me";
const HACKATIME_AUTHORIZE_URL: &str = "https://hackatime.hackclub.com/oauth/authorize";
const HACKATIME_TOKEN_URL: &str = "https://hackatime.hackclub.com/oauth/token";

#[derive(Clone)]
pub struct AuthConfig {
    pub hca_client_id: String,
    pub hca_client_secret: String,
    pub hackatime_client_id: String,
    pub hackatime_client_secret: String,
    pub base_url: String,
    pub session_secret: String,
}

#[derive(Serialize, Deserialize)]
pub struct Session {
    pub slack_id: String,
    pub name: String,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

#[derive(Deserialize)]
struct HcaMeResponse {
    identity: HcaIdentity,
}

#[derive(Deserialize)]
pub struct HcaIdentity {
    pub slack_id: Option<String>,
    pub first_name: Option<String>,
    #[serde(default)]
    pub last_name: Option<String>,
}

fn sign(data: &str, secret: &[u8]) -> String {
    let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(secret).expect("hmac key");
    mac.update(data.as_bytes());
    let sig = mac.finalize().into_bytes();
    URL_SAFE_NO_PAD.encode(sig)
}

pub fn issue_session(session: &Session, secret: &str) -> String {
    let payload = serde_json::to_string(session).expect("session serialize");
    let payload = URL_SAFE_NO_PAD.encode(payload.as_bytes());
    let sig = sign(&payload, secret.as_bytes());
    format!("{payload}.{sig}")
}

pub fn parse_session(cookie: Option<&str>, secret: &str) -> Option<Session> {
    let (payload, sig) = cookie?.rsplit_once('.')?;
    let expected = sign(payload, secret.as_bytes());
    if sig != expected {
        return None;
    }
    let decoded = URL_SAFE_NO_PAD.decode(payload).ok()?;
    serde_json::from_slice(&decoded).ok()
}

pub fn random_state() -> String {
    let mut bytes = [0u8; 16];
    rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Stateless CSRF token derived from a signed session cookie: an HMAC over the session's payload
pub fn csrf_token(cookie: &str, secret: &str) -> Option<String> {
    let payload = cookie.rsplit_once('.').map(|(payload, _sig)| payload)?;
    Some(sign(&format!("csrf:{payload}"), secret.as_bytes()))
}

/// Compares a provided token against the expected one via SHA-256 digests so the comparison itself leaks nothing about the token bytes.
pub fn csrf_ok(expected: Option<&str>, provided: Option<&str>) -> bool {
    match (expected, provided) {
        (Some(expected), Some(provided)) => {
            Sha256::digest(expected.as_bytes()) == Sha256::digest(provided.as_bytes())
        }
        _ => false,
    }
}

pub fn hca_authorize_url(config: &AuthConfig, state: &str) -> String {
    let redirect = format!("{}/auth/hackclub/callback", config.base_url);
    format!(
        "{HCA_AUTHORIZE_URL}?client_id={}&redirect_uri={}&response_type=code&scope=slack_id&state={}",
        config.hca_client_id, redirect, state
    )
}

pub fn hackatime_authorize_url(config: &AuthConfig, state: &str) -> String {
    let redirect = format!("{}/auth/hackatime/callback", config.base_url);
    format!(
        "{HACKATIME_AUTHORIZE_URL}?client_id={}&redirect_uri={}&response_type=code&scope=profile+read&state={}",
        config.hackatime_client_id, redirect, state
    )
}

pub async fn exchange_hca_code(
    client: &reqwest::Client,
    config: &AuthConfig,
    code: &str,
) -> Result<String, String> {
    let redirect = format!("{}/auth/hackclub/callback", config.base_url);
    let token: TokenResponse = client
        .post(HCA_TOKEN_URL)
        .json(&serde_json::json!({
            "client_id": config.hca_client_id,
            "client_secret": config.hca_client_secret,
            "redirect_uri": redirect,
            "code": code,
            "grant_type": "authorization_code"
        }))
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    Ok(token.access_token)
}

pub async fn fetch_hca_identity(
    client: &reqwest::Client,
    access_token: &str,
) -> Result<HcaIdentity, String> {
    let me: HcaMeResponse = client
        .get(HCA_ME_URL)
        .bearer_auth(access_token)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    Ok(me.identity)
}

pub async fn exchange_hackatime_code(
    client: &reqwest::Client,
    config: &AuthConfig,
    code: &str,
) -> Result<String, String> {
    let redirect = format!("{}/auth/hackatime/callback", config.base_url);
    let response = client
        .post(HACKATIME_TOKEN_URL)
        .form(&[
            ("client_id", config.hackatime_client_id.as_str()),
            ("client_secret", config.hackatime_client_secret.as_str()),
            ("redirect_uri", redirect.as_str()),
            ("code", code),
            ("grant_type", "authorization_code"),
        ])
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = response.status();
    let body = response
        .text()
        .await
        .unwrap_or_else(|e| format!("(no body: {e})"));
    if !status.is_success() {
        return Err(format!("token endpoint {}: {}", status, body));
    }
    let token: TokenResponse = serde_json::from_str(&body)
        .map_err(|e| format!("token endpoint returned bad JSON ({status}, {body:?}): {e}"))?;
    Ok(token.access_token)
}

// Re-exports of the hackatime fetch/date helpers that moved to the shared
// ship-talkers-lib crate (used by the app's OAuth flow, the bot card, and the
// span-overlap tests).
pub use ship_talkers_lib::hackatime::{
    civil_from_days, date_plus_days, fetch_hackatime_me, span_overlap_seconds,
};
