use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, KeyInit, Mac};
use rand::Rng;
use rand::rng;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

const HCA_AUTHORIZE_URL: &str = "https://auth.hackclub.com/oauth/authorize";
const HCA_TOKEN_URL: &str = "https://auth.hackclub.com/oauth/token";
const HCA_ME_URL: &str = "https://auth.hackclub.com/api/v1/me";
const HACKATIME_AUTHORIZE_URL: &str = "https://hackatime.hackclub.com/oauth/authorize";
const HACKATIME_TOKEN_URL: &str = "https://hackatime.hackclub.com/oauth/token";
const HACKATIME_ME_URL: &str = "https://hackatime.hackclub.com/api/v1/authenticated/me";
const HACKATIME_HOURS_URL: &str = "https://hackatime.hackclub.com/api/v1/authenticated/hours";

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

#[derive(Deserialize)]
struct HackatimeMeResponse {
    slack_id: Option<String>,
}

#[derive(Deserialize)]
struct HoursResponse {
    total_seconds: Option<f64>,
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

/// First tuple element is the HTTP status when the failure was an HTTP error,
/// None for transport or parse failures.
pub async fn fetch_hackatime_me(
    client: &reqwest::Client,
    access_token: &str,
) -> Result<Option<String>, (Option<u16>, String)> {
    let response = client
        .get(HACKATIME_ME_URL)
        .bearer_auth(access_token)
        .send()
        .await
        .map_err(|e| (None, e.to_string()))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .unwrap_or_else(|e| format!("(no body: {e})"));
    if !status.is_success() {
        return Err((Some(status.as_u16()), body));
    }
    let me: HackatimeMeResponse = serde_json::from_str(&body).map_err(|e| {
        (
            None,
            format!("me endpoint returned bad JSON ({body:?}): {e}"),
        )
    })?;
    Ok(me.slack_id)
}

/// First tuple element is the HTTP status when the failure was an HTTP error,
/// None for transport or parse failures.
pub async fn fetch_hours_for_day(
    client: &reqwest::Client,
    access_token: &str,
    date: &str,
) -> Result<Option<u64>, (Option<u16>, String)> {
    let response = client
        .get(HACKATIME_HOURS_URL)
        .bearer_auth(access_token)
        .query(&[("start_date", date), ("end_date", date)])
        .send()
        .await
        .map_err(|e| (None, e.to_string()))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .unwrap_or_else(|e| format!("(no body: {e})"));
    if !status.is_success() {
        return Err((Some(status.as_u16()), body));
    }
    let hours: HoursResponse =
        serde_json::from_str(&body).map_err(|e| (None, format!("bad JSON ({body:?}): {e}")))?;
    let seconds = hours.total_seconds.unwrap_or(0.0);
    Ok(Some((seconds / 60.0).round() as u64))
}

pub fn next_date(date: &str) -> String {
    let mut parts = date
        .split('-')
        .map(|p| p.parse::<u32>().expect("date component"));
    let (mut year, mut month, mut day) = (
        parts.next().expect("year"),
        parts.next().expect("month"),
        parts.next().expect("day"),
    );
    day += 1;
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 30,
    };
    if day > days_in_month {
        day = 1;
        month += 1;
        if month > 12 {
            month = 1;
            year += 1;
        }
    }
    format!("{year:04}-{month:02}-{day:02}")
}

pub fn today_utc() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = secs / 86400;
    let secs_of_day = secs % 86400;
    let (mut year, month, day) = civil_from_days(days as i64);
    let _ = secs_of_day;
    let _ = &mut year;
    format!("{year:04}-{month:02}-{day:02}")
}

pub fn civil_from_days(z: i64) -> (u32, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    ((if m <= 2 { y + 1 } else { y }) as u32, m, d)
}
