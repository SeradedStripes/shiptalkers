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
const HACKATIME_USER_STATS_URL: &str = "https://hackatime.hackclub.com/api/v1/users";

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
struct SpansResponse {
    spans: Vec<CodingSpan>,
}

#[derive(Deserialize)]
pub struct CodingSpan {
    pub start_time: f64,
    pub end_time: f64,
    pub duration: f64,
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

/// Fetches coding spans for a Slack user from hackatime over the
/// `[start_date, end_date)` window. The endpoint is keyed by Slack UID;
/// unauthenticated it only works for profiles with public stats lookup
/// enabled, and with a token it also reads the token owner's private profile.
/// One request covers the whole window, so a full-history backfill is a single
/// call. A 403 means public stats are disabled (only reachable on the
/// unauthenticated path) and a 404 means no hackatime account exists for this
/// Slack UID; both must be distinguished by the caller.
pub async fn fetch_coding_spans(
    client: &reqwest::Client,
    slack_uid: &str,
    token: Option<&str>,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<CodingSpan>, (Option<u16>, String)> {
    let mut request = client
        .get(format!(
            "{HACKATIME_USER_STATS_URL}/{slack_uid}/heartbeats/spans"
        ))
        .query(&[("start_date", start_date), ("end_date", end_date)]);
    if let Some(tok) = token {
        request = request.bearer_auth(tok);
    }
    let response = request.send().await.map_err(|e| (None, e.to_string()))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .unwrap_or_else(|e| format!("(no body: {e})"));
    if !status.is_success() {
        return Err((Some(status.as_u16()), body));
    }
    let spans: SpansResponse =
        serde_json::from_str(&body).map_err(|e| (None, format!("bad JSON ({body:?}): {e}")))?;
    Ok(spans.spans)
}

/// Seconds of a coding span (`start_ts` unix seconds + `duration` seconds)
/// that fall inside the range `[range_start, range_end)` (unix seconds; `None`
/// means unbounded). Mirrors the overlap math in the stats bot query in
/// `slack/socket.rs` so tests can pin the exact-overlap semantics.
pub fn span_overlap_seconds(
    start_ts: u64,
    duration: u64,
    range_start: Option<i64>,
    range_end: Option<i64>,
) -> u64 {
    let start = start_ts;
    let end = start + duration;
    let start = match range_start {
        Some(rs) if end > rs as u64 => start.max(rs as u64),
        Some(_) => return 0,
        None => start,
    };
    let end = match range_end {
        Some(re) if start < re as u64 => end.min(re as u64),
        Some(_) => return 0,
        None => end,
    };
    end - start
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

/// UTC date `days` days after today, e.g. the end boundary for an all-time
/// hackatime total: tomorrow at midnight includes everything up to now.
pub fn days_from_now(days: i64) -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let (year, month, day) = civil_from_days((secs / 86400) as i64 + days);
    format!("{year:04}-{month:02}-{day:02}")
}

/// UTC date `days` days before today, e.g. the no-account retry cutoff.
pub fn date_days_ago(days: u64) -> String {
    days_from_now(-(days as i64))
}

/// UTC date `days` days after the given `YYYY-MM-DD` date, e.g. the start of an
/// incremental hackatime window one day before the last sync.
pub fn date_plus_days(date: &str, days: i64) -> Option<String> {
    let (y, m, d) = parse_iso_date(date)?;
    let day = days_from_civil(y, m, d) + days;
    let (y, m, d) = civil_from_days(day);
    Some(format!("{y:04}-{m:02}-{d:02}"))
}

fn parse_iso_date(date: &str) -> Option<(i64, i64, i64)> {
    let mut parts = date.split('-');
    let y = parts.next()?.parse().ok()?;
    let m = parts.next()?.parse().ok()?;
    let d = parts.next()?.parse().ok()?;
    if parts.next().is_some() || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    Some((y, m, d))
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
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
