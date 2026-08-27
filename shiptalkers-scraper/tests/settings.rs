use std::collections::HashMap;

use ship_talkers_scraper::settings::RuntimeSettings;

fn env(values: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let map: HashMap<String, String> = values
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    move |key| map.get(key).cloned()
}

#[test]
fn unset_keys_get_defaults() {
    let s = RuntimeSettings::from_env(env(&[]));
    assert_eq!(s.get("SLACK_REQUEST_DELAY_MS"), "1200");
    assert_eq!(s.get("SLACK_MAX_INFLIGHT"), "8");
    assert_eq!(s.get("SLACK_CHANNEL_CONCURRENCY"), "8");
    assert_eq!(s.get("SLACK_THREAD_RESCAN_HOURS"), "720");
    assert_eq!(s.get("SLACK_THREAD_RESCAN_INTERVAL_HOURS"), "6");
    assert_eq!(s.get("SLACK_USER_SYNC_DELAY_MS"), "3000");
    assert_eq!(s.get("DATABASE_URL"), "");
    assert_eq!(s.get("SLACK_BOT_TOKENS"), "");
}

#[test]
fn set_keys_override_defaults() {
    let s = RuntimeSettings::from_env(env(&[("SLACK_BOT_TOKENS", "xoxb-1")]));
    assert_eq!(s.get_list("SLACK_BOT_TOKENS"), vec!["xoxb-1"]);
    assert_eq!(s.get("SLACK_REQUEST_DELAY_MS"), "1200");
}

#[test]
fn get_u64_parses_or_zero() {
    let s = RuntimeSettings::from_env(env(&[("SLACK_MAX_INFLIGHT", "16")]));
    assert_eq!(s.get_u64("SLACK_MAX_INFLIGHT"), 16);
    assert_eq!(s.get_u64("SLACK_BOT_TOKENS"), 0);
}

#[test]
fn get_list_splits_trims_and_drops_empties() {
    let s = RuntimeSettings::from_env(env(&[("SLACK_USER_TOKENS", " a , b ,, c ")]));
    assert_eq!(s.get_list("SLACK_USER_TOKENS"), vec!["a", "b", "c"]);
    assert_eq!(s.get_list("SLACK_BOT_TOKENS"), Vec::<String>::new());
}

#[test]
fn get_list_merges_numbered_variants() {
    let s = RuntimeSettings::from_env(env(&[
        ("SLACK_BOT_TOKENS", "xoxb-0"),
        ("SLACK_BOT_TOKENS_1", "xoxb-1"),
        ("SLACK_BOT_TOKENS_2", " xoxb-2 ,"),
        ("SLACK_USER_TOKENS_3", "xoxp-3"),
    ]));
    assert_eq!(
        s.get_list("SLACK_BOT_TOKENS"),
        vec!["xoxb-0", "xoxb-1", "xoxb-2"]
    );
    assert_eq!(s.get_list("SLACK_USER_TOKENS"), vec!["xoxp-3"]);
}

#[test]
fn get_list_dedupes_across_base_and_variants() {
    let s = RuntimeSettings::from_env(env(&[
        ("SLACK_BOT_TOKENS", "xoxb-1"),
        ("SLACK_BOT_TOKENS_1", "xoxb-1"),
    ]));
    assert_eq!(s.get_list("SLACK_BOT_TOKENS"), vec!["xoxb-1"]);
}
