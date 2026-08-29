use std::collections::HashMap;

use ship_talkers::settings::RuntimeSettings;

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
    assert_eq!(s.get("PORT"), "3000");
    assert_eq!(s.get("HOST"), "0.0.0.0");
    assert_eq!(s.get("DATABASE_URL"), "");
    assert_eq!(s.get("SLACK_BOT_TOKENS"), "");
}

#[test]
fn set_keys_override_defaults() {
    let s = RuntimeSettings::from_env(env(&[("PORT", "8080"), ("SLACK_BOT_TOKENS", "xoxb-1")]));
    assert_eq!(s.get("PORT"), "8080");
    assert_eq!(s.get_list("SLACK_BOT_TOKENS"), vec!["xoxb-1"]);
    assert_eq!(s.get("HOST"), "0.0.0.0");
}

#[test]
fn get_list_splits_trims_and_drops_empties() {
    let s = RuntimeSettings::from_env(env(&[("SLACK_APP_TOKENS", " a , b ,, c ")]));
    assert_eq!(s.get_list("SLACK_APP_TOKENS"), vec!["a", "b", "c"]);
    assert_eq!(s.get_list("SLACK_BOT_TOKENS"), Vec::<String>::new());
}

#[test]
fn get_list_merges_numbered_variants() {
    let s = RuntimeSettings::from_env(env(&[
        ("SLACK_BOT_TOKENS", "xoxb-0"),
        ("SLACK_BOT_TOKENS_1", "xoxb-1"),
        ("SLACK_BOT_TOKENS_2", " xoxb-2 ,"),
        ("SLACK_APP_TOKENS_3", "xoxa-3"),
    ]));
    assert_eq!(
        s.get_list("SLACK_BOT_TOKENS"),
        vec!["xoxb-0", "xoxb-1", "xoxb-2"]
    );
    assert_eq!(s.get_list("SLACK_APP_TOKENS"), vec!["xoxa-3"]);
}

#[test]
fn get_list_dedupes_across_base_and_variants() {
    let s = RuntimeSettings::from_env(env(&[
        ("SLACK_BOT_TOKENS", "xoxb-1"),
        ("SLACK_BOT_TOKENS_1", "xoxb-1"),
    ]));
    assert_eq!(s.get_list("SLACK_BOT_TOKENS"), vec!["xoxb-1"]);
}

#[test]
fn auth_config_reads_oauth_credentials() {
    let s = RuntimeSettings::from_env(env(&[
        ("HCA_CLIENT_ID", "hca-id"),
        ("HCA_CLIENT_SECRET", "hca-secret"),
        ("HACKATIME_CLIENT_ID", "ht-id"),
        ("HACKATIME_CLIENT_SECRET", "ht-secret"),
        ("BASE_URL", "https://example.com"),
        ("SESSION_SECRET", "s3cret"),
    ]));
    let c = s.auth_config();
    assert_eq!(c.hca_client_id, "hca-id");
    assert_eq!(c.hca_client_secret, "hca-secret");
    assert_eq!(c.hackatime_client_id, "ht-id");
    assert_eq!(c.hackatime_client_secret, "ht-secret");
    assert_eq!(c.base_url, "https://example.com");
    assert_eq!(c.session_secret, "s3cret");
}
