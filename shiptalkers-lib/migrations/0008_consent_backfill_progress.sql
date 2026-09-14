CREATE TABLE IF NOT EXISTS consent_backfill_progress (
    identity_id INTEGER NOT NULL REFERENCES slack_identities(internal_id) ON DELETE CASCADE,
    channel_id TEXT NOT NULL,
    latest_message_ts BIGINT NOT NULL DEFAULT 0,
    completed SMALLINT NOT NULL DEFAULT 0,
    PRIMARY KEY (identity_id, channel_id)
);
