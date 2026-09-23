ALTER TABLE slack_channels ADD COLUMN IF NOT EXISTS is_private SMALLINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS slack_oauth_channel_access (
    slack_id TEXT NOT NULL REFERENCES slack_oauth_tokens(slack_id) ON DELETE CASCADE,
    channel_id TEXT NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (slack_id, channel_id)
);

CREATE INDEX IF NOT EXISTS slack_oauth_channel_access_channel_idx
    ON slack_oauth_channel_access (channel_id);
