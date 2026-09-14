ALTER TABLE thread_checkpoints
    ADD COLUMN IF NOT EXISTS slack_reply_count BIGINT NOT NULL DEFAULT -1,
    ADD COLUMN IF NOT EXISTS slack_latest_reply_ts BIGINT NOT NULL DEFAULT 0;
