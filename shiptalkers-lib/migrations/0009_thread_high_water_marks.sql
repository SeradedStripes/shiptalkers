ALTER TABLE thread_checkpoints
    ADD COLUMN IF NOT EXISTS latest_reply_ts BIGINT NOT NULL DEFAULT 0;
