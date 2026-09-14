ALTER TABLE scrape_checkpoints
    ADD COLUMN IF NOT EXISTS thread_rescan_at BIGINT NOT NULL DEFAULT 0;
