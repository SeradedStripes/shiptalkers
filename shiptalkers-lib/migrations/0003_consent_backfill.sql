ALTER TABLE slack_consents
    ADD COLUMN IF NOT EXISTS content_backfilled_at TIMESTAMPTZ;
