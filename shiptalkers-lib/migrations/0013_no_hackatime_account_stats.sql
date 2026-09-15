ALTER TABLE stats_meta ADD COLUMN IF NOT EXISTS no_hackatime_account_users BIGINT NOT NULL DEFAULT 0;
