DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = current_schema() AND table_name = 'slack_messages'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'slack_messages'
          AND column_name = 'identity_id'
    ) THEN
        DROP TABLE slack_messages CASCADE;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = current_schema() AND table_name = 'slack_channels'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'slack_channels'
          AND column_name = 'internal_id'
    ) THEN
        DROP TABLE slack_channels CASCADE;
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS slack_identities (
    internal_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    anonymous_id TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS slack_channels (
    internal_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    anonymous_id TEXT UNIQUE NOT NULL,
    channel_id TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    is_archived SMALLINT NOT NULL DEFAULT 0,
    num_members BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS slack_messages (
    identity_id INTEGER NOT NULL REFERENCES slack_identities(internal_id),
    channel_id INTEGER NOT NULL REFERENCES slack_channels(internal_id),
    message_ts BIGINT NOT NULL,
    char_count INTEGER NOT NULL DEFAULT 0,
    thread_ts BIGINT,
    PRIMARY KEY (channel_id, message_ts)
);

CREATE INDEX IF NOT EXISTS slack_messages_identity_ts_idx
    ON slack_messages (identity_id, message_ts);
CREATE INDEX IF NOT EXISTS slack_messages_identity_channel_ts_idx
    ON slack_messages (identity_id, channel_id, message_ts);
CREATE INDEX IF NOT EXISTS slack_messages_channel_ts_idx
    ON slack_messages (channel_id, message_ts);

CREATE TABLE IF NOT EXISTS slack_message_contents (
    channel_id INTEGER NOT NULL,
    message_ts BIGINT NOT NULL,
    text TEXT NOT NULL,
    PRIMARY KEY (channel_id, message_ts),
    FOREIGN KEY (channel_id, message_ts)
        REFERENCES slack_messages(channel_id, message_ts)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS slack_consents (
    identity_id INTEGER PRIMARY KEY
        REFERENCES slack_identities(internal_id) ON DELETE CASCADE,
    consented_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMPTZ,
    consent_source TEXT
);

CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    anonymous_id TEXT UNIQUE,
    merged_name TEXT NOT NULL DEFAULT '',
    display_name TEXT NOT NULL DEFAULT '',
    real_name TEXT NOT NULL DEFAULT '',
    username TEXT NOT NULL DEFAULT '',
    email TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    status_text TEXT NOT NULL DEFAULT '',
    status_emoji TEXT NOT NULL DEFAULT '',
    tz TEXT NOT NULL DEFAULT '',
    tz_label TEXT NOT NULL DEFAULT '',
    locale TEXT NOT NULL DEFAULT '',
    pfp TEXT NOT NULL DEFAULT '',
    updated BIGINT NOT NULL DEFAULT 0,
    is_bot SMALLINT NOT NULL DEFAULT 0,
    is_deleted SMALLINT NOT NULL DEFAULT 0,
    is_admin SMALLINT NOT NULL DEFAULT 0,
    is_owner SMALLINT NOT NULL DEFAULT 0,
    is_restricted SMALLINT NOT NULL DEFAULT 0,
    is_app_user SMALLINT NOT NULL DEFAULT 0
);
ALTER TABLE users ADD COLUMN IF NOT EXISTS anonymous_id TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS users_anonymous_id_idx
    ON users (anonymous_id) WHERE anonymous_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS scrape_checkpoints (
    channel_id TEXT PRIMARY KEY,
    fully_scraped SMALLINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS thread_checkpoints (
    channel_id TEXT NOT NULL,
    thread_ts TEXT NOT NULL,
    fully_scraped SMALLINT NOT NULL DEFAULT 0,
    PRIMARY KEY (channel_id, thread_ts)
);
CREATE TABLE IF NOT EXISTS scraped_channels (
    channel_id TEXT PRIMARY KEY,
    scraped_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS user_scores (
    user_id TEXT PRIMARY KEY,
    score BIGINT NOT NULL DEFAULT 0,
    total_time BIGINT NOT NULL DEFAULT 0,
    messages BIGINT NOT NULL DEFAULT 0,
    sessions BIGINT NOT NULL DEFAULT 0,
    longest BIGINT NOT NULL DEFAULT 0,
    days BIGINT NOT NULL DEFAULT 0,
    channels BIGINT NOT NULL DEFAULT 0,
    first_ts BIGINT NOT NULL DEFAULT 0,
    last_ts BIGINT NOT NULL DEFAULT 0,
    active_hour SMALLINT NOT NULL DEFAULT 0,
    updated BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS hackatime_connections (
    slack_id TEXT PRIMARY KEY,
    access_token TEXT NOT NULL DEFAULT '',
    last_synced_date TEXT,
    connected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL DEFAULT '',
    total_minutes BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS hackatime_spans (
    slack_id TEXT NOT NULL,
    start_ts BIGINT NOT NULL,
    duration BIGINT NOT NULL DEFAULT 0,
    updated BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (slack_id, start_ts)
);
CREATE TABLE IF NOT EXISTS slack_reactions (
    channel_id TEXT NOT NULL,
    message_ts BIGINT NOT NULL,
    emoji TEXT NOT NULL,
    user_id TEXT NOT NULL,
    PRIMARY KEY (channel_id, message_ts, emoji, user_id)
);
CREATE INDEX IF NOT EXISTS slack_reactions_message_idx
    ON slack_reactions (channel_id, message_ts);
CREATE TABLE IF NOT EXISTS word_counts (
    word TEXT NOT NULL,
    user_id TEXT NOT NULL DEFAULT '',
    channel_id TEXT NOT NULL DEFAULT '',
    message_ts BIGINT NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    inserted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (word, channel_id, message_ts)
);
CREATE INDEX IF NOT EXISTS word_counts_inserted_at_idx ON word_counts (inserted_at);
CREATE TABLE IF NOT EXISTS word_totals (
    word TEXT PRIMARY KEY,
    cnt BIGINT NOT NULL DEFAULT 0,
    updated BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS channel_scores (
    channel_id TEXT PRIMARY KEY,
    total_time BIGINT NOT NULL DEFAULT 0,
    messages BIGINT NOT NULL DEFAULT 0,
    updated BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS score_meta (
    id SMALLINT PRIMARY KEY,
    formula TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS backfill_meta (
    name TEXT PRIMARY KEY,
    done SMALLINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS scrape_sweep (
    id SMALLINT PRIMARY KEY,
    resume_channel TEXT NOT NULL DEFAULT '',
    updated TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS word_refresh_meta (
    id SMALLINT PRIMARY KEY,
    watermark BIGINT NOT NULL DEFAULT 0,
    last_full BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS daily_stats (
    date DATE PRIMARY KEY,
    slack_secs BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS stats_meta (
    id SMALLINT PRIMARY KEY,
    total_messages BIGINT NOT NULL DEFAULT 0,
    total_channels BIGINT NOT NULL DEFAULT 0,
    archived_channels BIGINT NOT NULL DEFAULT 0,
    total_users BIGINT NOT NULL DEFAULT 0,
    coding_minutes BIGINT NOT NULL DEFAULT 0,
    slack_time_secs BIGINT NOT NULL DEFAULT 0,
    db_size_bytes BIGINT NOT NULL DEFAULT 0,
    updated BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS message_count (
    id SMALLINT PRIMARY KEY,
    total BIGINT NOT NULL DEFAULT 0
);
CREATE OR REPLACE FUNCTION increment_message_count() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO message_count (id, total) VALUES (1, 1)
    ON CONFLICT (id) DO UPDATE SET total = message_count.total + 1;
    RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS message_count_insert ON slack_messages;
CREATE TRIGGER message_count_insert AFTER INSERT ON slack_messages
FOR EACH ROW EXECUTE FUNCTION increment_message_count();
CREATE TABLE IF NOT EXISTS linked_users (
    slack_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    linked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS api_keys (
    key_id TEXT PRIMARY KEY,
    slack_id TEXT NOT NULL,
    key_hash TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    last_used_at BIGINT
);
CREATE TABLE IF NOT EXISTS api_key_grants (
    grantor_id TEXT NOT NULL,
    key_id TEXT NOT NULL REFERENCES api_keys(key_id) ON DELETE CASCADE,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (grantor_id, key_id)
);
