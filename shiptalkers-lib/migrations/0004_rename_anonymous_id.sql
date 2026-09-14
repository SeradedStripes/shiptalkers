DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'slack_identities'
          AND column_name = 'anonymous_id'
    ) THEN
        ALTER TABLE slack_identities RENAME COLUMN anonymous_id TO ship_talkers_id;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'slack_channels'
          AND column_name = 'anonymous_id'
    ) THEN
        ALTER TABLE slack_channels RENAME COLUMN anonymous_id TO ship_talkers_id;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'users'
          AND column_name = 'anonymous_id'
    ) THEN
        ALTER TABLE users RENAME COLUMN anonymous_id TO ship_talkers_id;
    END IF;
END
$$;

ALTER INDEX IF EXISTS users_anonymous_id_idx RENAME TO users_ship_talkers_id_idx;
