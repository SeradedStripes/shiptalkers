CREATE OR REPLACE FUNCTION increment_message_count() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO message_count (id, total)
    SELECT 1, count(*)
    FROM inserted_messages
    ON CONFLICT (id) DO UPDATE
        SET total = message_count.total + EXCLUDED.total;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS message_count_insert ON slack_messages;
CREATE TRIGGER message_count_insert
AFTER INSERT ON slack_messages
REFERENCING NEW TABLE AS inserted_messages
FOR EACH STATEMENT
EXECUTE FUNCTION increment_message_count();
