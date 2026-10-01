BEGIN;

DROP TABLE IF EXISTS slack_oauth_channel_access;
DROP TABLE IF EXISTS slack_oauth_tokens;

DELETE FROM slack_reactions
WHERE channel_id IN (SELECT channel_id FROM slack_channels WHERE is_private = 1);
DELETE FROM slack_messages
WHERE channel_id IN (SELECT internal_id FROM slack_channels WHERE is_private = 1);
DELETE FROM channel_scores
WHERE channel_id IN (SELECT channel_id FROM slack_channels WHERE is_private = 1);
DELETE FROM thread_checkpoints
WHERE channel_id IN (SELECT channel_id FROM slack_channels WHERE is_private = 1);
DELETE FROM scrape_checkpoints
WHERE channel_id IN (SELECT channel_id FROM slack_channels WHERE is_private = 1);
DELETE FROM scraped_channels
WHERE channel_id IN (SELECT channel_id FROM slack_channels WHERE is_private = 1);
DELETE FROM slack_channels WHERE is_private = 1;

COMMIT;
