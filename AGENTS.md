# Agent Instructions

Use this file as the default guide for AI agents working in the repository.

## Human in the Loop

- Always keep a human in the loop. Present your work for review, and do not deploy, push, or run anything that changes shared state without explicit approval.
- Do not test with Docker. Building images, starting the dev server, and running containers is the user's job.

## Quick Rules

- Run `just lint` and `just fmt` before marking code as ready for review.
- Do not use emdashes in anything.
- Use proper markdown syntax.
- Follow existing code style and conventions.
- Stay minimalistic in your code and documentation.
- Do not divert from your active task unless explicitly instructed to do so.
- If you change something that affects anything in the "Overview" section of this file, update it accordingly.

## Commands

- `just check` - cargo check for all targets and features
- `just lint` - clippy with `-D warnings`
- `just fmt` - rustfmt
- `just test` - cargo test
- `just formula-test` - sessionizer tests with `--nocapture` so the demo tests print the computed numbers

## Overview

Scrapes every public channel and thread reply (plus their reactions) from Hack Club Slack into ClickHouse and serves a stats website with live leaderboards. Emoji names stay in the raw message text and in `slack_reactions` for later statistics; there is no emoji catalog yet.

Coding activity for every non-bot, non-deleted user is pulled from hackatime on a 30-minute cycle. Most users sync through the public per-user stats API (keyed by Slack UID, no OAuth); a user whose hackatime profile is not public gets a `private` flag and only syncs once they link via OAuth. Missing profiles are recorded as `no_account` and retried every 30 days. A user with a saved OAuth token always syncs through it.

## Repository Layout

- `Cargo.toml` (root) - Cargo workspace with a single member, `shiptalkers-app`; run `cargo`/`just` from the repo root
- `shiptalkers-app/` - the Rust app crate: `src/`, `templates/`, `tests/`, `Cargo.toml`, `Dockerfile`, `docker-entrypoint.sh`, `.env.example`; built from the workspace root (single `Cargo.lock`, context is the repo root) so the app's `Dockerfile` is used as `shiptalkers-app/Dockerfile` from `docker-compose.yml` and the CI build workflow
- `scripts/slack_app_creation/` - standalone Rust CLI (not a workspace member) for creating the Slack app and installing tokens
- `.github/` - `workflows/` (CI, build/deploy) and `actions/setup-rust`
- `docker-compose.yml` - local dev setup (app + ClickHouse)

## Architecture

- `shiptalkers-app/src/main.rs` - entry point, settings + env parsing, scraper orchestration; when none of `SLACK_BOT_TOKENS`/`SLACK_USER_TOKENS`/`SLACK_APP_TOKENS` are set, the scraper and user sync are skipped entirely and the site serves existing ClickHouse data read-only; otherwise message scrape cycles run every 30 minutes, pacing a full pass over every channel (user tokens pull from a shared work queue, so a token that finishes a slow channel immediately grabs the next instead of idling on a pre-computed shard) to the 30m boundary before repeating. Channel and thread history is fetched page-by-page (`stream_channel_history` / `stream_thread_replies` in `src/slack/mod.rs`), so memory stays flat no matter how big a channel or thread gets. Channel tasks have no wall-clock timeout, so a huge channel's first full scrape is allowed to run as long as it is making requests; a channel that fails to fetch (including a deleted channel's `channel_not_found`) is skipped instead, and the per-request 429 retry loop is capped so a persistently rate-limited token errors out rather than spinning forever. `conversations.list` page inserts are wrapped in a 2m timeout, so a stalled ClickHouse call can never wedge a pass or cycle. After a channel's messages and thread replies are inserted, the touched users' Slack Time scores are recomputed into `user_scores`; parallel startup backfills recompute stale users and channels (a full recompute of both only when the sessionizer changed, checked against `score_meta` once up front since the user backfill writes that row). Fully-scraped channels are also re-scanned for threads: every `SLACK_THREAD_RESCAN_INTERVAL_HOURS` (default 6h) a recent `SLACK_THREAD_RESCAN_HOURS` (default 30-day) window of history is fetched (throttled per channel in memory) so messages that gained a thread after their first scrape get their replies fetched too, and that window's reactions are refreshed in the same pass. `sync_users` runs `users.list` every 2h (retry after 5m on failure), upserts only users whose `updated` changed, who lack a pfp URL, or who just became deleted, so avatar URLs stay current. Message scraping falls back to `bot_id` when a message carries no `user` (classic apps and webhook integrations post that way), and those bot authors are upserted into `users` with `is_bot = 1` (display name from `username`/`bot_profile.name`) so they stay off leaderboards and remain searchable. `users` keeps every account (including bots and deleted ones) with `is_bot`/`is_deleted` flags; leaderboards, rankings, and stats counts exclude `is_bot = 1 OR is_deleted = 1`, but such users stay searchable and deleted accounts render as "Deleted account".
- `shiptalkers-app/src/slack/mod.rs` - SlackClient, per-method FIFO token-bucket rate limiter, 429 backoff, page-by-page streaming pagination (`stream_channel_history` / `stream_thread_replies`) so whole channels/threads never sit in memory; SlackClientPool round-robins `conversations.list` / `users.list` pages across bot tokens (one SlackClient per token)
- `shiptalkers-app/src/slack/socket.rs` - Slack Socket Mode (app events) via tokio-tungstenite; one connection per `SLACK_APP_TOKENS` app, message events sharded across apps so only one replies; each connection reconnects forever with exponential backoff (fresh `apps.connections.open` URL per attempt, 1s to 60s) since Slack recycles connections; a connection with no frames for 60s is treated as stale and reconnected, and the connect phase itself times out after 30s; stats bot replies to top-level messages in `SLACK_MAIN_CHANNEL` in a thread, via `chat.postMessage`, with a PNG card uploaded via `files.getUploadURLExternal` + `files.completeUploadExternal`. Replies always use the first `SLACK_BOT_TOKENS` entry (the main bot). The requested time range is parsed from keywords in the message (`parse_time_range`): rolling windows (e.g. `last 7 days`, `one day`, `one second`) and exact calendar windows (e.g. `today`, `yesterday`, `this`/`last` week/month/year) with `Between` bounds on the query.
- `shiptalkers-app/src/bot_image.rs` - renders the stats card SVG (`shiptalkers-app/templates/slack_image.html` + `shiptalkers-app/src/website/static/slack_image_stats.css`) to PNG via resvg/usvg, with bundled DejaVu fonts
- `shiptalkers-app/src/db/clickhouse_db.rs` - ClickHouse schema, inserts, checkpoint queries, startup migrations (ReplacingMergeTree engine, `message_ts` String to UInt64 micros, `coding_activity.date` String to Date), periodic `OPTIMIZE TABLE slack_messages` (24h, no startup dedup); `slack_reactions` (per-message reactions: emoji + reacting user, `ReplacingMergeTree`, replaced per re-fetched message by `insert_reactions` with a chunked `DELETE ... IN` then insert); `user_scores` (per-user Slack Time score and metrics: session time, longest session, sessions, days, active hour, message/channel counts; `ReplacingMergeTree(updated)`) refreshed by `recompute_user_scores` whenever a user's messages change (batches of 50 users) and read by the per-user stats pages; `channel_scores` (per-channel Slack Time and message counts, `ReplacingMergeTree(updated)`) refreshed by `recompute_channel_scores` (batches of 50 channels, excluding bot/deleted users) and read by the Top Channels leaderboard; `word_counts` (per-message word frequencies, one row per distinct lowercase word per message, `ReplacingMergeTree` deduped on read with `FINAL`) written by the scraper for each newly inserted message (with an `inserted_at` insert-time stamp so thread re-scans that add replies to old threads still count as dirty) plus a one-time `backfill_word_counts` (completion tracked in `backfill_meta` so a restart never re-runs it); the Top Words leaderboard reads the materialized `word_totals` (per-word totals excluding bot/deleted users, `ReplacingMergeTree(updated)`, compacted with `OPTIMIZE ... FINAL`) which `refresh_word_totals` maintains on a 30m background schedule by folding only words with rows inserted since the last pass (watermark tracked in `word_refresh_meta`, so the cycle never re-aggregates the whole table) with a daily full rebuild as the safety net so the page never scans every word row; `daily_stats` (per-day coding minutes and Slack Time seconds over all history) rebuilt by `refresh_daily_stats` on a 30m background schedule (a sessionizer pass over every message, excluding bot/deleted users, so it must never run on a page load) and read by the stats page charts; `hackatime_connections` (per-user Slack UID: `access_token`, `last_synced_date`, and a `status` of `''`/`private`/`no_account` where an empty `access_token` marks a non-OAuth row) written by `website/auth.rs`
- `shiptalkers-app/src/db/sqlite.rs` - SQLite auth DB (`linked_users`), the only non-ClickHouse datastore
- `shiptalkers-app/src/settings.rs` - settings read from environment variables at startup (with defaults), exposed as `RuntimeSettings`; `SETTING_KEYS` and `default_value` drive the env parsing
- `shiptalkers-app/src/website/mod.rs` - axum router, server-rendered `/stats`, `/stats/:id` (user or channel, dispatched by `U`/`C` prefix), `/leaderboard` and `/search` via askama; `/pfp/:id` looks up the stored Slack pfp URL and redirects to it
- `shiptalkers-app/src/website/auth.rs` - hackatime OAuth login/callback/disconnect plus `sync_coding_activity` (per-user day-by-day coding sync, chunked 24 days at a time, that fetches before writing and returns `SyncFailure` with `PrivateProfile`/`NoAccount`/`Message`) and the 30m `resync_all` loop over every non-bot user (token users via OAuth, everyone else via the public stats API with a single-day probe, skipping `no_account` users for 30 days and `private` users with no token)
- `shiptalkers-app/templates/stats.html` - askama template for the stats page

- `shiptalkers-app/templates/user.html` - askama template for the per-user stats page
- `shiptalkers-app/templates/channel.html` - askama template for the per-channel stats page
- `shiptalkers-app/templates/leaderboard.html` - askama template for the leaderboard page
- `shiptalkers-app/templates/search.html` - askama template for user and channel search results
- `shiptalkers-app/templates/search_form.html` - shared inline search form partial
- `shiptalkers-app/templates/slack_image.html` - askama SVG template for the stats bot card (CSS inlined from `slack_image_stats.css`)
- `shiptalkers-app/src/website/static/` - style.css, time.js, slack_image_stats.css
- `shiptalkers-app/src/sessionize.rs` - Slack Time sessionizer: the shared constants (`SESSION_GAP_BOUNDARY_SECS`, `MESSAGE_TYPING_CHARS_PER_SEC`, `MESSAGE_READ_OVERHEAD_SECS`, `SESSION_MAX_SECS`) and the Rust reference `sessionize` (edit here to change the algorithm)
- `scripts/slack_app_creation/` - standalone Rust CLI that creates a Slack app from a manifest (default `manifest.yml` in the same directory as `.env.example`, or one passed as an argument) via an app configuration token and runs a one-shot OAuth install to print the bot and user tokens and write them (plus an empty `SLACK_APP_TOKENS` line to fill in) to `.env.output` for the Rust app; each run's tokens get a numbered key (`SLACK_BOT_TOKENS_1`, `SLACK_USER_TOKENS_1`, ... next free number) so reruns append short one-per-line tokens instead of a giant comma list; configured through env vars (`SLACK_CONFIG_TOKEN`, `SLACK_CONFIG_REFRESH_TOKEN`, `SLACK_INSTALL_PORT`), see `.env.example`

## Slack Time Formula

Slack Time is the sessionizer output (`user_scores.total_time`, ranked by `score`) driving Top Talkers ranking, the per-user Slack Time report, and the Slack stats bot card (`shiptalkers-app/src/slack/socket.rs` runs the same sessionizer query over the requested range). There is no formula engine anymore, so to change the algorithm edit the sessionizer parameters in `shiptalkers-app/src/sessionize.rs` (`SESSION_GAP_BOUNDARY_SECS`, `MESSAGE_TYPING_CHARS_PER_SEC`, `MESSAGE_READ_OVERHEAD_SECS`, `SESSION_MAX_SECS`), which are shared constants injected into all three ClickHouse sessionizer queries and into the Rust reference `sessionize`; `shiptalkers-app/tests/sessionizer.rs` pins the semantics and the deployed sessionizer's output on a realistic day. A change to any constant flips `score_meta`'s stored fingerprint, so the next restart full-recomputes all user and channel scores.

## Conventions

- ClickHouse is the only analytics datastore. The stats page reads `slack_messages`, `slack_channels`, `coding_activity`, and `user_scores`; per-user pages read `user_scores` (plus `slack_messages_by_user` for the top-channels list). SQLite (`shiptalkers-app/src/db/sqlite.rs`) holds auth/linked-user state.
- Insert data before marking any checkpoint complete. Main channel messages are inserted before thread replies.
- Progress tracking uses `max(message_ts)` per channel and `max(thread reply ts)` per thread. `slack_messages.message_ts` is `UInt64` microseconds (Slack sends "seconds.microseconds" strings, converted via `slack_ts_to_micros`/`micros_to_slack_ts` in `shiptalkers-app/src/db/clickhouse_db.rs`); `thread_ts` stays `String` because Slack pagination uses it verbatim. `coding_activity.date` is `Date` (`time::Date` in Rust via `clickhouse::serde::time::date`).
- Logging is `tracing` only. Per-channel, per-thread, and per-fetch work logs at debug; inserts and page progress log at info; `Progress:` lines log at info but only when a run actually inserts new messages, never on an idle tick.
- Multi-token scraping pulls channels from a shared work queue across tokens and prefixes log lines with `[token k]`.
- The website has exactly one public JavaScript file (`shiptalkers-app/src/website/static/time.js`, loaded via `shiptalkers-app/templates/header.html`), which converts UTC `<time>` elements to the visitor's local timezone. Everything else renders server side with askama and auto refreshes via `<meta http-equiv="refresh">`. Number formatting lives in Rust (`fmt_thousands`).
- ClickHouse row structs use `#[derive(clickhouse::Row, serde::Deserialize)]`, plus `Serialize` when inserting.
- Tests live in `shiptalkers-app/tests/` (one file per area: `sessionizer`, `bot_image`, `time_range`, `formatting`, `settings`, `ts_conv`, `stats_pages`) and only reach `pub` items, so helpers under test stay `pub`. The crate is lib + bin: `shiptalkers-app/src/lib.rs` declares the modules, `shiptalkers-app/src/main.rs` imports them.
- Queries that must survive transient DB issues fall back with `unwrap_or` / `unwrap_or_default`, never panic.
- Errors use `Box<dyn std::error::Error>` (plus `Send + Sync` across await points) or `String` in scraper tasks.

## Environment Variables

All settings below are read from environment variables at startup (with the defaults noted); edit `shiptalkers-app/.env` and restart to change them.

- `SLACK_BOT_TOKENS` - required, comma-separated bot tokens (one per Slack app), or the numbered variants `SLACK_BOT_TOKENS_1`, `SLACK_BOT_TOKENS_2`, ... (one per app, `get_list` merges base + variants); `conversations.list` / `users.list` pages round-robin across them, stats bot replies always use the first entry (the main bot).
- `SLACK_USER_TOKENS` - comma-separated user tokens or numbered variants (`SLACK_USER_TOKENS_1`, ...), one SlackClient per token pulling from the shared channel work queue.
- `SLACK_APP_TOKENS` - optional, comma-separated app tokens or numbered variants (`SLACK_APP_TOKENS_1`, ...); each opens its own Socket Mode connection and message events are sharded across them so only one bot replies.
- `SLACK_MAIN_CHANNEL` - channel ID the stats bot watches; users posting a time range there get a threaded reply. Optional, disables the bot when unset.
- `SQLITE_DB_PATH` - SQLite auth DB path (linked users), default `data/auth.db`.
- `SLACK_REQUEST_DELAY_MS` - request pacing per method per token, default 1200 (tier 3, 50 req/min)
- `SLACK_MAX_INFLIGHT` - burst per method per token, default 8
- `SLACK_CHANNEL_CONCURRENCY` - channels scraped concurrently per token, default 8
- `SLACK_THREAD_RESCAN_HOURS` - thread rescan history window, default 720 (30 days)
- `SLACK_THREAD_RESCAN_INTERVAL_HOURS` - how often fully-scraped channels are re-scanned for threads, default 6
- `CLICKHOUSE_URL` - ClickHouse HTTP endpoint, default `http://clickhouse:8123`. Coolify internal URLs (`clickhouse://user:pass@host:9000/db`) are auto-converted to `http://host:8123`, and the credentials/database embedded in the URL are used unless `CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD`/`CLICKHOUSE_DB` are explicitly set (see `normalize_clickhouse_url`).
- `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DB` - ClickHouse credentials and database, default `ship_talkers`
- `HOST`, `PORT` - web server bind, default 0.0.0.0:3000.

## Gotchas

- Slack rate limits are per (token, method). `conversations.history` and `conversations.replies` have separate budgets, so the rate limiter stays per method.
- Every token gets its own rate-limiter budget: `SLACK_BOT_TOKENS` pages rotate one token per page, `SLACK_USER_TOKENS` workers each get their own SlackClient (pulling from the shared queue), and stats bot replies use the first bot token (the main bot).
- Socket Mode opens one connection per `SLACK_APP_TOKENS` app. Slack delivers every event to every app, so message events are sharded (FNV hash of `ts`) and only the owning socket replies. Duplicate `channel_created` events are harmless because `insert_new_channels` is idempotent.
- The rate limiter is a FIFO ticket queue that paces at exactly 1 request per delay, so one huge channel cannot stall the pass.
- Scrape passes split into full-scrape (new channels) and incremental check (already-scraped channels) using `scraped_channels`.
- Reactions are whatever `conversations.history` / `conversations.replies` returned for that fetch, so only re-fetched messages (new inserts, thread re-scans) get their reactions refreshed. Slack also truncates the `users` list of very popular reactions, so per-user reaction stats may undercount; the emoji name itself is always present.
- `coding_activity` is `ReplacingMergeTree` on new deployments but reads must not rely on `FINAL` (it errors on tables still created as plain `MergeTree`). Reads dedup with `max(minutes)` per `(user_id, date)` in SQL. Coding syncs are serialized per user (`CODING_SYNC_LOCKS` in `auth.rs`) and the clear-then-insert uses `SETTINGS mutations_sync = 2`, because concurrent syncs used to insert duplicate day rows that inflated coding time sums. Fetches run before any DB write, so a failed sync leaves the old rows intact; a 401/403 from the hours endpoint only deletes the connection if the `me` endpoint confirms the token is dead, so a hackatime outage (5xx or transport errors) never strips links or forces re-linking from `/link`. On the public path a single-day probe runs first (its result doubles as today's row) so a private (`403`) or missing (`404`) profile aborts with one request instead of a whole batch; `private` and `no_account` states are written back to `hackatime_connections` with an empty `access_token` so the resync loop skips them until a token appears or the `no_account` 30-day retry window passes.
- `slack_messages_by_user` is created with `max_suspicious_broken_parts = 1000` because it is fully derived from `slack_messages`: after an unclean shutdown ClickHouse refuses to attach a table with more than 100 broken parts, and letting this one sweep empty broken parts keeps a power loss from taking the whole service down. `slack_messages` keeps the default guard. A broken `slack_messages_by_user` poisons `slack_messages`'s startup load too (its load job waits on the MV, which waits on this table), so `init_tables` probes this table first and drops it (and its materialized view) whenever it cannot load; the startup backfill then rebuilds it and the app recovers on its own. On existing deployments the setting must be applied once via `ALTER TABLE slack_messages_by_user MODIFY SETTING max_suspicious_broken_parts = 1000` since `CREATE TABLE IF NOT EXISTS` does not update it.

## Finally

Thanks for your help! (To you the AI agent reading this or a human looking at this file) <3