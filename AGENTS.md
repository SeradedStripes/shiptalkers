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

## Overview

Scrapes every public channel and thread reply from Hack Club Slack into ClickHouse and serves a stats website with live leaderboards.

## Architecture

- `src/main.rs` - entry point, env parsing, scraper orchestration
- `src/slack/mod.rs` - SlackClient, per-method FIFO token-bucket rate limiter, 429 backoff
- `src/slack/socket.rs` - Slack Socket Mode (app events) via tokio-tungstenite; stats bot replies to top-level messages in `SLACK_MAIN_CHANNEL` in a thread, via `chat.postMessage`, with a PNG card uploaded via `files.getUploadURLExternal` + `files.completeUploadExternal`
- `src/bot_image.rs` - renders the stats card SVG (`templates/slack_image.html` + `src/website/static/slack_image_stats.css`) to PNG via resvg/usvg, with bundled DejaVu fonts
- `src/db/clickhouse_db.rs` - ClickHouse schema, inserts, checkpoint queries
- `src/db/sqlite.rs` - SQLite auth DB (`linked_users`), the only non-ClickHouse datastore
- `src/website/mod.rs` - axum router, server-rendered `/stats`, `/stats/:id` (user or channel, dispatched by `U`/`C` prefix), `/leaderboard` and `/search` via askama
- `templates/stats.html` - askama template for the stats page
- `templates/user.html` - askama template for the per-user stats page
- `templates/channel.html` - askama template for the per-channel stats page
- `templates/leaderboard.html` - askama template for the leaderboard page
- `templates/search.html` - askama template for user and channel search results
- `templates/search_form.html` - shared inline search form partial
- `templates/slack_image.html` - askama SVG template for the stats bot card (CSS inlined from `slack_image_stats.css`)
- `src/website/static/` - style.css, time.js, slack_image_stats.css
- `src/formula.rs` - Slack Time formula evaluator and the `SLACK_TIME_CALCULATION_FORMULA` code constant (edit here to change the algorithm)

## Slack Time Formula

`SLACK_TIME_CALCULATION_FORMULA` in `src/formula.rs` drives Top Talkers ranking and the per-user Slack Time report. Variables: `SESSION_SECONDS` (sessionizer output, 5 min windows split after 30 min inactivity, capped at 4 h), `MESSAGE_COUNT`, `SESSION_COUNT`, `TOTAL_CHARS`, `AVG_MESSAGE_LENGTH`. Functions: `log10`, `ln`, `sqrt`, `exp`, `abs`, `pow`. Supports `+ - * / ()` and implicit multiplication like `2MESSAGE_COUNT`. Invalid formulas fail at startup. Comments above the constant document each variable's source.

## Conventions

- ClickHouse is the only analytics datastore. The stats page reads `slack_messages`, `slack_channels`, and `coding_activity`. SQLite (`src/db/sqlite.rs`) holds auth/linked-user state only.
- Insert data before marking any checkpoint complete. Main channel messages are inserted before thread replies.
- Progress tracking uses `max(message_ts)` per channel and `max(thread reply ts)` per thread.
- Logging is `tracing` only. Per-channel, per-thread, and per-fetch work logs at debug; inserts, page progress, and 15s `Progress:` lines log at info.
- Multi-token scraping round-robins channel shards across tokens and prefixes log lines with `[token k]`.
- The website has exactly one JavaScript file (`src/website/static/time.js`, loaded via `header.html`), which converts UTC `<time>` elements to the visitor's local timezone. Everything else renders server side with askama and auto refreshes via `<meta http-equiv="refresh">`. Number formatting lives in Rust (`fmt_thousands`).
- ClickHouse row structs use `#[derive(clickhouse::Row, serde::Deserialize)]`, plus `Serialize` when inserting.
- Queries that must survive transient DB issues fall back with `unwrap_or` / `unwrap_or_default`, never panic.
- Errors use `Box<dyn std::error::Error>` (plus `Send + Sync` across await points) or `String` in scraper tasks.

## Environment Variables

- `SLACK_BOT_TOKEN` - required, bot token for channel listing and `chat.postMessage` replies
- `SLACK_USER_TOKENS` - comma-separated user tokens, sharded round-robin per channel; falls back to `SLACK_USER_TOKEN`
- `SLACK_APP_TOKEN` - optional, enables Socket Mode
- `SLACK_MAIN_CHANNEL` - channel ID the stats bot watches; users posting a time range there get a threaded reply. Optional, disables the bot when unset
- `SQLITE_DB_PATH` - SQLite auth DB path (linked users), default `data/auth.db`
- `SLACK_REQUEST_DELAY_MS` - request pacing per method per token, default 1200 (tier 3, 50 req/min)
- `SLACK_MAX_INFLIGHT` - burst per method per token, default 8
- `SLACK_CHANNEL_CONCURRENCY` - channels scraped concurrently per token, default 64
- `CLICKHOUSE_URL`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DB`
- `HOST`, `PORT` - web server bind, default 0.0.0.0:3000

## Gotchas

- Slack rate limits are per (token, method). `conversations.history` and `conversations.replies` have separate budgets, so the rate limiter stays per method.
- The rate limiter is a FIFO ticket queue that paces at exactly 1 request per delay, so one huge channel cannot stall the pass.
- Scrape passes split into full-scrape (new channels) and incremental check (already-scraped channels) using `scraped_channels`.

## Finally

Thanks for your help! (To you the AI agent reading this or a human looking at this file) <3