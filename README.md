# Ship Talkers

## Overview

You link your slack and hackatime accounts. The bot can then generate a chart to compare them in the ship-talkers channel of Hack Club slack!

## Why

So the old person maintaing shiptalkers is leaving Hack Club, this is a faster, and more efficient remake.

## Stack

Rust - Idk what to tell you other than that

Clickhouse - For storing the data
  - Slack Messages
  - Hackatime Coding Activity

SQLite - The small relation db
  - Users
  - oauth tokens
  - settings (if any)
  - sync state

## How it works

It gets Slack message count by scraping channels, messages & thread replies through the Slack API.
It pulls and stores coding activity from Hackatime via OAuth.
The bot then shows you a chart comparing your Slack activity to your Hackatime activity.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.