# Ship Talkers

## Overview

Ship Talkers is a Slack bot and website for exploring activity across Hack Club Slack.
It estimates time spent writing messages, compares that time with Hackatime coding time, and provides user, channel, and word statistics.

It is a fun way to see how much time you spend on Slack versus coding.  
Are you a real maker or is it all just shiptalk?  
Drop a message in the [#ship-talkers](https://hackclub.enterprise.slack.com/archives/C07TCQ45NTS) channel to find out.
Website: [here](https://shiptalkers.kirze.de/)

## How it works

The scraper uses Slack bot and user tokens to collect messages, threads, users, channels, reactions, and profile data.
It stores the data in PostgreSQL and uses ShipTalkers IDs, generated from Slack identifiers, for stable public URLs.

The website reads the stored data to provide stats pages, search, ranked boards, and paginated user and channel directories.
A sessionizer estimates Slack time from message activity, while the scoring jobs calculate user, channel, and word totals.

Users can link their accounts to retrieve Hackatime data through the Hackatime OAuth2 API.
Ship Talkers then compares coding time with estimated Slack time to show how a user spends time across both services.

## Website Boards

The website provides ranked boards for talkers, coders, channels, words, and combined time at `/boards`.
It also provides directory boards for all users at `/boards/users/` and all channels at `/boards/channels/`.

Directory boards are paginated and searchable by name, Slack ID, or ShipTalkers ID.
Numeric searches jump directly to the matching row.
User and channel links use ShipTalkers IDs by default, while stats and avatar URLs continue to accept Slack IDs.

## Needed Scopes

### Slack

#### Bot Events
- app_mention
- message.channels
- channel_created
- channel_history_changed
- team_join

#### Bot Token Scopes
- app_mentions:read
- channels:history
- channels:join
- channels:read
- chat:write
- groups:history
- groups:read
- mpim:history
- users:read
- users:read.email
- files:read
- files:write

#### User Token Scopes
- channels:history
- channels:read

### Hackatime
- user
- profile

### HCA (Hack Club Auth)
- slack_id
- email (optional)
- name

## License

This project is licensed under the MIT License, see the [LICENSE.md](LICENSE.md) file for details.
