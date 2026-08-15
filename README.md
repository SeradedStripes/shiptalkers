# Ship Talkers

## Overview

Shiptalkers is a Slack bot & Website that scrapes Hack Club Slack and displays your estimated slack time to your Hackatime coding time.  
It is a fun way to see how much time you spend on Slack vs coding!  
Are you a real maker or is it all just shiptalk? Drop a message in the [#shiptalkers](https://hackclub.enterprise.slack.com/archives/C07TCQ45NTS) channel to find out!

## How it works

It uses bot & user tokens to access the Slack API then scrapes everything.  
When you send a message or look at the website, it uses your stored data to display any analytical data we have.  
It pulls your Hackatime data using the Hackatime OAuth2 API (Which is why you have to link your accounts)  
It then runs a comparison between your Hackatime coding time and your Slack time to give you a percentage of how much time you spend on Slack vs coding.

## Needed Scopes

### Slack

#### Bot Events
- app_mention
- message.channels
- channel_created
- channel_history_changed

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

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.