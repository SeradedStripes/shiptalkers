# Ship Talkers

## Overview

You link your slack and hackatime accounts. You can message the bot in HC Slack to see a chart comparing your Hackatime coding activity to your Slack activity.

Additionally, it has a full website and many more features will be added in the future!

## How it works

It gets Slack message count by scraping channels, messages & thread replies through the Slack API.  
It pulls and stores coding activity from Hackatime via OAuth.  
It has an algorithm that compares your Slack activity to your Hackatime activity and generates a chart.  
The bot then shows you a chart comparing your Slack activity to your Hackatime activity.  

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