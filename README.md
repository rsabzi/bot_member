# Telegram Member Bot

A powerful Telegram bot for managing and exporting channel members with advanced filtering capabilities.

## Features

- **Smart Scanning**: Efficiently scans channel members using recursive search to bypass limitations.
- **Filtering**: Export members based on activity status:
  - `Recently`: Online + Active in last 3 days
  - `Week`: Active in last 7 days
  - `Month`: Active in last 30 days
  - `Long`: All members (including long offline)
- **Batch Export**: Generate all filter lists at once.
- **Admin Dashboard**: Manage monitoring and exports directly from Telegram.
- **Resumable Scans**: Automatically resumes scanning if interrupted.

## Setup

1. **Clone the repository**:
   ```bash
   git clone git@github.com:rsabzi/bot_member.git
   cd bot_member
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Environment**:
   Create a `.env` file in the root directory:
   ```ini
   API_ID=your_api_id
   API_HASH=your_api_hash
   ```
   (Get these from [my.telegram.org](https://my.telegram.org))

4. **Run the Bot**:
   ```bash
   python bot.py
   ```

## Usage

- Add the bot to your channel as an Admin.
- Send `/monitor` in the channel (or `/monitor <link>`).
- Use the dashboard buttons to export member lists.

## Security

- Never commit your `.env` file or session files.
- The `.gitignore` is pre-configured to exclude sensitive data.
