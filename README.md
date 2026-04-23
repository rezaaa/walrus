# Tele2Rub

Send a video to a Telegram bot and have it uploaded to your Rubika Saved Messages.

## Disclaimer

This project is shared for research, learning, and personal experimentation.
Do not use it for abuse, spam, unauthorized access, privacy violations, or any harmful or unlawful purpose.
You are responsible for using it in a way that respects platform rules, local laws, and other people's rights.

Tele2Rub uses a simple queue-based flow:

1. The Telegram bot receives a video in a private chat.
2. The file is downloaded into `downloads/`.
3. A task is added to `queue/tasks.jsonl`.
4. The Rubika worker uploads the file.
5. The Telegram status message is updated during the whole transfer.

## Features

- Accepts video messages in Telegram private chat
- Keeps a local upload queue to avoid overlapping jobs
- Shows live download and upload progress
- Supports upload retries for temporary Rubika errors
- Supports canceling active or queued transfers
- Supports retrying failed transfers when the downloaded file still exists
- Provides quick action buttons for status, transfers, cleanup, cancel, and retry
- Uploads videos to Rubika Saved Messages with their original filename

## Bot Controls

Main menu buttons:

- `📊 Status`
- `📋 Transfers`
- `🧹 Cleanup`
- `🛑 Cancel`

Available commands:

- `/start` - open the main menu
- `/status` - show active downloads, active upload, queue, failed count, and local storage usage
- `/transfers` - show current downloads, current upload, queued items, and retryable failed transfers
- `/cleanup` - preview removable files in `downloads/`
- `/cleanup confirm` - delete safe cleanup candidates
- `/cancel` - show clickable cancel buttons for active jobs
- `/cancel <task_id>` - cancel a specific task
- `/retry <task_id>` - requeue a failed task if its local file still exists

Transfer message buttons:

- `🛑 Cancel` on active and queued transfers
- `🔁 Retry` on failed transfers

## Retry Policy

Rubika uploads retry automatically on temporary errors.

- Max attempts: `5`
- Base retry delay: `3` seconds
- Backoff: `3s`, `6s`, `9s`, `12s`, `15s`

If all retries fail, the task is written to `queue/failed.jsonl` and the local downloaded file is kept for inspection or retry.

## Cancellation

You can cancel a transfer in three ways:

- tap a `🛑 Cancel` button on the transfer message
- run `/cancel` and choose a task from the buttons
- run `/cancel <task_id>`

Behavior:

- Telegram download: stops as soon as possible
- Upload queue: removed immediately
- Rubika upload: stops at the next safe checkpoint

## Storage

Runtime files:

- `downloads/` - temporary downloaded videos
- `queue/tasks.jsonl` - pending jobs
- `queue/processing.json` - the job currently being uploaded
- `queue/failed.jsonl` - failed jobs log
- `queue/cancelled/` - cancellation markers

Cleanup behavior:

- successful upload: local file is deleted
- canceled task: local file is deleted
- failed upload: local file is kept

If `queue/processing.json` was emptied manually, use this valid default:

```json
{}
```

Or simply delete the file and let the app recreate it when needed.

## Requirements

- Python 3.9+
- Telegram `API_ID`
- Telegram `API_HASH`
- Telegram bot token
- Valid Rubika session support through `rubpy`

## Installation

```bash
git clone https://github.com/rezaaa/Tele2Rub.git
cd Tele2Rub
pip install -r requirements.txt
```

## Configuration

Create `.env` in the project root:

```env
API_ID=your_telegram_api_id
API_HASH=your_telegram_api_hash
BOT_TOKEN=your_telegram_bot_token
RUBIKA_SESSION=rubsession
```

Variables:

- `API_ID` - from https://my.telegram.org
- `API_HASH` - from https://my.telegram.org
- `BOT_TOKEN` - from BotFather
- `RUBIKA_SESSION` - session name or path used by `rubpy`

## First Run

On the first worker run, Rubika login may ask for:

1. Your phone number
2. The OTP code sent by Rubika

After that, the saved session is reused.

## Run

Start both processes with:

```bash
python3 main.py
```

This starts:

- `telebot.py` - Telegram receiver and downloader
- `rub.py` - Rubika upload worker

## Run with Screen

```bash
screen -S tele2rub
cd /opt/Tele2Rub
source venv/bin/activate
python main.py
```

Detach without stopping the app:

```text
Ctrl + A, then D
```

Useful commands:

- `screen -ls`
- `screen -r tele2rub` - attach to the running session
- `screen -S tele2rub -X quit`

If multiple old sessions exist:

```bash
for s in $(screen -ls | awk '/tele2rub/ {print $1}'); do
  screen -S "$s" -X quit
done
```

## Update on Server

Typical restart flow:

```bash
cd /opt/Tele2Rub
git pull origin main
source venv/bin/activate
pip install -r requirements.txt

for s in $(screen -ls | awk '/tele2rub/ {print $1}'); do
  screen -S "$s" -X quit
done

screen -dmS tele2rub bash -lc 'cd /opt/Tele2Rub && source venv/bin/activate && python main.py'
```

Then verify:

```bash
screen -ls
```

Optional update script:

```bash
#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/Tele2Rub"
BRANCH="main"
SCREEN_NAME="tele2rub"

echo "==> Updating code"
cd "$APP_DIR"
git pull --ff-only origin "$BRANCH"

echo "==> Installing dependencies"
"$APP_DIR/venv/bin/python" -m pip install -r requirements.txt

echo "==> Stopping old screen sessions"
for s in $(screen -ls | awk '/tele2rub/ {print $1}'); do
  screen -S "$s" -X quit || true
done

echo "==> Starting app in screen"
screen -dmS "$SCREEN_NAME" bash -lc "cd '$APP_DIR' && exec '$APP_DIR/venv/bin/python' main.py"

echo "==> Done"
echo "Check sessions with: screen -ls"
echo "Attach to session with: screen -r $SCREEN_NAME"
```

## Project Files

- `main.py` - starts both processes
- `telebot.py` - Telegram bot, downloads, queueing, status updates, buttons
- `rub.py` - Rubika uploader, retries, cleanup
- `task_store.py` - queue and task state helpers

## Troubleshooting

If the bot does not start:

- verify `.env`
- install dependencies
- confirm Telegram credentials are valid

If uploads fail:

- check the Rubika session
- review `queue/failed.jsonl`
- confirm the file still exists in `downloads/`
- check server memory and swap if the process was killed

## Notes

- README documents video support only.
- Rubika Saved Messages is shown in English here intentionally.
