#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/walrus}"
BRANCH="${BRANCH:-main}"
SCREEN_NAME="${SCREEN_NAME:-walrus}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_PYTHON="$APP_DIR/venv/bin/python"

echo "==> Updating code"
cd "$APP_DIR"
git pull --ff-only origin "$BRANCH"

if [ ! -x "$VENV_PYTHON" ]; then
  echo "==> Creating virtualenv"
  "$PYTHON_BIN" -m venv "$APP_DIR/venv" || {
    echo "Could not create venv. On Ubuntu, install it with: apt update && apt install -y python3-venv"
    exit 1
  }
fi

echo "==> Installing dependencies"
"$VENV_PYTHON" -m pip install -r requirements.txt

if ! command -v screen >/dev/null 2>&1; then
  echo "screen is not installed. Install it with: apt update && apt install -y screen"
  exit 1
fi

echo "==> Stopping old screen sessions"
while read -r session; do
  [ -n "$session" ] || continue
  screen -S "$session" -X quit || true
done < <(screen -ls | awk -v name="$SCREEN_NAME" '$0 ~ name {print $1}' || true)

echo "==> Starting app in screen"
screen -dmS "$SCREEN_NAME" bash -lc "cd '$APP_DIR' && exec '$VENV_PYTHON' main.py"

echo "==> Done"
echo "Check sessions with: screen -ls"
echo "Attach with: screen -r $SCREEN_NAME"
