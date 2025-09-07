#!/bin/zsh
set -euo pipefail

PID=${1:-}
if [[ -z "$PID" ]]; then
  echo "Укажите PID для остановки: stop.sh <PID>" >&2
  exit 1
fi

if ps -p "$PID" >/dev/null 2>&1; then
  kill -15 "$PID" || true
  sleep 1
  if ps -p "$PID" >/dev/null 2>&1; then
    kill -9 "$PID" || true
  fi
  echo "Остановлено PID $PID"
else
  echo "Процесс не найден: $PID"
fi

