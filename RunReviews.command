#!/bin/zsh
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR="$SCRIPT_DIR"

cd "$ROOT_DIR"

echo "Старт загрузки..."
"$ROOT_DIR/run.sh" &
CHILD_PID=$!
PGID=$(ps -o pgid= "$CHILD_PID" | tr -d ' ')
echo "PID: $CHILD_PID  PGID: $PGID"
echo "Нажмите любую клавишу, чтобы остановить, или дождитесь завершения."

trap 'echo "\nОстановка..."; kill -15 -$PGID 2>/dev/null || true; sleep 1; kill -9 -$PGID 2>/dev/null || true; wait $CHILD_PID 2>/dev/null || true' INT TERM

while ps -p "$CHILD_PID" >/dev/null 2>&1; do
  if read -k 1 -s -t 1 _; then
    echo "\nОстановка по запросу пользователя..."
    kill -15 -$PGID 2>/dev/null || true
    sleep 1
    if ps -p "$CHILD_PID" >/dev/null 2>&1; then
      kill -9 -$PGID 2>/dev/null || true
    fi
    break
  fi
done

wait $CHILD_PID 2>/dev/null || true

echo "\nГотово. Нажмите любую клавишу, чтобы закрыть окно..."
read -k 1 -s _

# Попробуем автоматически закрыть окно Terminal после нажатия клавиши
if [[ "${TERM_PROGRAM:-}" == "Apple_Terminal" ]]; then
  osascript -e 'tell application "Terminal" to if (count of windows) > 0 then close front window' >/dev/null 2>&1 || true
fi

