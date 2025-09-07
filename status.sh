#!/bin/zsh
set -euo pipefail
setopt NULL_GLOB

SCRIPT_DIR=$(cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR="$SCRIPT_DIR"
OUT_DIR="$ROOT_DIR/output"

# Load config if present
CONFIG_FILE="$ROOT_DIR/config.env"
if [[ -f "$CONFIG_FILE" ]]; then
  set -o allexport
  source "$CONFIG_FILE"
  set +o allexport
fi

APP_ID_VAL=${APP_ID:-6464476667}
APP_ID_VAL=$(echo -n "$APP_ID_VAL" | sed -E 's/[^0-9]+//g')
SAFE_APP_NAME=$(echo -n "${APP_NAME:-app}" | sed -E 's/[^A-Za-z0-9_-]+/-/g')

COUNTRY_VAL=${COUNTRY:-us}
COUNTRY_LIST=""
if [[ "$COUNTRY_VAL" == "all" || "$COUNTRY_VAL" == "ALL" ]]; then
  CODES_FILE="$ROOT_DIR/ALL_COUNTRY_CODES.txt"
  if [[ -f "$CODES_FILE" ]]; then
    COUNTRY_LIST=$(cat "$CODES_FILE")
  else
    COUNTRY_LIST="us"
  fi
else
  COUNTRY_LIST=$(echo -n "$COUNTRY_VAL" | tr -d ' ')
fi
echo "Status: checking countries: $COUNTRY_LIST"

for C in $(echo "$COUNTRY_LIST" | tr ',' ' '); do
  matches=( "$OUT_DIR"/reviews_"$SAFE_APP_NAME"_"$C"_*.json )
  OUT_FILE_FINAL=""
  if (( ${#matches[@]} > 0 )); then
    OUT_FILE_FINAL=$(ls -t ${matches[@]} | head -n 1)
  fi

  OUT_FILE_WORKING="$OUT_DIR/${APP_ID_VAL}_reviews.json"

  TARGET_FILE=""
  if [[ -n "$OUT_FILE_FINAL" ]]; then
    TARGET_FILE="$OUT_FILE_FINAL"
  elif [[ -f "$OUT_FILE_WORKING" ]]; then
    TARGET_FILE="$OUT_FILE_WORKING"
  fi

  if [[ -n "$TARGET_FILE" && -f "$TARGET_FILE" ]]; then
    OUT_FILE_PATH="$TARGET_FILE" python3 - <<'PY'
import json, os
path = os.environ.get('OUT_FILE_PATH')
with open(path,'r') as f:
    data = json.load(f)
print(f"Current reviews count: {len(data)} (file: {path})")
PY
  else
    echo "No file found for $C"
  fi
done

