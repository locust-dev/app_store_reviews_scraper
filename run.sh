#!/bin/zsh
set -euo pipefail
setopt NULL_GLOB

SCRIPT_DIR=$(cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR="$SCRIPT_DIR"
OUT_DIR="$ROOT_DIR/output"
PID_FILE="$SCRIPT_DIR/app.pid"
LOG_FILE="$OUT_DIR/parallel_fetch.log"

# Load config
CONFIG_FILE="$ROOT_DIR/config.env"
if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Config not found: $CONFIG_FILE" >&2
  exit 1
fi
set -o allexport
source "$CONFIG_FILE"
set +o allexport

# Санитизация APP_ID: оставить только цифры
APP_ID=${APP_ID:-0000000000}
APP_ID=$(echo -n "$APP_ID" | sed -E 's/[^0-9]+//g')

# Страны: поддержка csv без пробелов, например ru,kz,pl. Также поддержка COUNTRY=all из файла ALL_COUNTRY_CODES
COUNTRY=${COUNTRY:-us}
COUNTRY_LIST=""
if [[ "$COUNTRY" == "all" || "$COUNTRY" == "ALL" ]]; then
  CODES_FILE="$ROOT_DIR/ALL_COUNTRY_CODES.txt"
  if [[ -f "$CODES_FILE" ]]; then
    COUNTRY_LIST=$(cat "$CODES_FILE")
  else
    echo "Файл со всеми кодами не найден: $CODES_FILE" >&2
    exit 1
  fi
else
  COUNTRY_LIST=$(echo -n "$COUNTRY" | tr -d ' ')
fi

# Безопасное имя приложения для файлов
SAFE_APP_NAME=$(echo -n "${APP_NAME:-app}" | sed -E 's/[^A-Za-z0-9_-]+/-/g')
APP_NAME=${APP_NAME:-example}
APP_ID=${APP_ID:-0000000000}
WORKERS=${WORKERS:-4}
MAX_REVIEWS=${MAX_REVIEWS:-10000}
CHECKPOINT_EVERY=${CHECKPOINT_EVERY:-200}

mkdir -p "$OUT_DIR"
ERROR_LOG_FILE="$OUT_DIR/errors.log"
rm -f "$ERROR_LOG_FILE" 2>/dev/null || true

# Create venv if missing
if [[ ! -d "$ROOT_DIR/.venv" ]]; then
  python3 -m venv "$ROOT_DIR/.venv"
fi
source "$ROOT_DIR/.venv/bin/activate"
python -m pip install --upgrade pip >/dev/null
# Фиксируем совместимую версию urllib3 (<2), затем ставим остальные пакеты
pip install --disable-pip-version-check --no-input 'urllib3<2' requests tqdm >/dev/null

# Run fetcher по всем странам из списка
export PYTHONPATH="$ROOT_DIR/src"
for C in $(echo "$COUNTRY_LIST" | tr ',' ' '); do
  C_CLEAN=${C//$'\r'/}
  C_CLEAN=${C_CLEAN//$'\n'/}
  if [[ -z "$C_CLEAN" ]]; then
    continue
  fi
  echo "=== Запуск для страны: $C_CLEAN ==="
  ERROR_LOG_FILE="$ERROR_LOG_FILE" python "$ROOT_DIR/parallel_fetch.py" \
    --country "$C_CLEAN" \
    --app-name "$APP_NAME" \
    --app-id "$APP_ID" \
    --workers "$WORKERS" \
    --max-reviews "$MAX_REVIEWS" \
    --checkpoint-every "$CHECKPOINT_EVERY"
done

# Если передано несколько локалей (через запятую) или выбран режим all, создадим объединённый файл
if [[ "$COUNTRY_LIST" == *,* || "$COUNTRY" == "all" || "$COUNTRY" == "ALL" ]]; then
  GREEN='\033[32m'
  YELLOW='\033[33m'
  RESET='\033[0m'
  echo "${YELLOW}=== Агрегирую результаты для: $COUNTRY_LIST ===${RESET}"
  FILES_LIST=""
  AGG_COUNTRIES=()
  for C in $(echo "$COUNTRY_LIST" | tr ',' ' '); do
    C_CLEAN=${C//$'\r'/}
    C_CLEAN=${C_CLEAN//$'\n'/}
    if [[ -z "$C_CLEAN" ]]; then
      continue
    fi
    matches=( "$OUT_DIR"/reviews_"$SAFE_APP_NAME"_"$C_CLEAN"_*.json )
    if (( ${#matches[@]} > 0 )); then
      latest=$(ls -t ${matches[@]} | head -n 1)
      if [[ -f "$latest" ]]; then
        # Добавляем путь + реальный перевод строки; важно, чтобы $latest подставился
        FILES_LIST+="${latest}"$'\n'
        AGG_COUNTRIES+=("$C_CLEAN")
      fi
    fi
  done

  if [[ -n "$FILES_LIST" ]]; then
    if (( ${#AGG_COUNTRIES[@]} > 0 )); then
      COUNTRY_LIST_FOR_FILE=$(IFS=,; echo "${AGG_COUNTRIES[*]}")
    else
      COUNTRY_LIST_FOR_FILE=""
    fi
    COMBINED_PREFIX="reviews_${SAFE_APP_NAME}_${COUNTRY_LIST_FOR_FILE}_"
    TMP_OUT="$OUT_DIR/${COMBINED_PREFIX}tmp.json"
    export OUT="$TMP_OUT"
    export FILES="$FILES_LIST"
    COMBINED_COUNT=$(python3 - <<'PY'
import json, os, sys
files = [p for p in os.environ.get('FILES','').split('\n') if p.strip()]
by_id = {}
for path in files:
    try:
        with open(path,'r') as f:
            for r in json.load(f):
                rid = r.get('id')
                if rid and rid not in by_id:
                    by_id[rid] = r
    except Exception:
        pass
data = list(by_id.values())
out = os.environ['OUT']
with open(out,'w') as f:
    json.dump(data, f, ensure_ascii=False)
print(len(data))
PY
)
    if [[ -n "$COMBINED_COUNT" ]]; then
      mv -f "$TMP_OUT" "$OUT_DIR/${COMBINED_PREFIX}${COMBINED_COUNT}.json"
      echo "${GREEN}Создан объединённый файл: $OUT_DIR/${COMBINED_PREFIX}${COMBINED_COUNT}.json${RESET}"
    else
      rm -f "$TMP_OUT" 2>/dev/null || true
    fi
  else
    echo "Нет файлов для агрегации."
  fi
fi

# Итоговая сводка по ошибкам за все выгрузки
YELLOW='\033[33m'
RESET='\033[0m'
if [[ -f "$ERROR_LOG_FILE" ]]; then
  TOTAL_ERR=$(wc -l < "$ERROR_LOG_FILE" | tr -d ' ')
else
  TOTAL_ERR=0
fi
echo "${YELLOW}Всего ошибок за время выгрузки: ${TOTAL_ERR}${RESET}"

