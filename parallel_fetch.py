import argparse
import json
import re
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from threading import Lock


def ensure_src_on_path() -> None:
    project_root = os.path.abspath(os.path.dirname(__file__))
    src_path = os.path.join(project_root, "src")
    if src_path not in sys.path:
        sys.path.insert(0, src_path)


ensure_src_on_path()

from apple_app_reviews_scraper import get_token, fetch_reviews  # noqa: E402


DEFAULT_USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


@dataclass
class Args:
    country: str
    app_name: str
    app_id: str
    workers: int
    max_reviews: int
    checkpoint_every: int


class TokenManager:
    def __init__(self, country: str, app_name: str, app_id: str, user_agents: list[str]):
        self.country = country
        self.app_name = app_name
        self.app_id = app_id
        self.user_agents = user_agents
        self._token = None
        self._lock = Lock()

    @property
    def token(self) -> str:
        if self._token is None:
            with self._lock:
                if self._token is None:
                    self._token = get_token(self.country, self.app_name, self.app_id, self.user_agents)
        return self._token

    def refresh(self) -> str:
        with self._lock:
            self._token = get_token(self.country, self.app_name, self.app_id, self.user_agents)
            return self._token


def log(message: str) -> None:
    # Печатаем только финальную строку завершения
    if isinstance(message, str) and message.startswith("Finished:"):
        ts = datetime.now().isoformat(timespec="seconds")
        print(f"[{ts}] {message}")


def read_existing(out_file: str) -> list[dict]:
    if not os.path.exists(out_file):
        return []
    try:
        with open(out_file, "r") as f:
            return json.load(f)
    except Exception:
        return []


def compute_start_offset(existing: list[dict]) -> int:
    offsets = [int(r.get("offset")) for r in existing if r.get("offset") and str(r.get("offset")).isdigit()]
    if not offsets:
        return 1
    return max(offsets)


def write_checkpoint(out_file: str, by_id: dict[str, dict]) -> None:
    with open(out_file, "w") as f:
        json.dump(list(by_id.values()), f, ensure_ascii=False)


def fetch_page_at_offset(
    offset_value: int,
    args: Args,
    token_mgr: TokenManager,
    user_agents: list[str],
):
    # First attempt with current token
    reviews, next_offset, status = fetch_reviews(
        args.country,
        args.app_name,
        args.app_id,
        user_agents,
        token_mgr.token,
        str(offset_value),
    )

    # Handle unauthorized by refreshing token once
    if status == 401:
        log(f"Offset {offset_value}: 401 Unauthorized -> refreshing token")
        token_mgr.refresh()
        reviews, next_offset, status = fetch_reviews(
            args.country,
            args.app_name,
            args.app_id,
            user_agents,
            token_mgr.token,
            str(offset_value),
        )

    return reviews, next_offset, status


def main() -> None:
    parser = argparse.ArgumentParser(description="Parallel App Store reviews fetcher")
    parser.add_argument("--country", required=True)
    parser.add_argument("--app-name", required=True)
    parser.add_argument("--app-id", required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--max-reviews", type=int, default=1000000)
    parser.add_argument("--checkpoint-every", type=int, default=200)
    args_ns = parser.parse_args()

    args = Args(
        country=args_ns.country,
        app_name=args_ns["app_name"] if isinstance(args_ns, dict) else getattr(args_ns, "app_name"),
        app_id=re.sub(r"[^0-9]+", "", args_ns.app_id),
        workers=args_ns.workers,
        max_reviews=args_ns.max_reviews,
        checkpoint_every=args_ns.checkpoint_every,
    )

    project_root = os.path.abspath(os.path.dirname(__file__))
    out_dir = os.path.join(project_root, "output")
    os.makedirs(out_dir, exist_ok=True)
    # Final artifact name: reviews_[appname]_[country]_[count].json
    safe_app_name = re.sub(r"[^A-Za-z0-9_-]+", "-", args.app_name)

    # Перед стартом удалим любые старые итоговые файлы для данной пары (app/country),
    # чтобы оставался только один актуальный файл с количеством.
    final_prefix = f"reviews_{safe_app_name}_{args.country}_"
    for f in list(os.listdir(out_dir)):
        if f.startswith(final_prefix) and f.endswith(".json"):
            try:
                os.remove(os.path.join(out_dir, f))
            except Exception:
                pass
    existing_file = None
    existing = []
    by_id: dict[str, dict] = {r.get("id"): r for r in existing if isinstance(r, dict) and r.get("id")}
    log(f"Loaded existing: {len(by_id)}")

    start_offset = 1
    log(f"Start offset: {start_offset}")

    token_mgr = TokenManager(args.country, args.app_name, args.app_id, DEFAULT_USER_AGENTS)

    start_ts = time.time()
    max_workers_used = 0
    scheduled_pages_total = 0

    # Producer pointer for next offsets to schedule
    next_offset_to_schedule = start_offset
    encountered_end = False

    added_since_checkpoint = 0

    def schedule(executor, count: int):
        nonlocal next_offset_to_schedule
        nonlocal scheduled_pages_total
        futures = []
        for _ in range(count):
            off = next_offset_to_schedule
            next_offset_to_schedule += 20
            futures.append(
                executor.submit(
                    fetch_page_at_offset,
                    off,
                    args,
                    token_mgr,
                    DEFAULT_USER_AGENTS,
                )
            )
            scheduled_pages_total += 1
        return futures

    def remaining_reviews() -> int:
        return max(0, args.max_reviews - len(by_id))

    def effective_workers() -> int:
        # Каждая страница = 20 отзывов
        pages_needed = (remaining_reviews() + 19) // 20
        return max(1, min(args.workers, pages_needed))

    # Выполняем первый запрос синхронно (offset=1), чтобы быстро понять, есть ли данные
    reviews, next_offset, status = fetch_page_at_offset(
        start_offset,
        args,
        token_mgr,
        DEFAULT_USER_AGENTS,
    )

    if status == 404:
        encountered_end = True

    batch_added = 0
    for r in reviews:
        rid = r.get("id")
        if rid and rid not in by_id:
            by_id[rid] = r
            batch_added += 1
    added_since_checkpoint += batch_added
    scheduled_pages_total += 1

    # После первого синхронного вызова, следующий оффсет для планирования
    next_offset_to_schedule = start_offset + 20

    # Если после первой страницы нет продолжения — завершаем без запуска пула
    if next_offset is None or encountered_end or len(by_id) >= args.max_reviews:
        futures = []
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            # Начальная волна с ограничением по оставшемуся лимиту
            initial_workers = effective_workers()
            log(f"Using up to {initial_workers} workers (requested {args.workers})")
            if initial_workers > max_workers_used:
                max_workers_used = initial_workers
            futures = schedule(executor, initial_workers)

            while futures:
                for future in as_completed(futures):
                    reviews, next_offset, status = future.result()

                    if status == 404:
                        encountered_end = True

                    batch_added = 0
                    for r in reviews:
                        rid = r.get("id")
                        if rid and rid not in by_id:
                            by_id[rid] = r
                            batch_added += 1

                    added_since_checkpoint += batch_added
                    log(
                        f"Batch added={batch_added} total={len(by_id)} next_offset={next_offset} status={status}"
                    )

                    if added_since_checkpoint >= args.checkpoint_every or next_offset is None:
                        # Single-file mode: не пишем промежуточные файлы, только логируем прогресс
                        log(f"Checkpoint: total={len(by_id)}")
                        added_since_checkpoint = 0

                    if len(by_id) >= args.max_reviews:
                        encountered_end = True

                # Drain completed futures list
                futures = []

                if not encountered_end:
                    next_workers = effective_workers()
                    if next_workers > 0:
                        if next_workers > max_workers_used:
                            max_workers_used = next_workers
                        futures.extend(schedule(executor, next_workers))

    # Finalize
    total = len(by_id)
    final_file = os.path.join(out_dir, f"reviews_{safe_app_name}_{args.country}_{total}.json")
    # Удалим любые файлы этой пары (на случай предыдущих запусков)
    for f in list(os.listdir(out_dir)):
        if f.startswith(final_prefix) and f.endswith(".json"):
            try:
                os.remove(os.path.join(out_dir, f))
            except Exception:
                pass
    # Пишем файл только если есть отзывы
    if total > 0:
        with open(final_file, "w") as f:
            json.dump(list(by_id.values()), f, ensure_ascii=False)
    # Также удалим старый рабочий файл, если он остался от прошлых версий
    legacy_working = os.path.join(out_dir, f"{args.app_id}_reviews.json")
    if os.path.exists(legacy_working):
        try:
            os.remove(legacy_working)
        except Exception:
            pass
    duration_sec = int(time.time() - start_ts)
    log(f"Finished: total={total} file={final_file}")
    GREEN = "\033[32m"
    RESET = "\033[0m"
    if total > 0:
        print(
            GREEN
            + (
                f"Завершено. Запрошено {args.max_reviews}, скачано {total}, "
                f"макс. воркеров {max_workers_used}, страниц {scheduled_pages_total}, "
                f"время {duration_sec} c. Файл: {final_file}"
            )
            + RESET
        )
    else:
        print(
            GREEN
            + (
                f"Завершено. Запрошено {args.max_reviews}, скачано 0. Файл не сохранен."
            )
            + RESET
        )


if __name__ == "__main__":
    main()


