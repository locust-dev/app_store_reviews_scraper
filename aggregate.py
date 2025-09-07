import os
import re
import json
from glob import glob


def main() -> None:
    project_root = os.path.abspath(os.path.dirname(__file__))
    out_dir = os.path.join(project_root, "output")
    cfg_path = os.path.join(project_root, "config.env")

    app_name = None
    try:
        with open(cfg_path, "r") as f:
            for line in f:
                if line.startswith("APP_NAME="):
                    app_name = line.strip().split("=", 1)[1]
                    break
    except Exception:
        pass

    safe_app = re.sub(r"[^A-Za-z0-9_-]+", "-", app_name) if app_name else "app"

    files = glob(os.path.join(out_dir, f"reviews_{safe_app}_*_*.json"))
    if not files:
        print("No per-country files found")
        return

    by_id: dict[str, dict] = {}
    countries: set[str] = set()

    for path in files:
        base = os.path.basename(path)
        m = re.match(rf"^reviews_{re.escape(safe_app)}_(.+?)_(\d+)\.json$", base)
        if not m:
            continue
        country = m.group(1)
        # пропускаем уже объединённые файлы (у которых в имени список стран через запятую)
        if "," in country:
            continue
        try:
            with open(path, "r") as f:
                data = json.load(f)
            for r in data:
                rid = r.get("id")
                if rid and rid not in by_id:
                    by_id[rid] = r
            countries.add(country)
        except Exception:
            # тихо пропускаем битые файлы
            pass

    all_reviews = list(by_id.values())
    if not countries or not all_reviews:
        print("Nothing to aggregate")
        return

    countries_list = ",".join(sorted(countries))
    prefix = f"reviews_{safe_app}_{countries_list}_"
    tmp_path = os.path.join(out_dir, prefix + "tmp.json")
    with open(tmp_path, "w") as f:
        json.dump(all_reviews, f, ensure_ascii=False)
    final_path = os.path.join(out_dir, prefix + f"{len(all_reviews)}.json")
    os.replace(tmp_path, final_path)
    print(final_path)


if __name__ == "__main__":
    main()


