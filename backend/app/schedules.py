"""
Scheduled reports — persistent SOQL queries that run on a cron-like schedule.

Storage: {data_dir}/users/{username}/schedules.json
Runs:    {data_dir}/users/{username}/reports/{schedule_id}/{timestamp}.csv + .json

Schedule shape:
    {
        "id": "sched_...",
        "username": "alice",
        "name": "Weekly student count",
        "question": "How many students are in market?",
        "soql": "SELECT COUNT() FROM Student__c WHERE ...",
        "frequency": "daily" | "weekly" | "monthly",
        "time": "09:00",
        "weekday": 0-6,          # weekly only (0=Mon)
        "day_of_month": 1-28,    # monthly only
        "recipients": ["a@b.com", ...],
        "created_at": iso,
        "last_run": iso | null,
        "last_status": "ok" | "error" | null,
        "last_error": str | null,
        "last_row_count": int | null,
        "next_run": iso,
        "enabled": true,
    }

Execution: a background thread ticks every ~60s, runs anything whose
next_run has passed, saves CSV + metadata, and advances next_run.
Email delivery is stubbed — results are saved to disk and recipients are
recorded. Wiring a real SMTP/Gmail sender is a separate step.
"""
import asyncio, csv, json, logging, re, threading, time, uuid
from datetime import datetime, timedelta
from pathlib import Path
from app.config import settings
from app.salesforce.soql_executor import execute_soql

logger = logging.getLogger(__name__)
_lock = threading.Lock()
_stop_event = threading.Event()
_ticker_thread: threading.Thread | None = None


# ── Paths ──────────────────────────────────────────────

def _user_dir(username: str) -> Path:
    d = Path(settings.data_dir) / "users" / username
    d.mkdir(parents=True, exist_ok=True)
    return d


def _schedules_file(username: str) -> Path:
    return _user_dir(username) / "schedules.json"


def _reports_dir(username: str, schedule_id: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_\-]", "_", schedule_id)
    d = _user_dir(username) / "reports" / safe
    d.mkdir(parents=True, exist_ok=True)
    return d


# ── Storage ────────────────────────────────────────────

def _load(username: str) -> list[dict]:
    p = _schedules_file(username)
    if not p.exists():
        return []
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load schedules for {username}: {e}")
        return []


def _save(username: str, items: list[dict]) -> None:
    with _lock:
        p = _schedules_file(username)
        tmp = p.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2, default=str)
        tmp.replace(p)


def _all_usernames() -> list[str]:
    root = Path(settings.data_dir) / "users"
    if not root.exists():
        return []
    return [p.name for p in root.iterdir() if p.is_dir()]


# ── Schedule math ──────────────────────────────────────

def compute_next_run(
    frequency: str,
    time_str: str,
    weekday: int | None = None,
    day_of_month: int | None = None,
    after: datetime | None = None,
) -> datetime:
    after = after or datetime.now()
    hour, minute = map(int, time_str.split(":"))
    candidate = after.replace(hour=hour, minute=minute, second=0, microsecond=0)

    if frequency == "daily":
        if candidate <= after:
            candidate += timedelta(days=1)
        return candidate

    if frequency == "weekly":
        wd = (weekday or 0) % 7
        days_ahead = (wd - candidate.weekday()) % 7
        candidate += timedelta(days=days_ahead)
        if candidate <= after:
            candidate += timedelta(days=7)
        return candidate

    if frequency == "monthly":
        dom = min(max(day_of_month or 1, 1), 28)
        try:
            candidate = candidate.replace(day=dom)
        except ValueError:
            candidate = candidate.replace(day=dom)
        if candidate <= after:
            # Next month
            year = candidate.year + (1 if candidate.month == 12 else 0)
            month = 1 if candidate.month == 12 else candidate.month + 1
            candidate = candidate.replace(year=year, month=month, day=dom)
        return candidate

    # Fallback: daily
    if candidate <= after:
        candidate += timedelta(days=1)
    return candidate


# ── CRUD ───────────────────────────────────────────────

def list_schedules(username: str) -> list[dict]:
    return _load(username)


def create_schedule(username: str, payload: dict) -> dict:
    items = _load(username)
    now = datetime.now()
    sid = f"sched_{uuid.uuid4().hex[:10]}"
    rec = {
        "id": sid,
        "username": username,
        "name": payload.get("name") or (payload.get("question") or "Scheduled report")[:60],
        "question": payload.get("question") or "",
        "soql": payload.get("soql") or "",
        "frequency": payload.get("frequency", "daily"),
        "time": payload.get("time", "09:00"),
        "weekday": payload.get("weekday"),
        "day_of_month": payload.get("day_of_month"),
        "recipients": payload.get("recipients") or [],
        "enabled": True,
        "created_at": now.isoformat(),
        "last_run": None,
        "last_status": None,
        "last_error": None,
        "last_row_count": None,
    }
    rec["next_run"] = compute_next_run(
        rec["frequency"], rec["time"], rec.get("weekday"), rec.get("day_of_month"), after=now
    ).isoformat()
    items.append(rec)
    _save(username, items)
    return rec


def delete_schedule(username: str, schedule_id: str) -> bool:
    items = _load(username)
    new_items = [s for s in items if s.get("id") != schedule_id]
    if len(new_items) == len(items):
        return False
    _save(username, new_items)
    return True


def update_schedule(username: str, schedule_id: str, patch: dict) -> dict | None:
    items = _load(username)
    for s in items:
        if s.get("id") == schedule_id:
            s.update({k: v for k, v in patch.items() if k in {
                "name", "question", "soql", "frequency", "time", "weekday",
                "day_of_month", "recipients", "enabled",
            }})
            s["next_run"] = compute_next_run(
                s["frequency"], s["time"], s.get("weekday"), s.get("day_of_month")
            ).isoformat()
            _save(username, items)
            return s
    return None


def list_runs(username: str, schedule_id: str, limit: int = 20) -> list[dict]:
    d = _reports_dir(username, schedule_id)
    runs = []
    for meta in sorted(d.glob("*.json"), reverse=True)[:limit]:
        try:
            with open(meta, encoding="utf-8") as f:
                runs.append(json.load(f))
        except Exception:
            pass
    return runs


# ── Runner ─────────────────────────────────────────────

async def _run_schedule(username: str, schedule: dict) -> dict:
    sid = schedule["id"]
    ts = datetime.now()
    ts_str = ts.strftime("%Y%m%d_%H%M%S")
    out_dir = _reports_dir(username, sid)
    csv_path = out_dir / f"{ts_str}.csv"
    meta_path = out_dir / f"{ts_str}.json"

    meta = {
        "schedule_id": sid,
        "ran_at": ts.isoformat(),
        "soql": schedule.get("soql", ""),
        "recipients": schedule.get("recipients", []),
    }

    try:
        result = await execute_soql(schedule.get("soql", ""))
        if "error" in result:
            raise RuntimeError(result["error"])
        records = result.get("records", [])
        for r in records:
            r.pop("attributes", None)

        if records:
            headers = list(records[0].keys())
            with open(csv_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
                writer.writeheader()
                for r in records:
                    writer.writerow(r)
            meta["csv_file"] = csv_path.name

        meta["row_count"] = len(records)
        meta["total_size"] = result.get("totalSize", len(records))
        meta["status"] = "ok"

        if schedule.get("recipients"):
            # Real delivery is stubbed — just log it.
            logger.info(
                f"[schedule {sid}] would email {len(records)} rows to "
                f"{', '.join(schedule['recipients'])}"
            )
    except Exception as e:
        meta["status"] = "error"
        meta["error"] = str(e)
        logger.error(f"[schedule {sid}] failed: {e}")

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2, default=str)
    return meta


def _advance(schedule: dict, meta: dict) -> None:
    schedule["last_run"] = meta["ran_at"]
    schedule["last_status"] = meta.get("status")
    schedule["last_error"] = meta.get("error")
    schedule["last_row_count"] = meta.get("row_count")
    schedule["next_run"] = compute_next_run(
        schedule["frequency"],
        schedule["time"],
        schedule.get("weekday"),
        schedule.get("day_of_month"),
        after=datetime.now(),
    ).isoformat()


async def _tick_once() -> None:
    now = datetime.now()
    for username in _all_usernames():
        items = _load(username)
        changed = False
        for s in items:
            if not s.get("enabled", True):
                continue
            nr = s.get("next_run")
            if not nr:
                continue
            try:
                nr_dt = datetime.fromisoformat(nr)
            except ValueError:
                continue
            if nr_dt <= now:
                meta = await _run_schedule(username, s)
                _advance(s, meta)
                changed = True
        if changed:
            _save(username, items)


def _runner_loop() -> None:
    while not _stop_event.is_set():
        try:
            asyncio.run(_tick_once())
        except Exception as e:
            logger.error(f"Schedule tick failed: {e}")
        _stop_event.wait(60)


def start_runner() -> None:
    global _ticker_thread
    if _ticker_thread and _ticker_thread.is_alive():
        return
    _stop_event.clear()
    _ticker_thread = threading.Thread(target=_runner_loop, daemon=True, name="schedule-runner")
    _ticker_thread.start()
    logger.info("Schedule runner started (60s tick)")


def stop_runner() -> None:
    _stop_event.set()


async def run_schedule_now(username: str, schedule_id: str) -> dict | None:
    items = _load(username)
    for s in items:
        if s.get("id") == schedule_id:
            meta = await _run_schedule(username, s)
            _advance(s, meta)
            _save(username, items)
            return meta
    return None
