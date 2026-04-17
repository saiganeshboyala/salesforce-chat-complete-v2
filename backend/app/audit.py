"""Append-only audit log with rotation and filtering."""
import json
import logging
from datetime import datetime
from pathlib import Path
from threading import Lock

from app.config import settings

logger = logging.getLogger(__name__)

_MAX_ENTRIES = 10000
_lock = Lock()


def _log_path() -> Path:
    d = Path(settings.data_dir)
    d.mkdir(parents=True, exist_ok=True)
    return d / "audit_log.json"


def _load() -> list[dict]:
    p = _log_path()
    if not p.exists():
        return []
    try:
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.warning(f"Failed to load audit log: {e}")
        return []


def _save(entries: list[dict]) -> None:
    p = _log_path()
    tmp = p.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, default=str)
    tmp.replace(p)


def log_action(username: str | None, action: str, details: dict | None = None, ip_address: str | None = None) -> None:
    entry = {
        "timestamp": datetime.now().isoformat(),
        "username": username or "anonymous",
        "action": action,
        "details": details or {},
        "ip_address": ip_address or "",
    }
    try:
        with _lock:
            entries = _load()
            entries.append(entry)
            if len(entries) > _MAX_ENTRIES:
                entries = entries[-_MAX_ENTRIES:]
            _save(entries)
    except Exception as e:
        logger.warning(f"Audit log write failed: {e}")


def query_log(
    user: str | None = None,
    action: str | None = None,
    start: str | None = None,
    end: str | None = None,
    page: int = 1,
    page_size: int = 50,
) -> dict:
    entries = _load()
    entries.reverse()  # newest first

    if user:
        entries = [e for e in entries if e.get("username") == user]
    if action:
        entries = [e for e in entries if e.get("action") == action]
    if start:
        entries = [e for e in entries if e.get("timestamp", "") >= start]
    if end:
        entries = [e for e in entries if e.get("timestamp", "") <= end]

    total = len(entries)
    page = max(1, page)
    page_size = max(1, min(page_size, 500))
    start_idx = (page - 1) * page_size
    chunk = entries[start_idx : start_idx + page_size]

    users = sorted({e.get("username", "") for e in _load() if e.get("username")})
    actions = sorted({e.get("action", "") for e in _load() if e.get("action")})

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "entries": chunk,
        "users": users,
        "actions": actions,
    }
