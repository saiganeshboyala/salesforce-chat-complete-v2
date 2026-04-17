"""Per-user custom dashboard configs."""
import json
import logging
from pathlib import Path
from threading import Lock

from app.config import settings

logger = logging.getLogger(__name__)

_lock = Lock()
DEFAULT_CONFIG = {"widgets": []}


def _config_path(username: str) -> Path:
    d = Path(settings.data_dir) / "users" / username
    d.mkdir(parents=True, exist_ok=True)
    return d / "dashboard_config.json"


def load_config(username: str) -> dict:
    if not username:
        return DEFAULT_CONFIG
    p = _config_path(username)
    if not p.exists():
        return DEFAULT_CONFIG
    try:
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "widgets" not in data:
            return DEFAULT_CONFIG
        return data
    except Exception as e:
        logger.warning(f"Failed to load dashboard config for {username}: {e}")
        return DEFAULT_CONFIG


def save_config(username: str, config: dict) -> dict:
    if not username:
        raise ValueError("username required")
    widgets = config.get("widgets") if isinstance(config, dict) else None
    if not isinstance(widgets, list):
        raise ValueError("config.widgets must be a list")
    clean = []
    for w in widgets:
        if not isinstance(w, dict):
            continue
        clean.append({
            "id": str(w.get("id") or "")[:64],
            "type": str(w.get("type") or "metric")[:16],
            "title": str(w.get("title") or "")[:100],
            "soql": str(w.get("soql") or "")[:2000],
            "chartType": str(w.get("chartType") or "auto")[:16],
            "position": int(w.get("position") or 0),
        })
    out = {"widgets": clean}
    with _lock:
        p = _config_path(username)
        tmp = p.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        tmp.replace(p)
    return out
