"""Per-user alert rules that watch SQL query results against thresholds."""
import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from threading import Lock

from app.config import settings
from app.salesforce.soql_executor import execute_soql
from app.timezone import now_cst

logger = logging.getLogger(__name__)

_lock = Lock()

CONDITIONS = {"less_than", "greater_than", "equals", "changed"}
FREQUENCIES = {"hourly", "daily", "weekly"}


def _alerts_path(username: str) -> Path:
    d = Path(settings.data_dir) / "users" / username
    d.mkdir(parents=True, exist_ok=True)
    return d / "alerts.json"


def _history_path(username: str) -> Path:
    d = Path(settings.data_dir) / "users" / username
    d.mkdir(parents=True, exist_ok=True)
    return d / "alert_history.json"


def _load(path: Path, default):
    if not path.exists():
        return default
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load {path}: {e}")
        return default


def _atomic_write(path: Path, data) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def _clean_rule(r: dict) -> dict:
    cond = str(r.get("condition") or "greater_than")
    if cond not in CONDITIONS:
        cond = "greater_than"
    freq = str(r.get("frequency") or "daily")
    if freq not in FREQUENCIES:
        freq = "daily"
    try:
        threshold = float(r.get("threshold") or 0)
    except (TypeError, ValueError):
        threshold = 0.0
    return {
        "id": str(r.get("id") or uuid.uuid4().hex)[:64],
        "name": str(r.get("name") or "Untitled")[:120],
        "soql": str(r.get("soql") or "")[:2000],
        "condition": cond,
        "threshold": threshold,
        "frequency": freq,
        "enabled": bool(r.get("enabled", True)),
        "last_value": r.get("last_value"),
        "last_checked": r.get("last_checked"),
        "last_triggered": r.get("last_triggered"),
        "triggered": bool(r.get("triggered", False)),
        "created_at": r.get("created_at") or now_cst().isoformat(),
    }


def list_rules(username: str) -> list:
    if not username:
        return []
    data = _load(_alerts_path(username), {"rules": []})
    return data.get("rules") or []


def _save_rules(username: str, rules: list) -> None:
    with _lock:
        _atomic_write(_alerts_path(username), {"rules": rules})


def create_rule(username: str, payload: dict) -> dict:
    if not username:
        raise ValueError("username required")
    if not payload.get("soql"):
        raise ValueError("soql required")
    if not payload.get("name"):
        raise ValueError("name required")
    rule = _clean_rule(payload)
    rules = list_rules(username)
    rules.append(rule)
    _save_rules(username, rules)
    return rule


def update_rule(username: str, rule_id: str, patch: dict) -> dict:
    rules = list_rules(username)
    for i, r in enumerate(rules):
        if r.get("id") == rule_id:
            merged = {**r, **patch, "id": rule_id}
            rules[i] = _clean_rule(merged)
            _save_rules(username, rules)
            return rules[i]
    raise KeyError(rule_id)


def delete_rule(username: str, rule_id: str) -> None:
    rules = list_rules(username)
    rules = [r for r in rules if r.get("id") != rule_id]
    _save_rules(username, rules)


def _extract_value(result: dict) -> float:
    if "error" in result:
        return 0.0
    records = result.get("records") or []
    if not records:
        return float(result.get("totalSize", 0) or 0)
    r = records[0]
    for k, v in r.items():
        if k == "attributes":
            continue
        if isinstance(v, (int, float)):
            return float(v)
    return float(result.get("totalSize", len(records)))


def _evaluate(value: float, condition: str, threshold: float, last_value) -> bool:
    if condition == "greater_than":
        return value > threshold
    if condition == "less_than":
        return value < threshold
    if condition == "equals":
        return value == threshold
    if condition == "changed":
        return last_value is not None and float(last_value) != value
    return False


def _append_history(username: str, entry: dict) -> None:
    hp = _history_path(username)
    hist = _load(hp, {"entries": []})
    entries = hist.get("entries") or []
    entries.append(entry)
    if len(entries) > 500:
        entries = entries[-500:]
    with _lock:
        _atomic_write(hp, {"entries": entries})


def list_history(username: str) -> list:
    if not username:
        return []
    data = _load(_history_path(username), {"entries": []})
    return list(reversed(data.get("entries") or []))


async def check_rule(username: str, rule_id: str) -> dict:
    rules = list_rules(username)
    rule = next((r for r in rules if r.get("id") == rule_id), None)
    if not rule:
        raise KeyError(rule_id)
    return await _run_check(username, rule, rules)


async def check_all(username: str) -> list:
    rules = list_rules(username)
    out = []
    for rule in list(rules):
        if not rule.get("enabled"):
            continue
        try:
            out.append(await _run_check(username, rule, rules))
        except Exception as e:
            logger.warning(f"alert check failed for {rule.get('id')}: {e}")
    return out


async def _run_check(username: str, rule: dict, all_rules: list) -> dict:
    result = await execute_soql(rule["soql"])
    value = _extract_value(result)
    now = now_cst().isoformat()
    last_value = rule.get("last_value")
    triggered = _evaluate(value, rule["condition"], rule["threshold"], last_value)

    rule["last_value"] = value
    rule["last_checked"] = now
    rule["triggered"] = triggered
    if triggered:
        rule["last_triggered"] = now
        _append_history(username, {
            "timestamp": now,
            "rule_id": rule["id"],
            "rule_name": rule["name"],
            "value": value,
            "threshold": rule["threshold"],
            "condition": rule["condition"],
        })

    for i, r in enumerate(all_rules):
        if r.get("id") == rule["id"]:
            all_rules[i] = rule
            break
    _save_rules(username, all_rules)
    return rule
