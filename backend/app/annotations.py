"""Per-user record annotations — notes + tags attached to records."""
import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from threading import Lock

from app.config import settings
from app.timezone import now_cst

logger = logging.getLogger(__name__)

_lock = Lock()


def _path(username: str) -> Path:
    d = Path(settings.data_dir) / "users" / username
    d.mkdir(parents=True, exist_ok=True)
    return d / "annotations.json"


def _load(username: str) -> dict:
    p = _path(username)
    if not p.exists():
        return {"notes": []}
    try:
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "notes" not in data:
            return {"notes": []}
        return data
    except Exception as e:
        logger.warning(f"Failed to load annotations for {username}: {e}")
        return {"notes": []}


def _save(username: str, data: dict) -> None:
    p = _path(username)
    tmp = p.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(p)


def _clean_tags(tags) -> list:
    if not isinstance(tags, list):
        return []
    out = []
    for t in tags:
        t = str(t or "").strip().lower()[:24]
        if t and t not in out:
            out.append(t)
        if len(out) >= 10:
            break
    return out


def list_notes(username: str, record_id: str | None = None, tag: str | None = None, q: str | None = None) -> list:
    notes = _load(username).get("notes") or []
    if record_id:
        notes = [n for n in notes if n.get("record_id") == record_id]
    if tag:
        tag = tag.lower()
        notes = [n for n in notes if tag in (n.get("tags") or [])]
    if q:
        ql = q.lower()
        notes = [n for n in notes if ql in (n.get("text") or "").lower()
                 or ql in (n.get("record_name") or "").lower()]
    return sorted(notes, key=lambda n: n.get("updated_at", ""), reverse=True)


def get_for_records(username: str, record_ids: list) -> dict:
    """Return { record_id: [notes] } map — useful for bulk tooltip lookups."""
    if not record_ids:
        return {}
    notes = _load(username).get("notes") or []
    ids = set(record_ids)
    out: dict = {}
    for n in notes:
        rid = n.get("record_id")
        if rid in ids:
            out.setdefault(rid, []).append(n)
    return out


def create_note(username: str, payload: dict) -> dict:
    if not username:
        raise ValueError("username required")
    rid = str(payload.get("record_id") or "").strip()
    text = str(payload.get("text") or "").strip()
    if not rid:
        raise ValueError("record_id required")
    if not text:
        raise ValueError("text required")
    now = now_cst().isoformat()
    note = {
        "id": uuid.uuid4().hex,
        "record_id": rid[:64],
        "record_name": str(payload.get("record_name") or "")[:200],
        "object_type": str(payload.get("object_type") or "")[:64],
        "text": text[:2000],
        "tags": _clean_tags(payload.get("tags")),
        "created_at": now,
        "updated_at": now,
    }
    with _lock:
        data = _load(username)
        notes = data.get("notes") or []
        notes.append(note)
        data["notes"] = notes
        _save(username, data)
    return note


def update_note(username: str, note_id: str, patch: dict) -> dict:
    with _lock:
        data = _load(username)
        notes = data.get("notes") or []
        for i, n in enumerate(notes):
            if n.get("id") == note_id:
                if "text" in patch:
                    n["text"] = str(patch["text"] or "").strip()[:2000]
                if "tags" in patch:
                    n["tags"] = _clean_tags(patch["tags"])
                n["updated_at"] = now_cst().isoformat()
                notes[i] = n
                data["notes"] = notes
                _save(username, data)
                return n
    raise KeyError(note_id)


def delete_note(username: str, note_id: str) -> None:
    with _lock:
        data = _load(username)
        notes = data.get("notes") or []
        new_notes = [n for n in notes if n.get("id") != note_id]
        if len(new_notes) == len(notes):
            raise KeyError(note_id)
        data["notes"] = new_notes
        _save(username, data)


def list_tags(username: str) -> list:
    notes = _load(username).get("notes") or []
    counts: dict = {}
    for n in notes:
        for t in (n.get("tags") or []):
            counts[t] = counts.get(t, 0) + 1
    return sorted(
        [{"tag": t, "count": c} for t, c in counts.items()],
        key=lambda x: -x["count"],
    )
