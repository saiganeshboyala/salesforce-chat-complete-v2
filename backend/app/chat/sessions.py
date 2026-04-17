"""
Chat session persistence — one JSON file per session on disk.

Path: {data_dir}/users/{username}/sessions/{session_id}.json
File layout:
    {
        "id": "s_...",
        "title": "First question (truncated)",
        "created_at": "2026-...",
        "updated_at": "2026-...",
        "messages": [
            {"id": "m_...", "role": "user"|"assistant", "content": "...",
             "ts": "...", "soql": "...", "data": {...}}
        ]
    }

Anonymous users (no username) are kept in-memory only.
"""
import json, logging, re
from datetime import datetime
from pathlib import Path
from threading import Lock
from app.config import settings

logger = logging.getLogger(__name__)
_lock = Lock()
_anon_sessions: dict[str, dict] = {}
MAX_TITLE_LEN = 60


def _user_dir(username: str) -> Path:
    d = Path(settings.data_dir) / "users" / username / "sessions"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _session_path(username: str, session_id: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_\-]", "_", session_id)
    return _user_dir(username) / f"{safe}.json"


def _make_title(first_user_msg: str) -> str:
    t = (first_user_msg or "New Chat").strip().replace("\n", " ")
    return t[:MAX_TITLE_LEN] + ("…" if len(t) > MAX_TITLE_LEN else "")


def load_session(username: str | None, session_id: str) -> dict:
    if not username:
        return _anon_sessions.get(session_id) or _new_session(session_id)
    p = _session_path(username, session_id)
    if not p.exists():
        return _new_session(session_id)
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load session {session_id}: {e}")
        return _new_session(session_id)


def _new_session(session_id: str) -> dict:
    now = datetime.now().isoformat()
    return {
        "id": session_id,
        "title": "New Chat",
        "created_at": now,
        "updated_at": now,
        "pinned": False,
        "messages": [],
    }


def save_session(username: str | None, session: dict) -> None:
    session["updated_at"] = datetime.now().isoformat()
    if not username:
        _anon_sessions[session["id"]] = session
        return
    with _lock:
        p = _session_path(username, session["id"])
        tmp = p.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(session, f, ensure_ascii=False, indent=2, default=str)
        tmp.replace(p)


def append_message(username: str | None, session_id: str, message: dict) -> dict:
    session = load_session(username, session_id)
    session["messages"].append(message)
    if session["title"] == "New Chat" and message.get("role") == "user":
        session["title"] = _make_title(message.get("content", ""))
    save_session(username, session)
    return session


def list_sessions(username: str | None) -> list[dict]:
    if not username:
        return [
            {"id": s["id"], "title": s["title"], "created_at": s["created_at"],
             "updated_at": s["updated_at"], "message_count": len(s["messages"])}
            for s in _anon_sessions.values()
        ]
    d = _user_dir(username)
    out = []
    for f in d.glob("*.json"):
        try:
            with open(f, encoding="utf-8") as fh:
                s = json.load(fh)
            out.append({
                "id": s.get("id", f.stem),
                "title": s.get("title", "Untitled"),
                "created_at": s.get("created_at", ""),
                "updated_at": s.get("updated_at", ""),
                "pinned": bool(s.get("pinned", False)),
                "message_count": len(s.get("messages", [])),
            })
        except Exception as e:
            logger.warning(f"Failed to read {f}: {e}")
    # Pinned first, then by updated_at desc within each group
    out.sort(key=lambda x: (not x.get("pinned", False), -_ts(x.get("updated_at", ""))))
    return out


def _ts(s: str) -> float:
    try:
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return 0.0


def toggle_pin(username: str | None, session_id: str) -> bool:
    """Flip pinned flag on a session. Returns new pinned state."""
    session = load_session(username, session_id)
    new_state = not bool(session.get("pinned", False))
    session["pinned"] = new_state
    save_session(username, session)
    return new_state


def delete_session(username: str | None, session_id: str) -> bool:
    if not username:
        return _anon_sessions.pop(session_id, None) is not None
    p = _session_path(username, session_id)
    if p.exists():
        p.unlink()
        return True
    return False


def search_sessions(username: str | None, query: str) -> list[dict]:
    q = (query or "").strip().lower()
    if not q:
        return list_sessions(username)
    results = []
    sessions_meta = list_sessions(username)
    for meta in sessions_meta:
        full = load_session(username, meta["id"])
        if q in (full.get("title") or "").lower():
            results.append(meta)
            continue
        for m in full.get("messages", []):
            if q in (m.get("content") or "").lower():
                results.append(meta)
                break
    return results
