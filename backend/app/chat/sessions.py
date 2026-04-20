"""
Chat session persistence — PostgreSQL with JSON file fallback.

DB tables: chat_sessions, chat_messages
Fallback: {data_dir}/users/{username}/sessions/{session_id}.json
"""
import json, logging, re
from datetime import datetime
from pathlib import Path
from threading import Lock
from app.config import settings

logger = logging.getLogger(__name__)
_lock = Lock()
_anon_sessions: dict[str, dict] = {}
_use_db = False
MAX_TITLE_LEN = 60


def enable_db():
    global _use_db
    _use_db = True
    logger.info("Chat sessions: PostgreSQL")


# ── JSON fallback helpers ────────────────────────────
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


# ── DB operations ────────────────────────────────────
async def _db_load_session(username, session_id):
    from app.database.engine import async_session
    from app.database.models import ChatSession, ChatMessage
    from sqlalchemy import select

    async with async_session() as session:
        result = await session.execute(
            select(ChatSession).where(ChatSession.id == session_id, ChatSession.username == username)
        )
        cs = result.scalars().first()
        if not cs:
            return _new_session(session_id)

        msg_result = await session.execute(
            select(ChatMessage).where(ChatMessage.session_id == session_id).order_by(ChatMessage.ts)
        )
        messages = []
        for m in msg_result.scalars().all():
            msg = {"id": m.id, "role": m.role, "content": m.content, "ts": m.ts.isoformat() if m.ts else ""}
            if m.soql:
                msg["soql"] = m.soql
            if m.data:
                try:
                    msg["data"] = json.loads(m.data)
                except Exception:
                    msg["data"] = m.data
            messages.append(msg)

        return {
            "id": cs.id,
            "title": cs.title or "New Chat",
            "created_at": cs.created_at.isoformat() if cs.created_at else "",
            "updated_at": cs.updated_at.isoformat() if cs.updated_at else "",
            "pinned": bool(cs.pinned),
            "messages": messages,
        }


async def _db_save_session(username, session_data):
    from app.database.engine import async_session
    from app.database.models import ChatSession, ChatMessage
    from sqlalchemy import select, delete

    async with async_session() as session:
        result = await session.execute(
            select(ChatSession).where(ChatSession.id == session_data["id"])
        )
        cs = result.scalars().first()
        now = datetime.utcnow()

        if cs:
            cs.title = session_data.get("title", cs.title)
            cs.pinned = session_data.get("pinned", cs.pinned)
            cs.updated_at = now
        else:
            cs = ChatSession(
                id=session_data["id"],
                username=username,
                title=session_data.get("title", "New Chat"),
                pinned=session_data.get("pinned", False),
                created_at=now,
                updated_at=now,
            )
            session.add(cs)

        await session.execute(delete(ChatMessage).where(ChatMessage.session_id == session_data["id"]))
        for m in session_data.get("messages", []):
            data_str = None
            if m.get("data"):
                data_str = json.dumps(m["data"], default=str) if isinstance(m["data"], (dict, list)) else str(m["data"])
            session.add(ChatMessage(
                id=m.get("id", f"m_{datetime.utcnow().timestamp()}"),
                session_id=session_data["id"],
                role=m.get("role", "user"),
                content=m.get("content", ""),
                soql=m.get("soql"),
                data=data_str,
                ts=datetime.fromisoformat(m["ts"]) if m.get("ts") else now,
            ))

        await session.commit()


async def _db_list_sessions(username):
    from app.database.engine import async_session
    from app.database.models import ChatSession, ChatMessage
    from sqlalchemy import select, func

    async with async_session() as sess:
        stmt = (
            select(
                ChatSession.id,
                ChatSession.title,
                ChatSession.created_at,
                ChatSession.updated_at,
                ChatSession.pinned,
                func.count(ChatMessage.id).label("message_count"),
            )
            .outerjoin(ChatMessage, ChatSession.id == ChatMessage.session_id)
            .where(ChatSession.username == username)
            .group_by(ChatSession.id)
            .order_by(ChatSession.pinned.desc(), ChatSession.updated_at.desc())
        )
        result = await sess.execute(stmt)
        return [
            {
                "id": r.id,
                "title": r.title or "Untitled",
                "created_at": r.created_at.isoformat() if r.created_at else "",
                "updated_at": r.updated_at.isoformat() if r.updated_at else "",
                "pinned": bool(r.pinned),
                "message_count": r.message_count or 0,
            }
            for r in result.all()
        ]


async def _db_delete_session(username, session_id):
    from app.database.engine import async_session
    from app.database.models import ChatSession, ChatMessage
    from sqlalchemy import delete

    async with async_session() as session:
        await session.execute(delete(ChatMessage).where(ChatMessage.session_id == session_id))
        result = await session.execute(
            delete(ChatSession).where(ChatSession.id == session_id, ChatSession.username == username)
        )
        await session.commit()
        return result.rowcount > 0


async def _db_toggle_pin(username, session_id):
    from app.database.engine import async_session
    from app.database.models import ChatSession
    from sqlalchemy import select

    async with async_session() as session:
        result = await session.execute(
            select(ChatSession).where(ChatSession.id == session_id, ChatSession.username == username)
        )
        cs = result.scalars().first()
        if not cs:
            return False
        cs.pinned = not cs.pinned
        new_state = cs.pinned
        await session.commit()
        return new_state


# ── Sync wrappers (called from sync code) ───────────
def _run_async(coro):
    import asyncio
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                return pool.submit(lambda: asyncio.run(coro)).result()
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


# ── Public API ───────────────────────────────────────
def load_session(username: str | None, session_id: str) -> dict:
    if not username:
        return _anon_sessions.get(session_id) or _new_session(session_id)
    if _use_db:
        try:
            return _run_async(_db_load_session(username, session_id))
        except Exception as e:
            logger.warning(f"DB load session failed: {e}")
    p = _session_path(username, session_id)
    if not p.exists():
        return _new_session(session_id)
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load session {session_id}: {e}")
        return _new_session(session_id)


def save_session(username: str | None, session: dict) -> None:
    session["updated_at"] = datetime.now().isoformat()
    if not username:
        _anon_sessions[session["id"]] = session
        return
    if _use_db:
        try:
            _run_async(_db_save_session(username, session))
            return
        except Exception as e:
            logger.warning(f"DB save session failed: {e}")
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
    if _use_db:
        try:
            return _run_async(_db_list_sessions(username))
        except Exception as e:
            logger.warning(f"DB list sessions failed: {e}")

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
    out.sort(key=lambda x: (not x.get("pinned", False), -_ts(x.get("updated_at", ""))))
    return out


def _ts(s: str) -> float:
    try:
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return 0.0


def toggle_pin(username: str | None, session_id: str) -> bool:
    if _use_db and username:
        try:
            return _run_async(_db_toggle_pin(username, session_id))
        except Exception as e:
            logger.warning(f"DB toggle pin failed: {e}")

    session = load_session(username, session_id)
    new_state = not bool(session.get("pinned", False))
    session["pinned"] = new_state
    save_session(username, session)
    return new_state


def delete_session(username: str | None, session_id: str) -> bool:
    if not username:
        return _anon_sessions.pop(session_id, None) is not None
    if _use_db:
        try:
            return _run_async(_db_delete_session(username, session_id))
        except Exception as e:
            logger.warning(f"DB delete session failed: {e}")

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
