"""Audit log — PostgreSQL with JSON file fallback."""
import json, logging
from datetime import datetime
from pathlib import Path
from threading import Lock
from app.config import settings

logger = logging.getLogger(__name__)
_MAX_ENTRIES = 10000
_lock = Lock()
_use_db = False


def enable_db():
    global _use_db
    _use_db = True
    logger.info("Audit log: PostgreSQL")


# ── JSON fallback ────────────────────────────────────
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


# ── DB operations ────────────────────────────────────
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


async def _db_log(username, action, details, ip_address):
    from app.database.engine import async_session
    from app.database.models import AuditLog
    async with async_session() as session:
        session.add(AuditLog(
            username=username or "anonymous",
            action=action,
            details=json.dumps(details, default=str) if details else None,
            ip_address=ip_address or "",
        ))
        await session.commit()


async def _db_query(user, action, start, end, page, page_size):
    from app.database.engine import async_session
    from app.database.models import AuditLog
    from sqlalchemy import select, func, distinct

    async with async_session() as session:
        stmt = select(AuditLog).order_by(AuditLog.timestamp.desc())

        if user:
            stmt = stmt.where(AuditLog.username == user)
        if action:
            stmt = stmt.where(AuditLog.action == action)
        if start:
            stmt = stmt.where(AuditLog.timestamp >= start)
        if end:
            stmt = stmt.where(AuditLog.timestamp <= end)

        count_result = await session.execute(
            select(func.count()).select_from(stmt.subquery())
        )
        total = count_result.scalar() or 0

        page = max(1, page)
        page_size = max(1, min(page_size, 500))
        stmt = stmt.offset((page - 1) * page_size).limit(page_size)

        result = await session.execute(stmt)
        entries = [
            {
                "timestamp": e.timestamp.isoformat() if e.timestamp else "",
                "username": e.username or "anonymous",
                "action": e.action or "",
                "details": json.loads(e.details) if e.details else {},
                "ip_address": e.ip_address or "",
            }
            for e in result.scalars().all()
        ]

        users_result = await session.execute(select(distinct(AuditLog.username)))
        users = sorted([u for u in users_result.scalars().all() if u])

        actions_result = await session.execute(select(distinct(AuditLog.action)))
        actions = sorted([a for a in actions_result.scalars().all() if a])

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "entries": entries,
            "users": users,
            "actions": actions,
        }


# ── Public API ───────────────────────────────────────
def log_action(username: str | None, action: str, details: dict | None = None, ip_address: str | None = None) -> None:
    if _use_db:
        try:
            _run_async(_db_log(username, action, details, ip_address))
            return
        except Exception as e:
            logger.warning(f"DB audit log failed: {e}")

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
    if _use_db:
        try:
            return _run_async(_db_query(user, action, start, end, page, page_size))
        except Exception as e:
            logger.warning(f"DB audit query failed: {e}")

    entries = _load()
    entries.reverse()

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
