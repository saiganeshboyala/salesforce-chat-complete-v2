"""
Connector manager — central registry for all third-party connectors.

Each connector is a module under `app.connectors` that exposes at least:
  - name: str
  - is_configured() -> bool        (server has client id/secret)
  - is_connected(username) -> bool (user has a valid token)
  - disconnect(username) -> bool

The manager aggregates them so the frontend can render a single page.
"""
from __future__ import annotations
import json, logging
from pathlib import Path
from app.config import settings

logger = logging.getLogger(__name__)


def user_data_dir(username: str) -> Path:
    d = Path(settings.data_dir) / "users" / username
    d.mkdir(parents=True, exist_ok=True)
    return d


def token_path(username: str, name: str) -> Path:
    return user_data_dir(username) / f"{name}_token.json"


def load_token(username: str, name: str) -> dict | None:
    p = token_path(username, name)
    if not p.exists():
        return None
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to read {name} token for {username}: {e}")
        return None


def save_token(username: str, name: str, data: dict) -> None:
    p = token_path(username, name)
    tmp = p.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    tmp.replace(p)


def delete_token(username: str, name: str) -> bool:
    p = token_path(username, name)
    if not p.exists():
        return False
    try:
        p.unlink()
        return True
    except Exception as e:
        logger.warning(f"Failed to delete {name} token for {username}: {e}")
        return False


# ── Registry ───────────────────────────────────────────

def list_connectors(username: str) -> list[dict]:
    """
    Return status snapshot for every known connector for a given user.
    Each entry: { id, name, description, configured, connected, account? }
    """
    from app.connectors import gmail, sheets, slack, calendar, openai_conn, grok  # noqa: WPS433

    out = []
    for mod in (gmail, sheets, calendar, slack, openai_conn, grok):
        try:
            out.append(mod.status(username))
        except Exception as e:
            logger.warning(f"status() failed for {getattr(mod, 'NAME', '?')}: {e}")
    return out


def disconnect(username: str, connector_id: str) -> bool:
    from app.connectors import gmail, sheets, slack, calendar, openai_conn, grok  # noqa: WPS433

    mapping = {
        gmail.NAME: gmail,
        sheets.NAME: sheets,
        calendar.NAME: calendar,
        slack.NAME: slack,
        openai_conn.NAME: openai_conn,
        grok.NAME: grok,
    }
    mod = mapping.get(connector_id)
    if not mod:
        return False
    return mod.disconnect(username)
