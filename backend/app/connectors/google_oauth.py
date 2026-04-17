"""
Shared Google OAuth helpers — used by Gmail, Sheets, and Calendar.

All three Google connectors share a single token file
(`google_token.json`) scoped with the union of all scopes we've
ever requested. The scope list grows as the user enables more
connectors, and we re-prompt for consent only when new scopes appear.
"""
from __future__ import annotations
import json, logging, secrets
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from app.config import settings
from app.connectors import load_token, save_token, delete_token, token_path

logger = logging.getLogger(__name__)

GOOGLE_TOKEN_NAME = "google"

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
                 "https://www.googleapis.com/auth/drive.file"]
CALENDAR_SCOPES = ["https://www.googleapis.com/auth/calendar.events"]
USERINFO_SCOPES = ["openid", "https://www.googleapis.com/auth/userinfo.email"]


def is_configured() -> bool:
    return bool(settings.google_client_id and settings.google_client_secret)


# ── In-memory CSRF state store ─────────────────────────
# state_token -> (username, return_connector, created_at)
_state_store: dict[str, tuple[str, str, datetime]] = {}
_STATE_TTL = timedelta(minutes=10)


def _cleanup_states() -> None:
    now = datetime.utcnow()
    dead = [k for k, (_, _, t) in _state_store.items() if now - t > _STATE_TTL]
    for k in dead:
        _state_store.pop(k, None)


def create_state(username: str, return_connector: str) -> str:
    _cleanup_states()
    token = secrets.token_urlsafe(24)
    _state_store[token] = (username, return_connector, datetime.utcnow())
    return token


def consume_state(token: str) -> tuple[str, str] | None:
    _cleanup_states()
    entry = _state_store.pop(token, None)
    if not entry:
        return None
    username, return_connector, _ = entry
    return username, return_connector


# ── Authorization URL ──────────────────────────────────

def authorize_url(username: str, scopes: Iterable[str], return_connector: str) -> str:
    """
    Build a Google OAuth consent URL. Always include any scopes the
    user already granted so the resulting token can access every
    connector they've previously enabled.
    """
    from urllib.parse import urlencode

    if not is_configured():
        raise RuntimeError("Google OAuth not configured — set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET")

    existing = load_token(username, GOOGLE_TOKEN_NAME) or {}
    granted = set(existing.get("scopes") or [])
    wanted = set(scopes) | granted | set(USERINFO_SCOPES)

    state = create_state(username, return_connector)
    params = {
        "client_id": settings.google_client_id,
        "redirect_uri": settings.google_redirect_uri,
        "response_type": "code",
        "scope": " ".join(sorted(wanted)),
        "access_type": "offline",
        "include_granted_scopes": "true",
        "prompt": "consent",
        "state": state,
    }
    return "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)


# ── Callback: exchange code for tokens ────────────────

def exchange_code(code: str, username: str) -> dict:
    import httpx

    if not is_configured():
        raise RuntimeError("Google OAuth not configured")

    with httpx.Client(timeout=20) as client:
        resp = client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": code,
                "client_id": settings.google_client_id,
                "client_secret": settings.google_client_secret,
                "redirect_uri": settings.google_redirect_uri,
                "grant_type": "authorization_code",
            },
        )
    if resp.status_code >= 400:
        raise RuntimeError(f"Token exchange failed: {resp.status_code} {resp.text}")

    payload = resp.json()
    existing = load_token(username, GOOGLE_TOKEN_NAME) or {}
    # Preserve refresh_token if Google omits it on subsequent consents
    refresh = payload.get("refresh_token") or existing.get("refresh_token")

    scope_str = payload.get("scope") or ""
    new_scopes = set(scope_str.split()) if scope_str else set()
    all_scopes = sorted(new_scopes | set(existing.get("scopes") or []))

    # Best-effort: fetch email of the authorized account
    email = existing.get("email")
    try:
        with httpx.Client(timeout=10) as client:
            me = client.get(
                "https://www.googleapis.com/oauth2/v2/userinfo",
                headers={"Authorization": f"Bearer {payload['access_token']}"},
            )
        if me.status_code < 400:
            email = me.json().get("email") or email
    except Exception as e:
        logger.warning(f"userinfo fetch failed: {e}")

    token_data = {
        "access_token": payload["access_token"],
        "refresh_token": refresh,
        "token_type": payload.get("token_type", "Bearer"),
        "expires_in": payload.get("expires_in"),
        "obtained_at": datetime.utcnow().isoformat(),
        "scopes": all_scopes,
        "email": email,
    }
    save_token(username, GOOGLE_TOKEN_NAME, token_data)
    return token_data


# ── Access token refresh ──────────────────────────────

def _token_expired(tok: dict) -> bool:
    try:
        obtained = datetime.fromisoformat(tok.get("obtained_at", ""))
    except Exception:
        return True
    expires_in = int(tok.get("expires_in") or 0)
    if not expires_in:
        return True
    return datetime.utcnow() >= obtained + timedelta(seconds=expires_in - 60)


def get_access_token(username: str, required_scopes: Iterable[str]) -> str:
    """
    Return a valid access token for the user, refreshing it if needed.
    Raises RuntimeError if not connected or missing required scope.
    """
    import httpx

    tok = load_token(username, GOOGLE_TOKEN_NAME)
    if not tok:
        raise RuntimeError("Google account not connected")

    granted = set(tok.get("scopes") or [])
    missing = set(required_scopes) - granted
    if missing:
        raise RuntimeError(f"Missing Google scopes: {', '.join(sorted(missing))} — reconnect to grant access")

    if _token_expired(tok):
        refresh = tok.get("refresh_token")
        if not refresh:
            raise RuntimeError("Google token expired and no refresh_token available — reconnect")
        with httpx.Client(timeout=20) as client:
            resp = client.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "client_id": settings.google_client_id,
                    "client_secret": settings.google_client_secret,
                    "refresh_token": refresh,
                    "grant_type": "refresh_token",
                },
            )
        if resp.status_code >= 400:
            raise RuntimeError(f"Refresh failed: {resp.status_code} {resp.text}")
        data = resp.json()
        tok["access_token"] = data["access_token"]
        tok["expires_in"] = data.get("expires_in", tok.get("expires_in"))
        tok["obtained_at"] = datetime.utcnow().isoformat()
        if data.get("refresh_token"):
            tok["refresh_token"] = data["refresh_token"]
        save_token(username, GOOGLE_TOKEN_NAME, tok)

    return tok["access_token"]


# ── Helpers for connector modules ─────────────────────

def user_email(username: str) -> str | None:
    tok = load_token(username, GOOGLE_TOKEN_NAME)
    return (tok or {}).get("email")


def has_scopes(username: str, required_scopes: Iterable[str]) -> bool:
    tok = load_token(username, GOOGLE_TOKEN_NAME)
    if not tok:
        return False
    granted = set(tok.get("scopes") or [])
    return set(required_scopes).issubset(granted)


def disconnect_google(username: str) -> bool:
    """Removes the shared Google token (affects Gmail, Sheets, Calendar)."""
    return delete_token(username, GOOGLE_TOKEN_NAME)
