"""
Gmail connector — send email on behalf of the authenticated user.

Uses the shared Google OAuth token (see google_oauth.py) scoped with
`gmail.send`. Messages are composed locally and POSTed to the Gmail
REST API as raw RFC 2822 content.
"""
from __future__ import annotations
import base64, logging
from email.message import EmailMessage

from app.connectors import google_oauth as g

logger = logging.getLogger(__name__)

NAME = "gmail"
DISPLAY_NAME = "Gmail"
DESCRIPTION = "Send emails from your Google account."
SCOPES = g.GMAIL_SCOPES


# ── Registry hooks ─────────────────────────────────────

def status(username: str) -> dict:
    configured = g.is_configured()
    has_token = g.has_scopes(username, SCOPES)
    return {
        "id": NAME,
        "name": DISPLAY_NAME,
        "description": DESCRIPTION,
        "configured": configured,
        "connected": has_token,
        "account": g.user_email(username) if has_token else None,
    }


def disconnect(username: str) -> bool:
    """Disconnecting Gmail removes the shared Google token entirely."""
    return g.disconnect_google(username)


# ── OAuth URL / callback ──────────────────────────────

def authorize_url(username: str) -> str:
    return g.authorize_url(username, SCOPES, return_connector=NAME)


# ── Send ──────────────────────────────────────────────

def send_email(username: str, to: str, subject: str, body: str,
               cc: str | None = None, bcc: str | None = None) -> dict:
    import httpx

    access_token = g.get_access_token(username, SCOPES)
    sender = g.user_email(username) or "me"

    msg = EmailMessage()
    msg["To"] = to
    if cc:
        msg["Cc"] = cc
    if bcc:
        msg["Bcc"] = bcc
    msg["Subject"] = subject or "(no subject)"
    msg["From"] = sender
    msg.set_content(body or "")

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("ascii")
    with httpx.Client(timeout=30) as client:
        resp = client.post(
            "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json={"raw": raw},
        )
    if resp.status_code >= 400:
        raise RuntimeError(f"Gmail send failed: {resp.status_code} {resp.text}")
    return resp.json()
