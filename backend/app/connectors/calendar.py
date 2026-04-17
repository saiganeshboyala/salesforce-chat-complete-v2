"""
Google Calendar connector — placeholder (wired in a later pass).
"""
from __future__ import annotations
from app.connectors import google_oauth as g

NAME = "calendar"
DISPLAY_NAME = "Google Calendar"
DESCRIPTION = "Create calendar events from chat results."
SCOPES = g.CALENDAR_SCOPES


def status(username: str) -> dict:
    return {
        "id": NAME,
        "name": DISPLAY_NAME,
        "description": DESCRIPTION,
        "configured": g.is_configured(),
        "connected": g.has_scopes(username, SCOPES),
        "account": g.user_email(username) if g.has_scopes(username, SCOPES) else None,
    }


def disconnect(username: str) -> bool:
    return g.disconnect_google(username)


def authorize_url(username: str) -> str:
    return g.authorize_url(username, SCOPES, return_connector=NAME)
