"""
Google Sheets connector — placeholder (wired in a later pass).

Exposes `status()` + `disconnect()` so the connector registry can
render the card. Actual export endpoint will be added when this
connector is built out.
"""
from __future__ import annotations
from app.connectors import google_oauth as g

NAME = "sheets"
DISPLAY_NAME = "Google Sheets"
DESCRIPTION = "Export query results to a new Google Sheet."
SCOPES = g.SHEETS_SCOPES


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
