"""
Slack connector — placeholder (wired in a later pass).

Does not use Google OAuth. Will need its own client id/secret and
token storage file (`slack_token.json`) when built out.
"""
from __future__ import annotations
from app.config import settings
from app.connectors import load_token, delete_token

NAME = "slack"
DISPLAY_NAME = "Slack"
DESCRIPTION = "Share chat answers to a Slack channel."


def is_configured() -> bool:
    return bool(settings.slack_client_id and settings.slack_client_secret)


def status(username: str) -> dict:
    tok = load_token(username, NAME) or {}
    connected = bool(tok.get("access_token"))
    return {
        "id": NAME,
        "name": DISPLAY_NAME,
        "description": DESCRIPTION,
        "configured": is_configured(),
        "connected": connected,
        "account": tok.get("team_name") if connected else None,
    }


def disconnect(username: str) -> bool:
    return delete_token(username, NAME)
