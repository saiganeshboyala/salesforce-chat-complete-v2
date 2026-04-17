"""
Grok connector — shows connection status and allows testing the API key.
No OAuth needed — just checks if the API key is configured in .env.
"""
from __future__ import annotations
import logging
from app.config import settings

logger = logging.getLogger(__name__)

NAME = "grok"
DISPLAY_NAME = "Grok (xAI)"
DESCRIPTION = "Grok-3 for fast analytics, SOQL generation, and chat answers."


def is_configured() -> bool:
    return bool(settings.grok_api_key)


def status(username: str) -> dict:
    configured = is_configured()
    return {
        "id": NAME,
        "name": DISPLAY_NAME,
        "description": DESCRIPTION,
        "configured": configured,
        "connected": configured,
        "account": f"Model: {settings.grok_model}" if configured else None,
    }


def disconnect(username: str) -> bool:
    return False  # API key based — no per-user disconnect


def test_connection() -> dict:
    """Test the Grok API key by making a small request."""
    if not is_configured():
        raise RuntimeError("Grok API key not configured")
    try:
        from openai import OpenAI
        client = OpenAI(api_key=settings.grok_api_key, base_url="https://api.x.ai/v1")
        r = client.chat.completions.create(
            model=settings.grok_model,
            max_tokens=10,
            messages=[{"role": "user", "content": "Hi"}],
        )
        return {"status": "ok", "model": settings.grok_model}
    except Exception as e:
        raise RuntimeError(f"Grok connection failed: {e}")
