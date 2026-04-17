from pydantic import BaseModel
from typing import Any

class ChatRequest(BaseModel):
    session_id: str
    question: str
    attachment_id: str | None = None

class ChatResponse(BaseModel):
    answer: str
    soql: str | None = None
    data: dict[str, Any] | None = None
