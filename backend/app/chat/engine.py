import logging
from datetime import datetime
from app.chat.ai_engine import answer_question, answer_question_stream
from app.chat.sessions import load_session, save_session, append_message
from app.salesforce.schema import get_schema

logger = logging.getLogger(__name__)


def _last_soql_in(messages):
    """Return the most recent assistant SOQL in a message list, or None."""
    for m in reversed(messages or []):
        if m.get("role") == "assistant" and m.get("soql"):
            return m["soql"]
    return None


class ChatEngine:
    async def answer(self, session_id, question, username=None):
        now = datetime.now().isoformat()
        user_msg = {
            "id": f"m_{int(datetime.now().timestamp() * 1000)}",
            "role": "user",
            "content": question,
            "ts": now,
        }
        session = append_message(username, session_id, user_msg)

        history = [
            {"role": m["role"], "content": m["content"]}
            for m in session["messages"][-11:-1]
            if m.get("role") in ("user", "assistant")
        ]
        last_soql = _last_soql_in(session["messages"][:-1])

        result = await answer_question(question, history or None, username=username, last_soql=last_soql)

        asst_msg = {
            "id": f"m_{int(datetime.now().timestamp() * 1000)}",
            "role": "assistant",
            "content": result.get("answer", ""),
            "soql": result.get("soql"),
            "data": result.get("data"),
            "suggestions": result.get("suggestions") or [],
            "question": question,
            "ts": datetime.now().isoformat(),
        }
        append_message(username, session_id, asst_msg)
        return result

    async def answer_stream(self, session_id, question, username=None):
        """
        Async generator wrapper around answer_question_stream that:
          - persists the user message before streaming
          - forwards every event to the caller
          - persists the assistant message once `done` arrives
        """
        now = datetime.now().isoformat()
        user_msg = {
            "id": f"m_{int(datetime.now().timestamp() * 1000)}",
            "role": "user",
            "content": question,
            "ts": now,
        }
        session = append_message(username, session_id, user_msg)

        history = [
            {"role": m["role"], "content": m["content"]}
            for m in session["messages"][-11:-1]
            if m.get("role") in ("user", "assistant")
        ]
        last_soql = _last_soql_in(session["messages"][:-1])

        final = None
        async for event in answer_question_stream(question, history or None, username=username, last_soql=last_soql):
            if event.get("type") == "done":
                final = event.get("data") or {}
            yield event

        if final is not None:
            asst_msg = {
                "id": f"m_{int(datetime.now().timestamp() * 1000)}",
                "role": "assistant",
                "content": final.get("answer", ""),
                "soql": final.get("soql"),
                "data": final.get("data"),
                "suggestions": final.get("suggestions") or [],
                "question": question,
                "ts": datetime.now().isoformat(),
            }
            append_message(username, session_id, asst_msg)

    def get_welcome(self):
        schema = get_schema()
        if not schema:
            return {"answer": "Schema not loaded. Run: python -m scripts.refresh_schema", "data": None}
        total = sum(s.get("record_count", 0) or 0 for s in schema.values())
        return {
            "answer": f"Connected to Salesforce — {total:,} records across {len(schema)} objects.\n\nAsk me anything!",
            "data": {"total_records": total, "total_objects": len(schema)},
        }
