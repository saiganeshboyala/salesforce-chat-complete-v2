"""Comparison mode — execute two SQL queries and summarize the delta."""
import logging

from app.chat.ai_engine import _call_ai
from app.database.query import execute_query

logger = logging.getLogger(__name__)


COMPARE_PROMPT = """You are a PostgreSQL SQL expert. Given a comparison question, return TWO SQL queries \
(one for each period or group) as JSON: {"query1":"...","label1":"...","query2":"...","label2":"..."}. \
Only output the JSON object, nothing else. Use aggregate queries (COUNT, SUM, GROUP BY) when possible.
CRITICAL: All table and column names MUST be double-quoted (case-sensitive PostgreSQL).
Use PostgreSQL date functions: CURRENT_DATE, DATE_TRUNC(), INTERVAL."""


def _records_total(result: dict) -> float:
    """Pick a single comparable number from a SQL result."""
    if "error" in result:
        return 0
    records = result.get("records") or []
    if not records:
        return result.get("totalSize", 0) or 0
    r = records[0]
    for k, v in r.items():
        if k == "attributes":
            continue
        if isinstance(v, (int, float)):
            return float(v)
    return float(result.get("totalSize", len(records)))


async def run_compare(query1: str, query2: str, label1: str = "A", label2: str = "B") -> dict:
    r1 = await execute_query(query1)
    r2 = await execute_query(query2)
    v1 = _records_total(r1)
    v2 = _records_total(r2)
    diff = v2 - v1
    pct = (diff / v1 * 100) if v1 else None
    return {
        "label1": label1,
        "label2": label2,
        "query1": query1,
        "query2": query2,
        "result1": r1,
        "result2": r2,
        "value1": v1,
        "value2": v2,
        "diff": diff,
        "pct_change": pct,
    }


async def run_compare_question(question: str) -> dict:
    import json as _json
    raw = await _call_ai(COMPARE_PROMPT, question, max_tokens=400)
    if not raw:
        raise ValueError("Empty response from AI")
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.lower().startswith("json"):
            raw = raw[4:]
        raw = raw.strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1:
        raise ValueError(f"Invalid AI response: {raw[:100]}")
    payload = _json.loads(raw[start : end + 1])
    q1 = payload.get("query1")
    q2 = payload.get("query2")
    l1 = payload.get("label1") or "Period 1"
    l2 = payload.get("label2") or "Period 2"
    if not q1 or not q2:
        raise ValueError("AI did not return two queries")
    return await run_compare(q1, q2, l1, l2)
