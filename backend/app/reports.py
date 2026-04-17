"""Per-user report builder — save, run, and AI-suggest Salesforce reports."""
import json
import logging
import re
import uuid
from datetime import datetime
from pathlib import Path
from threading import Lock

from app.config import settings
from app.salesforce.schema import get_schema
from app.salesforce.soql_executor import execute_soql

logger = logging.getLogger(__name__)

_lock = Lock()

VALID_OPERATORS = {
    "equals": "=",
    "not_equals": "!=",
    "greater_than": ">",
    "less_than": "<",
    "greater_equals": ">=",
    "less_equals": "<=",
    "contains": "LIKE",
    "starts_with": "LIKE",
    "ends_with": "LIKE",
    "in": "IN",
    "not_in": "NOT IN",
    "is_null": "= NULL",
    "is_not_null": "!= NULL",
}

VALID_CHART_TYPES = {"none", "bar", "pie", "line"}

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _path(username: str) -> Path:
    d = Path(settings.data_dir) / "users" / username
    d.mkdir(parents=True, exist_ok=True)
    return d / "reports.json"


def _load(username: str) -> dict:
    p = _path(username)
    if not p.exists():
        return {"reports": []}
    try:
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or "reports" not in data:
            return {"reports": []}
        return data
    except Exception as e:
        logger.warning(f"Failed to load reports for {username}: {e}")
        return {"reports": []}


def _save(username: str, data: dict) -> None:
    p = _path(username)
    tmp = p.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(p)


def _safe_ident(s: str) -> str:
    s = str(s or "").strip()
    if not _IDENT_RE.match(s):
        raise ValueError(f"invalid identifier: {s}")
    return s


def _field_type_map(object_name: str) -> dict:
    schema = get_schema()
    obj = schema.get(object_name) or {}
    return {f["name"]: f.get("type", "string") for f in (obj.get("fields") or [])}


def _reference_fields(object_name: str) -> set:
    """Return set of field names that are reference (lookup) fields."""
    schema = get_schema()
    obj = schema.get(object_name) or {}
    return {f["name"] for f in (obj.get("fields") or []) if f.get("type") == "reference"}


def _format_value(raw, field_type: str, operator: str) -> str:
    if raw is None:
        return "NULL"
    s = str(raw)
    numeric_types = {"int", "integer", "double", "currency", "percent", "long"}
    if field_type in numeric_types:
        try:
            float(s)
            return s
        except ValueError:
            return "0"
    if field_type == "boolean":
        return "true" if s.strip().lower() in ("true", "1", "yes") else "false"
    if field_type == "date":
        return s if re.match(r"^\d{4}-\d{2}-\d{2}$", s) else f"'{s}'"
    if field_type == "datetime":
        return s if "T" in s else f"'{s}'"
    escaped = s.replace("\\", "\\\\").replace("'", "\\'")
    if operator == "contains":
        return f"'%{escaped}%'"
    if operator == "starts_with":
        return f"'{escaped}%'"
    if operator == "ends_with":
        return f"'%{escaped}'"
    return f"'{escaped}'"


def build_soql(config: dict) -> str:
    obj = _safe_ident(config.get("object") or "")
    fields_in = config.get("fields") or []
    filters_in = config.get("filters") or []
    group_by = config.get("groupBy") or None
    sort_by = config.get("sortBy") or None
    sort_dir = (config.get("sortDir") or "asc").lower()
    limit = int(config.get("limit") or 200)
    limit = max(1, min(limit, 2000))

    type_map = _field_type_map(obj)
    ref_fields = _reference_fields(obj)

    clean_fields = []
    for f in fields_in:
        name = _safe_ident(f)
        if name not in clean_fields:
            clean_fields.append(name)

    def _to_select_expr(field_name):
        """Convert reference fields to __r.Name for human-readable output."""
        if field_name in ref_fields and field_name.endswith("__c"):
            rel_name = field_name[:-3] + "__r"
            return f"{rel_name}.Name"
        return field_name

    if group_by:
        group_by = _safe_ident(group_by)
        select_parts = [group_by, "COUNT(Id) cnt"]
        for f in clean_fields:
            if f != group_by and f != "Id":
                select_parts.append(f"COUNT({f}) {f.lower()}_cnt")
        select_clause = ", ".join(select_parts[:6])
    else:
        if not clean_fields:
            clean_fields = ["Name"] if "Name" in type_map else ["Id"]
        select_clause = ", ".join(_to_select_expr(f) for f in clean_fields)

    where_parts = []
    for flt in filters_in:
        field = _safe_ident(flt.get("field") or "")
        op_key = flt.get("operator") or "equals"
        if op_key not in VALID_OPERATORS:
            raise ValueError(f"invalid operator: {op_key}")
        ftype = type_map.get(field, "string")
        if op_key == "is_null":
            where_parts.append(f"{field} = NULL")
        elif op_key == "is_not_null":
            where_parts.append(f"{field} != NULL")
        elif op_key in ("in", "not_in"):
            raw = flt.get("value") or ""
            items = [x.strip() for x in str(raw).split(",") if x.strip()]
            if not items:
                continue
            vals = ", ".join(_format_value(v, ftype, "equals") for v in items)
            sql_op = "IN" if op_key == "in" else "NOT IN"
            where_parts.append(f"{field} {sql_op} ({vals})")
        elif op_key in ("contains", "starts_with", "ends_with"):
            val = _format_value(flt.get("value"), "string", op_key)
            where_parts.append(f"{field} LIKE {val}")
        else:
            val = _format_value(flt.get("value"), ftype, op_key)
            sql_op = VALID_OPERATORS[op_key]
            where_parts.append(f"{field} {sql_op} {val}")

    soql = f"SELECT {select_clause} FROM {obj}"
    if where_parts:
        soql += " WHERE " + " AND ".join(where_parts)
    if group_by:
        soql += f" GROUP BY {group_by}"
    if sort_by:
        sb = _safe_ident(sort_by)
        direction = "DESC" if sort_dir == "desc" else "ASC"
        if group_by and sb == group_by:
            soql += f" ORDER BY {sb} {direction}"
        elif group_by:
            soql += f" ORDER BY COUNT(Id) {direction}"
        else:
            soql += f" ORDER BY {sb} {direction}"
    elif group_by:
        soql += " ORDER BY COUNT(Id) DESC"

    soql += f" LIMIT {limit}"
    return soql


def _validate_config(config: dict) -> dict:
    obj = str(config.get("object") or "").strip()
    if not obj:
        raise ValueError("object is required")
    _safe_ident(obj)

    fields = [str(f).strip() for f in (config.get("fields") or []) if str(f).strip()]
    for f in fields:
        _safe_ident(f)

    filters = []
    for flt in (config.get("filters") or []):
        field = str(flt.get("field") or "").strip()
        if not field:
            continue
        _safe_ident(field)
        op = flt.get("operator") or "equals"
        if op not in VALID_OPERATORS:
            raise ValueError(f"invalid operator: {op}")
        filters.append({
            "field": field,
            "operator": op,
            "value": flt.get("value"),
        })

    chart = config.get("chartType") or "none"
    if chart not in VALID_CHART_TYPES:
        chart = "none"

    group_by = (config.get("groupBy") or "").strip() or None
    if group_by:
        _safe_ident(group_by)

    sort_by = (config.get("sortBy") or "").strip() or None
    if sort_by:
        _safe_ident(sort_by)
    sort_dir = (config.get("sortDir") or "asc").lower()
    if sort_dir not in ("asc", "desc"):
        sort_dir = "asc"

    return {
        "object": obj,
        "fields": fields,
        "filters": filters,
        "groupBy": group_by,
        "chartType": chart,
        "sortBy": sort_by,
        "sortDir": sort_dir,
        "limit": int(config.get("limit") or 200),
    }


def list_reports(username: str) -> list:
    reports = _load(username).get("reports") or []
    return sorted(reports, key=lambda r: r.get("updated_at", ""), reverse=True)


def get_report(username: str, report_id: str) -> dict:
    for r in _load(username).get("reports") or []:
        if r.get("id") == report_id:
            return r
    raise KeyError(report_id)


def create_report(username: str, payload: dict) -> dict:
    name = str(payload.get("name") or "").strip()
    if not name:
        raise ValueError("name is required")
    config = _validate_config(payload)
    now = datetime.now().isoformat()
    report = {
        "id": uuid.uuid4().hex,
        "name": name[:120],
        "description": str(payload.get("description") or "")[:500],
        **config,
        "created_at": now,
        "updated_at": now,
    }
    with _lock:
        data = _load(username)
        reports = data.get("reports") or []
        reports.append(report)
        data["reports"] = reports
        _save(username, data)
    return report


def update_report(username: str, report_id: str, patch: dict) -> dict:
    with _lock:
        data = _load(username)
        reports = data.get("reports") or []
        for i, r in enumerate(reports):
            if r.get("id") == report_id:
                merged = {**r, **patch}
                validated = _validate_config(merged)
                r.update(validated)
                if "name" in patch and patch["name"]:
                    r["name"] = str(patch["name"])[:120]
                if "description" in patch:
                    r["description"] = str(patch["description"] or "")[:500]
                r["updated_at"] = datetime.now().isoformat()
                reports[i] = r
                data["reports"] = reports
                _save(username, data)
                return r
    raise KeyError(report_id)


def delete_report(username: str, report_id: str) -> None:
    with _lock:
        data = _load(username)
        reports = data.get("reports") or []
        new = [r for r in reports if r.get("id") != report_id]
        if len(new) == len(reports):
            raise KeyError(report_id)
        data["reports"] = new
        _save(username, data)


async def run_report(username: str, report_id: str | None = None, config: dict | None = None) -> dict:
    if report_id:
        report = get_report(username, report_id)
        cfg = report
    elif config is not None:
        cfg = _validate_config(config)
        report = None
    else:
        raise ValueError("report_id or config required")

    soql = build_soql(cfg)
    result = await execute_soql(soql)
    if "error" in result:
        return {"error": result["error"], "soql": soql}
    for r in result.get("records", []):
        r.pop("attributes", None)
    return {
        "soql": soql,
        "records": result.get("records", []),
        "totalSize": result.get("totalSize", 0),
        "done": result.get("done", True),
        "chartType": cfg.get("chartType") or "none",
        "groupBy": cfg.get("groupBy"),
        "report_id": report_id,
        "report_name": report["name"] if report else None,
    }


def _schema_summary(max_objects: int = 20, max_fields: int = 10) -> str:
    schema = get_schema()
    if not schema:
        return "(no schema available)"
    items = sorted(
        schema.items(),
        key=lambda kv: -(kv[1].get("record_count") or 0),
    )[:max_objects]
    lines = []
    for obj_name, meta in items:
        fields = (meta.get("fields") or [])[:max_fields]
        field_list = ", ".join(f"{f['name']}({f.get('type','?')})" for f in fields)
        lines.append(f"- {obj_name} ({meta.get('record_count', 0)} rows): {field_list}")
    return "\n".join(lines)


async def suggest_report(prompt: str) -> dict:
    """AI-assisted report config generation from a natural-language prompt."""
    from app.chat.ai_engine import _call_ai

    schema = get_schema()
    if not schema:
        raise ValueError("schema not available")

    system = (
        "You are a Salesforce report builder assistant. Given a user's goal and a schema "
        "summary, return a JSON report configuration. ONLY return valid JSON, no prose. "
        "Schema:\n" + _schema_summary() +
        "\n\nOperators: equals, not_equals, greater_than, less_than, greater_equals, "
        "less_equals, contains, starts_with, ends_with, in, not_in, is_null, is_not_null. "
        "Chart types: none, bar, pie, line.\n"
        "Schema format to return:\n"
        '{"name": "...", "object": "ObjectName", "fields": ["Field1", "Field2"], '
        '"filters": [{"field":"F","operator":"equals","value":"V"}], '
        '"groupBy": "Field or null", "chartType": "bar|pie|line|none", '
        '"sortBy": "Field or null", "sortDir": "asc|desc"}'
    )

    raw = await _call_ai(system, f"User goal: {prompt}\n\nReturn JSON only.", max_tokens=800)
    if not raw:
        raise ValueError("AI provider not configured")

    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

    try:
        config = json.loads(text)
    except Exception as e:
        raise ValueError(f"AI returned invalid JSON: {str(e)[:80]}")

    if not isinstance(config, dict) or "object" not in config:
        raise ValueError("AI response missing 'object' field")

    validated = _validate_config(config)
    validated["name"] = str(config.get("name") or "AI Suggested Report")[:120]
    return validated
