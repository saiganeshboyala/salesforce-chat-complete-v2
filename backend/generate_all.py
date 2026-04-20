"""
Auto-generate models.py, sync.py, and query.py from schema_cache.json
ALL fields, exact Salesforce names (no renaming).
"""
import json
import re

with open("data/schema_cache.json", encoding="utf-8") as f:
    schema = json.load(f)

# 18 Salesforce objects — class names for Python, table names = exact SF object names
OBJ_MAP = {
    "Account":          "Account",
    "Contact":          "Contact",
    "User":             "SFUser",
    "Report":           "SFReport",
    "Student__c":       "Student",
    "Submissions__c":   "Submission",
    "Interviews__c":    "Interview",
    "Manager__c":       "Manager",
    "Job__c":           "Job",
    "Employee__c":      "Employee",
    "BU_Performance__c":"BUPerformance",
    "BS__c":            "BS",
    "Tech_Support__c":  "TechSupport",
    "New_Student__c":   "NewStudent",
    "Manager_Card__c":  "ManagerCard",
    "Cluster__c":       "Cluster",
    "Organization__c":  "Organization",
    "Pay_Off__c":       "PayOff",
}

SF_TYPE_MAP = {
    "id": ("String(18)", "String"),
    "string": ("Text", "Text"),
    "picklist": ("Text", "Text"),
    "textarea": ("Text", "Text"),
    "email": ("Text", "Text"),
    "phone": ("Text", "Text"),
    "url": ("Text", "Text"),
    "reference": ("String(18)", "String"),
    "double": ("Float", "Float"),
    "currency": ("Float", "Float"),
    "percent": ("Float", "Float"),
    "int": ("Integer", "Integer"),
    "boolean": ("Boolean", "Boolean"),
    "date": ("Date", "Date"),
    "datetime": ("DateTime", "DateTime"),
}


def safe_attr(name):
    """Make SF field name safe as Python class attribute."""
    # Python keywords
    if name in ("type", "format", "class", "import", "from", "global", "return", "is", "in", "not", "and", "or"):
        return name + "_"
    return name


def get_parse_fn(sf_type):
    if sf_type == "date":
        return "_parse_sf_date"
    elif sf_type == "datetime":
        return "_parse_sf_datetime"
    return None


# ─── GENERATE models.py ───
models_lines = []
models_lines.append('"""')
models_lines.append('SQLAlchemy models mirroring ALL Salesforce objects with exact field names.')
models_lines.append('Auto-generated from schema_cache.json')
models_lines.append('"""')
models_lines.append('from sqlalchemy import Column, String, Float, Integer, Boolean, Date, DateTime, Text')
models_lines.append('from sqlalchemy.orm import DeclarativeBase')
models_lines.append('from datetime import datetime')
models_lines.append('')
models_lines.append('')
models_lines.append('class Base(DeclarativeBase):')
models_lines.append('    pass')

for sf_obj, cls_name in OBJ_MAP.items():
    fields = schema[sf_obj]["fields"]
    # Table name = exact SF object name
    tbl_name = sf_obj
    models_lines.append('')
    models_lines.append('')
    models_lines.append(f'class {cls_name}(Base):')
    models_lines.append(f'    __tablename__ = "{tbl_name}"')
    models_lines.append('')

    seen = set()
    for f in fields:
        sf_name = f["name"]
        sf_type = f["type"]
        if sf_name in seen:
            continue
        seen.add(sf_name)

        type_info = SF_TYPE_MAP.get(sf_type, ("String(255)", "String"))
        col_type = type_info[0]
        attr = safe_attr(sf_name)

        if sf_name == "Id":
            if attr != sf_name:
                models_lines.append(f'    {attr} = Column("{sf_name}", {col_type}, primary_key=True)')
            else:
                models_lines.append(f'    {attr} = Column({col_type}, primary_key=True)')
        elif sf_name == "Name":
            models_lines.append(f'    {attr} = Column({col_type}, index=True)')
        else:
            if attr != sf_name:
                models_lines.append(f'    {attr} = Column("{sf_name}", {col_type})')
            else:
                models_lines.append(f'    {attr} = Column({col_type})')

    models_lines.append(f'    synced_at = Column(DateTime, default=datetime.utcnow)')

# Add app-specific models
models_lines.append('')
models_lines.append('')
models_lines.append('# ─── App-specific models (not from Salesforce) ───')
models_lines.append('')
models_lines.append('')
models_lines.append('class User(Base):')
models_lines.append('    __tablename__ = "users"')
models_lines.append('')
models_lines.append('    username = Column(String(100), primary_key=True)')
models_lines.append('    password_hash = Column(String(255), nullable=False)')
models_lines.append('    name = Column(String(255))')
models_lines.append('    role = Column(String(20), default="user")')
models_lines.append('    created_at = Column(DateTime, default=datetime.utcnow)')
models_lines.append('')
models_lines.append('')
models_lines.append('class ChatSession(Base):')
models_lines.append('    __tablename__ = "chat_sessions"')
models_lines.append('')
models_lines.append('    id = Column(String(100), primary_key=True)')
models_lines.append('    username = Column(String(100), index=True, nullable=False)')
models_lines.append('    title = Column(String(255), default="New Chat")')
models_lines.append('    pinned = Column(Boolean, default=False)')
models_lines.append('    created_at = Column(DateTime, default=datetime.utcnow)')
models_lines.append('    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)')
models_lines.append('')
models_lines.append('')
models_lines.append('class ChatMessage(Base):')
models_lines.append('    __tablename__ = "chat_messages"')
models_lines.append('')
models_lines.append('    id = Column(String(100), primary_key=True)')
models_lines.append('    session_id = Column(String(100), index=True, nullable=False)')
models_lines.append('    role = Column(String(20), nullable=False)')
models_lines.append('    content = Column(Text)')
models_lines.append('    soql = Column(Text)')
models_lines.append('    data = Column(Text)')
models_lines.append('    ts = Column(DateTime, default=datetime.utcnow)')
models_lines.append('')
models_lines.append('')
models_lines.append('class AuditLog(Base):')
models_lines.append('    __tablename__ = "audit_log"')
models_lines.append('')
models_lines.append('    id = Column(Integer, primary_key=True, autoincrement=True)')
models_lines.append('    timestamp = Column(DateTime, default=datetime.utcnow, index=True)')
models_lines.append('    username = Column(String(100), index=True)')
models_lines.append('    action = Column(String(100), index=True)')
models_lines.append('    details = Column(Text)')
models_lines.append('    ip_address = Column(String(50))')
models_lines.append('')
models_lines.append('')
models_lines.append('class SyncLog(Base):')
models_lines.append('    __tablename__ = "sync_log"')
models_lines.append('')
models_lines.append('    id = Column(Integer, primary_key=True, autoincrement=True)')
models_lines.append('    object_name = Column(String(100), index=True)')
models_lines.append('    records_synced = Column(Integer)')
models_lines.append('    started_at = Column(DateTime)')
models_lines.append('    finished_at = Column(DateTime)')
models_lines.append('    status = Column(String(20))')
models_lines.append('    error = Column(Text)')
models_lines.append('')

with open("app/database/models.py", "w", encoding="utf-8") as f:
    f.write("\n".join(models_lines))
print("OK models.py generated")


# ─── GENERATE sync.py ───
sync_lines = []
sync_lines.append('"""')
sync_lines.append('Salesforce -> PostgreSQL sync engine.')
sync_lines.append('Incremental sync with exact Salesforce field names preserved.')
sync_lines.append('Auto-generated from schema_cache.json')
sync_lines.append('"""')
sync_lines.append('import logging')
sync_lines.append('import asyncio')
sync_lines.append('import threading')
sync_lines.append('from datetime import datetime')
sync_lines.append('from sqlalchemy import text')
sync_lines.append('from sqlalchemy.dialects.postgresql import insert as pg_insert')
sync_lines.append('from app.database.engine import engine, async_session')
sync_lines.append('from app.database.models import (')
sync_lines.append('    Base, ' + ', '.join(OBJ_MAP[o] for o in OBJ_MAP) + ', SyncLog')
sync_lines.append(')')
sync_lines.append('from app.config import settings')
sync_lines.append('')
sync_lines.append('logger = logging.getLogger(__name__)')
sync_lines.append('')
sync_lines.append('_sync_running = False')
sync_lines.append('_last_sync: datetime | None = None')
sync_lines.append('')
sync_lines.append('')
sync_lines.append('def get_sync_status():')
sync_lines.append('    return {')
sync_lines.append('        "running": _sync_running,')
sync_lines.append('        "last_sync": _last_sync.isoformat() if _last_sync else None,')
sync_lines.append('        "interval_minutes": settings.sync_interval_minutes,')
sync_lines.append('    }')
sync_lines.append('')
sync_lines.append('')
sync_lines.append('async def init_db():')
sync_lines.append('    async with engine.begin() as conn:')
sync_lines.append('        await conn.run_sync(Base.metadata.create_all)')
sync_lines.append('    logger.info("Database tables created/verified")')
sync_lines.append('')
sync_lines.append('')
sync_lines.append('async def _get_last_successful_sync(session, object_name):')
sync_lines.append('    result = await session.execute(')
sync_lines.append('        text(')
sync_lines.append('            "SELECT finished_at FROM sync_log "')
sync_lines.append('            "WHERE object_name = :obj AND status = \'success\' "')
sync_lines.append('            "ORDER BY finished_at DESC LIMIT 1"')
sync_lines.append('        ),')
sync_lines.append('        {"obj": object_name},')
sync_lines.append('    )')
sync_lines.append('    row = result.fetchone()')
sync_lines.append('    return row[0] if row else None')
sync_lines.append('')
sync_lines.append('')
sync_lines.append('async def _fetch_all(soql):')
sync_lines.append('    import httpx')
sync_lines.append('    from app.salesforce.auth import ensure_authenticated')
sync_lines.append('')
sync_lines.append('    creds = await ensure_authenticated()')
sync_lines.append('    api_url = f"{creds.instance_url}/services/data/{settings.salesforce_api_version}/query/"')
sync_lines.append('    headers = {"Authorization": f"Bearer {creds.access_token}"}')
sync_lines.append('')
sync_lines.append('    async with httpx.AsyncClient(timeout=120.0) as client:')
sync_lines.append('        resp = await client.get(api_url, params={"q": soql}, headers=headers)')
sync_lines.append('')
sync_lines.append('        if resp.status_code == 401:')
sync_lines.append('            from app.salesforce.auth import login_client_credentials')
sync_lines.append('            creds = await login_client_credentials()')
sync_lines.append('            headers = {"Authorization": f"Bearer {creds.access_token}"}')
sync_lines.append('            resp = await client.get(api_url, params={"q": soql}, headers=headers)')
sync_lines.append('')
sync_lines.append('        if resp.status_code != 200:')
sync_lines.append('            raise Exception(f"SOQL error {resp.status_code}: {resp.text[:300]}")')
sync_lines.append('')
sync_lines.append('        data = resp.json()')
sync_lines.append('        records = data.get("records", [])')
sync_lines.append('        next_url = data.get("nextRecordsUrl")')
sync_lines.append('')
sync_lines.append('        while next_url:')
sync_lines.append('            resp = await client.get(f"{creds.instance_url}{next_url}", headers=headers)')
sync_lines.append('            if resp.status_code != 200:')
sync_lines.append('                break')
sync_lines.append('            page = resp.json()')
sync_lines.append('            records.extend(page.get("records", []))')
sync_lines.append('            next_url = page.get("nextRecordsUrl")')
sync_lines.append('            if len(records) % 10000 == 0:')
sync_lines.append('                logger.info(f"  ... fetched {len(records)} records so far")')
sync_lines.append('')
sync_lines.append('    for r in records:')
sync_lines.append('        r.pop("attributes", None)')
sync_lines.append('    return records')
sync_lines.append('')
sync_lines.append('')
sync_lines.append('def _parse_sf_date(val):')
sync_lines.append('    if not val:')
sync_lines.append('        return None')
sync_lines.append('    try:')
sync_lines.append('        if "T" in str(val):')
sync_lines.append('            return datetime.fromisoformat(str(val).replace("Z", "+00:00"))')
sync_lines.append('        return datetime.strptime(str(val)[:10], "%Y-%m-%d").date()')
sync_lines.append('    except Exception:')
sync_lines.append('        return None')
sync_lines.append('')
sync_lines.append('')
sync_lines.append('def _parse_sf_datetime(val):')
sync_lines.append('    if not val:')
sync_lines.append('        return None')
sync_lines.append('    try:')
sync_lines.append('        s = str(val).replace("Z", "+00:00")')
sync_lines.append('        dt = datetime.fromisoformat(s)')
sync_lines.append('        return dt.replace(tzinfo=None)')
sync_lines.append('    except Exception:')
sync_lines.append('        return None')
sync_lines.append('')
sync_lines.append('')
sync_lines.append('def _since_clause(last_sync):')
sync_lines.append('    if not last_sync:')
sync_lines.append('        return ""')
sync_lines.append('    ts = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")')
sync_lines.append('    return f" WHERE LastModifiedDate > {ts}"')
sync_lines.append('')
sync_lines.append('')
sync_lines.append('async def _upsert_batch(session, model, records_data, batch_size=5000):')
sync_lines.append('    if not records_data:')
sync_lines.append('        return 0')
sync_lines.append('')
sync_lines.append('    data_keys = set(records_data[0].keys()) - {"Id"}')
sync_lines.append('    total = 0')
sync_lines.append('    for i in range(0, len(records_data), batch_size):')
sync_lines.append('        batch = records_data[i:i + batch_size]')
sync_lines.append('        stmt = pg_insert(model.__table__).values(batch)')
sync_lines.append('        update_cols = {k: stmt.excluded[k] for k in data_keys}')
sync_lines.append('        stmt = stmt.on_conflict_do_update(index_elements=["Id"], set_=update_cols)')
sync_lines.append('        await session.execute(stmt)')
sync_lines.append('        total += len(batch)')
sync_lines.append('')
sync_lines.append('    await session.flush()')
sync_lines.append('    return total')
sync_lines.append('')

# Generate per-object sync functions
for sf_obj, cls_name in OBJ_MAP.items():
    fields = schema[sf_obj]["fields"]
    fn_name = f"_sync_{sf_obj.lower().replace('__c', '').replace('__', '_')}"

    # Get unique field names
    seen = set()
    unique_fields = []
    for f in fields:
        if f["name"] not in seen:
            seen.add(f["name"])
            unique_fields.append(f)

    soql_fields = [f["name"] for f in unique_fields]

    sync_lines.append('')
    sync_lines.append(f'async def {fn_name}(session, last_sync=None):')
    sync_lines.append(f'    since = _since_clause(last_sync)')
    sync_lines.append(f'    mode = "incremental" if last_sync else "full"')
    sync_lines.append(f'    logger.info(f"Syncing {sf_obj} ({{mode}})...")')
    sync_lines.append(f'')

    # SOQL field list
    soql_str = ", ".join(soql_fields)
    sync_lines.append(f'    soql_fields = "{soql_str}"')
    sync_lines.append(f'    records = await _fetch_all(f"SELECT {{soql_fields}} FROM {sf_obj}{{since}}")')
    sync_lines.append(f'')
    sync_lines.append(f'    now = datetime.utcnow()')
    sync_lines.append(f'    records_data = []')
    sync_lines.append(f'    for r in records:')
    sync_lines.append(f'        records_data.append({{')

    for f in unique_fields:
        sf_name = f["name"]
        sf_type = f["type"]
        parse_fn = get_parse_fn(sf_type)

        if sf_name == "Id":
            sync_lines.append(f'            "{sf_name}": r["Id"],')
        elif parse_fn:
            sync_lines.append(f'            "{sf_name}": {parse_fn}(r.get("{sf_name}")),')
        elif sf_type == "boolean":
            sync_lines.append(f'            "{sf_name}": r.get("{sf_name}", False),')
        else:
            sync_lines.append(f'            "{sf_name}": r.get("{sf_name}"),')

    sync_lines.append(f'            "synced_at": now,')
    sync_lines.append(f'        }})')
    sync_lines.append(f'')
    sync_lines.append(f'    return await _upsert_batch(session, {cls_name}, records_data)')
    sync_lines.append(f'')

# Build fn_name lookup for SYNC_TASKS
sync_lines.append('')
sync_lines.append('SYNC_TASKS = [')
for sf_obj in OBJ_MAP:
    fn_name = f"_sync_{sf_obj.lower().replace('__c', '').replace('__', '_')}"
    sync_lines.append(f'    ("{sf_obj}", {fn_name}),')
sync_lines.append(']')
sync_lines.append('')

# run_sync function
sync_lines.append('')
sync_lines.append('async def run_sync(full=False):')
sync_lines.append('    global _sync_running, _last_sync')
sync_lines.append('    if _sync_running:')
sync_lines.append('        logger.warning("Sync already running, skipping")')
sync_lines.append('        return {"status": "already_running"}')
sync_lines.append('')
sync_lines.append('    try:')
sync_lines.append('        async with async_session() as lock_session:')
sync_lines.append('            result = await lock_session.execute(text("SELECT pg_try_advisory_lock(12345)"))')
sync_lines.append('            got_lock = result.scalar()')
sync_lines.append('            if not got_lock:')
sync_lines.append('                logger.warning("Another worker is syncing, skipping")')
sync_lines.append('                return {"status": "already_running"}')
sync_lines.append('    except Exception:')
sync_lines.append('        pass')
sync_lines.append('')
sync_lines.append('    _sync_running = True')
sync_lines.append('    results = []')
sync_lines.append('    logger.info("=== Starting Salesforce -> PostgreSQL sync ===")')
sync_lines.append('')
sync_lines.append('    for obj_name, sync_fn in SYNC_TASKS:')
sync_lines.append('        async with async_session() as session:')
sync_lines.append('            log_entry = SyncLog(')
sync_lines.append('                object_name=obj_name,')
sync_lines.append('                started_at=datetime.utcnow(),')
sync_lines.append('                status="running",')
sync_lines.append('            )')
sync_lines.append('            try:')
sync_lines.append('                last_sync_time = None')
sync_lines.append('                if not full:')
sync_lines.append('                    last_sync_time = await _get_last_successful_sync(session, obj_name)')
sync_lines.append('')
sync_lines.append('                count = await sync_fn(session, last_sync=last_sync_time)')
sync_lines.append('                log_entry.records_synced = count')
sync_lines.append('                log_entry.status = "success"')
sync_lines.append('                log_entry.finished_at = datetime.utcnow()')
sync_lines.append('                results.append({"object": obj_name, "records": count, "status": "success"})')
sync_lines.append('                logger.info(f"  OK {obj_name}: {count} records synced")')
sync_lines.append('            except Exception as e:')
sync_lines.append('                await session.rollback()')
sync_lines.append('                log_entry.status = "error"')
sync_lines.append('                log_entry.error = str(e)[:500]')
sync_lines.append('                log_entry.finished_at = datetime.utcnow()')
sync_lines.append('                results.append({"object": obj_name, "records": 0, "status": "error", "error": str(e)[:200]})')
sync_lines.append('                logger.error(f"  FAIL {obj_name}: {e}")')
sync_lines.append('')
sync_lines.append('            session.add(log_entry)')
sync_lines.append('            await session.commit()')
sync_lines.append('')
sync_lines.append('    try:')
sync_lines.append('        async with async_session() as lock_session:')
sync_lines.append('            await lock_session.execute(text("SELECT pg_advisory_unlock(12345)"))')
sync_lines.append('            await lock_session.commit()')
sync_lines.append('    except Exception:')
sync_lines.append('        pass')
sync_lines.append('')
sync_lines.append('    _sync_running = False')
sync_lines.append('    _last_sync = datetime.utcnow()')
sync_lines.append('    total = sum(r["records"] for r in results)')
sync_lines.append('    success_count = sum(1 for r in results if r["status"] == "success")')
sync_lines.append('    error_count = sum(1 for r in results if r["status"] == "error")')
sync_lines.append('')
sync_lines.append('    logger.info("=" * 60)')
sync_lines.append('    logger.info("  SYNC SUMMARY")')
sync_lines.append('    logger.info("=" * 60)')
sync_lines.append('    for r in results:')
sync_lines.append('        status_icon = "OK" if r["status"] == "success" else "FAIL"')
sync_lines.append('        err = f" -- {r.get(\'error\', \'\')[:80]}" if r["status"] == "error" else ""')
sync_lines.append('        logger.info(f"  {status_icon} {r[\'object\']:<25} {r[\'records\']:>8,} records{err}")')
sync_lines.append('    logger.info("-" * 60)')
sync_lines.append('    logger.info(f"  TOTAL: {total:,} records | {success_count} succeeded | {error_count} failed")')
sync_lines.append('    logger.info(f"  Completed at: {_last_sync.isoformat()}")')
sync_lines.append('    logger.info("=" * 60)')
sync_lines.append('')
sync_lines.append('    return {"status": "complete", "total_records": total, "details": results}')
sync_lines.append('')
sync_lines.append('')
sync_lines.append('def start_sync_scheduler():')
sync_lines.append('    interval = settings.sync_interval_minutes')
sync_lines.append('    if interval <= 0:')
sync_lines.append('        logger.info("Sync scheduler disabled (interval=0)")')
sync_lines.append('        return')
sync_lines.append('')
sync_lines.append('    logger.info(f"Sync scheduler started -- every {interval} minutes")')
sync_lines.append('')
sync_lines.append('    def loop():')
sync_lines.append('        import time as _time')
sync_lines.append('        while True:')
sync_lines.append('            _time.sleep(interval * 60)')
sync_lines.append('            try:')
sync_lines.append('                asyncio.run(run_sync())')
sync_lines.append('            except Exception as e:')
sync_lines.append('                logger.error(f"Scheduled sync failed: {e}")')
sync_lines.append('')
sync_lines.append('    threading.Thread(target=loop, daemon=True).start()')
sync_lines.append('')

with open("app/database/sync.py", "w", encoding="utf-8") as f:
    f.write("\n".join(sync_lines))
print("OK sync.py generated")


# ─── GENERATE query.py SF_TO_PG section ───
with open("app/database/query.py", encoding="utf-8") as f:
    existing = f.read()

sf_to_pg_start = existing.find("SF_TO_PG = {")
soql_fn_start = existing.find("\n\ndef soql_to_sql")
if soql_fn_start == -1:
    soql_fn_start = existing.find("\ndef soql_to_sql")

if sf_to_pg_start == -1:
    print("ERROR: Could not find SF_TO_PG in query.py")
else:
    pg_lines = []
    pg_lines.append("SF_TO_PG = {")
    for sf_obj in OBJ_MAP:
        fields = schema[sf_obj]["fields"]
        tbl_name = sf_obj
        pg_lines.append(f'    "{sf_obj}": {{')
        pg_lines.append(f'        "table": "{tbl_name}",')
        pg_lines.append(f'        "fields": {{')
        seen = set()
        for fld in fields:
            sf_name = fld["name"]
            if sf_name in seen:
                continue
            seen.add(sf_name)
            # Column name in PG = exact SF field name (PG will lowercase it)
            pg_lines.append(f'            "{sf_name}": "{sf_name}",')
        pg_lines.append(f'        }},')
        pg_lines.append(f'    }},')
    pg_lines.append("}")

    new_content = existing[:sf_to_pg_start] + "\n".join(pg_lines) + existing[soql_fn_start:]
    with open("app/database/query.py", "w", encoding="utf-8") as f:
        f.write(new_content)
    print("OK query.py SF_TO_PG updated")

# Summary
total_fields = 0
for sf_obj in OBJ_MAP:
    n = len(schema[sf_obj]["fields"])
    total_fields += n
    print(f"  {sf_obj}: {n} fields")
print(f"\nTotal: {len(OBJ_MAP)} objects, {total_fields} fields")
print("Table names = exact SF object names")
print("Column names = exact SF field names")
