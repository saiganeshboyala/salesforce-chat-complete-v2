"""
SOQL Executor — runs queries against Salesforce standard REST API.
READ-ONLY. Only SELECT queries allowed.
"""
import httpx, logging, re, time
from collections import OrderedDict
from app.config import settings

logger = logging.getLogger(__name__)

# ── Query cache ───────────────────────────────────────
# In-memory LRU-ish cache so the same question reruns cheap within a session.
# Skipped for relative-date queries (TODAY / YESTERDAY / LAST_N_DAYS) where
# staleness would silently change the answer.
_CACHE_TTL = 300          # 5 minutes
_CACHE_MAX = 100
_cache: "OrderedDict[str, tuple[float, dict]]" = OrderedDict()

_VOLATILE_KEYWORDS = re.compile(
    r"\b(TODAY|YESTERDAY|TOMORROW|THIS_WEEK|LAST_WEEK|NEXT_WEEK|"
    r"THIS_MONTH|LAST_MONTH|NEXT_MONTH|THIS_QUARTER|LAST_QUARTER|NEXT_QUARTER|"
    r"THIS_YEAR|LAST_YEAR|NEXT_YEAR|LAST_N_DAYS|NEXT_N_DAYS|"
    r"LAST_N_WEEKS|NEXT_N_WEEKS|LAST_N_MONTHS|NEXT_N_MONTHS|"
    r"LAST_N_QUARTERS|NEXT_N_QUARTERS|LAST_N_YEARS|NEXT_N_YEARS|"
    r"LAST_90_DAYS|N_DAYS_AGO|N_WEEKS_AGO|N_MONTHS_AGO|N_QUARTERS_AGO|N_YEARS_AGO)\b"
)


def _cache_key(query):
    return re.sub(r"\s+", " ", query.strip()).upper()


def _is_cacheable(query):
    return not _VOLATILE_KEYWORDS.search(query.upper())


def _cache_get(query):
    if not _is_cacheable(query):
        return None
    key = _cache_key(query)
    entry = _cache.get(key)
    if not entry:
        return None
    ts, value = entry
    if time.time() - ts > _CACHE_TTL:
        _cache.pop(key, None)
        return None
    _cache.move_to_end(key)
    return value


def _cache_put(query, value):
    if not _is_cacheable(query):
        return
    if "error" in value:
        return
    key = _cache_key(query)
    _cache[key] = (time.time(), value)
    _cache.move_to_end(key)
    while len(_cache) > _CACHE_MAX:
        _cache.popitem(last=False)


def validate_soql(query):
    """Ensure query is SELECT only — never allow DML."""
    q = query.strip().upper()
    if not q.startswith("SELECT"):
        raise ValueError("Only SELECT queries allowed")
    dangerous = ["INSERT", "UPDATE", "DELETE", "UPSERT", "MERGE", "UNDELETE"]
    for word in dangerous:
        if word in q:
            raise ValueError(f"Dangerous operation '{word}' not allowed")
    return True


async def execute_soql(query, instance_url=None, access_token=None, force_salesforce=False):
    """Execute a SOQL query. Tries PostgreSQL first, falls back to Salesforce API."""
    validate_soql(query)

    cached = _cache_get(query)
    if cached is not None:
        logger.info(f"SOQL cache hit: {query[:120]}")
        return cached

    # Try PostgreSQL first (if sync is available and not forced to Salesforce)
    if not force_salesforce:
        try:
            from app.database.query import soql_to_sql, execute_sql
            sql = soql_to_sql(query)
            if sql:
                result = await execute_sql(sql)
                if "error" not in result:
                    logger.info(f"PostgreSQL hit: {query[:100]}")
                    _cache_put(query, result)
                    return result
                else:
                    logger.warning(f"PostgreSQL query failed, falling back to Salesforce: {result['error'][:100]}")
        except Exception as e:
            logger.warning(f"PostgreSQL unavailable, using Salesforce: {str(e)[:80]}")

    url = instance_url or settings.salesforce_instance_url
    from app.salesforce.auth import ensure_authenticated
    creds = await ensure_authenticated()

    api_url = f"{creds.instance_url}/services/data/{settings.salesforce_api_version}/query/"
    headers = {"Authorization": f"Bearer {creds.access_token}"}

    logger.info(f"SOQL: {query[:200]}")

    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get(api_url, params={"q": query}, headers=headers)

        if resp.status_code == 401:
            # Token expired — re-auth and retry
            from app.salesforce.auth import login_client_credentials
            creds = await login_client_credentials()
            headers = {"Authorization": f"Bearer {creds.access_token}"}
            resp = await client.get(api_url, params={"q": query}, headers=headers)

        if resp.status_code != 200:
            error = resp.text[:500]
            logger.error(f"SOQL error {resp.status_code}: {error}")
            return {"error": error, "status": resp.status_code}

        data = resp.json()

        # Handle pagination for large results
        records = data.get("records", [])
        next_url = data.get("nextRecordsUrl")

        while next_url and len(records) < 2000:
            resp = await client.get(f"{creds.instance_url}{next_url}", headers=headers)
            if resp.status_code != 200: break
            page = resp.json()
            records.extend(page.get("records", []))
            next_url = page.get("nextRecordsUrl")

        # Clean up attributes field from each record
        for r in records:
            r.pop("attributes", None)

        result = {
            "totalSize": data.get("totalSize", len(records)),
            "records": records,
            "done": data.get("done", True),
        }
        _cache_put(query, result)
        return result
