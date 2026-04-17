"""
Build RAG Index — fetches records from Salesforce and indexes them
with OpenAI embeddings for semantic search.

Usage:
    python -m scripts.sync_rag

This is optional. The app works without RAG (SOQL-only mode).
RAG adds semantic/fuzzy search on top of exact SOQL queries.

Cost: ~$0.01 for 30K records (OpenAI embedding API).
Time: ~2 minutes for 30K records.
"""
import asyncio, logging, sys, httpx
from app.config import settings
from app.salesforce.auth import ensure_authenticated
from app.salesforce.schema import get_schema, discover_schema, PRIORITY_OBJECTS
from app.chat.rag import index_records

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


async def main():
    print("\n" + "=" * 56)
    print("  RAG Index Builder")
    print("  OpenAI Embeddings + Qdrant")
    print("=" * 56 + "\n")

    creds = await ensure_authenticated()
    print(f"  Salesforce: {creds.instance_url}")

    schema = get_schema()
    if not schema:
        print("  No schema cache. Discovering...")
        schema = await discover_schema(creds.instance_url, creds.access_token)

    use_all = "--all" in sys.argv
    priority_set = set(PRIORITY_OBJECTS)

    if not use_all:
        schema = {k: v for k, v in schema.items() if k in priority_set}
        print(f"  Mode: PRIORITY objects only ({len(schema)})")
    print(f"  Objects with data: {len(schema)}\n")

    # Fetch ALL records from every object
    DEFAULT_LIMIT = 999999

    # Fetch records from each object
    records_by_object = {}
    total = 0

    for obj_name, obj_data in schema.items():
        count = obj_data.get("record_count", 0)
        if not count or count == 0:
            continue

        fields = [f["name"] for f in obj_data.get("fields", [])
                 if f["type"] not in ("boolean",)
                 and not f["name"].startswith("UserPref")
                 and not f["name"].startswith("Is")][:30]

        if len(fields) < 2:
            continue

        limit = count
        field_str = ", ".join(fields)

        print(f"  Fetching {obj_name} ({limit:,} of {count:,} records, {len(fields)} fields)...")

        try:
            all_recs = []
            q = f"SELECT {field_str} FROM {obj_name}"
            creds = await ensure_authenticated()
            api_url = f"{creds.instance_url}/services/data/{settings.salesforce_api_version}/query/"
            headers = {"Authorization": f"Bearer {creds.access_token}"}

            async with httpx.AsyncClient(timeout=120.0) as client:
                resp = await client.get(api_url, params={"q": q}, headers=headers)
                if resp.status_code != 200:
                    print(f"    ✗ {resp.text[:80]}")
                    continue
                data = resp.json()
                recs = data.get("records", [])
                for r in recs:
                    r.pop("attributes", None)
                all_recs.extend(recs)

                next_url = data.get("nextRecordsUrl")
                while next_url:
                    resp = await client.get(f"{creds.instance_url}{next_url}", headers=headers)
                    if resp.status_code != 200:
                        break
                    page = resp.json()
                    recs = page.get("records", [])
                    for r in recs:
                        r.pop("attributes", None)
                    all_recs.extend(recs)
                    next_url = page.get("nextRecordsUrl")
                    if len(all_recs) % 10000 < 2001:
                        print(f"    ... {len(all_recs):,} records fetched")

            if all_recs:
                records_by_object[obj_name] = all_recs
                total += len(all_recs)
                print(f"    ✓ {len(all_recs):,} records")
        except Exception as e:
            print(f"    ✗ {str(e)[:80]}")

    print(f"\n  Total records to index: {total}")
    print(f"  Estimated embedding cost: ~${total * 0.0000001:.4f}\n")

    # Build the index
    print("  Building RAG index with OpenAI embeddings...")
    indexed = index_records(records_by_object)
    print(f"\n  ✓ RAG index complete: {indexed} records indexed")
    print(f"  Now the app can do semantic search + SOQL queries\n")


if __name__ == "__main__":
    asyncio.run(main())
