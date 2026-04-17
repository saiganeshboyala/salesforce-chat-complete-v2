import asyncio, logging, sys
from app.salesforce.auth import ensure_authenticated
from app.salesforce.schema import discover_schema, get_schema, PRIORITY_OBJECTS

logging.basicConfig(level=logging.INFO, format="%(asctime)s │ %(message)s", datefmt="%H:%M:%S")

async def main():
    use_all = "--all" in sys.argv
    mode = "ALL objects" if use_all else f"PRIORITY objects ({len(PRIORITY_OBJECTS)})"

    print("\n" + "=" * 56)
    print("  Salesforce Schema Discovery")
    print(f"  Mode: {mode}")
    print("=" * 56 + "\n")

    if not use_all:
        print("  Priority objects:")
        for obj in PRIORITY_OBJECTS:
            print(f"    - {obj}")
        print()

    creds = await ensure_authenticated()
    print(f"  Instance: {creds.instance_url}\n")
    schema = await discover_schema(creds.instance_url, creds.access_token, only_priority=not use_all)
    print(f"\n  ✓ Discovered {len(schema)} objects with data:\n")
    for name, data in sorted(schema.items(), key=lambda x: -(x[1].get("record_count") or 0)):
        count = data.get("record_count", 0)
        fields = len(data.get("fields", []))
        print(f"    {name:<40} {(count or 0):>8,} records  ({fields} fields)")
    print(f"\n  Schema cached. Start the app: uvicorn app.main:app --host 0.0.0.0 --port 8000")
    if not use_all:
        print(f"  Tip: Run with --all to discover all objects\n")

if __name__ == "__main__":
    asyncio.run(main())
