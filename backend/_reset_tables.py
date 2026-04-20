"""One-time script: drop and recreate app tables in PostgreSQL."""
import asyncio
from sqlalchemy import text
from app.database.engine import engine
from app.database.models import Base


async def reset():
    async with engine.begin() as conn:
        # Drop all our tables with CASCADE to handle foreign key dependencies
        for table in reversed(Base.metadata.sorted_tables):
            await conn.execute(text(f'DROP TABLE IF EXISTS "{table.name}" CASCADE'))
            print(f"  Dropped: {table.name}")
        # Recreate all tables
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()
    print("\nAll app tables recreated successfully!")


if __name__ == "__main__":
    asyncio.run(reset())
