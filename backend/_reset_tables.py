"""One-time script: drop and recreate app tables in PostgreSQL."""
import asyncio
from app.database.engine import engine
from app.database.models import Base


async def reset():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()
    print("All app tables recreated successfully!")


if __name__ == "__main__":
    asyncio.run(reset())
