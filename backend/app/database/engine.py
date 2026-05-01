"""
Database engine — async PostgreSQL connection via SQLAlchemy.
Supports AWS RDS with SSL.
"""
import ssl
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from app.config import settings


def _build_engine():
    url = settings.database_url

    # Convert standard postgresql:// to asyncpg driver
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)

    # Strip sslmode from URL (asyncpg handles SSL via connect_args)
    clean_url = url.replace("?sslmode=require", "").replace("&sslmode=require", "")

    connect_args = {}
    if "sslmode=require" in settings.database_url or "rds.amazonaws.com" in settings.database_url:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        connect_args["ssl"] = ssl_ctx

    return create_async_engine(
        clean_url,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=300,
        echo=False,
        connect_args=connect_args,
    )


engine = _build_engine()
async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_session() -> AsyncSession:
    async with async_session() as session:
        yield session
