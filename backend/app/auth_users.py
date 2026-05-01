"""
User Authentication — JWT tokens + PostgreSQL storage.
Falls back to JSON file if database is unavailable.
"""
import json, logging
from datetime import datetime, timedelta
from pathlib import Path
from jose import JWTError, jwt
from app.timezone import now_cst
from passlib.context import CryptContext
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from app.config import settings

logger = logging.getLogger(__name__)

_jwt = getattr(settings, 'jwt_secret', '')
if not _jwt:
    raise RuntimeError("JWT_SECRET is not set in .env — refusing to start with an empty secret")
SECRET_KEY = _jwt

MIN_PASSWORD_LENGTH = 8
ALGORITHM = "HS256"
TOKEN_EXPIRE_HOURS = 24
USERS_FILE = Path(settings.data_dir) / "users.json"

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer(auto_error=False)

_use_db = False


async def _init_db_users():
    """Migrate existing JSON users to PostgreSQL and enable DB mode."""
    global _use_db
    try:
        from app.database.engine import async_session
        from app.database.models import User
        from sqlalchemy import select

        async with async_session() as session:
            result = await session.execute(select(User).limit(1))
            existing = result.scalars().first()

            if not existing:
                if USERS_FILE.exists():
                    with open(USERS_FILE) as f:
                        json_users = json.load(f)
                    for uname, udata in json_users.items():
                        session.add(User(
                            username=uname,
                            password_hash=udata["password"],
                            name=udata.get("name", uname),
                            role=udata.get("role", "user"),
                            created_at=datetime.fromisoformat(udata["created"]) if udata.get("created") else datetime.utcnow(),
                        ))
                    await session.commit()
                    logger.info(f"Migrated {len(json_users)} users from JSON to PostgreSQL")
                else:
                    session.add(User(
                        username="admin",
                        password_hash=pwd_context.hash("admin123"),
                        name="Admin",
                        role="admin",
                    ))
                    await session.commit()
                    logger.info("Created default admin user in PostgreSQL")

        _use_db = True
        logger.info("User storage: PostgreSQL")
    except Exception as e:
        logger.warning(f"PostgreSQL not available for users, using JSON: {e}")
        _use_db = False


# ── JSON fallback ────────────────────────────────────
def _load_users():
    if USERS_FILE.exists():
        with open(USERS_FILE) as f:
            return json.load(f)
    default = {
        "admin": {
            "username": "admin",
            "password": pwd_context.hash("admin123"),
            "name": "Admin",
            "role": "admin",
            "created": now_cst().isoformat(),
        }
    }
    _save_users(default)
    return default


def _save_users(users):
    USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(USERS_FILE, "w") as f:
        json.dump(users, f, indent=2, default=str)


# ── DB operations ────────────────────────────────────
async def _db_get_user(username):
    from app.database.engine import async_session
    from app.database.models import User
    from sqlalchemy import select
    async with async_session() as session:
        result = await session.execute(select(User).where(User.username == username))
        return result.scalars().first()


async def _db_authenticate(username, password):
    user = await _db_get_user(username)
    if not user or not pwd_context.verify(password, user.password_hash):
        return None
    return {"username": user.username, "name": user.name, "role": user.role, "password": user.password_hash}


async def _db_create_user(username, password, name, role="user"):
    from app.database.engine import async_session
    from app.database.models import User
    existing = await _db_get_user(username)
    if existing:
        return None
    async with async_session() as session:
        session.add(User(
            username=username,
            password_hash=pwd_context.hash(password),
            name=name,
            role=role,
        ))
        await session.commit()
    return {"username": username, "name": name, "role": role}


async def _db_list_users():
    from app.database.engine import async_session
    from app.database.models import User
    from sqlalchemy import select
    async with async_session() as session:
        result = await session.execute(select(User).order_by(User.created_at))
        return [
            {"username": u.username, "name": u.name or "", "role": u.role or "user", "created": u.created_at.isoformat() if u.created_at else ""}
            for u in result.scalars().all()
        ]


async def _db_delete_user(username):
    if username == "admin":
        return False
    from app.database.engine import async_session
    from app.database.models import User
    from sqlalchemy import delete
    async with async_session() as session:
        result = await session.execute(delete(User).where(User.username == username))
        await session.commit()
        return result.rowcount > 0


async def _db_change_password(username, new_password):
    from app.database.engine import async_session
    from app.database.models import User
    from sqlalchemy import update
    async with async_session() as session:
        result = await session.execute(
            update(User).where(User.username == username).values(password_hash=pwd_context.hash(new_password))
        )
        await session.commit()
        return result.rowcount > 0


# ── Public API (all async) ────────────────────────────
def verify_password(plain, hashed):
    return pwd_context.verify(plain, hashed)


def hash_password(password):
    return pwd_context.hash(password)


async def authenticate_user(username, password):
    if _use_db:
        try:
            return await _db_authenticate(username, password)
        except Exception as e:
            logger.warning(f"DB auth failed, falling back to JSON: {e}")

    users = _load_users()
    user = users.get(username)
    if not user or not verify_password(password, user["password"]):
        return None
    return user


def create_token(username, role="user"):
    expire = datetime.utcnow() + timedelta(hours=TOKEN_EXPIRE_HOURS)
    return jwt.encode({"sub": username, "role": role, "exp": expire}, SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token):
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        return None


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if not credentials:
        raise HTTPException(status_code=401, detail="Not authenticated")

    payload = decode_token(credentials.credentials)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    username = payload.get("sub")

    if _use_db:
        user = await _db_get_user(username)
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        return {"username": user.username, "name": user.name or username, "role": user.role or "user"}

    users = _load_users()
    user = users.get(username)
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return {"username": username, "name": user.get("name", username), "role": user.get("role", "user")}


async def get_optional_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if not credentials:
        return None
    payload = decode_token(credentials.credentials)
    if not payload:
        return None
    username = payload.get("sub")

    if _use_db:
        user = await _db_get_user(username)
        if not user:
            return None
        return {"username": user.username, "name": user.name or username, "role": user.role or "user"}

    users = _load_users()
    user = users.get(username)
    if not user:
        return None
    return {"username": username, "name": user.get("name", username), "role": user.get("role", "user")}


# ── User Management (all async) ──────────────────────
def _validate_password(password: str):
    if len(password) < MIN_PASSWORD_LENGTH:
        raise ValueError(f"Password must be at least {MIN_PASSWORD_LENGTH} characters")
    if not any(c.isupper() for c in password):
        raise ValueError("Password must contain at least one uppercase letter")
    if not any(c.isdigit() for c in password):
        raise ValueError("Password must contain at least one digit")


async def create_user(username, password, name, role="user"):
    _validate_password(password)
    if _use_db:
        try:
            return await _db_create_user(username, password, name, role)
        except Exception as e:
            logger.warning(f"DB create user failed: {e}")

    users = _load_users()
    if username in users:
        return None
    users[username] = {
        "username": username,
        "password": hash_password(password),
        "name": name,
        "role": role,
        "created": now_cst().isoformat(),
    }
    _save_users(users)
    return users[username]


async def list_users():
    if _use_db:
        try:
            return await _db_list_users()
        except Exception as e:
            logger.warning(f"DB list users failed: {e}")

    users = _load_users()
    return [
        {"username": u["username"], "name": u.get("name", ""), "role": u.get("role", "user"), "created": u.get("created", "")}
        for u in users.values()
    ]


async def delete_user(username):
    if _use_db:
        try:
            return await _db_delete_user(username)
        except Exception as e:
            logger.warning(f"DB delete user failed: {e}")

    users = _load_users()
    if username not in users or username == "admin":
        return False
    del users[username]
    _save_users(users)
    return True


async def change_password(username, new_password):
    _validate_password(new_password)
    if _use_db:
        try:
            return await _db_change_password(username, new_password)
        except Exception as e:
            logger.warning(f"DB change password failed: {e}")

    users = _load_users()
    if username not in users:
        return False
    users[username]["password"] = hash_password(new_password)
    _save_users(users)
    return True
