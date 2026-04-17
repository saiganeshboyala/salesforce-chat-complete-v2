"""
User Authentication — JWT tokens + per-user data storage.

Users are stored in a JSON file (no database needed).
Each user gets their own chat history and learning memory.

Default admin: admin / admin123 (change on first login)
"""
import json, logging
from datetime import datetime, timedelta
from pathlib import Path
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from app.config import settings

logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────
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


# ── User Storage ───────────────────────────────────────
def _load_users():
    if USERS_FILE.exists():
        with open(USERS_FILE) as f:
            return json.load(f)
    # Create default admin user
    default = {
        "admin": {
            "username": "admin",
            "password": pwd_context.hash("admin123"),
            "name": "Admin",
            "role": "admin",
            "created": datetime.now().isoformat(),
        }
    }
    _save_users(default)
    return default


def _save_users(users):
    USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(USERS_FILE, "w") as f:
        json.dump(users, f, indent=2, default=str)


# ── Auth Functions ─────────────────────────────────────
def verify_password(plain, hashed):
    return pwd_context.verify(plain, hashed)


def hash_password(password):
    return pwd_context.hash(password)


def authenticate_user(username, password):
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
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """FastAPI dependency — extracts user from JWT token."""
    if not credentials:
        raise HTTPException(status_code=401, detail="Not authenticated")

    payload = decode_token(credentials.credentials)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    username = payload.get("sub")
    users = _load_users()
    user = users.get(username)
    if not user:
        raise HTTPException(status_code=401, detail="User not found")

    return {
        "username": username,
        "name": user.get("name", username),
        "role": user.get("role", "user"),
    }


async def get_optional_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Same as get_current_user but returns None instead of 401."""
    if not credentials:
        return None
    payload = decode_token(credentials.credentials)
    if not payload:
        return None
    username = payload.get("sub")
    users = _load_users()
    user = users.get(username)
    if not user:
        return None
    return {"username": username, "name": user.get("name", username), "role": user.get("role", "user")}


# ── User Management ───────────────────────────────────
def _validate_password(password: str):
    if len(password) < MIN_PASSWORD_LENGTH:
        raise ValueError(f"Password must be at least {MIN_PASSWORD_LENGTH} characters")
    if not any(c.isupper() for c in password):
        raise ValueError("Password must contain at least one uppercase letter")
    if not any(c.isdigit() for c in password):
        raise ValueError("Password must contain at least one digit")


def create_user(username, password, name, role="user"):
    _validate_password(password)
    users = _load_users()
    if username in users:
        return None
    users[username] = {
        "username": username,
        "password": hash_password(password),
        "name": name,
        "role": role,
        "created": datetime.now().isoformat(),
    }
    _save_users(users)
    return users[username]


def list_users():
    users = _load_users()
    return [
        {"username": u["username"], "name": u.get("name", ""), "role": u.get("role", "user"), "created": u.get("created", "")}
        for u in users.values()
    ]


def delete_user(username):
    users = _load_users()
    if username not in users:
        return False
    if username == "admin":
        return False  # can't delete admin
    del users[username]
    _save_users(users)
    return True


def change_password(username, new_password):
    _validate_password(new_password)
    users = _load_users()
    if username not in users:
        return False
    users[username]["password"] = hash_password(new_password)
    _save_users(users)
    return True
