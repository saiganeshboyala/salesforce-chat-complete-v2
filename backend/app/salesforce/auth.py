import httpx, logging
from dataclasses import dataclass
from app.config import settings

logger = logging.getLogger(__name__)

@dataclass
class SalesforceCredentials:
    access_token: str
    instance_url: str
    token_type: str = "Bearer"
    issued_at: str | None = None

_stored_credentials = None

def store_credentials(creds):
    global _stored_credentials
    _stored_credentials = creds

async def login_client_credentials():
    cid = settings.salesforce_client_id
    csecret = settings.salesforce_client_secret
    url = settings.salesforce_login_url
    if not cid or not csecret:
        raise Exception("SALESFORCE_CLIENT_ID and SALESFORCE_CLIENT_SECRET required")
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(f"{url}/services/oauth2/token", data={"grant_type": "client_credentials", "client_id": cid, "client_secret": csecret})
        if resp.status_code != 200:
            raise Exception(f"Auth failed ({resp.status_code}): {resp.text[:300]}")
        data = resp.json()
        creds = SalesforceCredentials(access_token=data["access_token"], instance_url=data["instance_url"])
        store_credentials(creds)
        logger.info(f"Authenticated: {creds.instance_url}")
        return creds

async def ensure_authenticated():
    if _stored_credentials and _stored_credentials.access_token:
        return _stored_credentials
    if settings.salesforce_access_token:
        creds = SalesforceCredentials(access_token=settings.salesforce_access_token, instance_url=settings.salesforce_instance_url)
        store_credentials(creds)
        return creds
    return await login_client_credentials()
