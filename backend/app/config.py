from pydantic_settings import BaseSettings
from pathlib import Path

class Settings(BaseSettings):
    # Salesforce
    salesforce_instance_url: str = ""
    salesforce_access_token: str = ""
    salesforce_api_version: str = "v59.0"
    salesforce_client_id: str = ""
    salesforce_client_secret: str = ""
    salesforce_login_url: str = "https://login.salesforce.com"

    # AI API keys (tries in order: Claude → Grok → OpenAI)
    anthropic_api_key: str = ""
    grok_api_key: str = ""
    openai_api_key: str = ""

    # AI model selection
    claude_model: str = "claude-sonnet-4-20250514"
    grok_model: str = "grok-3-mini-fast"
    openai_model: str = "gpt-4o"

    # Embedding model (for RAG semantic search)
    embedding_model: str = "text-embedding-3-small"
    embedding_dimensions: int = 1536

    # Storage
    data_dir: str = str(Path(__file__).parent.parent / "data")

    # Limits
    max_records_per_object: int = 100000
    embedding_batch_size: int = 100

    # Auto-sync schedule
    auto_sync_time: str = "02:00"

    # Auth
    jwt_secret: str = ""

    # Google connectors (Gmail + Sheets + Calendar share one OAuth token)
    google_client_id: str = ""
    google_client_secret: str = ""
    google_redirect_uri: str = "http://localhost:8000/api/connectors/google/callback"

    # Slack connector
    slack_client_id: str = ""
    slack_client_secret: str = ""
    slack_redirect_uri: str = "http://localhost:8000/api/connectors/slack/callback"

    # Public origin (used to redirect back to frontend after OAuth)
    frontend_origin: str = "http://localhost:5173"

    # Server
    cors_origins: list[str] = ["http://localhost:5173", "http://localhost:8000"]

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()
