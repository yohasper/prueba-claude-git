# ============================================================
# config/settings.py
# Gestión centralizada de configuración con Pydantic Settings
# ============================================================

from pydantic_settings import BaseSettings
from pydantic import Field
from pathlib import Path

# Ruta absoluta al .env — funciona sin importar desde dónde se ejecute
_ENV_FILE = str(Path(__file__).parent.parent / ".env")


class DatabaseSettings(BaseSettings):
    host: str = Field(default="localhost", alias="DB_HOST")
    port: int = Field(default=5432, alias="DB_PORT")
    name: str = Field(default="datawarehouse", alias="DB_NAME")
    user: str = Field(default="dw_user", alias="DB_USER")
    password: str = Field(default="", alias="DB_PASSWORD")
    schema_bronze: str = Field(default="bronze", alias="DB_SCHEMA_BRONZE")

    @property
    def connection_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.name}"
        )

    @property
    def connection_url_safe(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:****"
            f"@{self.host}:{self.port}/{self.name}"
        )

    model_config = {
        "env_file": _ENV_FILE,
        "env_file_encoding": "utf-8",
        "populate_by_name": True,
        "extra": "ignore",
    }


class GoogleDriveSettings(BaseSettings):
    credentials_path: str = Field(
        default="config/credentials/google_credentials.json",
        alias="GOOGLE_CREDENTIALS_PATH"
    )
    token_path: str = Field(
        default="config/credentials/token.json",
        alias="GOOGLE_TOKEN_PATH"
    )
    folder_id: str = Field(default="", alias="GDRIVE_FOLDER_ID")

    scopes: list[str] = [
        "https://www.googleapis.com/auth/drive.readonly",
        "https://www.googleapis.com/auth/spreadsheets.readonly",
    ]

    model_config = {
        "env_file": _ENV_FILE,
        "env_file_encoding": "utf-8",
        "populate_by_name": True,
        "extra": "ignore",
    }


class IngestSettings(BaseSettings):
    batch_size: int = Field(default=1000, alias="BATCH_SIZE")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_path: str = Field(default="logs/", alias="LOG_PATH")
    env: str = Field(default="development", alias="ENV")

    model_config = {
        "env_file": _ENV_FILE,
        "env_file_encoding": "utf-8",
        "populate_by_name": True,
        "extra": "ignore",
    }


class Settings:
    def __init__(self):
        self.db = DatabaseSettings()
        self.gdrive = GoogleDriveSettings()
        self.ingest = IngestSettings()
        self.root_path = Path(__file__).parent.parent

    def __repr__(self):
        return (
            f"Settings("
            f"db={self.db.connection_url_safe}, "
            f"env={self.ingest.env})"
        )


# Instancia global de configuración
settings = Settings()
