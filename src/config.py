"""
Application configuration — single source of truth for all settings.

All settings are loaded from the ``.env`` file in the project root.
The ``.env`` file is the highest-priority source; built-in defaults are
the fallback when a key is absent from the file.

Usage::

    from src.config import settings

    print(settings.pg_host)          # "localhost"
    print(settings.database_url)     # full DSN, assembled if not set explicitly
    print(settings.log_level)        # "INFO"

All config keys map directly to variable names in ``.env`` (case-insensitive).
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple, Type

from pydantic import Field, computed_field, model_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

# Resolve .env relative to this file's parent-parent (project root)
_PROJECT_ROOT = Path(__file__).parent.parent
_ENV_FILE = _PROJECT_ROOT / ".env"


# Typed, validated application settings loaded from the .env file at the project root.
# All fields carry defaults so the app starts without any environment configuration.
class Settings(BaseSettings):
    """
    Typed, validated application settings.

    All fields have defaults so the application starts without any
    environment configuration (useful for tests and local development).
    """

    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",        # silently ignore unknown env vars
        populate_by_name=True,
    )

    # ------------------------------------------------------------------
    # PostgreSQL connection
    # ------------------------------------------------------------------

    # Full DSN takes precedence over individual vars when set.
    database_url: Optional[str] = Field(
        default=None,
        description=(
            "Full PostgreSQL DSN e.g. postgresql://user:pass@host:5432/dbname. "
            "When set, individual PG* vars are ignored."
        ),
    )

    pg_host: str = Field(default="localhost", alias="pghost")
    pg_port: int = Field(default=5432, alias="pgport")
    pg_database: str = Field(default="meter_db", alias="pgdatabase")
    pg_user: str = Field(default="postgres", alias="pguser")
    pg_password: str = Field(default="postgres", alias="pgpassword")

    # DB pool sizing
    pg_min_connections: int = Field(default=2, alias="pg_min_connections")
    pg_max_connections: int = Field(default=10, alias="pg_max_connections")

    # ------------------------------------------------------------------
    # Anthropic / Claude API
    # ------------------------------------------------------------------

    anthropic_api_key: Optional[str] = Field(default=None)

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    log_level: str = Field(default="INFO")
    log_format: str = Field(default="console")   # "json" | "console"

    # ------------------------------------------------------------------
    # SQL output (dry-run)
    # ------------------------------------------------------------------

    enable_sql_output: bool = Field(default=False)
    sql_output_path: str = Field(default="output/meter_readings.sql")

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    metrics_port: int = Field(default=0)   # 0 = disabled

    # ------------------------------------------------------------------
    # Notifications
    # ------------------------------------------------------------------

    notification_webhook_url: Optional[str] = Field(default=None)

    # ------------------------------------------------------------------
    # NEM12 processing
    # ------------------------------------------------------------------

    nem12_queue_max_size: int = Field(default=100)
    nem12_stop_timeout_seconds: float = Field(default=10.0)

    # ------------------------------------------------------------------
    # Computed helpers
    # ------------------------------------------------------------------

    @computed_field  # type: ignore[misc]
    @property
    def effective_database_url(self) -> str:
        """
        Return the DSN to use for database connections.

        If ``DATABASE_URL`` is explicitly set, return it as-is.
        Otherwise assemble from individual ``PG*`` fields.
        """
        if self.database_url:
            return self.database_url
        return (
            f"postgresql://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_database}"
        )

    # ------------------------------------------------------------------
    # Source priority: .env file wins over OS environment variables
    # ------------------------------------------------------------------

    # Overrides the Pydantic-settings source priority so .env file wins over OS env vars.
    # This ensures local developer overrides in .env always take precedence at runtime.
    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        # dotenv (.env) has highest priority; OS env vars are a fallback
        return dotenv_settings, env_settings, init_settings, file_secret_settings

    @model_validator(mode="after")
    def _validate_log_level(self) -> "Settings":
        valid = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if self.log_level.upper() not in valid:
            raise ValueError(f"log_level must be one of {valid}")
        self.log_level = self.log_level.upper()
        return self

    @model_validator(mode="after")
    def _validate_log_format(self) -> "Settings":
        if self.log_format.lower() not in {"json", "console"}:
            raise ValueError("log_format must be 'json' or 'console'")
        self.log_format = self.log_format.lower()
        return self


# ---------------------------------------------------------------------------
# Module-level singleton — import this in all other modules
# ---------------------------------------------------------------------------

settings: Settings = Settings()
