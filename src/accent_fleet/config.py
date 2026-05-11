"""
Central configuration module.

Loads:
  - Infrastructure settings (DB, Kafka) from .env via pydantic-settings
  - Pipeline behaviour from config/pipeline.yaml
  - Cleaning rules from config/cleaning_rules.yaml
  - Feature definitions from config/feature_definitions.yaml

Everything is typed and validated at startup. A config error fails fast,
before any DB connection is attempted.
"""

from __future__ import annotations

import hashlib
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import yaml
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


# Project root = two levels up from this file (src/accent_fleet/config.py)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"
SQL_DIR = PROJECT_ROOT / "sql"


# -----------------------------------------------------------------------------
# Infrastructure settings (from .env)
# -----------------------------------------------------------------------------
class Settings(BaseSettings):
    """Environment-driven settings. Loaded once at startup."""

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- PostgreSQL ---
    pg_host: str = Field("localhost", alias="PG_HOST")
    pg_port: int = Field(5432, alias="PG_PORT")
    pg_database: str = Field("accent_fleet", alias="PG_DATABASE")
    pg_user: str = Field("accent", alias="PG_USER")
    pg_password: str = Field("change_me", alias="PG_PASSWORD")
    pg_schema_staging: str = Field("staging", alias="PG_SCHEMA_STAGING")
    pg_schema_warehouse: str = Field("warehouse", alias="PG_SCHEMA_WAREHOUSE")
    pg_schema_marts: str = Field("marts", alias="PG_SCHEMA_MARTS")
    # SSL mode for psycopg. "prefer" works locally; Azure Database for PostgreSQL
    # requires "require" (or stricter). Valid values: disable|allow|prefer|require|verify-ca|verify-full
    pg_sslmode: str = Field("prefer", alias="PG_SSLMODE")

    # --- Pipeline ---
    pipeline_env: str = Field("dev", alias="PIPELINE_ENV")
    pipeline_log_level: str = Field("INFO", alias="PIPELINE_LOG_LEVEL")
    pipeline_batch_size: int = Field(50_000, alias="PIPELINE_BATCH_SIZE")
    pipeline_overlap_minutes: int = Field(10, alias="PIPELINE_OVERLAP_MINUTES")
    pipeline_incremental_lookback_minutes: int = Field(
        5, alias="PIPELINE_INCREMENTAL_LOOKBACK_MINUTES"
    )

    # --- Kafka (future streaming) ---
    kafka_bootstrap_servers: str = Field("localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_path: str = Field("fleet.path.v1", alias="KAFKA_TOPIC_PATH")
    kafka_topic_stop: str = Field("fleet.stop.v1", alias="KAFKA_TOPIC_STOP")
    kafka_topic_overspeed: str = Field("fleet.overspeed.v1", alias="KAFKA_TOPIC_OVERSPEED")
    kafka_consumer_group: str = Field("accent-fleet-phase3", alias="KAFKA_CONSUMER_GROUP")

    # --- v0.5 services ---
    mlflow_tracking_uri: str = Field("http://localhost:5000", alias="MLFLOW_TRACKING_URI")
    mlflow_experiment_name: str = Field(
        "device-behavior-clustering", alias="MLFLOW_EXPERIMENT_NAME"
    )
    mlflow_model_name: str = Field("device-clustering", alias="MLFLOW_MODEL_NAME")
    mlflow_model_stage: str = Field("Production", alias="MLFLOW_MODEL_STAGE")

    api_host: str = Field("0.0.0.0", alias="API_HOST")
    api_port: int = Field(8000, alias="API_PORT")
    api_base_url: str = Field("http://localhost:8000", alias="API_BASE_URL")

    dashboard_port: int = Field(8501, alias="DASHBOARD_PORT")
    dashboard_base_url: str = Field("http://localhost:8501", alias="DASHBOARD_BASE_URL")

    @field_validator(
        "pipeline_batch_size",
        "pipeline_overlap_minutes",
        "pipeline_incremental_lookback_minutes",
        mode="before",
    )
    @classmethod
    def _parse_int_with_optional_inline_comment(cls, value: Any) -> Any:
        """
        Accept values like "10 # comment" from env vars and coerce them to ints.
        This guards against editors/tools that inject raw .env lines into process env.
        """
        if isinstance(value, str):
            value = value.split("#", 1)[0].strip()
        return value

    @property
    def sqlalchemy_url(self) -> str:
        """
        Assemble the SQLAlchemy URL for psycopg v3.

        Includes sslmode in the query string so it works against both local
        Postgres and Azure Database for PostgreSQL (which requires TLS).
        """
        user = quote_plus(self.pg_user)
        password = quote_plus(self.pg_password)
        sslmode = quote_plus(self.pg_sslmode)
        return (
            f"postgresql+psycopg://{user}:{password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_database}"
            f"?sslmode={sslmode}"
        )


# -----------------------------------------------------------------------------
# YAML config loaders
# -----------------------------------------------------------------------------
def _load_yaml(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


@lru_cache(maxsize=1)
def load_pipeline_config() -> dict[str, Any]:
    return _load_yaml(CONFIG_DIR / "pipeline.yaml")


@lru_cache(maxsize=1)
def load_cleaning_rules() -> dict[str, Any]:
    return _load_yaml(CONFIG_DIR / "cleaning_rules.yaml")


@lru_cache(maxsize=1)
def load_feature_definitions() -> dict[str, Any]:
    return _load_yaml(CONFIG_DIR / "feature_definitions.yaml")


def config_hash() -> str:
    """
    MD5 of the three config YAMLs, stored on every etl_run_log row.
    Lets us tell later whether any two runs used the same rules/features.
    """
    h = hashlib.md5()
    for name in ("pipeline.yaml", "cleaning_rules.yaml", "feature_definitions.yaml"):
        h.update((CONFIG_DIR / name).read_bytes())
    return h.hexdigest()


@lru_cache(maxsize=1)
def settings() -> Settings:
    """Singleton accessor. Cached so `settings()` is cheap everywhere."""
    return Settings()
