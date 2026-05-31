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
    # requires "require" (or stricter). Valid values:
    #   disable | allow | prefer | require | verify-ca | verify-full
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
    # Admin endpoints (model reload) are gated by this key passed as X-API-Key.
    # When unset, the admin router refuses every request — fail closed.
    api_admin_key: str = Field("", alias="API_ADMIN_KEY")

    # --- v0.9.0 auth (docs/auth_design.md) ---
    # Enforcement mode for the JWT middleware.
    #   off       — middleware does nothing (back-compat fallback).
    #   advisory  — middleware verifies tokens when present and logs
    #               missing-or-invalid auth, but does NOT reject.
    #               This is the M3 default: lets us ship the endpoints
    #               and watch what breaks before flipping to enforce.
    #   enforce   — every /v1/* request (except /v1/auth/login) must
    #               carry a valid bearer token. M4 flips here.
    auth_enforcement: str = Field("advisory", alias="AUTH_ENFORCEMENT")

    # HMAC signing key for HS256 access tokens. 256-bit random. MUST be set
    # in production; the empty default is detected by the middleware on
    # startup and refused. Rotation: set JWT_SIGNING_KEY_PREVIOUS to the
    # outgoing key for a ~24h dual-verify window (see §6, §11 of the
    # design doc).
    jwt_signing_key: str = Field("", alias="JWT_SIGNING_KEY")
    jwt_signing_key_previous: str = Field("", alias="JWT_SIGNING_KEY_PREVIOUS")

    # Token TTLs. Defaults follow §6 of the design doc.
    jwt_access_ttl_seconds: int = Field(15 * 60, alias="JWT_ACCESS_TTL_SECONDS")
    jwt_refresh_ttl_seconds: int = Field(
        7 * 24 * 3600, alias="JWT_REFRESH_TTL_SECONDS"
    )

    # Rate limiter for /v1/auth/login. Token-bucket per source IP. The
    # design doc §11 calls for "5 attempts / 15 min"; both knobs are
    # tunable so tests can crank them down without monkeypatching.
    auth_login_rate_max: int = Field(5, alias="AUTH_LOGIN_RATE_MAX")
    auth_login_rate_window_seconds: int = Field(
        15 * 60, alias="AUTH_LOGIN_RATE_WINDOW_SECONDS"
    )

    # --- Fuel price (dashboards 33/36 value fuel cost in DT/L) ---
    # Provider-agnostic fetcher (src/accent_fleet/ingestion/fuel_price.py)
    # pulls the live diesel price and stores it in warehouse.ref_fuel_price.
    # When the URL is empty the fetcher is disabled and the dashboards use the
    # last stored value (or the seeded STIR reference). The price is a
    # regulated/slow-moving figure, so the refresh runs monthly.
    #   FUEL_PRICE_API_URL       — provider endpoint returning JSON. Empty = off.
    #   FUEL_PRICE_API_KEY       — sent as the Authorization header when set.
    #   FUEL_PRICE_JSON_PATH     — dotted path into the JSON to the numeric price.
    #   FUEL_PRICE_FUEL_TYPE     — registry key ('diesel'); matches the views.
    #   FUEL_PRICE_CURRENCY      — currency label stored alongside the price.
    #   FUEL_PRICE_FALLBACK_PER_LITRE — STIR "gasoil 50" reference, last resort.
    #   FUEL_PRICE_REFRESH_DAYS  — skip the fetch if a live row is newer than this.
    fuel_price_api_url: str = Field("", alias="FUEL_PRICE_API_URL")
    fuel_price_api_key: str = Field("", alias="FUEL_PRICE_API_KEY")
    fuel_price_json_path: str = Field("price", alias="FUEL_PRICE_JSON_PATH")
    fuel_price_fuel_type: str = Field("diesel", alias="FUEL_PRICE_FUEL_TYPE")
    fuel_price_currency: str = Field("DT", alias="FUEL_PRICE_CURRENCY")
    fuel_price_fallback_per_litre: float = Field(
        2.525, alias="FUEL_PRICE_FALLBACK_PER_LITRE"
    )
    fuel_price_refresh_days: int = Field(28, alias="FUEL_PRICE_REFRESH_DAYS")

    @field_validator(
        "pipeline_batch_size",
        "pipeline_overlap_minutes",
        "pipeline_incremental_lookback_minutes",
        "jwt_access_ttl_seconds",
        "jwt_refresh_ttl_seconds",
        "auth_login_rate_max",
        "auth_login_rate_window_seconds",
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
