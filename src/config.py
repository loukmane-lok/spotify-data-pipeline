"""Centralized configuration management for the Spotify Data Pipeline.

Loads environment variables from `.env`, validates required values,
and exposes typed configuration objects for each service.

Usage:
    from src.config import kafka_config, s3_config, snowflake_config
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load .env from project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")


def _require_env(key: str) -> str:
    """Retrieve a required environment variable or raise an error.

    Args:
        key: The environment variable name.

    Returns:
        The environment variable value.

    Raises:
        EnvironmentError: If the variable is not set or is empty.
    """
    value = os.getenv(key)
    if not value:
        raise OSError(
            f"Required environment variable '{key}' is not set. "
            f"See .env.example for reference."
        )
    return value


def _get_env(key: str, default: str = "") -> str:
    """Retrieve an optional environment variable with a fallback.

    Args:
        key: The environment variable name.
        default: Fallback value if the variable is not set.

    Returns:
        The environment variable value or the default.
    """
    return os.getenv(key, default)


# ── Kafka Configuration ──────────────────────────────────────────────


@dataclass(frozen=True)
class KafkaConfig:
    """Kafka broker and topic configuration."""

    bootstrap_servers: str = field(
        default_factory=lambda: _require_env("KAFKA_BOOTSTRAP_SERVERS")
    )
    topic: str = field(
        default_factory=lambda: _get_env("KAFKA_TOPIC", "spotify-events")
    )
    group_id: str = field(
        default_factory=lambda: _get_env("KAFKA_GROUP_ID", "spotify-s3-consumer")
    )
    batch_size: int = field(
        default_factory=lambda: int(_get_env("BATCH_SIZE", "10"))
    )


# ── AWS S3 Configuration ─────────────────────────────────────────────


@dataclass(frozen=True)
class S3Config:
    """AWS S3 storage configuration."""

    bucket_name: str = field(
        default_factory=lambda: _require_env("S3_BUCKET_NAME")
    )
    prefix: str = field(
        default_factory=lambda: _get_env("S3_PREFIX", "bronze/")
    )
    region: str = field(
        default_factory=lambda: _get_env("AWS_DEFAULT_REGION", "us-east-1")
    )
    access_key_id: str = field(
        default_factory=lambda: _require_env("AWS_ACCESS_KEY_ID")
    )
    secret_access_key: str = field(
        default_factory=lambda: _require_env("AWS_SECRET_ACCESS_KEY")
    )


# ── Snowflake Configuration ──────────────────────────────────────────


@dataclass(frozen=True)
class SnowflakeConfig:
    """Snowflake data warehouse configuration."""

    user: str = field(default_factory=lambda: _require_env("SNOWFLAKE_USER"))
    password: str = field(
        default_factory=lambda: _require_env("SNOWFLAKE_PASSWORD")
    )
    account: str = field(
        default_factory=lambda: _require_env("SNOWFLAKE_ACCOUNT")
    )
    warehouse: str = field(
        default_factory=lambda: _require_env("SNOWFLAKE_WAREHOUSE")
    )
    database: str = field(
        default_factory=lambda: _require_env("SNOWFLAKE_DATABASE")
    )
    schema: str = field(
        default_factory=lambda: _require_env("SNOWFLAKE_SCHEMA")
    )
    table: str = field(
        default_factory=lambda: _require_env("SNOWFLAKE_TABLE")
    )


# ── Simulator Configuration ──────────────────────────────────────────


@dataclass(frozen=True)
class SimulatorConfig:
    """Event simulator (producer) configuration."""

    user_count: int = field(
        default_factory=lambda: int(_get_env("USER_COUNT", "20"))
    )
    event_interval_seconds: int = field(
        default_factory=lambda: int(_get_env("EVENT_INTERVAL_SECONDS", "1"))
    )


# ── Lazy Singletons ──────────────────────────────────────────────────
# Instantiated on first access. If required env vars are missing,
# the error is raised at import time — fail fast.


def get_kafka_config() -> KafkaConfig:
    """Get Kafka configuration (lazy singleton)."""
    return KafkaConfig()


def get_s3_config() -> S3Config:
    """Get S3 configuration (lazy singleton)."""
    return S3Config()


def get_snowflake_config() -> SnowflakeConfig:
    """Get Snowflake configuration (lazy singleton)."""
    return SnowflakeConfig()


def get_simulator_config() -> SimulatorConfig:
    """Get simulator configuration (lazy singleton)."""
    return SimulatorConfig()
