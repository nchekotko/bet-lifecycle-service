"""Settings from environment variables (prefix ``BLS_``)."""

from __future__ import annotations

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="BLS_", env_file=".env", extra="ignore")

    database_url: str = "sqlite+aiosqlite:///./bet_lifecycle.db"
    broker: Literal["memory", "kafka"] = "memory"
    kafka_bootstrap: str = "localhost:19092"
    redis_url: str | None = None
    log_level: str = "INFO"

    # Run outbox relay + settlement consumer + staleness sweep inside the API process.
    # Default for local/memory mode; in docker-compose these run as separate workers.
    embedded_workers: bool = True

    stale_after_seconds: float = 30.0
    failover_after_seconds: float = 20.0
    feed_max_buffer: int = 16

    min_stake: int = 100
    max_stake: int = 100_000_00
    max_payout: int = 1_000_000_00
