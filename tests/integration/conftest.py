"""Integration tests against real PostgreSQL / Redpanda / Redis.

Start them with ``docker compose up -d postgres redpanda redis`` and run
``uv run pytest -m integration``. When a service is not reachable the tests are
skipped, so the default suite stays green without Docker.
"""

from __future__ import annotations

import os
import socket
from urllib.parse import urlparse

import pytest

PG_URL = os.getenv("BLS_TEST_POSTGRES_URL", "postgresql+asyncpg://bls:bls@localhost:5432/bls")
KAFKA = os.getenv("BLS_TEST_KAFKA", "localhost:19092")
REDIS_URL = os.getenv("BLS_TEST_REDIS_URL", "redis://localhost:6379/0")


def reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _url_hostport(url: str, default_port: int) -> tuple[str, int]:
    parsed = urlparse(url.replace("+asyncpg", ""))
    return parsed.hostname or "localhost", parsed.port or default_port


def require_postgres() -> str:
    if not reachable(*_url_hostport(PG_URL, 5432)):
        pytest.skip(f"PostgreSQL not reachable at {PG_URL}")
    return PG_URL


def require_kafka() -> str:
    host, _, port = KAFKA.partition(":")
    if not reachable(host, int(port or 9092)):
        pytest.skip(f"Kafka/Redpanda not reachable at {KAFKA}")
    return KAFKA


def require_redis() -> str:
    if not reachable(*_url_hostport(REDIS_URL, 6379)):
        pytest.skip(f"Redis not reachable at {REDIS_URL}")
    return REDIS_URL
