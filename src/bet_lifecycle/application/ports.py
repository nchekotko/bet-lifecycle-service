"""Ports: what the application layer needs from the outside world.

Adapters: ``bet_lifecycle.messaging.memory`` / ``bet_lifecycle.messaging.kafka`` for
the bus, ``bet_lifecycle.infrastructure.cache`` for cache and locks. Persistence goes
through SQLAlchemy Core (``bet_lifecycle.infrastructure``), which runs on SQLite in
tests/local mode and on PostgreSQL in production.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol


@dataclass(frozen=True, slots=True)
class Record:
    topic: str
    partition: int
    offset: int
    key: str | None
    value: bytes
    headers: dict[str, str] = field(default_factory=dict)


class MessagePublisher(Protocol):
    async def publish(
        self, topic: str, key: str | None, value: bytes, headers: dict[str, str] | None = None
    ) -> None: ...


class MessageSource(Protocol):
    """At-least-once source: records are redelivered until committed."""

    async def poll(self, max_records: int, timeout: float) -> list[Record]: ...

    async def commit(self, record: Record) -> None: ...

    async def lag(self) -> dict[tuple[str, int], int]: ...


class Cache(Protocol):
    async def get(self, key: str) -> str | None: ...

    async def set(self, key: str, value: str, ttl_seconds: float) -> None: ...

    async def delete(self, *keys: str) -> None: ...


class DistributedLock(Protocol):
    async def acquire(self, name: str, ttl_seconds: float) -> str | None:
        """Return an ownership token, or None if someone else holds the lock."""
        ...

    async def release(self, name: str, token: str) -> bool: ...


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class FixedClock:
    """Manually advanced clock for tests and deterministic simulations."""

    def __init__(self, start: datetime) -> None:
        self._now = start

    def now(self) -> datetime:
        return self._now

    def advance(self, **kwargs: float) -> None:
        from datetime import timedelta

        self._now += timedelta(**kwargs)
