"""Transactional outbox relay.

Business transactions write events into ``outbox`` in the same DB transaction as the
state change, so "committed but not published" and "published but rolled back" are
impossible. This relay publishes pending rows in id order and marks them published.

* Broker down -> the row stays pending with exponential backoff; the batch stops at the
  first failure so that later events are not published ahead of earlier ones.
* Crash after publish but before marking -> the row is published again on restart:
  delivery is at-least-once, consumers deduplicate by ``message_id`` (inbox).
* Several relay replicas on PostgreSQL use ``FOR UPDATE SKIP LOCKED``.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import timedelta

from bet_lifecycle.application.ports import Clock, MessagePublisher
from bet_lifecycle.infrastructure.database import Database
from bet_lifecycle.observability.logging import get_logger
from bet_lifecycle.observability.metrics import (
    OUTBOX_FAILURES_TOTAL,
    OUTBOX_PENDING,
    OUTBOX_PUBLISHED_TOTAL,
)

log = get_logger(__name__)


class OutboxRelay:
    def __init__(
        self,
        db: Database,
        publisher: MessagePublisher,
        clock: Clock,
        *,
        batch_size: int = 200,
        base_backoff: float = 0.5,
        max_backoff: float = 60.0,
    ) -> None:
        self.db = db
        self.publisher = publisher
        self.clock = clock
        self.batch_size = batch_size
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff

    def backoff(self, attempts: int) -> float:
        return float(min(self.base_backoff * 2 ** (attempts - 1), self.max_backoff))

    async def run_once(self) -> int:
        published = 0
        async with self.db.transaction() as tx:
            rows = await tx.outbox.fetch_pending(self.batch_size)
            for row in rows:
                if row.next_attempt_at > self.clock.now():
                    break  # head of the queue is backing off; keep order
                try:
                    await self.publisher.publish(
                        row.topic,
                        row.key,
                        row.payload.encode(),
                        row.headers | {"message_id": row.message_id},
                    )
                except Exception as exc:
                    attempts = row.attempts + 1
                    retry_at = self.clock.now() + timedelta(seconds=self.backoff(attempts))
                    await tx.outbox.mark_failed(row.id, attempts, retry_at, repr(exc))
                    OUTBOX_FAILURES_TOTAL.inc()
                    log.warning(
                        "outbox publish failed",
                        extra={
                            "outbox_id": row.id,
                            "topic": row.topic,
                            "attempts": attempts,
                            "error": repr(exc),
                        },
                    )
                    break
                await tx.outbox.mark_published(row.id, self.clock.now())
                OUTBOX_PUBLISHED_TOTAL.labels(row.topic).inc()
                published += 1
            OUTBOX_PENDING.set(await tx.outbox.pending_count())
        return published

    async def run_forever(self, stop: asyncio.Event, interval: float = 0.2) -> None:
        while not stop.is_set():
            try:
                n = await self.run_once()
            except Exception:
                log.exception("outbox relay iteration failed")
                n = 0
            if n == 0:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), interval)
