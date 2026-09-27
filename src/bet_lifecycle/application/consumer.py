"""Generic consumer loop: retry policy, dead-letter queue, commit-after-process.

For every record:

* success -> commit the offset;
* non-retryable error (poison message, schema error, domain rejection) -> publish
  the original bytes to ``<topic>.dlq`` with error headers, then commit;
* retryable error (DB/broker hiccup) -> retry in-process with exponential backoff
  and jitter; after ``max_attempts`` the record goes to the DLQ as well so that one
  bad record cannot stall the partition forever.

The offset is committed only after the handler's transaction committed, hence
at-least-once delivery. Handlers are responsible for idempotency (inbox, cursors).
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from bet_lifecycle.application.ports import MessagePublisher, MessageSource, Record
from bet_lifecycle.domain.errors import DomainError
from bet_lifecycle.feed.providers import FeedSchemaError
from bet_lifecycle.messaging.envelopes import PoisonMessage, dlq_topic
from bet_lifecycle.observability.logging import get_logger
from bet_lifecycle.observability.metrics import CONSUMER_LAG, CONSUMER_MESSAGES_TOTAL

log = get_logger(__name__)

Handler = Callable[[Record], Awaitable[object]]
Sleep = Callable[[float], Awaitable[None]]

NON_RETRYABLE: tuple[type[BaseException], ...] = (PoisonMessage, FeedSchemaError, DomainError)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 5
    base_delay: float = 0.1
    max_delay: float = 5.0
    jitter: float = 0.2

    def delay(self, attempt: int) -> float:
        d = min(self.base_delay * 2 ** (attempt - 1), self.max_delay)
        return d * (1 + random.uniform(-self.jitter, self.jitter))


class ConsumerRunner:
    def __init__(
        self,
        name: str,
        source: MessageSource,
        handler: Handler,
        dlq: MessagePublisher,
        retry: RetryPolicy | None = None,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self.name = name
        self.source = source
        self.handler = handler
        self.dlq = dlq
        self.retry = retry or RetryPolicy()
        self.sleep = sleep

    async def process(self, record: Record) -> str:
        attempt = 0
        while True:
            attempt += 1
            try:
                await self.handler(record)
                result = "ok"
                break
            except NON_RETRYABLE as exc:
                await self._dead_letter(record, exc, attempt)
                result = "dlq"
                break
            except Exception as exc:
                if attempt >= self.retry.max_attempts:
                    await self._dead_letter(record, exc, attempt)
                    result = "dlq"
                    break
                delay = self.retry.delay(attempt)
                log.warning(
                    "handler failed, retrying",
                    extra={
                        "consumer": self.name,
                        "topic": record.topic,
                        "offset": record.offset,
                        "attempt": attempt,
                        "delay": delay,
                        "error": repr(exc),
                    },
                )
                await self.sleep(delay)
        await self.source.commit(record)
        CONSUMER_MESSAGES_TOTAL.labels(self.name, result).inc()
        return result

    async def _dead_letter(self, record: Record, exc: BaseException, attempts: int) -> None:
        headers = dict(record.headers) | {
            "x-error": f"{type(exc).__name__}: {exc}"[:500],
            "x-attempts": str(attempts),
            "x-original-topic": record.topic,
            "x-original-partition": str(record.partition),
            "x-original-offset": str(record.offset),
            "x-consumer": self.name,
        }
        await self.dlq.publish(dlq_topic(record.topic), record.key, record.value, headers)
        log.error(
            "message dead-lettered",
            extra={
                "consumer": self.name,
                "topic": record.topic,
                "offset": record.offset,
                "error": repr(exc),
            },
        )

    async def update_lag(self) -> dict[tuple[str, int], int]:
        lag = await self.source.lag()
        for (topic, partition), value in lag.items():
            CONSUMER_LAG.labels(self.name, topic, str(partition)).set(value)
        return lag

    async def run_once(self, max_records: int = 100, timeout: float = 0.0) -> int:
        records = await self.source.poll(max_records, timeout)
        for record in records:
            await self.process(record)
        await self.update_lag()
        return len(records)

    async def run_forever(self, stop: asyncio.Event, poll_timeout: float = 0.5) -> None:
        while not stop.is_set():
            try:
                await self.run_once(timeout=poll_timeout)
            except Exception:
                # Source-level failure (e.g. broker connection); back off and resume.
                log.exception("consumer loop error", extra={"consumer": self.name})
                await self.sleep(1.0)
