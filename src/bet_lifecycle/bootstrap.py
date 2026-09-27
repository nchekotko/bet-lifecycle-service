"""Composition root: wires adapters into use cases for the API and the workers."""

from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import timedelta
from typing import Any

from bet_lifecycle.application.bets import BetService, WalletService
from bet_lifecycle.application.consumer import ConsumerRunner, RetryPolicy
from bet_lifecycle.application.ingestion import FeedIngestionService
from bet_lifecycle.application.markets import MarketService
from bet_lifecycle.application.outbox import OutboxRelay
from bet_lifecycle.application.ports import (
    Cache,
    Clock,
    DistributedLock,
    MessagePublisher,
    MessageSource,
    Record,
    SystemClock,
)
from bet_lifecycle.application.reconciliation import ReconciliationService
from bet_lifecycle.application.settlement import SettlementService
from bet_lifecycle.config import Settings
from bet_lifecycle.domain.bets import StakeLimits
from bet_lifecycle.feed.providers import NORMALIZERS, FeedSchemaError
from bet_lifecycle.infrastructure.cache import InMemoryCache, InMemoryLock, RedisCache, RedisLock
from bet_lifecycle.infrastructure.database import Database
from bet_lifecycle.messaging.envelopes import FEED_TOPIC_PREFIX, TOPIC_RESULTS, Envelope
from bet_lifecycle.messaging.memory import InMemoryBroker
from bet_lifecycle.observability.logging import get_logger

log = get_logger(__name__)

FEED_TOPICS = [FEED_TOPIC_PREFIX + p for p in NORMALIZERS]


class Container:
    def __init__(
        self,
        settings: Settings,
        *,
        clock: Clock | None = None,
        broker: InMemoryBroker | None = None,
    ) -> None:
        self.settings = settings
        self.clock: Clock = clock or SystemClock()
        self.db = Database(settings.database_url)
        self._redis: Any = None
        self.cache: Cache
        self.lock: DistributedLock
        if settings.redis_url:
            from redis.asyncio import Redis

            self._redis = Redis.from_url(settings.redis_url)
            self.cache, self.lock = RedisCache(self._redis), RedisLock(self._redis)
        else:
            self.cache, self.lock = InMemoryCache(), InMemoryLock()

        self.broker: InMemoryBroker | None = None
        self._kafka_publisher: Any = None
        self.publisher: MessagePublisher
        if settings.broker == "kafka":
            from bet_lifecycle.messaging.kafka import KafkaPublisher

            self._kafka_publisher = KafkaPublisher(settings.kafka_bootstrap)
            self.publisher = self._kafka_publisher
        else:
            self.broker = broker or InMemoryBroker()
            self.publisher = self.broker

        limits = StakeLimits(settings.min_stake, settings.max_stake, settings.max_payout)
        self.ingestion = FeedIngestionService(
            self.db,
            self.clock,
            self.cache,
            failover_after=timedelta(seconds=settings.failover_after_seconds),
            max_buffer=settings.feed_max_buffer,
        )
        self.bets = BetService(self.db, self.clock, limits)
        self.wallets = WalletService(self.db, self.clock)
        self.markets = MarketService(self.db, self.clock, self.cache, self.lock)
        self.settlement = SettlementService(self.db, self.clock)
        self.reconciliation = ReconciliationService(self.db, self.clock)
        self.outbox = OutboxRelay(self.db, self.publisher, self.clock)
        self._sources: list[Any] = []

    async def start(self) -> None:
        await self.db.create_schema()
        if self._kafka_publisher is not None:
            await self._kafka_publisher.start()

    async def stop(self) -> None:
        for s in self._sources:
            if hasattr(s, "stop"):
                await s.stop()
        if self._kafka_publisher is not None:
            await self._kafka_publisher.stop()
        if self._redis is not None:
            await self._redis.aclose()
        await self.db.dispose()

    async def source(self, group: str, topics: list[str]) -> MessageSource:
        if self.broker is not None:
            return self.broker.source(group, topics)
        from bet_lifecycle.messaging.kafka import KafkaSource

        src = KafkaSource(self.settings.kafka_bootstrap, group, topics)
        await src.start()
        self._sources.append(src)
        return src

    # ------------------------------------------------------------- consumers
    async def _settlement_handler(self, record: Record) -> None:
        await self.settlement.handle(Envelope.from_bytes(record.value))

    async def _feed_handler(self, record: Record) -> None:
        provider = record.topic.removeprefix(FEED_TOPIC_PREFIX)
        try:
            raw = json.loads(record.value)
        except ValueError as exc:
            raise FeedSchemaError(f"invalid JSON: {exc}") from exc
        await self.ingestion.ingest(provider, raw)

    async def settlement_consumer(self, retry: RetryPolicy | None = None) -> ConsumerRunner:
        src = await self.source("settlement", [TOPIC_RESULTS])
        return ConsumerRunner("settlement", src, self._settlement_handler, self.publisher, retry)

    async def feed_consumer(self, retry: RetryPolicy | None = None) -> ConsumerRunner:
        src = await self.source("feed-ingestion", FEED_TOPICS)
        return ConsumerRunner("feed", src, self._feed_handler, self.publisher, retry)

    async def staleness_loop(self, stop: asyncio.Event, interval: float = 5.0) -> None:
        stale_after = timedelta(seconds=self.settings.stale_after_seconds)
        while not stop.is_set():
            try:
                await self.markets.suspend_stale(stale_after)
            except Exception:
                log.exception("staleness sweep failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), interval)

    async def run_embedded_workers(self, stop: asyncio.Event) -> list[asyncio.Task[None]]:
        settlement = await self.settlement_consumer()
        tasks = [
            asyncio.create_task(self.outbox.run_forever(stop), name="outbox-relay"),
            asyncio.create_task(settlement.run_forever(stop), name="settlement-consumer"),
            asyncio.create_task(self.staleness_loop(stop), name="staleness-sweep"),
        ]
        if self.settings.broker == "kafka":
            feed = await self.feed_consumer()
            tasks.append(asyncio.create_task(feed.run_forever(stop), name="feed-consumer"))
        return tasks
