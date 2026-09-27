from __future__ import annotations

import asyncio
import uuid

import pytest

from bet_lifecycle.infrastructure.cache import RedisCache, RedisLock
from bet_lifecycle.messaging.kafka import KafkaPublisher, KafkaSource
from tests.factories import make_container, open_event, place
from tests.integration.conftest import require_kafka, require_redis

pytestmark = pytest.mark.integration


async def test_kafka_roundtrip_commit_and_lag() -> None:
    bootstrap = require_kafka()
    topic = f"it.{uuid.uuid4().hex[:8]}"
    pub = KafkaPublisher(bootstrap)
    await pub.start()
    try:
        for i in range(3):
            await pub.publish(topic, "k", f"m{i}".encode(), {"n": str(i)})
    finally:
        await pub.stop()
    src = KafkaSource(bootstrap, f"g-{uuid.uuid4().hex[:6]}", [topic])
    await src.start()
    try:
        got = []
        for _ in range(20):
            got += await src.poll(10, 1.0)
            if len(got) >= 3:
                break
        assert [r.value for r in got] == [b"m0", b"m1", b"m2"]
        assert got[1].headers == {"n": "1"}
        for r in got:
            await src.commit(r)
        assert sum((await src.lag()).values()) == 0
    finally:
        await src.stop()


async def test_full_pipeline_through_kafka() -> None:
    """outbox -> Redpanda -> settlement consumer, with SQLite as the database."""
    bootstrap = require_kafka()
    c = make_container(broker="kafka", kafka_bootstrap=bootstrap)
    await c.start()
    try:
        event_id, alpha = await open_event(c, fixture_id=f"A-{uuid.uuid4().hex[:6]}")
        await c.wallets.deposit("p1", 10_00, "dep-00000001")
        await place(c, "p1", f"{event_id}.1x2", "home", 10_00)
        await c.ingestion.ingest("alpha", alpha.result(1, 0))
        while await c.outbox.run_once():
            pass
        runner = await c.settlement_consumer()
        for _ in range(30):
            await runner.run_once(timeout=1.0)
            if (await c.wallets.balances("p1"))["cash"] > 0:
                break
        assert (await c.wallets.balances("p1"))["cash"] == 21_00
    finally:
        await c.stop()


async def test_redis_lock_and_cache() -> None:
    from redis.asyncio import Redis

    redis = Redis.from_url(require_redis())
    try:
        lock, cache = RedisLock(redis), RedisCache(redis)
        name = f"it-{uuid.uuid4().hex[:6]}"
        token = await lock.acquire(name, 5)
        assert token is not None
        assert await lock.acquire(name, 5) is None
        assert not await lock.release(name, "someone-else")
        assert await lock.release(name, token)
        await cache.set(name, "v", 0.2)
        assert await cache.get(name) == "v"
        await asyncio.sleep(0.3)
        assert await cache.get(name) is None
    finally:
        await redis.aclose()
