from __future__ import annotations

import json

from bet_lifecycle.application.consumer import ConsumerRunner, RetryPolicy
from bet_lifecycle.application.ports import FixedClock, Record
from bet_lifecycle.bootstrap import Container
from bet_lifecycle.messaging.envelopes import TOPIC_MARKETS, TOPIC_RESULTS, Envelope
from bet_lifecycle.messaging.memory import InMemoryBroker
from tests.factories import FAST_RETRY, open_event


async def test_outbox_publishes_in_order_once(container: Container) -> None:
    await open_event(container)
    published = await container.outbox.run_once()
    assert published > 0
    assert await container.outbox.run_once() == 0
    assert container.broker is not None
    ids = [json.loads(r.value)["message_id"] for r in container.broker.messages(TOPIC_MARKETS)]
    async with container.db.transaction() as tx:
        rows = await tx.outbox.all()
    assert ids == [r["message_id"] for r in rows if r["topic"] == TOPIC_MARKETS]
    assert all(r["published_at"] is not None for r in rows)


async def test_broker_outage_keeps_rows_and_backs_off(
    container: Container, clock: FixedClock
) -> None:
    await open_event(container)
    assert container.broker is not None
    container.broker.fail_next_publishes = 1
    assert await container.outbox.run_once() == 0
    async with container.db.transaction() as tx:
        rows = await tx.outbox.all()
        assert await tx.outbox.pending_count() == len(rows)
    assert rows[0]["attempts"] == 1 and "outage" in rows[0]["last_error"]
    assert await container.outbox.run_once() == 0  # still in backoff window
    clock.advance(seconds=1)
    assert await container.outbox.run_once() == len(rows)
    assert [
        json.loads(r.value)["message_id"] for r in container.broker.messages(TOPIC_MARKETS)
    ] == [r["message_id"] for r in rows]


def _runner(
    broker: InMemoryBroker, handler: object, policy: RetryPolicy = FAST_RETRY
) -> ConsumerRunner:
    async def no_sleep(_: float) -> None:
        return None

    return ConsumerRunner(
        "test",
        broker.source("g", ["in"]),
        handler,
        broker,  # type: ignore[arg-type]
        policy,
        sleep=no_sleep,
    )


async def test_poison_message_goes_to_dlq_without_retries() -> None:
    broker = InMemoryBroker()
    await broker.publish("in", "k", b"garbage", {"h": "1"})
    calls = 0

    async def handler(rec: Record) -> None:
        nonlocal calls
        calls += 1
        Envelope.from_bytes(rec.value)

    runner = _runner(broker, handler)
    await runner.run_once()
    assert calls == 1
    (dead,) = broker.messages("in.dlq")
    assert dead.value == b"garbage"
    assert dead.headers["x-original-offset"] == "0" and "PoisonMessage" in dead.headers["x-error"]
    assert await runner.update_lag() == {("in", 0): 0}


async def test_transient_error_is_retried_then_succeeds() -> None:
    broker = InMemoryBroker()
    await broker.publish("in", "k", b"x")
    attempts = 0

    async def handler(_: Record) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionError("db restarted")

    await _runner(broker, handler).run_once()
    assert attempts == 3 and broker.messages("in.dlq") == []


async def test_exhausted_retries_dead_letter_and_unblock_partition() -> None:
    broker = InMemoryBroker()
    await broker.publish("in", "k", b"bad")
    await broker.publish("in", "k", b"good")
    seen: list[bytes] = []

    async def handler(rec: Record) -> None:
        if rec.value == b"bad":
            raise TimeoutError("still down")
        seen.append(rec.value)

    await _runner(broker, handler).run_once()
    assert seen == [b"good"]
    assert broker.messages("in.dlq")[0].headers["x-attempts"] == "3"


async def test_uncommitted_records_are_redelivered_after_crash() -> None:
    broker = InMemoryBroker()
    await broker.publish(TOPIC_RESULTS, "k", b"1")
    src = broker.source("g", [TOPIC_RESULTS])
    assert len(await src.poll(10, 0)) == 1
    assert await src.poll(10, 0) == []
    src.rewind()
    (rec,) = await src.poll(10, 0)
    await src.commit(rec)
    src.rewind()
    assert await src.poll(10, 0) == []
    assert await src.lag() == {(TOPIC_RESULTS, 0): 0}


def test_retry_policy_backoff_is_capped() -> None:
    p = RetryPolicy(base_delay=0.1, max_delay=1.0, jitter=0.0)
    assert [p.delay(n) for n in (1, 2, 3, 10)] == [0.1, 0.2, 0.4, 1.0]
