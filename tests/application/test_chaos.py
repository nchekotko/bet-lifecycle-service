"""Chaos test: redeliver every settlement message N times in random order, with
simulated consumer crashes (lost offset commits + rewinds) and concurrent handlers.
Final balances and bet outcomes must equal a clean single, ordered delivery."""

from __future__ import annotations

import asyncio
import random
from typing import Any

import pytest

from bet_lifecycle.application.ports import Record
from bet_lifecycle.bootstrap import Container
from bet_lifecycle.domain.settlement import ResultStatus
from bet_lifecycle.infrastructure import tables as t
from bet_lifecycle.messaging.envelopes import (
    REGISTRY,
    TOPIC_RESULTS,
    Envelope,
    ResultPublishedV1,
    ResultPublishedV2,
)
from bet_lifecycle.messaging.memory import MemorySource
from tests.factories import FAST_RETRY, KICKOFF, make_container, open_event, place

PLAYERS = [f"p{i}" for i in range(6)]
BETS: list[tuple[int, str, str, int]] = [  # (event idx, market suffix, selection, stake)
    (0, "1x2", "home", 10_00),
    (0, "1x2", "draw", 7_00),
    (0, "ah-0.25", "home", 12_00),
    (0, "ah-0.25", "away", 9_00),
    (0, "ou2.25", "over", 15_00),
    (0, "ou2.25", "under", 5_00),
    (1, "1x2", "away", 8_00),
    (1, "ah-0.25", "home", 11_00),
    (1, "ou2.25", "under", 13_00),
    (2, "1x2", "home", 6_00),
    (2, "ou2.25", "over", 20_00),
    (2, "ah-0.25", "away", 4_00),
]


def results(event_ids: list[str]) -> list[Envelope]:
    """Result stream incl. corrections, an abandonment and a legacy v1 producer."""

    def v2(e: int, h: int, a: int, v: int, status: ResultStatus = ResultStatus.FINAL) -> Envelope:
        return REGISTRY.envelope(
            ResultPublishedV2(
                event_id=event_ids[e], home_score=h, away_score=a, status=status, result_version=v
            ),
            key=event_ids[e],
            message_id=f"res-{e}-{v}",
        )

    return [
        v2(0, 1, 1, 1),
        v2(0, 2, 1, 2),  # correction: draw -> home win
        v2(0, 2, 2, 3),  # second correction: back to a draw, different quarter lines
        v2(1, 0, 0, 1),
        v2(1, 0, 0, 2, ResultStatus.ABANDONED),  # abandoned after the fact -> void
        REGISTRY.envelope(
            ResultPublishedV1(event_id=event_ids[2], score="3-2", result_version=1),
            key=event_ids[2],
            message_id="res-2-1-legacy",
        ),
    ]


async def build(c: Container) -> list[str]:
    event_ids = []
    for i, (home, away) in enumerate(
        [("Arsenal", "Chelsea"), ("Inter", "Milan"), ("Porto", "Benfica")]
    ):
        e, _ = await open_event(c, f"A-{i}", home, away, KICKOFF)
        event_ids.append(e)
    for p in PLAYERS:
        await c.wallets.deposit(p, 50_00, f"dep-{p}-000001")
        await c.wallets.grant_bonus(p, 20_00, f"bon-{p}-000001")
    for n, (e, suffix, sel, stake) in enumerate(BETS):
        player = PLAYERS[n % len(PLAYERS)]
        await place(c, player, f"{event_ids[e]}.{suffix}", sel, stake, key=f"bet-{n:04d}")
    return event_ids


async def snapshot(c: Container, event_ids: list[str]) -> dict[str, Any]:
    async with c.db.transaction() as tx:
        balances = await tx.ledger.balances()
        rows = (await tx.conn.execute(t.bets.select())).mappings().all()
    alias = {e: f"E{i}" for i, e in enumerate(event_ids)}
    bets = {}
    for r in rows:
        ev, suffix = r["market_id"].split(".", 1)
        bets[(r["player_id"], alias[ev], suffix, r["selection"])] = (r["outcome"], r["payout"])
    return {"balances": dict(sorted(balances.items())), "bets": bets}


async def baseline() -> dict[str, Any]:
    c = make_container()
    await c.start()
    try:
        event_ids = await build(c)
        for env in results(event_ids):
            await c.settlement.handle(env)
        return await snapshot(c, event_ids)
    finally:
        await c.stop()


class CrashingSource:
    """Wraps the in-memory source: randomly 'crashes' before committing the offset,
    which rewinds the consumer to the last committed offset (redelivery)."""

    def __init__(self, inner: MemorySource, rng: random.Random, crash_p: float) -> None:
        self.inner, self.rng, self.crash_p = inner, rng, crash_p
        self.crashes = 0

    async def poll(self, max_records: int, timeout: float) -> list[Record]:
        return await self.inner.poll(self.rng.randint(1, 4), timeout)

    async def commit(self, record: Record) -> None:
        if self.rng.random() < self.crash_p:
            self.crashes += 1
            self.inner.rewind()
            return
        await self.inner.commit(record)

    async def lag(self) -> dict[tuple[str, int], int]:
        return await self.inner.lag()


@pytest.fixture(scope="module")
def expected() -> dict[str, Any]:
    return asyncio.run(baseline())


@pytest.mark.parametrize("seed", range(6))
async def test_redelivery_and_reordering_do_not_change_money(
    seed: int, expected: dict[str, Any]
) -> None:
    rng = random.Random(seed)
    c = make_container()
    await c.start()
    try:
        event_ids = await build(c)
        envelopes = results(event_ids)
        deliveries = [env for env in envelopes for _ in range(rng.randint(2, 5))]
        rng.shuffle(deliveries)
        assert c.broker is not None
        for env in deliveries:
            await c.broker.publish(TOPIC_RESULTS, env.key, env.to_bytes())

        runner = await c.settlement_consumer(FAST_RETRY)
        assert isinstance(runner.source, MemorySource)
        source = CrashingSource(runner.source, rng, crash_p=0.25)
        runner.source = source
        for _ in range(10_000):
            await runner.run_once()
            if (await source.lag())[(TOPIC_RESULTS, 0)] == 0:
                break
        else:
            pytest.fail("consumer did not converge")

        assert source.crashes > 0
        assert await snapshot(c, event_ids) == expected
        assert (await c.reconciliation.run()).ok
        assert c.broker.messages(TOPIC_RESULTS + ".dlq") == []
    finally:
        await c.stop()


@pytest.mark.parametrize("seed", range(3))
async def test_concurrent_duplicate_handlers(seed: int, expected: dict[str, Any]) -> None:
    rng = random.Random(1000 + seed)
    c = make_container()
    await c.start()
    try:
        event_ids = await build(c)
        deliveries = [env for env in results(event_ids) for _ in range(4)]
        rng.shuffle(deliveries)
        await asyncio.gather(*(c.settlement.handle(env) for env in deliveries))
        assert await snapshot(c, event_ids) == expected
        assert (await c.reconciliation.run()).ok
    finally:
        await c.stop()


async def test_baseline_actually_exercises_resettlement(expected: dict[str, Any]) -> None:
    outcomes = {o for o, _ in expected["bets"].values()}
    assert {"won", "lost", "void", "half_won", "half_lost"} <= outcomes
