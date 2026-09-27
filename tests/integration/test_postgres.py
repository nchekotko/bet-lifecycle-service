"""Same invariants as the SQLite suite, but with real row locks and concurrency."""

from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncIterator

import pytest

from bet_lifecycle.bootstrap import Container
from bet_lifecycle.domain.errors import InsufficientFunds
from tests.application.test_chaos import baseline, build, results, snapshot
from tests.factories import make_container, open_event, place
from tests.integration.conftest import require_postgres

pytestmark = pytest.mark.integration


@pytest.fixture
async def pg() -> AsyncIterator[Container]:
    c = make_container(url=require_postgres())
    await c.db.drop_schema()
    await c.start()
    try:
        yield c
    finally:
        await c.stop()


async def test_concurrent_bets_do_not_overdraw_with_row_locks(pg: Container) -> None:
    event_id, _ = await open_event(pg)
    await pg.wallets.deposit("p1", 50_00, "dep-00000001")

    async def attempt(i: int) -> bool:
        try:
            await place(pg, "p1", f"{event_id}.1x2", "home", 10_00, key=f"k-{i:04d}")
            return True
        except InsufficientFunds:
            return False

    accepted = await asyncio.gather(*(attempt(i) for i in range(20)))
    assert sum(accepted) == 5
    assert (await pg.wallets.balances("p1"))["cash"] == 0
    assert (await pg.reconciliation.run()).ok


async def test_concurrent_duplicate_settlement_on_postgres(pg: Container) -> None:
    expected = await baseline()
    event_ids = await build(pg)
    deliveries = [env for env in results(event_ids) for _ in range(5)]
    random.Random(7).shuffle(deliveries)
    await asyncio.gather(*(pg.settlement.handle(env) for env in deliveries))
    assert await snapshot(pg, event_ids) == expected
    assert (await pg.reconciliation.run()).ok
