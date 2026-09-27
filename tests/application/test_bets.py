from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from bet_lifecycle.application.bets import PlaceBetCommand
from bet_lifecycle.bootstrap import Container
from bet_lifecycle.domain.errors import (
    IdempotencyConflict,
    InsufficientFunds,
    MarketNotOpen,
    NotFound,
    Requote,
    StakeOutOfLimits,
)
from bet_lifecycle.domain.ledger import HOUSE_ESCROW
from bet_lifecycle.messaging.envelopes import TOPIC_BETS
from tests.factories import markets, open_event, place


async def test_bet_reserves_stake_in_escrow(container: Container) -> None:
    event_id, _ = await open_event(container)
    await container.wallets.deposit("p1", 100_00, "dep-00000001")
    body = await place(container, "p1", f"{event_id}.1x2", "home", 25_00)
    assert (body["odds"], body["price_version"], body["status"]) == ("2.1", 1, "open")
    assert await container.wallets.balances("p1") == {"cash": 75_00, "bonus": 0, "total": 75_00}
    async with container.db.transaction() as tx:
        assert await tx.ledger.balance(HOUSE_ESCROW) == 25_00
        assert len(await tx.outbox.all()) > 0
    await container.outbox.run_once()
    assert container.broker is not None
    assert len(container.broker.messages(TOPIC_BETS)) == 1


async def test_idempotency_key_replays_and_detects_conflict(container: Container) -> None:
    event_id, _ = await open_event(container)
    await container.wallets.deposit("p1", 100_00, "dep-00000001")
    cmd = PlaceBetCommand("p1", f"{event_id}.1x2", "home", 10_00, Decimal("2.1"), 1)
    first = await container.bets.place(cmd, "key-000000001")
    again = await container.bets.place(cmd, "key-000000001")
    assert again.replayed and again.body == first.body
    assert (await container.wallets.balances("p1"))["cash"] == 90_00  # charged once
    other = PlaceBetCommand("p1", f"{event_id}.1x2", "home", 20_00, Decimal("2.1"), 1)
    with pytest.raises(IdempotencyConflict):
        await container.bets.place(other, "key-000000001")
    # keys are scoped per player
    await container.wallets.deposit("p2", 100_00, "dep-00000001")
    cmd2 = PlaceBetCommand("p2", f"{event_id}.1x2", "home", 10_00, Decimal("2.1"), 1)
    assert not (await container.bets.place(cmd2, "key-000000001")).replayed


async def test_concurrent_duplicates_create_one_bet(container: Container) -> None:
    event_id, _ = await open_event(container)
    await container.wallets.deposit("p1", 100_00, "dep-00000001")
    cmd = PlaceBetCommand("p1", f"{event_id}.1x2", "home", 10_00, Decimal("2.1"), 1)
    results = await asyncio.gather(*(container.bets.place(cmd, "same-key-0001") for _ in range(10)))
    assert len({r.body["bet_id"] for r in results}) == 1
    assert sum(not r.replayed for r in results) == 1
    assert (await container.wallets.balances("p1"))["cash"] == 90_00


async def test_stale_price_version_requotes(container: Container) -> None:
    event_id, alpha = await open_event(container)
    await container.wallets.deposit("p1", 100_00, "dep-00000001")
    await container.ingestion.ingest("alpha", alpha.odds(("2.00", "3.50", "3.90")))
    cmd = PlaceBetCommand("p1", f"{event_id}.1x2", "home", 10_00, Decimal("2.1"), 1)
    with pytest.raises(Requote) as exc:
        await container.bets.place(cmd, "key-000000002")
    assert exc.value.details["current_price_version"] == 2
    assert exc.value.details["current_odds"] == "2"
    # odds that do not match the version are also a requote
    lying = PlaceBetCommand("p1", f"{event_id}.1x2", "home", 10_00, Decimal("5"), 2)
    with pytest.raises(Requote):
        await container.bets.place(lying, "key-000000003")
    assert (await container.wallets.balances("p1"))["cash"] == 100_00


async def test_suspended_market_rejects_bets(container: Container) -> None:
    event_id, alpha = await open_event(container)
    await container.wallets.deposit("p1", 100_00, "dep-00000001")
    await container.ingestion.ingest("alpha", alpha.goal())
    with pytest.raises(MarketNotOpen) as exc:
        await container.bets.place(
            PlaceBetCommand("p1", f"{event_id}.1x2", "home", 10_00, Decimal("2.1"), 1),
            "key-000000004",
        )
    assert exc.value.details["reason"] == "incident"


async def test_limits_funds_and_unknown_market(container: Container) -> None:
    event_id, _ = await open_event(container)
    await container.wallets.deposit("p1", 10_00, "dep-00000001")
    m = f"{event_id}.1x2"
    with pytest.raises(StakeOutOfLimits):
        await container.bets.place(PlaceBetCommand("p1", m, "home", 99, Decimal("2.1"), 1), "k-1")
    with pytest.raises(InsufficientFunds):
        await container.bets.place(
            PlaceBetCommand("p1", m, "home", 10_01, Decimal("2.1"), 1), "k-2"
        )
    with pytest.raises(NotFound):
        await container.bets.place(
            PlaceBetCommand("p1", "nope", "home", 1_00, Decimal("2"), 1), "k-3"
        )


async def test_bonus_funds_stake_after_cash(container: Container) -> None:
    event_id, _ = await open_event(container)
    await container.wallets.deposit("p1", 10_00, "dep-00000001")
    await container.wallets.grant_bonus("p1", 5_00, "bonus-000001")
    body = await place(container, "p1", f"{event_id}.1x2", "home", 12_00)
    assert (body["stake_cash"], body["stake_bonus"]) == (10_00, 2_00)
    assert await container.wallets.balances("p1") == {"cash": 0, "bonus": 3_00, "total": 3_00}


async def test_concurrent_bets_never_overdraw(container: Container) -> None:
    event_id, _ = await open_event(container)
    await container.wallets.deposit("p1", 50_00, "dep-00000001")
    mids = markets(event_id)

    async def attempt(i: int) -> bool:
        try:
            await place(container, "p1", mids[i % 3], "home" if i % 3 != 1 else "over", 10_00)
            return True
        except InsufficientFunds:
            return False

    accepted = await asyncio.gather(*(attempt(i) for i in range(12)))
    assert sum(accepted) == 5
    assert (await container.wallets.balances("p1"))["cash"] == 0


async def test_deposit_idempotency(container: Container) -> None:
    await container.wallets.deposit("p1", 10_00, "dep-00000001")
    await container.wallets.deposit("p1", 10_00, "dep-00000001")
    assert (await container.wallets.balances("p1"))["cash"] == 10_00
    with pytest.raises(IdempotencyConflict):
        await container.wallets.deposit("p1", 20_00, "dep-00000001")
