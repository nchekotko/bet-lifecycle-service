"""Shared helpers for application-level tests."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from bet_lifecycle.application.bets import PlaceBetCommand
from bet_lifecycle.application.consumer import ConsumerRunner, RetryPolicy
from bet_lifecycle.application.ports import FixedClock
from bet_lifecycle.bootstrap import Container
from bet_lifecycle.config import Settings
from bet_lifecycle.feed.simulator import AlphaFixture

KICKOFF = datetime(2026, 10, 3, 19, 0, tzinfo=UTC)
MATCH = ("2.10", "3.40", "3.60")
TOTAL = ("2.25", "1.90", "1.95")
AH = ("-0.25", "1.95", "1.90")

FAST_RETRY = RetryPolicy(max_attempts=3, base_delay=0.0, max_delay=0.0, jitter=0.0)


def make_container(
    clock: FixedClock | None = None,
    url: str = "sqlite+aiosqlite:///:memory:",
    **overrides: Any,
) -> Container:
    settings = Settings(database_url=url, embedded_workers=False, **overrides)
    return Container(settings, clock=clock or FixedClock(KICKOFF - timedelta(hours=1)))


async def open_event(
    c: Container,
    fixture_id: str = "A-1",
    home: str = "Arsenal FC",
    away: str = "Chelsea FC",
    kickoff: datetime = KICKOFF,
) -> tuple[str, AlphaFixture]:
    alpha = AlphaFixture(fixture_id, home, away, kickoff)
    r = await c.ingestion.ingest("alpha", alpha.fixture())
    assert r.event_id
    await c.ingestion.ingest("alpha", alpha.odds(MATCH, TOTAL, AH))
    return r.event_id, alpha


def markets(event_id: str) -> tuple[str, str, str]:
    return f"{event_id}.1x2", f"{event_id}.ou2.25", f"{event_id}.ah-0.25"


async def place(
    c: Container,
    player: str,
    market_id: str,
    selection: str,
    stake: int,
    key: str | None = None,
) -> dict[str, Any]:
    view = await c.markets.get(market_id)
    res = await c.bets.place(
        PlaceBetCommand(
            player,
            market_id,
            selection,
            stake,
            Decimal(view["prices"][selection]),
            view["price_version"],
        ),
        key or uuid.uuid4().hex,
    )
    return res.body


async def settlement_runner(c: Container) -> ConsumerRunner:
    return await c.settlement_consumer(FAST_RETRY)


async def pump(c: Container, runner: ConsumerRunner) -> None:
    """Relay the outbox and let the settlement consumer drain its topic."""
    while await c.outbox.run_once():
        pass
    while await runner.run_once():
        pass
