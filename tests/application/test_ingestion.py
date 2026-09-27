from __future__ import annotations

from datetime import timedelta

import pytest

from bet_lifecycle.application.ports import FixedClock
from bet_lifecycle.bootstrap import Container
from bet_lifecycle.feed.providers import FeedSchemaError
from bet_lifecycle.feed.simulator import AlphaFixture, BetaEvent
from bet_lifecycle.messaging.envelopes import REGISTRY, TOPIC_RESULTS, Envelope
from tests.factories import AH, KICKOFF, MATCH, TOTAL, markets, open_event


async def test_fixture_and_odds_create_open_markets(container: Container) -> None:
    event_id, _ = await open_event(container)
    listed = await container.markets.list_for_event(event_id)
    assert {m["market_id"] for m in listed} == set(markets(event_id))
    assert all(m["status"] == "open" and m["price_version"] == 1 for m in listed)
    history = await container.markets.price_history(f"{event_id}.1x2")
    assert history[0]["prices"] == {"home": "2.1", "draw": "3.4", "away": "3.6"}


async def test_second_provider_is_matched_to_same_event(container: Container) -> None:
    event_id, _ = await open_event(container, home="Manchester United FC", away="Tottenham")
    beta = BetaEvent(555, "MAN UTD", "SPURS", KICKOFF + timedelta(minutes=10))
    r = await container.ingestion.ingest("beta", beta.created())
    assert r.event_id == event_id
    # beta is secondary while alpha is alive: its prices do not move our markets
    await container.ingestion.ingest("beta", beta.prices(("5.0", "5.0", "1.5")))
    assert (await container.markets.get(f"{event_id}.1x2"))["price_version"] == 1


async def test_unrelated_fixture_creates_new_event(container: Container) -> None:
    e1, _ = await open_event(container)
    e2, _ = await open_event(container, "A-2", "Liverpool", "Everton")
    assert e1 != e2


async def test_duplicates_and_out_of_order_messages(container: Container) -> None:
    alpha = AlphaFixture("A-9", "Arsenal", "Chelsea", KICKOFF)
    m1 = alpha.fixture()
    m2 = alpha.odds(MATCH)
    m3 = alpha.odds(("2.00", "3.50", "3.90"))
    m4 = alpha.odds(("1.90", "3.60", "4.20"))
    ing = container.ingestion
    assert (await ing.ingest("alpha", m1)).applied == [1]
    assert (await ing.ingest("alpha", m3)).buffered
    assert (await ing.ingest("alpha", m4)).buffered
    assert (await ing.ingest("alpha", m1)).duplicate
    r = await ing.ingest("alpha", m2)
    assert r.applied == [2, 3, 4]
    assert (await ing.ingest("alpha", m3)).duplicate
    market = await container.markets.get(f"{r.event_id}.1x2")
    assert market["price_version"] == 3
    assert market["prices"]["home"] == "1.9"


async def test_cursor_survives_service_restart(container: Container) -> None:
    alpha = AlphaFixture("A-5", "Arsenal", "Chelsea", KICKOFF)
    m1, m2 = alpha.fixture(), alpha.odds(MATCH)
    await container.ingestion.ingest("alpha", m1)
    await container.ingestion.ingest("alpha", m2)
    # A fresh gate (new process) must still reject redelivered messages.
    container.ingestion.gate = type(container.ingestion.gate)()
    assert (await container.ingestion.ingest("alpha", m2)).duplicate


async def test_goal_suspends_and_fresh_prices_reopen(container: Container) -> None:
    event_id, alpha = await open_event(container)
    beta = BetaEvent(1, "Arsenal", "Chelsea", KICKOFF)
    await container.ingestion.ingest("beta", beta.created())
    await container.ingestion.ingest("beta", beta.goal("H"))  # incident from secondary counts
    listed = await container.markets.list_for_event(event_id)
    assert {(m["status"], m["suspend_reason"]) for m in listed} == {("suspended", "incident")}
    await container.ingestion.ingest("alpha", alpha.odds(("1.50", "4.00", "6.50")))
    m = await container.markets.get(f"{event_id}.1x2")
    assert (m["status"], m["price_version"]) == ("open", 2)
    # markets not re-quoted stay suspended until they get a fresh price
    assert (await container.markets.get(f"{event_id}.ou2.25"))["status"] == "suspended"


async def test_trader_suspension_is_not_lifted_by_feed(container: Container) -> None:
    event_id, alpha = await open_event(container)
    await container.markets.trader_suspend(f"{event_id}.1x2")
    await container.ingestion.ingest("alpha", alpha.odds(("1.50", "4.00", "6.50")))
    m = await container.markets.get(f"{event_id}.1x2")
    assert (m["status"], m["suspend_reason"], m["price_version"]) == ("suspended", "trader", 2)
    assert (await container.markets.trader_open(f"{event_id}.1x2"))["status"] == "open"


async def test_stale_feed_suspends_then_failover_to_secondary(
    container: Container, clock: FixedClock
) -> None:
    event_id, _ = await open_event(container)
    beta = BetaEvent(2, "Arsenal", "Chelsea", KICKOFF)
    await container.ingestion.ingest("beta", beta.created())
    clock.advance(seconds=31)
    assert await container.markets.suspend_stale(timedelta(seconds=30)) == 3
    assert (await container.markets.get(f"{event_id}.1x2"))["suspend_reason"] == "feed_stale"
    assert await container.markets.suspend_stale(timedelta(seconds=30)) == 0  # idempotent
    # alpha silent > failover_after: beta takes over and its prices reopen the market
    await container.ingestion.ingest("beta", beta.prices(("2.2", "3.3", "3.4")))
    m = await container.markets.get(f"{event_id}.1x2")
    assert (m["status"], m["price_version"], m["prices"]["home"]) == ("open", 2, "2.2")
    async with container.db.transaction() as tx:
        audit = await tx.audit.for_entity("event", event_id)
    assert any(a["action"] == "primary_failover" for a in audit)


async def test_staleness_sweep_respects_distributed_lock(
    container: Container, clock: FixedClock
) -> None:
    await open_event(container)
    token = await container.lock.acquire("staleness-sweep", 30)
    assert token
    clock.advance(minutes=5)
    assert await container.markets.suspend_stale(timedelta(seconds=30)) == 0
    await container.lock.release("staleness-sweep", token)
    assert await container.markets.suspend_stale(timedelta(seconds=30)) == 3


async def test_result_goes_to_outbox_and_correction_bumps_version(container: Container) -> None:
    event_id, alpha = await open_event(container)
    await container.ingestion.ingest("alpha", alpha.status("ENDED"))
    assert {m["status"] for m in await container.markets.list_for_event(event_id)} == {"closed"}
    await container.ingestion.ingest("alpha", alpha.result(1, 0))
    await container.ingestion.ingest("alpha", alpha.result(1, 0))  # re-sent, same score
    await container.ingestion.ingest("alpha", alpha.result(2, 0))  # correction
    await container.outbox.run_once()
    assert container.broker is not None
    results = [
        REGISTRY.decode(Envelope.from_bytes(r.value))
        for r in container.broker.messages(TOPIC_RESULTS)
    ]
    assert [(p.result_version, p.home_score) for p in results] == [(1, 1), (2, 2)]  # type: ignore[attr-defined]


async def test_abandoned_status_publishes_void_result(container: Container) -> None:
    _, alpha = await open_event(container)
    await container.ingestion.ingest("alpha", alpha.status("ABANDONED"))
    await container.outbox.run_once()
    assert container.broker is not None
    (rec,) = container.broker.messages(TOPIC_RESULTS)
    assert REGISTRY.decode(Envelope.from_bytes(rec.value)).status == "abandoned"  # type: ignore[attr-defined]


async def test_unmapped_message_without_fixture_is_rejected_not_blocking(
    container: Container,
) -> None:
    beta = BetaEvent(77, "X", "Y", KICKOFF)
    beta.seq = 0
    r = await container.ingestion.ingest("beta", beta.prices(MATCH))  # seq 1, no participants
    assert r.rejected == [(1, "unknown_event")]
    r2 = await container.ingestion.ingest("beta", beta.heartbeat())
    assert r2.rejected == [(2, "unknown_event")]  # stream keeps moving


async def test_schema_error_raises(container: Container) -> None:
    with pytest.raises(FeedSchemaError):
        await container.ingestion.ingest("alpha", {"type": "odds", "seq": "x"})


async def test_invalid_selection_rejected_in_savepoint(container: Container) -> None:
    alpha = AlphaFixture("A-3", "Arsenal", "Chelsea", KICKOFF)
    await container.ingestion.ingest("alpha", alpha.fixture())
    bad = alpha.odds(MATCH, TOTAL, AH)
    bad["markets"][0]["prices"].pop("X")  # 1x2 without draw price
    r = await container.ingestion.ingest("alpha", bad)
    assert r.rejected == [(2, "unknown_selection")]
    assert await container.markets.list_for_event(r.event_id or "") == []  # nothing half-applied
