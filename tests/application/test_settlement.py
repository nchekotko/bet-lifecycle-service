from __future__ import annotations

from sqlalchemy import func, select

from bet_lifecycle.application.ports import FixedClock
from bet_lifecycle.bootstrap import Container
from bet_lifecycle.domain.ledger import HOUSE_ESCROW, HOUSE_REVENUE
from bet_lifecycle.domain.settlement import ResultStatus
from bet_lifecycle.infrastructure import tables as t
from bet_lifecycle.messaging.envelopes import (
    REGISTRY,
    TOPIC_RESULTS,
    ResultPublishedV1,
    ResultPublishedV2,
)
from tests.factories import open_event, place, pump, settlement_runner


async def _setup(c: Container) -> tuple[str, object, dict[str, str]]:
    event_id, alpha = await open_event(c)
    for p in ("p1", "p2", "p3"):
        await c.wallets.deposit(p, 100_00, f"dep-{p}-000001")
    bets = {
        "1x2_home": (await place(c, "p1", f"{event_id}.1x2", "home", 10_00))["bet_id"],
        "ah_home": (await place(c, "p2", f"{event_id}.ah-0.25", "home", 20_00))["bet_id"],
        "ou_over": (await place(c, "p3", f"{event_id}.ou2.25", "over", 40_00))["bet_id"],
    }
    return event_id, alpha, bets


async def test_settles_all_outcome_kinds(container: Container) -> None:
    runner = await settlement_runner(container)
    event_id, alpha, bets = await _setup(container)
    await container.ingestion.ingest("alpha", alpha.result(1, 1))  # type: ignore[attr-defined]
    await pump(container, runner)
    got = {k: await container.bets.get(b) for k, b in bets.items()}
    assert (got["1x2_home"]["outcome"], got["1x2_home"]["payout"]) == ("lost", 0)
    assert (got["ah_home"]["outcome"], got["ah_home"]["payout"]) == ("half_lost", 10_00)
    assert (got["ou_over"]["outcome"], got["ou_over"]["payout"]) == ("half_lost", 20_00)
    assert (await container.wallets.balances("p2"))["cash"] == 90_00
    async with container.db.transaction() as tx:
        assert await tx.ledger.balance(HOUSE_ESCROW) == 0
        assert await tx.ledger.balance(HOUSE_REVENUE) == 10_00 + 10_00 + 20_00
    statuses = {m["status"] for m in await container.markets.list_for_event(event_id)}
    assert statuses == {"settled"}


async def test_half_won_asian_handicap(container: Container) -> None:
    runner = await settlement_runner(container)
    _, alpha, bets = await _setup(container)
    await container.ingestion.ingest("alpha", alpha.result(2, 1))  # type: ignore[attr-defined]
    await pump(container, runner)
    ou = await container.bets.get(bets["ou_over"])  # 3 goals over 2.25 -> won
    assert (ou["outcome"], ou["payout"]) == ("won", 76_00)


async def test_resettlement_uses_reversal_and_keeps_history(
    container: Container, clock: FixedClock
) -> None:
    runner = await settlement_runner(container)
    _, alpha, bets = await _setup(container)
    await container.ingestion.ingest("alpha", alpha.result(0, 1))  # type: ignore[attr-defined]
    await pump(container, runner)
    assert (await container.wallets.balances("p1"))["cash"] == 90_00

    async with container.db.transaction() as tx:
        before = [dict(r._mapping) for r in await tx.conn.execute(select(t.ledger_postings))]

    clock.advance(minutes=10)
    await container.ingestion.ingest("alpha", alpha.result(2, 1))  # type: ignore[attr-defined]
    await pump(container, runner)

    bet = await container.bets.get(bets["1x2_home"])
    assert (bet["outcome"], bet["payout"]) == ("won", 21_00)
    assert [s["outcome"] for s in bet["settlements"]] == ["lost", "won"]
    assert bet["settlements"][1]["reversal_txn_id"] is not None
    assert (await container.wallets.balances("p1"))["cash"] == 90_00 + 21_00

    async with container.db.transaction() as tx:
        after = [dict(r._mapping) for r in await tx.conn.execute(select(t.ledger_postings))]
        reversal = await tx.ledger.get(bet["settlements"][1]["reversal_txn_id"])
        audit = await tx.audit.for_entity("bet", bets["1x2_home"])
    assert after[: len(before)] == before  # history untouched, only appended
    assert reversal is not None and reversal.reverses == bet["settlements"][0]["txn_id"]
    assert [a["action"] for a in audit] == ["placed", "settled", "resettled"]


async def test_abandoned_voids_and_refunds_to_source(container: Container) -> None:
    runner = await settlement_runner(container)
    event_id, alpha = await open_event(container)
    await container.wallets.deposit("p1", 10_00, "dep-00000001")
    await container.wallets.grant_bonus("p1", 10_00, "bonus-000001")
    await place(container, "p1", f"{event_id}.1x2", "draw", 15_00)
    await container.ingestion.ingest("alpha", alpha.status("ABANDONED"))
    await pump(container, runner)
    assert await container.wallets.balances("p1") == {"cash": 10_00, "bonus": 10_00, "total": 20_00}
    assert {m["status"] for m in await container.markets.list_for_event(event_id)} == {"voided"}


async def test_redelivered_message_is_noop(container: Container) -> None:
    runner = await settlement_runner(container)
    _, alpha, _ = await _setup(container)
    await container.ingestion.ingest("alpha", alpha.result(3, 0))  # type: ignore[attr-defined]
    await pump(container, runner)
    balances = {p: await container.wallets.balances(p) for p in ("p1", "p2", "p3")}
    assert container.broker is not None
    rec = container.broker.messages(TOPIC_RESULTS)[0]
    for _ in range(5):
        await container.broker.publish(rec.topic, rec.key, rec.value, rec.headers)
    await pump(container, runner)
    assert {p: await container.wallets.balances(p) for p in balances} == balances
    async with container.db.transaction() as tx:
        n = (await tx.conn.execute(select(func.count()).select_from(t.inbox))).scalar_one()
    assert n == 1


async def test_older_result_version_is_ignored(container: Container) -> None:
    event_id, _, bets = await _setup(container)
    v2 = ResultPublishedV2(
        event_id=event_id, home_score=2, away_score=0, status=ResultStatus.FINAL, result_version=2
    )
    v1 = ResultPublishedV2(
        event_id=event_id, home_score=0, away_score=2, status=ResultStatus.FINAL, result_version=1
    )
    r2 = await container.settlement.handle(REGISTRY.envelope(v2, key=event_id))
    r1 = await container.settlement.handle(REGISTRY.envelope(v1, key=event_id))
    assert r2.settled == 3 and r1.stale_markets == 3
    assert (await container.bets.get(bets["1x2_home"]))["outcome"] == "won"


async def test_v1_producer_message_is_upcast_and_settled(container: Container) -> None:
    event_id, _, bets = await _setup(container)
    env = REGISTRY.envelope(
        ResultPublishedV1(event_id=event_id, score="1-0", result_version=1), key=event_id
    )
    report = await container.settlement.handle(env)
    assert report.settled == 3
    assert (await container.bets.get(bets["1x2_home"]))["outcome"] == "won"


async def test_same_outcome_on_newer_version_posts_nothing(container: Container) -> None:
    event_id, _, _ = await _setup(container)

    def res(h: int, a: int, v: int) -> ResultPublishedV2:
        return ResultPublishedV2(
            event_id=event_id,
            home_score=h,
            away_score=a,
            status=ResultStatus.FINAL,
            result_version=v,
        )

    await container.settlement.handle(REGISTRY.envelope(res(3, 0, 1), key=event_id))
    async with container.db.transaction() as tx:
        n1 = await tx.ledger.count_transactions()
    # 4-0: 1x2 home still won, AH -0.25 still won, over 2.25 still won
    report = await container.settlement.handle(REGISTRY.envelope(res(4, 0, 2), key=event_id))
    async with container.db.transaction() as tx:
        n2 = await tx.ledger.count_transactions()
    assert report.unchanged == 3 and n1 == n2
