from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import insert, update

from bet_lifecycle.bootstrap import Container
from bet_lifecycle.domain.ledger import HOUSE_ESCROW, HOUSE_REVENUE
from bet_lifecycle.domain.settlement import Score
from bet_lifecycle.infrastructure import tables as t
from bet_lifecycle.infrastructure.repositories import ResultRow
from tests.factories import open_event, place, pump, settlement_runner

NOW = datetime(2026, 10, 3, tzinfo=UTC)


async def _settled(c: Container) -> tuple[str, str]:
    runner = await settlement_runner(c)
    event_id, alpha = await open_event(c)
    await c.wallets.deposit("p1", 100_00, "dep-00000001")
    won = (await place(c, "p1", f"{event_id}.1x2", "home", 10_00))["bet_id"]
    await place(c, "p1", f"{event_id}.ou2.25", "over", 10_00)
    await c.ingestion.ingest("alpha", alpha.result(1, 0))
    await pump(c, runner)
    return event_id, won


def kinds(report: object) -> set[str]:
    return {m.kind for m in report.mismatches}  # type: ignore[attr-defined]


async def test_clean_state_reconciles(container: Container) -> None:
    await _settled(container)
    event_id, _ = await open_event(container, "A-2", "Liverpool", "Everton")
    await place(container, "p1", f"{event_id}.1x2", "draw", 5_00)  # open bet in escrow
    report = await container.reconciliation.run()
    assert report.ok, report.mismatches
    assert report.bets_checked == 3


async def test_detects_tampered_projection_and_unbalanced_txn(container: Container) -> None:
    _, bet_id = await _settled(container)
    async with container.db.transaction() as tx:
        await tx.ledger.ensure_accounts([HOUSE_REVENUE], NOW)
        await tx.conn.execute(update(t.bets).where(t.bets.c.id == bet_id).values(payout=1))
        await tx.conn.execute(
            insert(t.ledger_transactions).values(
                id="rogue",
                kind="deposit",
                reference="x",
                reverses=None,
                metadata={},
                created_at=NOW,
            )
        )
        await tx.conn.execute(
            insert(t.ledger_postings).values(txn_id="rogue", account_id=HOUSE_REVENUE, amount=5)
        )
    assert {"projection_mismatch", "unbalanced_txn", "ledger_not_zero"} <= kinds(
        await container.reconciliation.run()
    )


async def test_detects_escrow_drift(container: Container) -> None:
    event_id, _ = await open_event(container)
    await container.wallets.deposit("p1", 100_00, "dep-00000001")
    await place(container, "p1", f"{event_id}.1x2", "home", 10_00)
    async with container.db.transaction() as tx:
        await tx.ledger.ensure_accounts([HOUSE_ESCROW, HOUSE_REVENUE], NOW)
        await tx.conn.execute(
            insert(t.ledger_transactions).values(
                id="leak", kind="deposit", reference="x", reverses=None, metadata={}, created_at=NOW
            )
        )
        await tx.conn.execute(
            insert(t.ledger_postings),
            [
                {"txn_id": "leak", "account_id": HOUSE_ESCROW, "amount": -1_00},
                {"txn_id": "leak", "account_id": HOUSE_REVENUE, "amount": 1_00},
            ],
        )
    assert kinds(await container.reconciliation.run()) == {"escrow_mismatch"}


async def test_detects_unapplied_result_correction(container: Container) -> None:
    event_id, _ = await _settled(container)
    async with container.db.transaction() as tx:
        # a correction stored but never consumed by settlement
        await tx.results.add(ResultRow(event_id, 2, Score(0, 1), "alpha"), NOW)
    assert "stale_settlement" in kinds(await container.reconciliation.run())


async def test_detects_wrong_grade(container: Container) -> None:
    _, bet_id = await _settled(container)
    async with container.db.transaction() as tx:
        await tx.conn.execute(
            update(t.bet_settlements)
            .where(t.bet_settlements.c.bet_id == bet_id)
            .values(outcome="lost")
        )
    assert "grade_mismatch" in kinds(await container.reconciliation.run())
