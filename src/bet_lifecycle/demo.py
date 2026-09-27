"""End-to-end lifecycle walkthrough, in-process (SQLite + in-memory broker).

ingest feed -> open markets -> accept bets at versioned odds -> requote -> goal
suspends -> reopen -> close -> result -> settle -> result correction -> resettle ->
redelivery is a no-op -> reconcile.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from bet_lifecycle.application.bets import PlaceBetCommand
from bet_lifecycle.application.ports import FixedClock
from bet_lifecycle.bootstrap import Container
from bet_lifecycle.config import Settings
from bet_lifecycle.domain.errors import DomainError
from bet_lifecycle.feed.simulator import AlphaFixture, BetaEvent
from bet_lifecycle.messaging.memory import MemorySource

Printer = Callable[[str], None]


async def run_demo(
    echo: Printer = print, database_url: str = "sqlite+aiosqlite:///:memory:"
) -> dict[str, Any]:
    kickoff = datetime(2026, 10, 3, 19, 0, tzinfo=UTC)
    clock = FixedClock(kickoff - timedelta(hours=2))
    c = Container(Settings(database_url=database_url, embedded_workers=False), clock=clock)
    await c.start()
    settlement = await c.settlement_consumer()

    async def pump() -> None:
        await c.outbox.run_once()
        await settlement.run_once()

    try:
        echo("1. wallets: p1 deposits 100.00, p2 deposits 50.00 + bonus 20.00")
        await c.wallets.deposit("p1", 100_00, "dep-p1-0001")
        await c.wallets.deposit("p2", 50_00, "dep-p2-0001")
        await c.wallets.grant_bonus("p2", 20_00, "bonus-p2-0001")

        alpha = AlphaFixture("A-7781", "Manchester United FC", "Tottenham Hotspur", kickoff)
        beta = BetaEvent(99001, "MAN UTD", "SPURS", kickoff + timedelta(minutes=5))
        r = await c.ingestion.ingest("alpha", alpha.fixture())
        event_id = r.event_id
        await c.ingestion.ingest(
            "alpha",
            alpha.odds(
                ("2.10", "3.40", "3.60"), ("2.5", "1.90", "1.95"), ("-0.25", "1.95", "1.90")
            ),
        )
        rb = await c.ingestion.ingest("beta", beta.created())
        await c.ingestion.ingest("beta", beta.prices(("2.2", "3.3", "3.4")))
        echo(
            f"2. feed: alpha created {event_id}; beta 'MAN UTD v SPURS' matched to "
            f"{rb.event_id} (same={rb.event_id == event_id}); "
            "beta prices ignored (alpha is primary)"
        )
        m1x2, mou, mah = f"{event_id}.1x2", f"{event_id}.ou2.5", f"{event_id}.ah-0.25"
        listed = await c.markets.list_for_event(event_id)
        echo(f"   markets: {[(m['market_id'], m['status'], m['price_version']) for m in listed]}")

        async def bet(
            player: str, market: str, sel: str, stake: int, key: str, version: int | None = None
        ) -> dict[str, Any]:
            view = await c.markets.get(market)
            try:
                res = await c.bets.place(
                    PlaceBetCommand(
                        player,
                        market,
                        sel,
                        stake,
                        Decimal(view["prices"][sel]),
                        version or view["price_version"],
                    ),
                    key,
                )
                return res.body
            except DomainError as exc:
                return {"error": exc.code, **exc.details}

        b1 = await bet("p1", m1x2, "home", 20_00, "bet-p1-0001")
        b2 = await bet("p2", mah, "home", 30_00, "bet-p2-0001")
        b3 = await bet("p2", mou, "over", 30_00, "bet-p2-0002")
        echo(
            f"3. bets accepted: p1 1x2/home 20.00 @ {b1['odds']}; "
            f"p2 AH-0.25/home 30.00 @ {b2['odds']};"
            f" p2 O2.5 30.00 @ {b3['odds']} (cash {b3['stake_cash']}, bonus {b3['stake_bonus']})"
        )
        replay = await c.bets.place(
            PlaceBetCommand("p1", m1x2, "home", 20_00, Decimal(b1["odds"]), 1), "bet-p1-0001"
        )
        echo(
            f"   retry with same Idempotency-Key -> replayed={replay.replayed}, "
            f"same bet={replay.body['bet_id'] == b1['bet_id']}"
        )

        clock.advance(hours=2, minutes=20)
        await c.ingestion.ingest("alpha", alpha.status("LIVE"))
        await c.ingestion.ingest("alpha", alpha.odds(("2.00", "3.50", "3.90")))
        stale = await bet("p1", m1x2, "home", 10_00, "bet-p1-0002", version=1)
        echo(
            f"4. odds moved; bet at price_version=1 -> {stale['error']} "
            f"(current v{stale['current_price_version']} @ {stale['current_odds']})"
        )

        await c.ingestion.ingest("beta", beta.goal("H"))
        blocked = await bet("p1", m1x2, "home", 10_00, "bet-p1-0003")
        echo(
            f"5. goal reported by beta -> markets suspended; bet -> {blocked['error']} "
            f"({blocked.get('reason')})"
        )
        await c.ingestion.ingest(
            "alpha",
            alpha.odds(
                ("1.45", "4.50", "7.00"), ("2.5", "1.60", "2.30"), ("-0.25", "1.40", "2.90")
            ),
        )
        echo(
            f"   fresh alpha prices -> reopened: {(await c.markets.get(m1x2))['status']} "
            f"v{(await c.markets.get(m1x2))['price_version']}"
        )

        clock.advance(minutes=80)
        await c.ingestion.ingest("alpha", alpha.status("ENDED"))
        await c.ingestion.ingest("alpha", alpha.result(1, 1))
        await pump()
        echo(f"6. result 1-1 settled: {await _bets(c, [b1, b2, b3])}")
        echo(f"   balances p1={await c.wallets.balances('p1')} p2={await c.wallets.balances('p2')}")

        clock.advance(minutes=30)
        await c.ingestion.ingest("alpha", alpha.result(2, 1))
        await pump()
        echo(f"7. correction 2-1 resettled via reversal+new txn: {await _bets(c, [b1, b2, b3])}")
        balances = {p: await c.wallets.balances(p) for p in ("p1", "p2")}
        echo(f"   balances p1={balances['p1']} p2={balances['p2']}")

        src = settlement.source
        assert isinstance(src, MemorySource)
        c.broker.committed.clear()  # type: ignore[union-attr]
        src.rewind()
        await settlement.run_once()
        again = {p: await c.wallets.balances(p) for p in ("p1", "p2")}
        echo(f"8. all result messages redelivered -> balances unchanged: {again == balances}")

        report = await c.reconciliation.run()
        echo(
            f"9. reconciliation: ok={report.ok}, bets={report.bets_checked}, "
            f"txns={report.transactions_checked}, mismatches={len(report.mismatches)}"
        )
        return {"balances": balances, "reconciliation_ok": report.ok, "event_id": event_id}
    finally:
        await c.stop()


async def _bets(
    c: Container, bets: list[dict[str, Any]]
) -> list[tuple[str, str | None, int | None]]:
    out = []
    for b in bets:
        v = await c.bets.get(b["bet_id"])
        out.append((v["selection"], v["outcome"], v["payout"]))
    return out
