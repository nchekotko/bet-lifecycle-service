"""Reconciliation: bets vs ledger vs settlements vs results.

The checks are independent views of the same money; any disagreement is reported,
never auto-fixed (a fix is a new, audited ledger transaction decided by a human).

* ``unbalanced_txn``       – a ledger transaction whose postings do not sum to zero;
* ``ledger_not_zero``      – the sum of all balances is not zero;
* ``escrow_mismatch``      – ``house:escrow`` != sum of stakes of open bets;
* ``reserve_mismatch``     – reservation txn missing or not equal to the stake;
* ``settlement_missing``   – bet on a settled market is still open;
* ``settlement_txn``       – current settlement txn missing, reversed, or its player
  credit differs from the recorded payout/refund;
* ``projection_mismatch``  – ``bets.payout/outcome`` differ from the settlement record;
* ``stale_settlement``     – market settled with an older result version than the
  latest known result (a correction has not been applied yet);
* ``grade_mismatch``       – recorded outcome differs from grading the latest result.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

from bet_lifecycle.application.ports import Clock
from bet_lifecycle.domain.bets import BetStatus
from bet_lifecycle.domain.ledger import HOUSE_ESCROW, player_bonus, player_cash
from bet_lifecycle.domain.markets import TERMINAL
from bet_lifecycle.domain.settlement import grade
from bet_lifecycle.infrastructure.database import Database
from bet_lifecycle.observability.metrics import RECONCILIATION_MISMATCHES


@dataclass(frozen=True, slots=True)
class Mismatch:
    kind: str
    entity_id: str
    expected: Any
    actual: Any


@dataclass
class ReconciliationReport:
    run_at: datetime
    bets_checked: int = 0
    transactions_checked: int = 0
    mismatches: list[Mismatch] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.mismatches

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_at": self.run_at.isoformat(),
            "ok": self.ok,
            "bets_checked": self.bets_checked,
            "transactions_checked": self.transactions_checked,
            "mismatches": [asdict(m) for m in self.mismatches],
        }


class ReconciliationService:
    def __init__(self, db: Database, clock: Clock) -> None:
        self.db = db
        self.clock = clock

    async def run(self) -> ReconciliationReport:
        report = ReconciliationReport(run_at=self.clock.now())
        add = report.mismatches.append
        async with self.db.transaction() as tx:
            for txn_id, total in await tx.ledger.unbalanced_transactions():
                add(Mismatch("unbalanced_txn", txn_id, 0, total))
            balances = await tx.ledger.balances()
            if sum(balances.values()) != 0:
                add(Mismatch("ledger_not_zero", "*", 0, sum(balances.values())))
            report.transactions_checked = await tx.ledger.count_transactions()

            bets = await tx.bets.all_with_reserve()
            report.bets_checked = len(bets)
            markets = {m.id: m for m in await tx.markets.all()}
            latest = await tx.settlements.latest_all()
            reversed_ids = await tx.ledger.reversed_ids()
            txn_ids = [r for _, r in bets] + [s.txn_id for s in latest.values()]
            postings = await tx.ledger.postings_for(txn_ids)
            results: dict[str, Any] = {}

            open_stakes = 0
            for bet, reserve_id in bets:
                reserve = postings.get(reserve_id, {})
                if reserve.get(HOUSE_ESCROW) != bet.stake:
                    add(Mismatch("reserve_mismatch", bet.id, bet.stake, reserve.get(HOUSE_ESCROW)))
                market = markets[bet.market_id]
                stl = latest.get(bet.id)
                if bet.status is BetStatus.OPEN:
                    open_stakes += bet.stake
                    if market.status in TERMINAL:
                        add(Mismatch("settlement_missing", bet.id, "settled", "open"))
                    continue
                if stl is None:
                    add(Mismatch("settlement_missing", bet.id, "settlement row", None))
                    continue
                credited = postings.get(stl.txn_id, {})
                expected_cash = stl.payout + stl.refund_cash
                if stl.txn_id in reversed_ids:
                    add(Mismatch("settlement_txn", bet.id, "active txn", f"{stl.txn_id} reversed"))
                elif (
                    credited.get(player_cash(bet.player_id), 0) != expected_cash
                    or credited.get(player_bonus(bet.player_id), 0) != stl.refund_bonus
                    or credited.get(HOUSE_ESCROW) != -bet.stake
                ):
                    add(
                        Mismatch(
                            "settlement_txn",
                            bet.id,
                            {"cash": expected_cash, "bonus": stl.refund_bonus},
                            credited,
                        )
                    )
                if bet.outcome != stl.outcome or bet.payout != (
                    stl.payout + stl.refund_cash + stl.refund_bonus
                ):
                    add(
                        Mismatch(
                            "projection_mismatch",
                            bet.id,
                            (stl.outcome, stl.payout + stl.refund_cash + stl.refund_bonus),
                            (bet.outcome, bet.payout),
                        )
                    )
                if market.event_id not in results:
                    results[market.event_id] = await tx.results.latest(market.event_id)
                result = results[market.event_id]
                if result is None:
                    continue
                if (market.settled_result_version or 0) < result.result_version:
                    add(
                        Mismatch(
                            "stale_settlement",
                            market.id,
                            result.result_version,
                            market.settled_result_version,
                        )
                    )
                    continue
                expected_outcome = grade(market.type, market.line, bet.selection, result.score)
                if expected_outcome.value != stl.outcome:
                    add(Mismatch("grade_mismatch", bet.id, expected_outcome.value, stl.outcome))

            escrow = balances.get(HOUSE_ESCROW, 0)
            if escrow != open_stakes:
                add(Mismatch("escrow_mismatch", HOUSE_ESCROW, open_stakes, escrow))

        RECONCILIATION_MISMATCHES.clear()
        for m in report.mismatches:
            RECONCILIATION_MISMATCHES.labels(m.kind).inc()
        return report
