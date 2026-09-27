"""Settlement and resettlement.

Delivery is at-least-once; the business effect must be exactly-once. Three layers:

1. **Inbox**: ``(consumer, message_id)`` is inserted in the same transaction as the
   ledger postings. A redelivered message hits the primary key and is a no-op.
2. **Result versions**: each market remembers the result version it was settled
   with. An older version arriving late (reordering across retries/partitions) is
   ignored, so the final state depends only on the newest result, not on the order
   of delivery.
3. **Idempotent grading**: a newer result version that grades a bet the same way
   produces no postings at all.

A corrected result that changes the outcome is applied as a reversal of the previous
settlement transaction plus a new settlement transaction. Nothing is updated in the
ledger or in ``bet_settlements`` - both are append-only.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from bet_lifecycle.application.common import emit_market_status, new_id
from bet_lifecycle.application.ports import Clock
from bet_lifecycle.domain.bets import Bet
from bet_lifecycle.domain.ledger import bet_settle
from bet_lifecycle.domain.markets import Market
from bet_lifecycle.domain.settlement import ResultStatus, Score, compute_amounts, grade
from bet_lifecycle.infrastructure.database import Database, Tx
from bet_lifecycle.infrastructure.repositories import SettlementRow
from bet_lifecycle.messaging.envelopes import (
    REGISTRY,
    TOPIC_BETS,
    BetSettledV1,
    Envelope,
    PoisonMessage,
    ResultPublishedV2,
    SchemaRegistry,
)
from bet_lifecycle.observability.logging import get_logger
from bet_lifecycle.observability.metrics import (
    INBOX_DUPLICATES_TOTAL,
    RESETTLEMENTS_TOTAL,
    SETTLEMENTS_TOTAL,
)

log = get_logger(__name__)


@dataclass
class SettlementReport:
    message_id: str
    event_id: str
    result_version: int
    duplicate: bool = False
    stale_markets: int = 0
    settled: int = 0
    resettled: int = 0
    unchanged: int = 0


class SettlementService:
    CONSUMER = "settlement"

    def __init__(self, db: Database, clock: Clock, registry: SchemaRegistry = REGISTRY) -> None:
        self.db = db
        self.clock = clock
        self.registry = registry

    async def handle(self, env: Envelope) -> SettlementReport:
        payload = self.registry.decode(env)
        if not isinstance(payload, ResultPublishedV2):
            raise PoisonMessage(f"settlement cannot handle {env.type}")
        report = SettlementReport(env.message_id, payload.event_id, payload.result_version)
        score = Score(payload.home_score, payload.away_score, payload.status)
        now = self.clock.now()
        async with self.db.transaction() as tx:
            if not await tx.inbox.try_add(self.CONSUMER, env.message_id, now):
                INBOX_DUPLICATES_TOTAL.labels(self.CONSUMER).inc()
                report.duplicate = True
                return report
            for market in await tx.markets.by_event(payload.event_id, for_update=True):
                if (
                    market.settled_result_version is not None
                    and market.settled_result_version >= payload.result_version
                ):
                    report.stale_markets += 1
                    continue
                for bet in await tx.bets.by_market(market.id):
                    await self._settle_bet(
                        tx, bet, market, score, payload.result_version, now, report
                    )
                market.mark_settled(
                    payload.result_version, voided=score.status is ResultStatus.ABANDONED
                )
                await tx.markets.save(market, now)
                await emit_market_status(tx, market, now)
        log.info(
            "result applied",
            extra={
                "event_id": report.event_id,
                "result_version": report.result_version,
                "settled": report.settled,
                "resettled": report.resettled,
                "stale_markets": report.stale_markets,
            },
        )
        return report

    async def _settle_bet(
        self,
        tx: Tx,
        bet: Bet,
        market: Market,
        score: Score,
        result_version: int,
        now: datetime,
        report: SettlementReport,
    ) -> None:
        outcome = grade(market.type, market.line, bet.selection, score)
        amounts = compute_amounts(outcome, bet.stake, bet.odds, bet.stake_bonus)
        last = await tx.settlements.latest(bet.id)
        if last is not None and (
            last.outcome,
            last.payout,
            last.refund_cash,
            last.refund_bonus,
        ) == (amounts.outcome.value, amounts.payout, amounts.refund_cash, amounts.refund_bonus):
            report.unchanged += 1
            return

        reversal_id: str | None = None
        if last is not None:
            original = await tx.ledger.get(last.txn_id)
            if original is None:  # pragma: no cover - would be a corrupted ledger
                raise RuntimeError(f"settlement txn {last.txn_id} missing")
            reversal = original.reversal(new_id("txn"), now)
            await tx.ledger.post(reversal)
            reversal_id = reversal.id

        txn = bet_settle(
            new_id("txn"),
            bet.player_id,
            bet.id,
            bet.stake,
            amounts.payout,
            amounts.refund_cash,
            amounts.refund_bonus,
            now,
        )
        await tx.ledger.post(txn)
        seq = last.seq + 1 if last else 1
        await tx.settlements.add(
            SettlementRow(
                id=new_id("stl"),
                bet_id=bet.id,
                seq=seq,
                outcome=amounts.outcome.value,
                payout=amounts.payout,
                refund_cash=amounts.refund_cash,
                refund_bonus=amounts.refund_bonus,
                result_version=result_version,
                txn_id=txn.id,
                reversal_txn_id=reversal_id,
                created_at=now,
            )
        )
        await tx.bets.mark_settled(bet.id, amounts.outcome.value, amounts.total_return)
        resettle = last is not None
        payload = BetSettledV1(
            bet_id=bet.id,
            player_id=bet.player_id,
            market_id=bet.market_id,
            outcome=amounts.outcome.value,
            payout=amounts.payout,
            refund=amounts.refund_cash + amounts.refund_bonus,
            settlement_seq=seq,
            result_version=result_version,
            resettlement=resettle,
        )
        await tx.outbox.add(TOPIC_BETS, REGISTRY.envelope(payload, key=bet.id), now)
        await tx.audit.add(
            "bet",
            bet.id,
            "resettled" if resettle else "settled",
            {
                "outcome": amounts.outcome.value,
                "return": amounts.total_return,
                "result_version": result_version,
                "txn_id": txn.id,
                "reversal_txn_id": reversal_id,
                "previous_outcome": last.outcome if last else None,
            },
            now,
        )
        SETTLEMENTS_TOTAL.labels(amounts.outcome.value).inc()
        if resettle:
            RESETTLEMENTS_TOTAL.inc()
            report.resettled += 1
        else:
            report.settled += 1
