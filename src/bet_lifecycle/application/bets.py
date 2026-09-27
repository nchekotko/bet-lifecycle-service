"""Bet placement and wallet use cases."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sqlalchemy.exc import IntegrityError

from bet_lifecycle.application.common import new_id
from bet_lifecycle.application.ports import Clock
from bet_lifecycle.domain.bets import Bet, StakeLimits
from bet_lifecycle.domain.errors import (
    DomainError,
    IdempotencyConflict,
    NotFound,
    Requote,
)
from bet_lifecycle.domain.ledger import (
    bet_reserve,
    bonus_grant,
    deposit,
    player_bonus,
    player_cash,
    split_stake,
)
from bet_lifecycle.domain.money import odds_to_str
from bet_lifecycle.infrastructure.database import Database
from bet_lifecycle.messaging.envelopes import REGISTRY, TOPIC_BETS, BetPlacedV1
from bet_lifecycle.observability.logging import get_logger
from bet_lifecycle.observability.metrics import BET_PLACEMENT_SECONDS, BETS_TOTAL

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class PlaceBetCommand:
    player_id: str
    market_id: str
    selection: str
    stake: int
    odds: Decimal
    price_version: int

    def fingerprint(self) -> str:
        body = {
            "player_id": self.player_id,
            "market_id": self.market_id,
            "selection": self.selection,
            "stake": self.stake,
            "odds": odds_to_str(self.odds),
            "price_version": self.price_version,
        }
        return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class PlaceBetResult:
    status_code: int
    body: dict[str, Any]
    replayed: bool


def bet_view(bet: Bet) -> dict[str, Any]:
    return {
        "bet_id": bet.id,
        "player_id": bet.player_id,
        "market_id": bet.market_id,
        "selection": bet.selection,
        "stake": bet.stake,
        "odds": odds_to_str(bet.odds),
        "price_version": bet.price_version,
        "stake_cash": bet.stake_cash,
        "stake_bonus": bet.stake_bonus,
        "status": bet.status.value,
        "outcome": bet.outcome,
        "payout": bet.payout,
        "placed_at": bet.placed_at.isoformat(),
    }


class BetService:
    def __init__(self, db: Database, clock: Clock, limits: StakeLimits | None = None) -> None:
        self.db = db
        self.clock = clock
        self.limits = limits or StakeLimits()

    async def place(self, cmd: PlaceBetCommand, idempotency_key: str) -> PlaceBetResult:
        started = time.perf_counter()
        result_label = "error"
        try:
            try:
                res = await self._place(cmd, idempotency_key)
            except IntegrityError:
                # A concurrent request with the same key committed first; replay it.
                res = await self._place(cmd, idempotency_key)
            result_label = "replayed" if res.replayed else "accepted"
            return res
        except DomainError as exc:
            result_label = exc.code
            raise
        finally:
            BET_PLACEMENT_SECONDS.observe(time.perf_counter() - started)
            BETS_TOTAL.labels(result_label).inc()

    async def _place(self, cmd: PlaceBetCommand, key: str) -> PlaceBetResult:
        fingerprint = cmd.fingerprint()
        now = self.clock.now()
        async with self.db.transaction() as tx:
            stored = await tx.idempotency.get(cmd.player_id, key)
            if stored is not None:
                if stored.request_hash != fingerprint:
                    raise IdempotencyConflict(
                        "Idempotency-Key already used with a different request", key=key
                    )
                return PlaceBetResult(stored.status_code, stored.response, replayed=True)

            # Row lock on the market: a concurrent reprice/suspend/settle waits for us
            # (or we wait for it), so the version check below cannot race.
            market = await tx.markets.get(cmd.market_id, for_update=True)
            if market is None:
                raise NotFound(f"market {cmd.market_id} not found", market_id=cmd.market_id)
            odds = market.quote(cmd.selection, cmd.price_version)
            if odds != cmd.odds:
                raise Requote(market.id, market.price_version, odds_to_str(odds), cmd.price_version)
            self.limits.check(cmd.stake, odds)

            cash_acc, bonus_acc = player_cash(cmd.player_id), player_bonus(cmd.player_id)
            await tx.ledger.lock_accounts([cash_acc, bonus_acc], now)
            balances = await tx.ledger.balances([cash_acc, bonus_acc])
            cash_part, bonus_part = split_stake(cmd.stake, balances[cash_acc], balances[bonus_acc])

            bet = Bet(
                id=new_id("bet"),
                player_id=cmd.player_id,
                market_id=market.id,
                selection=cmd.selection,
                stake=cmd.stake,
                odds=odds,
                price_version=market.price_version,
                stake_cash=cash_part,
                stake_bonus=bonus_part,
                placed_at=now,
            )
            txn = bet_reserve(new_id("txn"), cmd.player_id, bet.id, cash_part, bonus_part, now)
            await tx.ledger.post(txn)
            await tx.bets.add(bet, txn.id)
            payload = BetPlacedV1(
                bet_id=bet.id,
                player_id=bet.player_id,
                market_id=bet.market_id,
                selection=bet.selection,
                stake=bet.stake,
                odds=odds_to_str(odds),
                price_version=bet.price_version,
                placed_at=now,
            )
            await tx.outbox.add(TOPIC_BETS, REGISTRY.envelope(payload, key=bet.id), now)
            await tx.audit.add(
                "bet",
                bet.id,
                "placed",
                {
                    "stake": bet.stake,
                    "odds": odds_to_str(odds),
                    "price_version": bet.price_version,
                    "reserve_txn": txn.id,
                    "idempotency_key": key,
                },
                now,
            )
            body = bet_view(bet)
            await tx.idempotency.save(cmd.player_id, key, fingerprint, 201, body, now)
        log.info(
            "bet accepted",
            extra={
                "bet_id": bet.id,
                "market_id": bet.market_id,
                "stake": bet.stake,
                "price_version": bet.price_version,
            },
        )
        return PlaceBetResult(201, body, replayed=False)

    async def get(self, bet_id: str) -> dict[str, Any]:
        async with self.db.transaction() as tx:
            bet = await tx.bets.get(bet_id)
            if bet is None:
                raise NotFound(f"bet {bet_id} not found", bet_id=bet_id)
            history = await tx.settlements.history(bet_id)
        view = bet_view(bet)
        view["settlements"] = [
            {
                "seq": s.seq,
                "outcome": s.outcome,
                "payout": s.payout,
                "refund": s.refund_cash + s.refund_bonus,
                "result_version": s.result_version,
                "txn_id": s.txn_id,
                "reversal_txn_id": s.reversal_txn_id,
                "created_at": s.created_at.isoformat(),
            }
            for s in history
        ]
        return view


def _deterministic_txn_id(prefix: str, player_id: str, key: str) -> str:
    return f"{prefix}_" + hashlib.sha256(f"{player_id}\x00{key}".encode()).hexdigest()[:40]


class WalletService:
    """Deposits and bonus grants are idempotent through a deterministic txn id."""

    def __init__(self, db: Database, clock: Clock) -> None:
        self.db = db
        self.clock = clock

    async def deposit(self, player_id: str, amount: int, idempotency_key: str) -> dict[str, int]:
        txn = deposit(
            _deterministic_txn_id("dep", player_id, idempotency_key),
            player_id,
            amount,
            self.clock.now(),
        )
        await self._post_once(txn)
        return await self.balances(player_id)

    async def grant_bonus(
        self, player_id: str, amount: int, idempotency_key: str
    ) -> dict[str, int]:
        txn = bonus_grant(
            _deterministic_txn_id("bon", player_id, idempotency_key),
            player_id,
            amount,
            self.clock.now(),
        )
        await self._post_once(txn)
        return await self.balances(player_id)

    async def _post_once(self, txn: Any) -> None:
        try:
            async with self.db.transaction() as tx:
                existing = await tx.ledger.get(txn.id)
                if existing is not None:
                    if sorted(existing.postings, key=str) != sorted(txn.postings, key=str):
                        raise IdempotencyConflict(
                            "Idempotency-Key already used with a different amount"
                        )
                    return
                await tx.ledger.post(txn)
                await tx.audit.add(
                    "ledger", txn.id, txn.kind.value, {"reference": txn.reference}, txn.created_at
                )
        except IntegrityError:
            return  # concurrent duplicate committed first

    async def balances(self, player_id: str) -> dict[str, int]:
        cash, bonus = player_cash(player_id), player_bonus(player_id)
        async with self.db.transaction() as tx:
            b = await tx.ledger.balances([cash, bonus])
        return {"cash": b[cash], "bonus": b[bonus], "total": b[cash] + b[bonus]}
