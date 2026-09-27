"""Repositories over SQLAlchemy Core. Each one is bound to the connection of a Tx."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import and_, func, insert, select, update
from sqlalchemy.engine import Row
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection

from bet_lifecycle.domain.bets import Bet, BetStatus
from bet_lifecycle.domain.ledger import (
    SYSTEM_ACCOUNTS,
    LedgerTransaction,
    Posting,
    TxnKind,
)
from bet_lifecycle.domain.markets import (
    Market,
    MarketStatus,
    MarketType,
    SuspendReason,
    line_to_str,
)
from bet_lifecycle.domain.money import odds_to_str
from bet_lifecycle.domain.settlement import ResultStatus, Score
from bet_lifecycle.infrastructure import tables as t
from bet_lifecycle.messaging.envelopes import Envelope


class _Repo:
    def __init__(self, conn: AsyncConnection) -> None:
        self.conn = conn


# ------------------------------------------------------------------------- events
@dataclass(slots=True)
class EventRow:
    id: str
    home: str
    away: str
    home_norm: str
    away_norm: str
    kickoff: datetime
    status: str
    primary_provider: str
    last_feed_at: datetime | None


class EventRepository(_Repo):
    async def get(self, event_id: str, *, for_update: bool = False) -> EventRow | None:
        q = select(t.events).where(t.events.c.id == event_id)
        if for_update:
            q = q.with_for_update()
        row = (await self.conn.execute(q)).first()
        return _event(row) if row else None

    async def near_kickoff(self, start: datetime, end: datetime) -> list[EventRow]:
        q = select(t.events).where(t.events.c.kickoff.between(start, end))
        return [_event(r) for r in await self.conn.execute(q)]

    async def add(self, e: EventRow, created_at: datetime) -> None:
        await self.conn.execute(
            insert(t.events).values(
                id=e.id,
                home=e.home,
                away=e.away,
                home_norm=e.home_norm,
                away_norm=e.away_norm,
                kickoff=e.kickoff,
                status=e.status,
                primary_provider=e.primary_provider,
                last_feed_at=e.last_feed_at,
                created_at=created_at,
            )
        )

    async def update(self, event_id: str, **values: Any) -> None:
        await self.conn.execute(update(t.events).where(t.events.c.id == event_id).values(**values))

    async def with_open_markets_stale(self, before: datetime) -> list[str]:
        q = (
            select(t.events.c.id)
            .join(t.markets, t.markets.c.event_id == t.events.c.id)
            .where(t.markets.c.status == MarketStatus.OPEN.value)
            .where(t.events.c.last_feed_at < before)
            .distinct()
        )
        return [r[0] for r in await self.conn.execute(q)]


def _event(row: Row[Any]) -> EventRow:
    m = row._mapping
    return EventRow(
        m["id"],
        m["home"],
        m["away"],
        m["home_norm"],
        m["away_norm"],
        m["kickoff"],
        m["status"],
        m["primary_provider"],
        m["last_feed_at"],
    )


# -------------------------------------------------------------------- feed streams
@dataclass(slots=True)
class StreamRow:
    provider: str
    provider_event_id: str
    event_id: str | None
    last_seq: int
    last_seen_at: datetime | None


class FeedStreamRepository(_Repo):
    async def get(
        self, provider: str, provider_event_id: str, *, for_update: bool = False
    ) -> StreamRow | None:
        q = select(t.provider_events).where(
            and_(
                t.provider_events.c.provider == provider,
                t.provider_events.c.provider_event_id == provider_event_id,
            )
        )
        if for_update:
            q = q.with_for_update()
        row = (await self.conn.execute(q)).first()
        if row is None:
            return None
        m = row._mapping
        return StreamRow(
            m["provider"], m["provider_event_id"], m["event_id"], m["last_seq"], m["last_seen_at"]
        )

    async def upsert(self, s: StreamRow) -> None:
        values = {
            "event_id": s.event_id,
            "last_seq": s.last_seq,
            "last_seen_at": s.last_seen_at,
        }
        res = await self.conn.execute(
            update(t.provider_events)
            .where(
                and_(
                    t.provider_events.c.provider == s.provider,
                    t.provider_events.c.provider_event_id == s.provider_event_id,
                )
            )
            .values(**values)
        )
        if res.rowcount == 0:
            await self.conn.execute(
                insert(t.provider_events).values(
                    provider=s.provider, provider_event_id=s.provider_event_id, **values
                )
            )

    async def last_seen(self, provider: str, event_id: str) -> datetime | None:
        q = select(func.max(t.provider_events.c.last_seen_at)).where(
            and_(t.provider_events.c.provider == provider, t.provider_events.c.event_id == event_id)
        )
        return (await self.conn.execute(q)).scalar()


# ------------------------------------------------------------------------ markets
class MarketRepository(_Repo):
    async def get(self, market_id: str, *, for_update: bool = False) -> Market | None:
        q = select(t.markets).where(t.markets.c.id == market_id)
        if for_update:
            q = q.with_for_update()
        row = (await self.conn.execute(q)).first()
        return _market(row) if row else None

    async def get_by_key(
        self, event_id: str, market_type: MarketType, line: Decimal | None
    ) -> Market | None:
        q = (
            select(t.markets)
            .where(t.markets.c.event_id == event_id)
            .where(t.markets.c.type == market_type.value)
            .where(t.markets.c.line == line_to_str(line))
            .with_for_update()
        )
        row = (await self.conn.execute(q)).first()
        return _market(row) if row else None

    async def by_event(self, event_id: str, *, for_update: bool = False) -> list[Market]:
        q = select(t.markets).where(t.markets.c.event_id == event_id).order_by(t.markets.c.id)
        if for_update:
            q = q.with_for_update()
        return [_market(r) for r in await self.conn.execute(q)]

    async def all(self) -> list[Market]:
        return [_market(r) for r in await self.conn.execute(select(t.markets))]

    async def add(self, m: Market, at: datetime) -> None:
        await self.conn.execute(insert(t.markets).values(**_market_values(m), updated_at=at))

    async def save(self, m: Market, at: datetime) -> None:
        values = _market_values(m)
        values.pop("id")
        await self.conn.execute(
            update(t.markets).where(t.markets.c.id == m.id).values(**values, updated_at=at)
        )

    async def add_price_version(self, m: Market, provider: str, at: datetime) -> None:
        await self.conn.execute(
            insert(t.market_prices).values(
                market_id=m.id,
                version=m.price_version,
                prices={k: odds_to_str(v) for k, v in m.prices.items()},
                provider=provider,
                created_at=at,
            )
        )

    async def price_history(self, market_id: str) -> list[dict[str, Any]]:
        q = (
            select(t.market_prices)
            .where(t.market_prices.c.market_id == market_id)
            .order_by(t.market_prices.c.version)
        )
        return [dict(r._mapping) for r in await self.conn.execute(q)]


def _market_values(m: Market) -> dict[str, Any]:
    return {
        "id": m.id,
        "event_id": m.event_id,
        "type": m.type.value,
        "line": line_to_str(m.line),
        "status": m.status.value,
        "price_version": m.price_version,
        "prices": {k: odds_to_str(v) for k, v in m.prices.items()},
        "suspend_reason": m.suspend_reason.value if m.suspend_reason else None,
        "settled_result_version": m.settled_result_version,
    }


def _market(row: Row[Any]) -> Market:
    m = row._mapping
    return Market(
        id=m["id"],
        event_id=m["event_id"],
        type=MarketType(m["type"]),
        line=Decimal(m["line"]) if m["line"] != "" else None,
        status=MarketStatus(m["status"]),
        price_version=m["price_version"],
        prices={k: Decimal(v) for k, v in m["prices"].items()},
        suspend_reason=SuspendReason(m["suspend_reason"]) if m["suspend_reason"] else None,
        settled_result_version=m["settled_result_version"],
    )


# ------------------------------------------------------------------------ results
@dataclass(frozen=True, slots=True)
class ResultRow:
    event_id: str
    result_version: int
    score: Score
    provider: str


class ResultRepository(_Repo):
    async def latest(self, event_id: str) -> ResultRow | None:
        q = (
            select(t.event_results)
            .where(t.event_results.c.event_id == event_id)
            .order_by(t.event_results.c.result_version.desc())
            .limit(1)
        )
        row = (await self.conn.execute(q)).first()
        if row is None:
            return None
        m = row._mapping
        return ResultRow(
            m["event_id"],
            m["result_version"],
            Score(m["home_score"], m["away_score"], ResultStatus(m["status"])),
            m["provider"],
        )

    async def add(self, r: ResultRow, at: datetime) -> None:
        await self.conn.execute(
            insert(t.event_results).values(
                event_id=r.event_id,
                result_version=r.result_version,
                home_score=r.score.home,
                away_score=r.score.away,
                status=r.score.status.value,
                provider=r.provider,
                created_at=at,
            )
        )


# --------------------------------------------------------------------------- bets
class BetRepository(_Repo):
    async def add(self, b: Bet, reserve_txn_id: str) -> None:
        await self.conn.execute(
            insert(t.bets).values(**_bet_values(b), reserve_txn_id=reserve_txn_id)
        )

    async def get(self, bet_id: str) -> Bet | None:
        row = (await self.conn.execute(select(t.bets).where(t.bets.c.id == bet_id))).first()
        return _bet(row) if row else None

    async def by_market(self, market_id: str) -> list[Bet]:
        q = select(t.bets).where(t.bets.c.market_id == market_id).order_by(t.bets.c.id)
        return [_bet(r) for r in await self.conn.execute(q)]

    async def all_with_reserve(self) -> list[tuple[Bet, str]]:
        return [
            (_bet(r), r._mapping["reserve_txn_id"]) for r in await self.conn.execute(select(t.bets))
        ]

    async def mark_settled(self, bet_id: str, outcome: str, payout: int) -> None:
        # Current-state projection; the history lives in bet_settlements.
        await self.conn.execute(
            update(t.bets)
            .where(t.bets.c.id == bet_id)
            .values(status=BetStatus.SETTLED.value, outcome=outcome, payout=payout)
        )


def _bet_values(b: Bet) -> dict[str, Any]:
    return {
        "id": b.id,
        "player_id": b.player_id,
        "market_id": b.market_id,
        "selection": b.selection,
        "stake": b.stake,
        "odds": odds_to_str(b.odds),
        "price_version": b.price_version,
        "stake_cash": b.stake_cash,
        "stake_bonus": b.stake_bonus,
        "status": b.status.value,
        "outcome": b.outcome,
        "payout": b.payout,
        "placed_at": b.placed_at,
    }


def _bet(row: Row[Any]) -> Bet:
    m = row._mapping
    return Bet(
        id=m["id"],
        player_id=m["player_id"],
        market_id=m["market_id"],
        selection=m["selection"],
        stake=m["stake"],
        odds=Decimal(m["odds"]),
        price_version=m["price_version"],
        stake_cash=m["stake_cash"],
        stake_bonus=m["stake_bonus"],
        placed_at=m["placed_at"],
        status=BetStatus(m["status"]),
        outcome=m["outcome"],
        payout=m["payout"],
    )


# -------------------------------------------------------------------- settlements
@dataclass(frozen=True, slots=True)
class SettlementRow:
    id: str
    bet_id: str
    seq: int
    outcome: str
    payout: int
    refund_cash: int
    refund_bonus: int
    result_version: int
    txn_id: str
    reversal_txn_id: str | None
    created_at: datetime


class SettlementRepository(_Repo):
    async def latest(self, bet_id: str) -> SettlementRow | None:
        q = (
            select(t.bet_settlements)
            .where(t.bet_settlements.c.bet_id == bet_id)
            .order_by(t.bet_settlements.c.seq.desc())
            .limit(1)
        )
        row = (await self.conn.execute(q)).first()
        return SettlementRow(**row._mapping) if row else None

    async def history(self, bet_id: str) -> list[SettlementRow]:
        q = (
            select(t.bet_settlements)
            .where(t.bet_settlements.c.bet_id == bet_id)
            .order_by(t.bet_settlements.c.seq)
        )
        return [SettlementRow(**r._mapping) for r in await self.conn.execute(q)]

    async def latest_all(self) -> dict[str, SettlementRow]:
        rows = await self.conn.execute(select(t.bet_settlements).order_by(t.bet_settlements.c.seq))
        return {r._mapping["bet_id"]: SettlementRow(**r._mapping) for r in rows}

    async def add(self, s: SettlementRow) -> None:
        await self.conn.execute(insert(t.bet_settlements).values(**asdict(s)))


# ------------------------------------------------------------------------- ledger
def account_kind(account_id: str) -> str:
    if account_id in SYSTEM_ACCOUNTS:
        return account_id.replace(":", "_")
    return "player_" + account_id.rsplit(":", 1)[-1]


class LedgerRepository(_Repo):
    async def ensure_accounts(self, account_ids: Iterable[str], at: datetime) -> None:
        ids = sorted(set(account_ids))
        existing = {
            r[0]
            for r in await self.conn.execute(
                select(t.ledger_accounts.c.id).where(t.ledger_accounts.c.id.in_(ids))
            )
        }
        missing = [a for a in ids if a not in existing]
        if missing:
            await self.conn.execute(
                insert(t.ledger_accounts),
                [{"id": a, "kind": account_kind(a), "created_at": at} for a in missing],
            )

    async def lock_accounts(self, account_ids: Sequence[str], at: datetime) -> None:
        """Serialise balance checks per account. Sorted order prevents deadlocks."""
        await self.ensure_accounts(account_ids, at)
        q = (
            select(t.ledger_accounts.c.id)
            .where(t.ledger_accounts.c.id.in_(sorted(account_ids)))
            .order_by(t.ledger_accounts.c.id)
            .with_for_update()
        )
        await self.conn.execute(q)

    async def post(self, txn: LedgerTransaction) -> None:
        # The domain object already guarantees sum(postings) == 0.
        await self.ensure_accounts((p.account_id for p in txn.postings), txn.created_at)
        await self.conn.execute(
            insert(t.ledger_transactions).values(
                id=txn.id,
                kind=txn.kind.value,
                reference=txn.reference,
                reverses=txn.reverses,
                metadata=dict(txn.metadata),
                created_at=txn.created_at,
            )
        )
        await self.conn.execute(
            insert(t.ledger_postings),
            [
                {"txn_id": txn.id, "account_id": p.account_id, "amount": p.amount}
                for p in txn.postings
            ],
        )

    async def get(self, txn_id: str) -> LedgerTransaction | None:
        row = (
            await self.conn.execute(
                select(t.ledger_transactions).where(t.ledger_transactions.c.id == txn_id)
            )
        ).first()
        if row is None:
            return None
        m = row._mapping
        postings = [
            Posting(p.account_id, p.amount)
            for p in await self.conn.execute(
                select(t.ledger_postings.c.account_id, t.ledger_postings.c.amount)
                .where(t.ledger_postings.c.txn_id == txn_id)
                .order_by(t.ledger_postings.c.id)
            )
        ]
        return LedgerTransaction(
            id=m["id"],
            kind=TxnKind(m["kind"]),
            reference=m["reference"],
            postings=tuple(postings),
            created_at=m["created_at"],
            reverses=m["reverses"],
            metadata=m["metadata"],
        )

    async def balance(self, account_id: str) -> int:
        q = select(func.coalesce(func.sum(t.ledger_postings.c.amount), 0)).where(
            t.ledger_postings.c.account_id == account_id
        )
        return int((await self.conn.execute(q)).scalar_one())

    async def balances(self, account_ids: Iterable[str] | None = None) -> dict[str, int]:
        q = select(t.ledger_postings.c.account_id, func.sum(t.ledger_postings.c.amount)).group_by(
            t.ledger_postings.c.account_id
        )
        ids = list(account_ids) if account_ids is not None else None
        if ids is not None:
            q = q.where(t.ledger_postings.c.account_id.in_(ids))
        out = {a: int(v) for a, v in await self.conn.execute(q)}
        if ids is not None:
            for a in ids:
                out.setdefault(a, 0)
        return out

    async def unbalanced_transactions(self) -> list[tuple[str, int]]:
        q = (
            select(t.ledger_postings.c.txn_id, func.sum(t.ledger_postings.c.amount))
            .group_by(t.ledger_postings.c.txn_id)
            .having(func.sum(t.ledger_postings.c.amount) != 0)
        )
        return [(txn, int(total)) for txn, total in await self.conn.execute(q)]

    async def reversed_ids(self) -> set[str]:
        q = select(t.ledger_transactions.c.reverses).where(
            t.ledger_transactions.c.reverses.is_not(None)
        )
        return {r[0] for r in await self.conn.execute(q)}

    async def postings_for(self, txn_ids: Iterable[str]) -> dict[str, dict[str, int]]:
        ids = list(txn_ids)
        out: dict[str, dict[str, int]] = {i: {} for i in ids}
        if not ids:
            return out
        q = select(
            t.ledger_postings.c.txn_id, t.ledger_postings.c.account_id, t.ledger_postings.c.amount
        ).where(t.ledger_postings.c.txn_id.in_(ids))
        for txn_id, acc, amount in await self.conn.execute(q):
            out[txn_id][acc] = out[txn_id].get(acc, 0) + int(amount)
        return out

    async def count_transactions(self) -> int:
        q = select(func.count()).select_from(t.ledger_transactions)
        return int((await self.conn.execute(q)).scalar_one())


# ------------------------------------------------------------------- idempotency
@dataclass(frozen=True, slots=True)
class StoredResponse:
    request_hash: str
    status_code: int
    response: dict[str, Any]


class IdempotencyRepository(_Repo):
    async def get(self, scope: str, key: str) -> StoredResponse | None:
        row = (
            await self.conn.execute(
                select(t.idempotency_keys).where(
                    and_(t.idempotency_keys.c.scope == scope, t.idempotency_keys.c.key == key)
                )
            )
        ).first()
        if row is None:
            return None
        m = row._mapping
        return StoredResponse(m["request_hash"], m["status_code"], m["response"])

    async def save(
        self,
        scope: str,
        key: str,
        request_hash: str,
        status_code: int,
        response: dict[str, Any],
        at: datetime,
    ) -> None:
        """Raises IntegrityError if a concurrent request stored the key first."""
        await self.conn.execute(
            insert(t.idempotency_keys).values(
                scope=scope,
                key=key,
                request_hash=request_hash,
                status_code=status_code,
                response=response,
                created_at=at,
            )
        )


# ------------------------------------------------------------------- inbox/outbox
class InboxRepository(_Repo):
    async def try_add(self, consumer: str, message_id: str, at: datetime) -> bool:
        """True if the message is new. Runs in a SAVEPOINT so a duplicate does not
        poison the surrounding transaction on PostgreSQL."""
        exists = (
            await self.conn.execute(
                select(t.inbox.c.message_id).where(
                    and_(t.inbox.c.consumer == consumer, t.inbox.c.message_id == message_id)
                )
            )
        ).first()
        if exists:
            return False
        try:
            async with self.conn.begin_nested():
                await self.conn.execute(
                    insert(t.inbox).values(
                        consumer=consumer, message_id=message_id, processed_at=at
                    )
                )
        except IntegrityError:
            return False
        return True


@dataclass(frozen=True, slots=True)
class OutboxRow:
    id: int
    message_id: str
    topic: str
    key: str | None
    payload: str
    headers: dict[str, str]
    attempts: int


class OutboxRepository(_Repo):
    async def add(self, topic: str, env: Envelope, at: datetime) -> None:
        await self.conn.execute(
            insert(t.outbox).values(
                message_id=env.message_id,
                topic=topic,
                key=env.key,
                payload=env.model_dump_json(),
                headers={"type": env.type, "version": str(env.version)},
                created_at=at,
                attempts=0,
                next_attempt_at=at,
            )
        )

    async def fetch_due(self, now: datetime, limit: int) -> list[OutboxRow]:
        q = (
            select(t.outbox)
            .where(t.outbox.c.published_at.is_(None))
            .where(t.outbox.c.next_attempt_at <= now)
            .order_by(t.outbox.c.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return [
            OutboxRow(
                r.id, r.message_id, r.topic, r.key, r.payload, dict(r.headers or {}), r.attempts
            )
            for r in await self.conn.execute(q)
        ]

    async def mark_published(self, row_id: int, at: datetime) -> None:
        await self.conn.execute(
            update(t.outbox).where(t.outbox.c.id == row_id).values(published_at=at)
        )

    async def mark_failed(self, row_id: int, attempts: int, retry_at: datetime, error: str) -> None:
        await self.conn.execute(
            update(t.outbox)
            .where(t.outbox.c.id == row_id)
            .values(attempts=attempts, next_attempt_at=retry_at, last_error=error[:1000])
        )

    async def pending_count(self) -> int:
        q = select(func.count()).select_from(t.outbox).where(t.outbox.c.published_at.is_(None))
        return int((await self.conn.execute(q)).scalar_one())

    async def all(self) -> list[dict[str, Any]]:
        return [
            dict(r._mapping)
            for r in await self.conn.execute(select(t.outbox).order_by(t.outbox.c.id))
        ]


class AuditRepository(_Repo):
    async def add(
        self, entity_type: str, entity_id: str, action: str, data: dict[str, Any], at: datetime
    ) -> None:
        await self.conn.execute(
            insert(t.audit_log).values(
                entity_type=entity_type,
                entity_id=entity_id,
                action=action,
                data=data,
                created_at=at,
            )
        )

    async def for_entity(self, entity_type: str, entity_id: str) -> list[dict[str, Any]]:
        q = (
            select(t.audit_log)
            .where(
                and_(t.audit_log.c.entity_type == entity_type, t.audit_log.c.entity_id == entity_id)
            )
            .order_by(t.audit_log.c.id)
        )
        return [dict(r._mapping) for r in await self.conn.execute(q)]
