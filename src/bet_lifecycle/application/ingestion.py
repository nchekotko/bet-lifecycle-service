"""Feed ingestion: provider message -> canonical event/market state + outbox events.

Per message:

1. normalise the provider schema (``FeedSchemaError`` -> DLQ, never retried);
2. pass it through the :class:`SequenceGate` of its stream, using the cursor persisted
   in ``provider_events.last_seq`` (duplicates / stale messages are dropped, gaps are
   buffered);
3. apply every message the gate releases, in order, inside one DB transaction that
   also advances the cursor. Each message runs in a SAVEPOINT: a message that is
   invalid for the domain (unknown event, wrong selections) is rejected and audited
   without blocking the stream.

Provider priority: odds, status and results are taken from the event's *primary*
provider only. If the primary goes silent for ``failover_after`` the next provider
that sends a message becomes primary. Incidents (goal, red card) are safety signals
and suspend markets whichever provider reports them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from bet_lifecycle.application.common import (
    emit_market_price,
    emit_market_status,
    market_cache_key,
    new_id,
)
from bet_lifecycle.application.ports import Cache, Clock
from bet_lifecycle.domain.errors import DomainError, UnknownEvent
from bet_lifecycle.domain.feed import (
    EventStatus,
    FeedKind,
    FeedMessage,
    MarketQuote,
    SequenceGate,
)
from bet_lifecycle.domain.markets import Market, SuspendReason, line_to_str
from bet_lifecycle.domain.settlement import ResultStatus, Score
from bet_lifecycle.feed.matching import Candidate, best_match, normalize_team
from bet_lifecycle.feed.providers import normalize
from bet_lifecycle.infrastructure.database import Database, Tx
from bet_lifecycle.infrastructure.repositories import EventRow, ResultRow, StreamRow
from bet_lifecycle.messaging.envelopes import REGISTRY, TOPIC_RESULTS, ResultPublishedV2
from bet_lifecycle.observability.logging import get_logger
from bet_lifecycle.observability.metrics import (
    FEED_GAPS_TOTAL,
    FEED_MESSAGES_TOTAL,
    MARKET_SUSPENSIONS_TOTAL,
)

log = get_logger(__name__)

MATCH_WINDOW = timedelta(minutes=90)


def market_id_for(event_id: str, quote: MarketQuote) -> str:
    return f"{event_id}.{quote.market_type.value}{line_to_str(quote.line)}"


@dataclass
class IngestResult:
    provider: str
    provider_event_id: str
    seq: int
    event_id: str | None = None
    applied: list[int] = field(default_factory=list)
    rejected: list[tuple[int, str]] = field(default_factory=list)
    duplicate: bool = False
    buffered: bool = False
    skipped: tuple[int, int] | None = None


class FeedIngestionService:
    def __init__(
        self,
        db: Database,
        clock: Clock,
        cache: Cache | None = None,
        *,
        failover_after: timedelta = timedelta(seconds=20),
        max_buffer: int = 16,
    ) -> None:
        self.db = db
        self.clock = clock
        self.cache = cache
        self.failover_after = failover_after
        self.gate = SequenceGate(max_buffer)

    async def ingest(self, provider: str, raw: dict[str, Any]) -> IngestResult:
        return await self.ingest_message(normalize(provider, raw))

    async def ingest_message(self, msg: FeedMessage) -> IngestResult:
        result = IngestResult(msg.provider, msg.provider_event_id, msg.seq)
        now = self.clock.now()
        touched: set[str] = set()
        decision = None
        try:
            async with self.db.transaction() as tx:
                stream = await tx.feed.get(msg.provider, msg.provider_event_id, for_update=True)
                decision = self.gate.offer(msg, stream.last_seq if stream else 0)
                if decision.duplicate or decision.buffered:
                    result.duplicate, result.buffered = decision.duplicate, decision.buffered
                    FEED_MESSAGES_TOTAL.labels(
                        msg.provider, "duplicate" if decision.duplicate else "buffered"
                    ).inc()
                    return result
                if decision.skipped:
                    result.skipped = decision.skipped
                    FEED_GAPS_TOTAL.labels(msg.provider).inc()
                    log.warning(
                        "feed sequence gap skipped",
                        extra={
                            "provider": msg.provider,
                            "stream": msg.provider_event_id,
                            "lost_from": decision.skipped[0],
                            "lost_to": decision.skipped[1],
                        },
                    )
                event_id = stream.event_id if stream else None
                for m in decision.ready:
                    try:
                        async with tx.conn.begin_nested():
                            event_id = await self._apply(tx, m, event_id, now, touched)
                        result.applied.append(m.seq)
                        FEED_MESSAGES_TOTAL.labels(m.provider, "applied").inc()
                    except DomainError as exc:
                        result.rejected.append((m.seq, exc.code))
                        FEED_MESSAGES_TOTAL.labels(m.provider, "rejected").inc()
                        await tx.audit.add(
                            "feed_stream",
                            f"{m.provider}:{m.provider_event_id}",
                            "rejected",
                            {"seq": m.seq, "kind": m.kind.value, "error": exc.message},
                            now,
                        )
                        log.warning(
                            "feed message rejected",
                            extra={"provider": m.provider, "seq": m.seq, "error": exc.message},
                        )
                result.event_id = event_id
                await tx.feed.upsert(
                    StreamRow(
                        msg.provider,
                        msg.provider_event_id,
                        event_id,
                        decision.ready[-1].seq,
                        now,
                    )
                )
                if self.cache and touched:
                    keys = [market_cache_key(m) for m in touched]
                    tx.after_commit(lambda: self.cache.delete(*keys))  # type: ignore[union-attr]
        except Exception:
            # Transaction rolled back: give released messages back to the gate so that
            # a redelivery of ``msg`` replays them instead of losing them.
            if decision is not None and decision.ready:
                self.gate.restore([m for m in decision.ready if m.seq != msg.seq])
            raise
        return result

    # ----------------------------------------------------------------- apply
    async def _apply(
        self, tx: Tx, m: FeedMessage, event_id: str | None, now: datetime, touched: set[str]
    ) -> str:
        if event_id is None:
            event_id = await self._resolve_event(tx, m, now)
        event = await tx.events.get(event_id, for_update=True)
        assert event is not None
        primary = await self._is_primary(tx, event, m, now)
        if primary:
            await tx.events.update(event_id, last_feed_at=now)

        if m.kind is FeedKind.INCIDENT:
            await self._suspend_event(tx, event_id, SuspendReason.INCIDENT, now, touched)
        elif not primary:
            return event_id
        elif m.kind is FeedKind.FIXTURE and m.fixture and m.fixture.kickoff != event.kickoff:
            await tx.events.update(event_id, kickoff=m.fixture.kickoff)
            await tx.audit.add(
                "event",
                event_id,
                "kickoff_changed",
                {"from": event.kickoff.isoformat(), "to": m.fixture.kickoff.isoformat()},
                now,
            )
        elif m.kind is FeedKind.ODDS:
            if event.status in (EventStatus.ENDED, EventStatus.ABANDONED):
                return event_id
            for quote in m.quotes:
                await self._apply_quote(tx, event_id, m.provider, quote, now, touched)
        elif m.kind is FeedKind.STATUS and m.status is not None:
            await self._apply_status(tx, event_id, m, now, touched)
        elif m.kind is FeedKind.RESULT and m.score is not None:
            await self._publish_result(tx, event_id, m.score, m.provider, now, touched)
        return event_id

    async def _resolve_event(self, tx: Tx, m: FeedMessage, now: datetime) -> str:
        if m.fixture is None:
            raise UnknownEvent(
                f"{m.provider}:{m.provider_event_id} is not mapped and carries no fixture",
                provider=m.provider,
                provider_event_id=m.provider_event_id,
            )
        fx = m.fixture
        nearby = await tx.events.near_kickoff(fx.kickoff - MATCH_WINDOW, fx.kickoff + MATCH_WINDOW)
        matched = best_match(
            fx.home,
            fx.away,
            fx.kickoff,
            [Candidate(e.id, e.home_norm, e.away_norm, e.kickoff) for e in nearby],
            max_kickoff_delta=MATCH_WINDOW,
        )
        if matched:
            await tx.audit.add(
                "event",
                matched,
                "provider_mapped",
                {"provider": m.provider, "provider_event_id": m.provider_event_id},
                now,
            )
            return matched
        event = EventRow(
            id=new_id("ev")[:15],
            home=fx.home,
            away=fx.away,
            home_norm=normalize_team(fx.home),
            away_norm=normalize_team(fx.away),
            kickoff=fx.kickoff,
            status=EventStatus.SCHEDULED.value,
            primary_provider=m.provider,
            last_feed_at=now,
        )
        await tx.events.add(event, now)
        await tx.audit.add(
            "event",
            event.id,
            "created",
            {
                "provider": m.provider,
                "provider_event_id": m.provider_event_id,
                "home": fx.home,
                "away": fx.away,
            },
            now,
        )
        return event.id

    async def _is_primary(self, tx: Tx, event: EventRow, m: FeedMessage, now: datetime) -> bool:
        if event.primary_provider == m.provider:
            return True
        seen = await tx.feed.last_seen(event.primary_provider, event.id)
        if seen is not None and now - seen <= self.failover_after:
            return False
        await tx.events.update(event.id, primary_provider=m.provider)
        await tx.audit.add(
            "event",
            event.id,
            "primary_failover",
            {
                "from": event.primary_provider,
                "to": m.provider,
                "primary_last_seen": seen.isoformat() if seen else None,
            },
            now,
        )
        log.warning(
            "primary feed failover",
            extra={"event_id": event.id, "from": event.primary_provider, "to": m.provider},
        )
        return True

    async def _apply_quote(
        self,
        tx: Tx,
        event_id: str,
        provider: str,
        quote: MarketQuote,
        now: datetime,
        touched: set[str],
    ) -> None:
        market = await tx.markets.get_by_key(event_id, quote.market_type, quote.line)
        is_new = market is None
        if market is None:
            market = Market(market_id_for(event_id, quote), event_id, quote.market_type, quote.line)
            await tx.markets.add(market, now)
        prev_version, prev_status = market.price_version, market.status
        market.reprice(quote.prices)
        if market.price_version == prev_version and market.status == prev_status and not is_new:
            return
        await tx.markets.save(market, now)
        touched.add(market.id)
        if market.price_version != prev_version:
            await emit_market_price(tx, market, provider, now)
        if market.status != prev_status:
            await emit_market_status(tx, market, now)

    async def _suspend_event(
        self, tx: Tx, event_id: str, reason: SuspendReason, now: datetime, touched: set[str]
    ) -> int:
        count = 0
        for market in await tx.markets.by_event(event_id, for_update=True):
            if market.suspend(reason):
                await tx.markets.save(market, now)
                await emit_market_status(tx, market, now)
                MARKET_SUSPENSIONS_TOTAL.labels(reason.value).inc()
                touched.add(market.id)
                count += 1
        return count

    async def _close_event_markets(
        self, tx: Tx, event_id: str, now: datetime, touched: set[str]
    ) -> None:
        for market in await tx.markets.by_event(event_id, for_update=True):
            if market.close():
                await tx.markets.save(market, now)
                await emit_market_status(tx, market, now)
                touched.add(market.id)

    async def _apply_status(
        self, tx: Tx, event_id: str, m: FeedMessage, now: datetime, touched: set[str]
    ) -> None:
        assert m.status is not None
        await tx.events.update(event_id, status=m.status.value)
        if m.status is EventStatus.ENDED:
            await self._close_event_markets(tx, event_id, now, touched)
        elif m.status is EventStatus.ABANDONED:
            await self._publish_result(
                tx, event_id, Score(0, 0, ResultStatus.ABANDONED), m.provider, now, touched
            )

    async def _publish_result(
        self,
        tx: Tx,
        event_id: str,
        score: Score,
        provider: str,
        now: datetime,
        touched: set[str],
    ) -> None:
        await self._close_event_markets(tx, event_id, now, touched)
        latest = await tx.results.latest(event_id)
        if latest is not None and latest.score == score:
            return  # same result re-sent: nothing to correct
        version = latest.result_version + 1 if latest else 1
        await tx.results.add(ResultRow(event_id, version, score, provider), now)
        payload = ResultPublishedV2(
            event_id=event_id,
            home_score=score.home,
            away_score=score.away,
            status=score.status,
            result_version=version,
        )
        await tx.outbox.add(TOPIC_RESULTS, REGISTRY.envelope(payload, key=event_id), now)
        await tx.audit.add(
            "event",
            event_id,
            "result_published" if latest is None else "result_corrected",
            {
                "version": version,
                "home": score.home,
                "away": score.away,
                "status": score.status.value,
                "provider": provider,
            },
            now,
        )
        log.info("result published", extra={"event_id": event_id, "result_version": version})
