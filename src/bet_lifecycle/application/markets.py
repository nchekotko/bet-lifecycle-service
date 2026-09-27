"""Market operations: read model with cache, trader actions, feed-staleness guard."""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from bet_lifecycle.application.common import emit_market_status, market_cache_key
from bet_lifecycle.application.ports import Cache, Clock, DistributedLock
from bet_lifecycle.domain.errors import NotFound
from bet_lifecycle.domain.markets import Market, SuspendReason, line_to_str
from bet_lifecycle.domain.money import odds_to_str
from bet_lifecycle.infrastructure.database import Database
from bet_lifecycle.observability.logging import get_logger
from bet_lifecycle.observability.metrics import MARKET_SUSPENSIONS_TOTAL

log = get_logger(__name__)


def market_view(m: Market) -> dict[str, Any]:
    return {
        "market_id": m.id,
        "event_id": m.event_id,
        "type": m.type.value,
        "line": line_to_str(m.line) or None,
        "status": m.status.value,
        "suspend_reason": m.suspend_reason.value if m.suspend_reason else None,
        "price_version": m.price_version,
        "prices": {k: odds_to_str(v) for k, v in m.prices.items()},
        "settled_result_version": m.settled_result_version,
    }


class MarketService:
    CACHE_TTL = 5.0

    def __init__(
        self,
        db: Database,
        clock: Clock,
        cache: Cache | None = None,
        lock: DistributedLock | None = None,
    ) -> None:
        self.db = db
        self.clock = clock
        self.cache = cache
        self.lock = lock

    async def get(self, market_id: str) -> dict[str, Any]:
        """Read-through cache. The cache serves display only: bet placement always
        re-checks the price version against the database under a row lock."""
        key = market_cache_key(market_id)
        if self.cache is not None:
            cached = await self.cache.get(key)
            if cached is not None:
                return json.loads(cached)
        async with self.db.transaction() as tx:
            market = await tx.markets.get(market_id)
        if market is None:
            raise NotFound(f"market {market_id} not found", market_id=market_id)
        view = market_view(market)
        if self.cache is not None:
            await self.cache.set(key, json.dumps(view), self.CACHE_TTL)
        return view

    async def list_for_event(self, event_id: str) -> list[dict[str, Any]]:
        async with self.db.transaction() as tx:
            return [market_view(m) for m in await tx.markets.by_event(event_id)]

    async def price_history(self, market_id: str) -> list[dict[str, Any]]:
        async with self.db.transaction() as tx:
            rows = await tx.markets.price_history(market_id)
        return [
            {
                "version": r["version"],
                "prices": r["prices"],
                "provider": r["provider"],
                "created_at": r["created_at"].isoformat(),
            }
            for r in rows
        ]

    async def trader_suspend(self, market_id: str) -> dict[str, Any]:
        return await self._change(market_id, suspend=True)

    async def trader_open(self, market_id: str) -> dict[str, Any]:
        return await self._change(market_id, suspend=False)

    async def _change(self, market_id: str, *, suspend: bool) -> dict[str, Any]:
        now = self.clock.now()
        async with self.db.transaction() as tx:
            market = await tx.markets.get(market_id, for_update=True)
            if market is None:
                raise NotFound(f"market {market_id} not found", market_id=market_id)
            prev = (market.status, market.suspend_reason)
            if suspend:
                market.suspend(SuspendReason.TRADER)
            else:
                market.open()
            if (market.status, market.suspend_reason) != prev:
                await tx.markets.save(market, now)
                await emit_market_status(tx, market, now)
            await self._invalidate_after_commit(tx, market_id)
        return market_view(market)

    async def suspend_stale(self, stale_after: timedelta) -> int:
        """Suspend open markets of events whose primary feed went silent.

        Guarded by a distributed lock so only one replica runs the sweep."""
        token = None
        if self.lock is not None:
            token = await self.lock.acquire("staleness-sweep", ttl_seconds=30)
            if token is None:
                return 0
        try:
            now = self.clock.now()
            suspended = 0
            async with self.db.transaction() as tx:
                for event_id in await tx.events.with_open_markets_stale(now - stale_after):
                    for market in await tx.markets.by_event(event_id, for_update=True):
                        if market.suspend(SuspendReason.FEED_STALE):
                            await tx.markets.save(market, now)
                            await emit_market_status(tx, market, now)
                            await self._invalidate_after_commit(tx, market.id)
                            MARKET_SUSPENSIONS_TOTAL.labels(SuspendReason.FEED_STALE.value).inc()
                            suspended += 1
            if suspended:
                log.warning("markets suspended on stale feed", extra={"count": suspended})
            return suspended
        finally:
            if self.lock is not None and token is not None:
                await self.lock.release("staleness-sweep", token)

    async def _invalidate_after_commit(self, tx: Any, market_id: str) -> None:
        if self.cache is not None:
            cache = self.cache
            tx.after_commit(lambda: cache.delete(market_cache_key(market_id)))
