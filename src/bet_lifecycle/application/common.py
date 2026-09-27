"""Small helpers shared by the use cases."""

from __future__ import annotations

import uuid
from datetime import datetime

from bet_lifecycle.domain.markets import Market
from bet_lifecycle.domain.money import odds_to_str
from bet_lifecycle.infrastructure.database import Tx
from bet_lifecycle.messaging.envelopes import (
    REGISTRY,
    TOPIC_MARKETS,
    MarketPriceChangedV1,
    MarketStatusChangedV1,
)


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def market_cache_key(market_id: str) -> str:
    return f"market:{market_id}"


async def emit_market_status(tx: Tx, market: Market, at: datetime) -> None:
    payload = MarketStatusChangedV1(
        market_id=market.id,
        event_id=market.event_id,
        status=market.status.value,
        reason=market.suspend_reason.value if market.suspend_reason else None,
        price_version=market.price_version,
    )
    await tx.outbox.add(TOPIC_MARKETS, REGISTRY.envelope(payload, key=market.event_id), at)
    await tx.audit.add(
        "market",
        market.id,
        f"status:{market.status.value}",
        {"reason": payload.reason, "price_version": market.price_version},
        at,
    )


async def emit_market_price(tx: Tx, market: Market, provider: str, at: datetime) -> None:
    await tx.markets.add_price_version(market, provider, at)
    payload = MarketPriceChangedV1(
        market_id=market.id,
        event_id=market.event_id,
        price_version=market.price_version,
        prices={k: odds_to_str(v) for k, v in market.prices.items()},
    )
    await tx.outbox.add(TOPIC_MARKETS, REGISTRY.envelope(payload, key=market.event_id), at)
