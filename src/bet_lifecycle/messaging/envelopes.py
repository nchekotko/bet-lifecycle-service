"""Versioned event envelopes and upcasting.

Every message on the bus is an :class:`Envelope` with ``type`` + ``version``. Payload
models are registered per (type, version). Consumers always work with the *latest*
version: older payloads are upcast step by step (v1 -> v2 -> ...) before validation,
so producers can be upgraded independently of consumers. Messages with an unknown
type or a version newer than the consumer knows are poison and go to the DLQ.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from bet_lifecycle.domain.settlement import ResultStatus

TOPIC_RESULTS = "sportsbook.results"
TOPIC_BETS = "sportsbook.bets"
TOPIC_MARKETS = "sportsbook.markets"
FEED_TOPIC_PREFIX = "feed.raw."
DLQ_SUFFIX = ".dlq"


def dlq_topic(topic: str) -> str:
    return topic + DLQ_SUFFIX


class PoisonMessage(Exception):
    """Message can never be processed successfully; retrying is pointless."""


class Envelope(BaseModel):
    model_config = ConfigDict(frozen=True)

    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: str
    version: int
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    key: str
    payload: dict[str, Any]

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode()

    @classmethod
    def from_bytes(cls, raw: bytes) -> Envelope:
        try:
            return cls.model_validate(json.loads(raw))
        except (ValueError, ValidationError) as exc:
            raise PoisonMessage(f"not an envelope: {exc}") from exc


# ------------------------------------------------------------------ payload models
class _Payload(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ResultPublishedV1(_Payload):
    event_id: str
    score: str  # "2-1"
    abandoned: bool = False
    result_version: int


class ResultPublishedV2(_Payload):
    """v2: score split into integers, the boolean flag became an explicit status."""

    event_id: str
    home_score: int
    away_score: int
    status: ResultStatus
    result_version: int


class BetPlacedV1(_Payload):
    bet_id: str
    player_id: str
    market_id: str
    selection: str
    stake: int
    odds: str
    price_version: int
    placed_at: datetime


class BetSettledV1(_Payload):
    bet_id: str
    player_id: str
    market_id: str
    outcome: str
    payout: int
    refund: int
    settlement_seq: int
    result_version: int
    resettlement: bool


class MarketStatusChangedV1(_Payload):
    market_id: str
    event_id: str
    status: str
    reason: str | None
    price_version: int


class MarketPriceChangedV1(_Payload):
    market_id: str
    event_id: str
    price_version: int
    prices: dict[str, str]


# ------------------------------------------------------------------------ registry
Upcaster = Callable[[dict[str, Any]], dict[str, Any]]


class SchemaRegistry:
    def __init__(self) -> None:
        self._models: dict[tuple[str, int], type[BaseModel]] = {}
        self._upcasters: dict[tuple[str, int], Upcaster] = {}
        self._latest: dict[str, int] = {}

    def register(self, type_: str, version: int, model: type[BaseModel]) -> None:
        self._models[(type_, version)] = model
        self._latest[type_] = max(self._latest.get(type_, 0), version)

    def register_upcaster(self, type_: str, from_version: int, fn: Upcaster) -> None:
        self._upcasters[(type_, from_version)] = fn

    def latest(self, type_: str) -> int:
        try:
            return self._latest[type_]
        except KeyError as exc:
            raise PoisonMessage(f"unknown message type {type_!r}") from exc

    def version_of(self, model: type[BaseModel]) -> tuple[str, int]:
        for key, m in self._models.items():
            if m is model:
                return key
        raise KeyError(f"{model.__name__} is not registered")

    def envelope(
        self,
        payload: BaseModel,
        key: str,
        *,
        message_id: str | None = None,
        occurred_at: datetime | None = None,
    ) -> Envelope:
        type_, version = self.version_of(type(payload))
        return Envelope(
            message_id=message_id or str(uuid.uuid4()),
            type=type_,
            version=version,
            occurred_at=occurred_at or datetime.now(UTC),
            key=key,
            payload=payload.model_dump(mode="json"),
        )

    def decode(self, env: Envelope) -> BaseModel:
        """Validate the payload at the latest known version of its type."""
        latest = self.latest(env.type)
        if env.version > latest:
            raise PoisonMessage(f"{env.type} v{env.version} is newer than supported v{latest}")
        data = dict(env.payload)
        version = env.version
        try:
            self._models[(env.type, version)].model_validate(data)
            while version < latest:
                data = self._upcasters[(env.type, version)](data)
                version += 1
            return self._models[(env.type, latest)].model_validate(data)
        except KeyError as exc:
            raise PoisonMessage(f"no schema/upcaster for {env.type} v{version}") from exc
        except (ValidationError, ValueError) as exc:
            raise PoisonMessage(f"invalid {env.type} v{version} payload: {exc}") from exc


def _result_v1_to_v2(data: dict[str, Any]) -> dict[str, Any]:
    home, away = (int(x) for x in str(data["score"]).split("-"))
    return {
        "event_id": data["event_id"],
        "home_score": home,
        "away_score": away,
        "status": ResultStatus.ABANDONED if data.get("abandoned") else ResultStatus.FINAL,
        "result_version": data["result_version"],
    }


def default_registry() -> SchemaRegistry:
    r = SchemaRegistry()
    r.register("result.published", 1, ResultPublishedV1)
    r.register("result.published", 2, ResultPublishedV2)
    r.register_upcaster("result.published", 1, _result_v1_to_v2)
    r.register("bet.placed", 1, BetPlacedV1)
    r.register("bet.settled", 1, BetSettledV1)
    r.register("market.status_changed", 1, MarketStatusChangedV1)
    r.register("market.price_changed", 1, MarketPriceChangedV1)
    return r


REGISTRY = default_registry()
