"""Provider adapters: raw provider payloads -> canonical :class:`FeedMessage`.

Two mock providers with deliberately different schemas:

``alpha`` – ISO timestamps, decimal odds, selections "1"/"X"/"2", fixture info on
every message, string fixture ids.

``beta`` – epoch timestamps, UK fractional odds, numeric event ids, participants only
on ``EVENT_CREATED``, scores as ``"2:1"`` strings.

Anything that cannot be parsed raises :class:`FeedSchemaError`; the consumer routes
such messages to the DLQ instead of retrying them.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from bet_lifecycle.domain.feed import EventStatus, FeedKind, FeedMessage, Fixture, MarketQuote
from bet_lifecycle.domain.markets import MarketType
from bet_lifecycle.domain.money import fractional_to_decimal, parse_odds
from bet_lifecycle.domain.settlement import ResultStatus, Score


class FeedSchemaError(ValueError):
    """Raw message does not match the provider schema (poison message)."""


def _require(obj: dict[str, Any], key: str) -> Any:
    try:
        return obj[key]
    except (KeyError, TypeError) as exc:
        raise FeedSchemaError(f"missing field {key!r}") from exc


def _iso(value: str) -> datetime:
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise FeedSchemaError(f"bad timestamp {value!r}") from exc
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _epoch(value: float | int, *, millis: bool) -> datetime:
    try:
        return datetime.fromtimestamp(value / 1000 if millis else value, tz=UTC)
    except (TypeError, ValueError, OverflowError) as exc:
        raise FeedSchemaError(f"bad epoch {value!r}") from exc


# --------------------------------------------------------------------------- alpha
_ALPHA_KIND = {
    "fixture": FeedKind.FIXTURE,
    "odds": FeedKind.ODDS,
    "incident": FeedKind.INCIDENT,
    "status": FeedKind.STATUS,
    "result": FeedKind.RESULT,
    "heartbeat": FeedKind.HEARTBEAT,
}
_ALPHA_MARKETS = {
    "MATCH_ODDS": (MarketType.MATCH_RESULT, {"1": "home", "X": "draw", "2": "away"}),
    "TOTAL": (MarketType.TOTAL_GOALS, {"OVER": "over", "UNDER": "under"}),
    "HANDICAP": (MarketType.ASIAN_HANDICAP, {"HOME": "home", "AWAY": "away"}),
}
_ALPHA_STATUS = {
    "NOT_STARTED": EventStatus.SCHEDULED,
    "LIVE": EventStatus.LIVE,
    "ENDED": EventStatus.ENDED,
    "ABANDONED": EventStatus.ABANDONED,
}


def _quote(market_type: MarketType, line: Any, prices: dict[str, Decimal]) -> MarketQuote:
    try:
        parsed_line = None if line in (None, "") else Decimal(str(line))
        return MarketQuote(market_type, parsed_line, prices)
    except ArithmeticError as exc:
        raise FeedSchemaError(f"bad line {line!r}") from exc


def normalize_alpha(raw: dict[str, Any]) -> FeedMessage:
    kind = _ALPHA_KIND.get(_require(raw, "type"))
    if kind is None:
        raise FeedSchemaError(f"alpha: unknown type {raw.get('type')!r}")
    fx = raw.get("fixture")
    fixture = (
        Fixture(str(_require(fx, "home")), str(_require(fx, "away")), _iso(_require(fx, "start")))
        if fx
        else None
    )
    quotes: list[MarketQuote] = []
    for m in raw.get("markets") or []:
        spec = _ALPHA_MARKETS.get(_require(m, "market"))
        if spec is None:
            continue  # markets we do not offer are ignored, not rejected
        market_type, sel_map = spec
        try:
            prices = {sel_map[k]: parse_odds(v) for k, v in _require(m, "prices").items()}
        except (KeyError, ValueError) as exc:
            raise FeedSchemaError(f"alpha: bad prices {m!r}") from exc
        quotes.append(_quote(market_type, m.get("line"), prices))
    status = None
    if kind is FeedKind.STATUS:
        status = _ALPHA_STATUS.get(_require(raw, "status"))
        if status is None:
            raise FeedSchemaError(f"alpha: unknown status {raw.get('status')!r}")
    score = None
    if kind is FeedKind.RESULT:
        s = _require(raw, "score")
        score = Score(
            int(_require(s, "home")),
            int(_require(s, "away")),
            ResultStatus.ABANDONED if s.get("abandoned") else ResultStatus.FINAL,
        )
    incident = None
    if kind is FeedKind.INCIDENT:
        incident = str(_require(_require(raw, "incident"), "type")).lower()
    try:
        seq = int(_require(raw, "seq"))
    except (TypeError, ValueError) as exc:
        raise FeedSchemaError("alpha: bad seq") from exc
    return FeedMessage(
        provider="alpha",
        provider_event_id=str(_require(raw, "fixture_id")),
        seq=seq,
        kind=kind,
        sent_at=_iso(_require(raw, "ts")),
        fixture=fixture,
        quotes=tuple(quotes),
        incident=incident,
        status=status,
        score=score,
    )


# ---------------------------------------------------------------------------- beta
_BETA_KIND = {
    "EVENT_CREATED": FeedKind.FIXTURE,
    "PRICE_UPDATE": FeedKind.ODDS,
    "GOAL": FeedKind.INCIDENT,
    "RED_CARD": FeedKind.INCIDENT,
    "STATE": FeedKind.STATUS,
    "FINAL": FeedKind.RESULT,
    "HB": FeedKind.HEARTBEAT,
}
_BETA_MARKETS = {
    "win-draw-win": (MarketType.MATCH_RESULT, {"H": "home", "D": "draw", "A": "away"}),
    "totals": (MarketType.TOTAL_GOALS, {"O": "over", "U": "under"}),
    "asian": (MarketType.ASIAN_HANDICAP, {"H": "home", "A": "away"}),
}
_BETA_STATE = {
    "NS": EventStatus.SCHEDULED,
    "LIVE": EventStatus.LIVE,
    "FT": EventStatus.ENDED,
    "ABD": EventStatus.ABANDONED,
}


def normalize_beta(raw: dict[str, Any]) -> FeedMessage:
    kind_raw = _require(raw, "kind")
    kind = _BETA_KIND.get(kind_raw)
    if kind is None:
        raise FeedSchemaError(f"beta: unknown kind {kind_raw!r}")
    event = _require(raw, "event")
    payload = raw.get("payload") or {}
    fixture = None
    participants = event.get("participants")
    if participants:
        sides = {p["side"]: p["name"] for p in participants}
        try:
            fixture = Fixture(sides["H"], sides["A"], _epoch(event["kickoff_ts"], millis=False))
        except KeyError as exc:
            raise FeedSchemaError("beta: incomplete participants") from exc
    quotes: list[MarketQuote] = []
    for offer in payload.get("offers") or []:
        spec = _BETA_MARKETS.get(_require(offer, "type"))
        if spec is None:
            continue
        market_type, sel_map = spec
        try:
            prices = {
                sel_map[o["id"]]: fractional_to_decimal(o["frac"])
                for o in _require(offer, "outcomes")
            }
        except (KeyError, ValueError, TypeError) as exc:
            raise FeedSchemaError(f"beta: bad outcomes {offer!r}") from exc
        quotes.append(_quote(market_type, offer.get("handicap"), prices))
    status = None
    if kind is FeedKind.STATUS:
        status = _BETA_STATE.get(_require(payload, "state"))
        if status is None:
            raise FeedSchemaError(f"beta: unknown state {payload.get('state')!r}")
    score = None
    if kind is FeedKind.RESULT:
        try:
            home_s, away_s = str(_require(payload, "score")).split(":")
            score = Score(
                int(home_s),
                int(away_s),
                ResultStatus.ABANDONED if payload.get("abandoned") else ResultStatus.FINAL,
            )
        except ValueError as exc:
            raise FeedSchemaError(f"beta: bad score {payload.get('score')!r}") from exc
    incident = {"GOAL": "goal", "RED_CARD": "red_card"}.get(kind_raw)
    try:
        seq = int(_require(raw, "msg_seq"))
    except (TypeError, ValueError) as exc:
        raise FeedSchemaError("beta: bad msg_seq") from exc
    return FeedMessage(
        provider="beta",
        provider_event_id=str(_require(event, "id")),
        seq=seq,
        kind=kind,
        sent_at=_epoch(_require(raw, "sent"), millis=True),
        fixture=fixture,
        quotes=tuple(quotes),
        incident=incident,
        status=status,
        score=score,
    )


NORMALIZERS: dict[str, Callable[[dict[str, Any]], FeedMessage]] = {
    "alpha": normalize_alpha,
    "beta": normalize_beta,
}


def normalize(provider: str, raw: dict[str, Any]) -> FeedMessage:
    try:
        fn = NORMALIZERS[provider]
    except KeyError as exc:
        raise FeedSchemaError(f"unknown provider {provider!r}") from exc
    if not isinstance(raw, dict):
        raise FeedSchemaError("payload must be a JSON object")
    msg = fn(raw)
    if msg.seq <= 0:
        raise FeedSchemaError("seq must be positive")
    return msg
