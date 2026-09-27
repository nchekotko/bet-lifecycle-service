"""Markets: state machine and versioned prices."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum

from bet_lifecycle.domain.errors import (
    InvalidStateTransition,
    MarketNotOpen,
    Requote,
    UnknownSelection,
)
from bet_lifecycle.domain.money import odds_to_str


class MarketType(StrEnum):
    MATCH_RESULT = "1x2"
    TOTAL_GOALS = "ou"  # over/under total goals, Asian lines allowed
    ASIAN_HANDICAP = "ah"  # handicap applied to the home team


SELECTIONS: dict[MarketType, tuple[str, ...]] = {
    MarketType.MATCH_RESULT: ("home", "draw", "away"),
    MarketType.TOTAL_GOALS: ("over", "under"),
    MarketType.ASIAN_HANDICAP: ("home", "away"),
}


class MarketStatus(StrEnum):
    OPEN = "open"
    SUSPENDED = "suspended"
    CLOSED = "closed"
    SETTLED = "settled"
    VOIDED = "voided"


class SuspendReason(StrEnum):
    INCIDENT = "incident"  # goal / red card: price is no longer fair
    FEED_STALE = "feed_stale"  # no messages from the primary provider
    TRADER = "trader"  # manual; never lifted automatically


AUTO_REOPEN_REASONS = frozenset({SuspendReason.INCIDENT, SuspendReason.FEED_STALE})

_TRANSITIONS: dict[MarketStatus, frozenset[MarketStatus]] = {
    MarketStatus.OPEN: frozenset(
        {MarketStatus.SUSPENDED, MarketStatus.CLOSED, MarketStatus.SETTLED, MarketStatus.VOIDED}
    ),
    MarketStatus.SUSPENDED: frozenset(
        {MarketStatus.OPEN, MarketStatus.CLOSED, MarketStatus.SETTLED, MarketStatus.VOIDED}
    ),
    MarketStatus.CLOSED: frozenset({MarketStatus.SETTLED, MarketStatus.VOIDED}),
    # Result corrections: a settled market can be resettled or turned void and back.
    MarketStatus.SETTLED: frozenset({MarketStatus.SETTLED, MarketStatus.VOIDED}),
    MarketStatus.VOIDED: frozenset({MarketStatus.SETTLED, MarketStatus.VOIDED}),
}

TERMINAL = frozenset({MarketStatus.SETTLED, MarketStatus.VOIDED})


def line_to_str(line: Decimal | None) -> str:
    return "" if line is None else format(line.normalize(), "f")


def validate_line(market_type: MarketType, line: Decimal | None) -> None:
    if market_type is MarketType.MATCH_RESULT:
        if line is not None:
            raise ValueError("1x2 market has no line")
        return
    if line is None:
        raise ValueError(f"{market_type} market requires a line")
    if (line * 4) != (line * 4).to_integral_value():
        raise ValueError(f"line must be a multiple of 0.25, got {line}")
    if market_type is MarketType.TOTAL_GOALS and line <= 0:
        raise ValueError("total goals line must be positive")


@dataclass
class Market:
    id: str
    event_id: str
    type: MarketType
    line: Decimal | None
    status: MarketStatus = MarketStatus.SUSPENDED
    price_version: int = 0
    prices: dict[str, Decimal] = field(default_factory=dict)
    suspend_reason: SuspendReason | None = None
    settled_result_version: int | None = None

    def __post_init__(self) -> None:
        validate_line(self.type, self.line)

    @property
    def selections(self) -> tuple[str, ...]:
        return SELECTIONS[self.type]

    def _move(self, target: MarketStatus) -> None:
        if target not in _TRANSITIONS[self.status]:
            raise InvalidStateTransition(
                f"market {self.id}: {self.status} -> {target} is not allowed",
                market_id=self.id,
                status=str(self.status),
                target=str(target),
            )
        self.status = target

    def reprice(self, prices: dict[str, Decimal]) -> bool:
        """Publish a new price version. Returns False when nothing changed.

        A fresh price from the feed is also the signal that an automatic suspension
        (incident / stale feed) can be lifted; a trader suspension stays.
        """
        if set(prices) != set(self.selections):
            raise UnknownSelection(
                f"market {self.type} expects selections {self.selections}",
                got=sorted(prices),
            )
        if self.status in TERMINAL or self.status is MarketStatus.CLOSED:
            return False
        changed = prices != self.prices
        if changed:
            self.prices = dict(prices)
            self.price_version += 1
        if self.status is MarketStatus.SUSPENDED and (
            self.suspend_reason in AUTO_REOPEN_REASONS
            or (self.suspend_reason is None and self.price_version > 0)
        ):
            self.open()
            return True
        return changed

    def open(self) -> None:
        if not self.prices:
            raise InvalidStateTransition(f"market {self.id} has no prices", market_id=self.id)
        self._move(MarketStatus.OPEN)
        self.suspend_reason = None

    def suspend(self, reason: SuspendReason) -> bool:
        """Returns True if the state changed."""
        if self.status is MarketStatus.SUSPENDED:
            # A trader suspension must not be downgraded to an auto-reopenable one.
            if reason is SuspendReason.TRADER:
                self.suspend_reason = reason
            return False
        if self.status is not MarketStatus.OPEN:
            return False
        self._move(MarketStatus.SUSPENDED)
        self.suspend_reason = reason
        return True

    def close(self) -> bool:
        if self.status in (MarketStatus.OPEN, MarketStatus.SUSPENDED):
            self._move(MarketStatus.CLOSED)
            self.suspend_reason = None
            return True
        return False

    def mark_settled(self, result_version: int, *, voided: bool) -> None:
        self._move(MarketStatus.VOIDED if voided else MarketStatus.SETTLED)
        self.suspend_reason = None
        self.settled_result_version = result_version

    def quote(self, selection: str, requested_version: int) -> Decimal:
        """Validate that a bet can be accepted at ``requested_version``; return the odds."""
        if selection not in self.selections:
            raise UnknownSelection(
                f"unknown selection {selection!r}", allowed=list(self.selections)
            )
        if self.status is not MarketStatus.OPEN:
            raise MarketNotOpen(
                f"market {self.id} is {self.status}",
                market_id=self.id,
                status=str(self.status),
                reason=str(self.suspend_reason) if self.suspend_reason else None,
            )
        if requested_version != self.price_version:
            raise Requote(
                self.id,
                self.price_version,
                odds_to_str(self.prices[selection]),
                requested_version,
            )
        return self.prices[selection]
