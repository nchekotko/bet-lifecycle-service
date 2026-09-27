"""Canonical feed model and per-stream sequencing.

Every provider has its own schema; provider adapters (``bet_lifecycle.feed``) turn raw
payloads into :class:`FeedMessage`. Each provider event is an independent stream with
a monotonically increasing ``seq``. The :class:`SequenceGate` turns an at-least-once,
possibly reordered stream into an exactly-once, in-order one:

* ``seq <= last_applied`` or already buffered -> duplicate, dropped;
* ``seq == last_applied + 1`` -> applied, followed by any buffered successors;
* ``seq > last_applied + 1`` -> buffered until the gap is filled;
* if the buffer grows beyond ``max_buffer`` the gap is declared lost and the buffered
  messages are applied in order (liveness beats completeness for live odds; the
  skipped range is reported so it can be alerted on).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from bet_lifecycle.domain.markets import MarketType
from bet_lifecycle.domain.settlement import Score


class FeedKind(StrEnum):
    FIXTURE = "fixture"
    ODDS = "odds"
    INCIDENT = "incident"
    STATUS = "status"
    RESULT = "result"
    HEARTBEAT = "heartbeat"


class EventStatus(StrEnum):
    SCHEDULED = "scheduled"
    LIVE = "live"
    ENDED = "ended"
    ABANDONED = "abandoned"


@dataclass(frozen=True, slots=True)
class Fixture:
    home: str
    away: str
    kickoff: datetime


@dataclass(frozen=True, slots=True)
class MarketQuote:
    market_type: MarketType
    line: Decimal | None
    prices: dict[str, Decimal]


@dataclass(frozen=True, slots=True)
class FeedMessage:
    provider: str
    provider_event_id: str
    seq: int
    kind: FeedKind
    sent_at: datetime
    fixture: Fixture | None = None
    quotes: tuple[MarketQuote, ...] = ()
    incident: str | None = None
    status: EventStatus | None = None
    score: Score | None = None

    @property
    def stream(self) -> tuple[str, str]:
        return (self.provider, self.provider_event_id)


@dataclass
class GateDecision:
    ready: list[FeedMessage] = field(default_factory=list)
    duplicate: bool = False
    buffered: bool = False
    skipped: tuple[int, int] | None = None  # inclusive range of lost seqs

    @property
    def new_cursor(self) -> int | None:
        return self.ready[-1].seq if self.ready else None


class SequenceGate:
    def __init__(self, max_buffer: int = 16) -> None:
        self.max_buffer = max_buffer
        self._buffers: dict[tuple[str, str], dict[int, FeedMessage]] = {}

    def buffered(self, stream: tuple[str, str]) -> int:
        return len(self._buffers.get(stream, {}))

    def restore(self, messages: list[FeedMessage]) -> None:
        """Put released messages back (their transaction was rolled back)."""
        for m in messages:
            self._buffers.setdefault(m.stream, {})[m.seq] = m

    def offer(self, msg: FeedMessage, last_applied: int) -> GateDecision:
        buf = self._buffers.setdefault(msg.stream, {})
        # Drop anything the persisted cursor has already passed (e.g. after restart).
        for stale in [s for s in buf if s <= last_applied]:
            del buf[stale]
        if msg.seq <= last_applied or msg.seq in buf:
            return GateDecision(duplicate=True)
        buf[msg.seq] = msg
        decision = GateDecision()
        expected = last_applied + 1
        if expected not in buf and len(buf) > self.max_buffer:
            first = min(buf)
            decision.skipped = (expected, first - 1)
            expected = first
        while expected in buf:
            decision.ready.append(buf.pop(expected))
            expected += 1
        if not decision.ready:
            decision.buffered = True
        if not buf:
            self._buffers.pop(msg.stream, None)
        return decision
