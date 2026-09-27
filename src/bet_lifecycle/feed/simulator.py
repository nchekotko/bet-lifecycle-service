"""Mock providers: build raw messages in the alpha / beta wire formats.

Used by the demo, the tests, the load test and the Kafka feed producer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from fractions import Fraction
from typing import Any


def _iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def to_fractional(decimal_odds: str) -> str:
    frac = Fraction(Decimal(decimal_odds) - 1).limit_denominator(100)
    return f"{frac.numerator}/{frac.denominator}"


@dataclass
class AlphaFixture:
    fixture_id: str
    home: str
    away: str
    start: datetime
    seq: int = 0
    clock: datetime | None = None

    def _base(self, type_: str, ts: datetime | None) -> dict[str, Any]:
        self.seq += 1
        return {
            "provider": "alpha",
            "fixture_id": self.fixture_id,
            "seq": self.seq,
            "ts": _iso(ts or self.clock or self.start),
            "type": type_,
            "fixture": {"home": self.home, "away": self.away, "start": _iso(self.start)},
        }

    def fixture(self, ts: datetime | None = None) -> dict[str, Any]:
        return self._base("fixture", ts)

    def odds(
        self,
        match: tuple[str, str, str] | None = None,
        total: tuple[str, str, str] | None = None,
        handicap: tuple[str, str, str] | None = None,
        ts: datetime | None = None,
    ) -> dict[str, Any]:
        """match=(home, draw, away); total=(line, over, under); handicap=(line, home, away)."""
        msg = self._base("odds", ts)
        markets: list[dict[str, Any]] = []
        if match:
            markets.append(
                {
                    "market": "MATCH_ODDS",
                    "line": None,
                    "prices": {"1": match[0], "X": match[1], "2": match[2]},
                }
            )
        if total:
            markets.append(
                {
                    "market": "TOTAL",
                    "line": total[0],
                    "prices": {"OVER": total[1], "UNDER": total[2]},
                }
            )
        if handicap:
            markets.append(
                {
                    "market": "HANDICAP",
                    "line": handicap[0],
                    "prices": {"HOME": handicap[1], "AWAY": handicap[2]},
                }
            )
        msg["markets"] = markets
        return msg

    def goal(self, team: str = "home", ts: datetime | None = None) -> dict[str, Any]:
        msg = self._base("incident", ts)
        msg["incident"] = {"type": "GOAL", "team": team}
        return msg

    def status(self, status: str, ts: datetime | None = None) -> dict[str, Any]:
        msg = self._base("status", ts)
        msg["status"] = status
        return msg

    def result(
        self, home: int, away: int, abandoned: bool = False, ts: datetime | None = None
    ) -> dict[str, Any]:
        msg = self._base("result", ts)
        msg["score"] = {"home": home, "away": away, "abandoned": abandoned}
        return msg

    def heartbeat(self, ts: datetime | None = None) -> dict[str, Any]:
        return self._base("heartbeat", ts)


@dataclass
class BetaEvent:
    event_id: int
    home: str
    away: str
    kickoff: datetime
    seq: int = 0
    clock: datetime | None = None

    def _base(
        self, kind: str, ts: datetime | None, with_participants: bool = False
    ) -> dict[str, Any]:
        self.seq += 1
        event: dict[str, Any] = {"id": self.event_id}
        if with_participants:
            event["participants"] = [
                {"name": self.home, "side": "H"},
                {"name": self.away, "side": "A"},
            ]
            event["kickoff_ts"] = int(self.kickoff.timestamp())
        return {
            "src": "beta",
            "msg_seq": self.seq,
            "sent": int((ts or self.clock or self.kickoff).timestamp() * 1000),
            "kind": kind,
            "event": event,
            "payload": {},
        }

    def created(self, ts: datetime | None = None) -> dict[str, Any]:
        return self._base("EVENT_CREATED", ts, with_participants=True)

    def prices(
        self,
        match: tuple[str, str, str] | None = None,
        total: tuple[str, str, str] | None = None,
        handicap: tuple[str, str, str] | None = None,
        ts: datetime | None = None,
    ) -> dict[str, Any]:
        msg = self._base("PRICE_UPDATE", ts)
        offers: list[dict[str, Any]] = []
        if match:
            offers.append(
                {
                    "type": "win-draw-win",
                    "outcomes": [
                        {"id": "H", "frac": to_fractional(match[0])},
                        {"id": "D", "frac": to_fractional(match[1])},
                        {"id": "A", "frac": to_fractional(match[2])},
                    ],
                }
            )
        if total:
            offers.append(
                {
                    "type": "totals",
                    "handicap": float(total[0]),
                    "outcomes": [
                        {"id": "O", "frac": to_fractional(total[1])},
                        {"id": "U", "frac": to_fractional(total[2])},
                    ],
                }
            )
        if handicap:
            offers.append(
                {
                    "type": "asian",
                    "handicap": float(handicap[0]),
                    "outcomes": [
                        {"id": "H", "frac": to_fractional(handicap[1])},
                        {"id": "A", "frac": to_fractional(handicap[2])},
                    ],
                }
            )
        msg["payload"] = {"offers": offers}
        return msg

    def goal(self, side: str = "H", ts: datetime | None = None) -> dict[str, Any]:
        msg = self._base("GOAL", ts)
        msg["payload"] = {"side": side}
        return msg

    def state(self, state: str, ts: datetime | None = None) -> dict[str, Any]:
        msg = self._base("STATE", ts)
        msg["payload"] = {"state": state}
        return msg

    def final(
        self, home: int, away: int, abandoned: bool = False, ts: datetime | None = None
    ) -> dict[str, Any]:
        msg = self._base("FINAL", ts)
        msg["payload"] = {"score": f"{home}:{away}", "abandoned": abandoned}
        return msg

    def heartbeat(self, ts: datetime | None = None) -> dict[str, Any]:
        return self._base("HB", ts)
