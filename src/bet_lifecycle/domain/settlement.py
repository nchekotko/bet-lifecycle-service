"""Grading of selections against a final score and payout computation.

Asian lines: a quarter line (e.g. 2.25 or -0.75) is split into two half-stakes on
the two neighbouring lines (2.0 and 2.5). Each half is graded independently, which
produces the half-won / half-lost outcomes. Whole lines (2.0) can end in a push.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from bet_lifecycle.domain.markets import SELECTIONS, MarketType
from bet_lifecycle.domain.money import win_amount


class Outcome(StrEnum):
    WON = "won"
    LOST = "lost"
    VOID = "void"
    PUSH = "push"
    HALF_WON = "half_won"
    HALF_LOST = "half_lost"


class ResultStatus(StrEnum):
    FINAL = "final"
    ABANDONED = "abandoned"


@dataclass(frozen=True, slots=True)
class Score:
    home: int
    away: int
    status: ResultStatus = ResultStatus.FINAL

    def __post_init__(self) -> None:
        if self.home < 0 or self.away < 0:
            raise ValueError("score must be non-negative")


_QUARTER = Decimal("0.25")


def _split_line(line: Decimal) -> tuple[Decimal, Decimal]:
    """2.25 -> (2.0, 2.5); 2.5 -> (2.5, 2.5); 2.0 -> (2.0, 2.0)."""
    quarters = line / _QUARTER
    if quarters % 2 == 1 or quarters % 2 == -1:
        return line - _QUARTER, line + _QUARTER
    return line, line


def _sign(value: Decimal) -> int:
    return (value > 0) - (value < 0)


_COMBINE: dict[tuple[int, int], Outcome] = {
    (1, 1): Outcome.WON,
    (1, 0): Outcome.HALF_WON,
    (0, 1): Outcome.HALF_WON,
    (0, 0): Outcome.PUSH,
    (-1, 0): Outcome.HALF_LOST,
    (0, -1): Outcome.HALF_LOST,
    (-1, -1): Outcome.LOST,
}


def _grade_margin(margin: Decimal, line: Decimal) -> Outcome:
    """Grade a bet that wins when ``margin + line > 0`` (per half line)."""
    low, high = _split_line(line)
    return _COMBINE[(_sign(margin + low), _sign(margin + high))]


def grade(market_type: MarketType, line: Decimal | None, selection: str, score: Score) -> Outcome:
    if selection not in SELECTIONS[market_type]:
        raise ValueError(f"unknown selection {selection!r} for {market_type}")
    if score.status is ResultStatus.ABANDONED:
        return Outcome.VOID
    if market_type is MarketType.MATCH_RESULT:
        winner = (
            "home" if score.home > score.away else "away" if score.away > score.home else "draw"
        )
        return Outcome.WON if selection == winner else Outcome.LOST
    assert line is not None
    total = Decimal(score.home + score.away)
    if market_type is MarketType.TOTAL_GOALS:
        # over wins when total - line > 0; under wins when line - total > 0
        if selection == "over":
            return _grade_margin(total, -line)
        return _grade_margin(-total, line)
    # Asian handicap: line applies to home, the opposite line to away.
    diff = Decimal(score.home - score.away)
    if selection == "home":
        return _grade_margin(diff, line)
    return _grade_margin(-diff, -line)


@dataclass(frozen=True, slots=True)
class SettlementAmounts:
    outcome: Outcome
    payout: int  # paid to player cash (winning part incl. its stake)
    refund_cash: int  # refunded stake back to cash wallet
    refund_bonus: int  # refunded stake back to bonus wallet

    @property
    def total_return(self) -> int:
        return self.payout + self.refund_cash + self.refund_bonus


def _split_refund(refund: int, stake: int, stake_bonus: int) -> tuple[int, int]:
    """Refund goes back pro rata to where the stake came from (no bonus -> cash laundering)."""
    if refund == 0:
        return 0, 0
    bonus = refund * stake_bonus // stake
    return refund - bonus, bonus


def compute_amounts(
    outcome: Outcome, stake: int, odds: Decimal, stake_bonus: int = 0
) -> SettlementAmounts:
    if stake <= 0:
        raise ValueError("stake must be positive")
    if not 0 <= stake_bonus <= stake:
        raise ValueError("bonus part must be within stake")
    half = stake // 2
    match outcome:
        case Outcome.WON:
            payout, refund = win_amount(stake, odds), 0
        case Outcome.LOST:
            payout, refund = 0, 0
        case Outcome.VOID | Outcome.PUSH:
            payout, refund = 0, stake
        case Outcome.HALF_WON:
            payout, refund = win_amount(half, odds), stake - half
        case Outcome.HALF_LOST:
            payout, refund = 0, stake - half
    refund_cash, refund_bonus = _split_refund(refund, stake, stake_bonus)
    return SettlementAmounts(outcome, payout, refund_cash, refund_bonus)
