from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from bet_lifecycle.domain.markets import MarketType
from bet_lifecycle.domain.money import fractional_to_decimal, parse_odds, win_amount
from bet_lifecycle.domain.settlement import (
    Outcome,
    ResultStatus,
    Score,
    compute_amounts,
    grade,
)

D = Decimal
W, L, P, HW, HL, V = (
    Outcome.WON,
    Outcome.LOST,
    Outcome.PUSH,
    Outcome.HALF_WON,
    Outcome.HALF_LOST,
    Outcome.VOID,
)


@pytest.mark.parametrize(
    ("home", "away", "selection", "expected"),
    [(2, 1, "home", W), (2, 1, "draw", L), (1, 1, "draw", W), (0, 3, "away", W), (0, 3, "home", L)],
)
def test_match_result(home: int, away: int, selection: str, expected: Outcome) -> None:
    assert grade(MarketType.MATCH_RESULT, None, selection, Score(home, away)) is expected


@pytest.mark.parametrize(
    ("line", "total", "over", "under"),
    [
        ("2.5", 3, W, L),
        ("2.5", 2, L, W),
        ("2", 2, P, P),
        ("2", 3, W, L),
        ("2.25", 2, HL, HW),  # 2.0 push + 2.5 lose (over)
        ("2.25", 3, W, L),
        ("2.75", 3, HW, HL),  # 2.5 win + 3.0 push (over)
        ("2.75", 2, L, W),
    ],
)
def test_total_goals_asian_lines(line: str, total: int, over: Outcome, under: Outcome) -> None:
    score = Score(total, 0)
    assert grade(MarketType.TOTAL_GOALS, D(line), "over", score) is over
    assert grade(MarketType.TOTAL_GOALS, D(line), "under", score) is under


@pytest.mark.parametrize(
    ("line", "home", "away", "home_res", "away_res"),
    [
        ("-0.5", 1, 0, W, L),
        ("-0.5", 1, 1, L, W),
        ("0", 1, 1, P, P),
        ("-0.25", 1, 1, HL, HW),  # home: 0 push, -0.5 lose
        ("-0.25", 2, 1, W, L),
        ("-0.75", 2, 1, HW, HL),  # home: -0.5 win, -1 push
        ("-1", 2, 1, P, P),
        ("+0.25", 0, 0, HW, HL),
        ("+1.5", 0, 1, W, L),
        ("-1.25", 2, 1, HL, HW),  # home: -1 push, -1.5 lose
    ],
)
def test_asian_handicap(
    line: str, home: int, away: int, home_res: Outcome, away_res: Outcome
) -> None:
    score = Score(home, away)
    assert grade(MarketType.ASIAN_HANDICAP, D(line), "home", score) is home_res
    assert grade(MarketType.ASIAN_HANDICAP, D(line), "away", score) is away_res


def test_abandoned_voids_everything() -> None:
    s = Score(0, 0, ResultStatus.ABANDONED)
    assert grade(MarketType.MATCH_RESULT, None, "draw", s) is V
    assert grade(MarketType.ASIAN_HANDICAP, D("-0.25"), "home", s) is V


@pytest.mark.parametrize(
    ("outcome", "payout", "refund"),
    [(W, 2100, 0), (L, 0, 0), (V, 0, 1000), (P, 0, 1000), (HW, 1050, 500), (HL, 0, 500)],
)
def test_amounts(outcome: Outcome, payout: int, refund: int) -> None:
    a = compute_amounts(outcome, 1000, D("2.1"))
    assert (a.payout, a.refund_cash + a.refund_bonus) == (payout, refund)


def test_refund_returns_to_funding_sources_pro_rata() -> None:
    a = compute_amounts(Outcome.VOID, 3000, D("2"), stake_bonus=1000)
    assert (a.refund_cash, a.refund_bonus) == (2000, 1000)
    hl = compute_amounts(Outcome.HALF_LOST, 3001, D("2"), stake_bonus=1000)
    assert hl.refund_cash + hl.refund_bonus == 3001 - 3001 // 2


def test_payout_rounds_down() -> None:
    assert win_amount(333, D("1.333")) == 443  # 443.889 -> 443


def test_odds_parsing() -> None:
    assert parse_odds("2.10") == D("2.1")
    assert fractional_to_decimal("11/10") == D("2.1")
    assert fractional_to_decimal("1/4") == D("1.25")
    for bad in ("1.0", "abc", "0.5", "1001"):
        with pytest.raises(ValueError, match="odds"):
            parse_odds(bad)
    with pytest.raises(ValueError, match="fractional"):
        fractional_to_decimal("3-1")


@given(
    stake=st.integers(min_value=1, max_value=10**9),
    odds=st.decimals(min_value=D("1.001"), max_value=D("1000"), places=3),
    bonus_share=st.floats(min_value=0, max_value=1),
    outcome=st.sampled_from(list(Outcome)),
)
def test_amounts_are_bounded(
    stake: int, odds: Decimal, bonus_share: float, outcome: Outcome
) -> None:
    bonus = int(stake * bonus_share)
    a = compute_amounts(outcome, stake, odds, bonus)
    assert a.payout >= 0 and a.refund_cash >= 0 and a.refund_bonus >= 0
    assert a.total_return <= win_amount(stake, odds)
    assert a.refund_bonus <= bonus
    if outcome in (Outcome.VOID, Outcome.PUSH):
        assert a.total_return == stake


@given(
    home=st.integers(0, 9),
    away=st.integers(0, 9),
    quarters=st.integers(-16, 16),
)
def test_handicap_sides_are_mirror_images(home: int, away: int, quarters: int) -> None:
    line = D(quarters) / 4
    mirror = {W: L, L: W, P: P, HW: HL, HL: HW}
    h = grade(MarketType.ASIAN_HANDICAP, line, "home", Score(home, away))
    a = grade(MarketType.ASIAN_HANDICAP, line, "away", Score(home, away))
    assert mirror[h] is a
