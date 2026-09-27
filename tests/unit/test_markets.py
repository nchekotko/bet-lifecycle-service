from __future__ import annotations

from decimal import Decimal

import pytest

from bet_lifecycle.domain.errors import (
    InvalidStateTransition,
    MarketNotOpen,
    Requote,
    UnknownSelection,
)
from bet_lifecycle.domain.markets import Market, MarketStatus, MarketType, SuspendReason

D = Decimal
PRICES = {"home": D("2.1"), "draw": D("3.4"), "away": D("3.6")}


def new_market() -> Market:
    return Market("m1", "e1", MarketType.MATCH_RESULT, None)


def test_first_price_opens_market_with_version_1() -> None:
    m = new_market()
    assert m.status is MarketStatus.SUSPENDED
    assert m.reprice(PRICES)
    assert (m.status, m.price_version) == (MarketStatus.OPEN, 1)


def test_same_prices_do_not_bump_version() -> None:
    m = new_market()
    m.reprice(PRICES)
    assert not m.reprice(dict(PRICES))
    assert m.price_version == 1
    m.reprice(PRICES | {"home": D("2.05")})
    assert m.price_version == 2


def test_incident_suspension_lifted_by_fresh_prices_but_trader_is_not() -> None:
    m = new_market()
    m.reprice(PRICES)
    assert m.suspend(SuspendReason.INCIDENT)
    m.reprice(PRICES | {"home": D("1.5")})
    assert m.status is MarketStatus.OPEN

    m.suspend(SuspendReason.TRADER)
    m.reprice(PRICES | {"home": D("1.4")})
    assert m.status is MarketStatus.SUSPENDED
    assert m.price_version == 3  # prices still versioned while suspended
    m.open()
    assert m.status is MarketStatus.OPEN


def test_trader_suspension_is_not_downgraded_by_incident() -> None:
    m = new_market()
    m.reprice(PRICES)
    m.suspend(SuspendReason.TRADER)
    assert not m.suspend(SuspendReason.INCIDENT)
    assert m.suspend_reason is SuspendReason.TRADER
    m.suspend(SuspendReason.FEED_STALE)
    m.reprice(PRICES | {"home": D("1.9")})
    assert m.status is MarketStatus.SUSPENDED


def test_quote_checks_status_version_selection() -> None:
    m = new_market()
    m.reprice(PRICES)
    assert m.quote("home", 1) == D("2.1")
    with pytest.raises(UnknownSelection):
        m.quote("over", 1)
    m.reprice(PRICES | {"home": D("2.2")})
    with pytest.raises(Requote) as exc:
        m.quote("home", 1)
    assert exc.value.details["current_price_version"] == 2
    assert exc.value.details["current_odds"] == "2.2"
    m.suspend(SuspendReason.INCIDENT)
    with pytest.raises(MarketNotOpen):
        m.quote("home", 2)


def test_state_machine() -> None:
    m = new_market()
    m.reprice(PRICES)
    assert m.close()
    assert not m.reprice(PRICES | {"home": D("9")})  # closed: prices ignored
    with pytest.raises(InvalidStateTransition):
        m.open()
    m.mark_settled(1, voided=False)
    assert m.status is MarketStatus.SETTLED
    m.mark_settled(2, voided=True)  # correction: abandoned
    assert (m.status, m.settled_result_version) == (MarketStatus.VOIDED, 2)
    with pytest.raises(InvalidStateTransition):
        m.close() or m._move(MarketStatus.OPEN)


def test_selections_and_lines_validated() -> None:
    with pytest.raises(ValueError, match="line"):
        Market("m", "e", MarketType.ASIAN_HANDICAP, None)
    with pytest.raises(ValueError, match=r"0.25"):
        Market("m", "e", MarketType.TOTAL_GOALS, D("2.3"))
    m = Market("m", "e", MarketType.TOTAL_GOALS, D("2.5"))
    with pytest.raises(UnknownSelection):
        m.reprice({"home": D("2")})
