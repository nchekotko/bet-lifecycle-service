"""Money and odds arithmetic.

Money is always an ``int`` of minor units (cents). Odds are ``Decimal`` in decimal
(European) format with at most 3 fractional digits. Payouts are rounded *down* to the
minor unit, which is the conventional sportsbook rule (the house never pays a
fraction of a cent it does not owe).
"""

from __future__ import annotations

from decimal import ROUND_DOWN, Decimal, InvalidOperation

ODDS_QUANT = Decimal("0.001")
MIN_ODDS = Decimal("1.001")
MAX_ODDS = Decimal("1000")


def parse_odds(value: str | float | Decimal) -> Decimal:
    """Parse decimal odds, rejecting anything outside the tradable range."""
    try:
        odds = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"invalid odds: {value!r}") from exc
    if not odds.is_finite() or odds < MIN_ODDS or odds > MAX_ODDS:
        raise ValueError(f"odds out of range: {value!r}")
    return odds.quantize(ODDS_QUANT, rounding=ROUND_DOWN).normalize()


def fractional_to_decimal(value: str) -> Decimal:
    """Convert UK fractional odds ("11/10") to decimal odds (2.1)."""
    try:
        num_s, den_s = value.split("/")
        num, den = Decimal(num_s), Decimal(den_s)
    except (ValueError, InvalidOperation) as exc:
        raise ValueError(f"invalid fractional odds: {value!r}") from exc
    if den <= 0 or num <= 0:
        raise ValueError(f"invalid fractional odds: {value!r}")
    return parse_odds(Decimal(1) + num / den)


def odds_to_str(odds: Decimal) -> str:
    """Canonical string form used in storage and on the wire (no exponent)."""
    return format(odds.normalize(), "f")


def win_amount(stake: int, odds: Decimal) -> int:
    """Total return (stake included) of a winning stake, rounded down to minor units."""
    if stake < 0:
        raise ValueError("stake must be non-negative")
    return int((Decimal(stake) * odds).to_integral_value(rounding=ROUND_DOWN))
