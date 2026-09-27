"""Bets and the stake-limit policy."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from bet_lifecycle.domain.errors import StakeOutOfLimits
from bet_lifecycle.domain.money import win_amount


class BetStatus(StrEnum):
    OPEN = "open"
    SETTLED = "settled"


@dataclass(frozen=True, slots=True)
class StakeLimits:
    min_stake: int = 100  # 1.00
    max_stake: int = 100_000_00  # 100 000.00
    max_payout: int = 1_000_000_00  # potential win cap

    def check(self, stake: int, odds: Decimal) -> None:
        if stake < self.min_stake or stake > self.max_stake:
            raise StakeOutOfLimits(
                "stake outside limits",
                stake=stake,
                min_stake=self.min_stake,
                max_stake=self.max_stake,
            )
        potential = win_amount(stake, odds)
        if potential > self.max_payout:
            raise StakeOutOfLimits(
                "potential payout above limit",
                potential_payout=potential,
                max_payout=self.max_payout,
            )


@dataclass(slots=True)
class Bet:
    id: str
    player_id: str
    market_id: str
    selection: str
    stake: int
    odds: Decimal
    price_version: int
    stake_cash: int
    stake_bonus: int
    placed_at: datetime
    status: BetStatus = BetStatus.OPEN
    outcome: str | None = None
    payout: int | None = None
