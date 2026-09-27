"""Double-entry ledger.

There is no mutable ``balance`` field anywhere: the balance of an account is the sum of
its postings. Every transaction is a set of postings whose amounts sum to zero, so
money is only ever *moved* between accounts, never created or destroyed. Corrections
are new transactions (a reversal that negates every posting of the original, plus a
new transaction), never an UPDATE of an existing one.

Sign convention: a positive amount increases what the account holds. Player accounts
therefore have non-negative balances in normal operation, ``house:escrow`` holds the
sum of open stakes, ``house:revenue`` is the house P&L and ``external:payments`` is the
contra-account for money that entered/left the system through the payment provider
(it goes negative on deposits).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum

from bet_lifecycle.domain.errors import InsufficientFunds, UnbalancedTransaction

HOUSE_ESCROW = "house:escrow"
HOUSE_REVENUE = "house:revenue"
EXTERNAL_PAYMENTS = "external:payments"
SYSTEM_ACCOUNTS = (HOUSE_ESCROW, HOUSE_REVENUE, EXTERNAL_PAYMENTS)


def player_cash(player_id: str) -> str:
    return f"player:{player_id}:cash"


def player_bonus(player_id: str) -> str:
    return f"player:{player_id}:bonus"


class TxnKind(StrEnum):
    DEPOSIT = "deposit"
    BONUS_GRANT = "bonus_grant"
    BET_RESERVE = "bet_reserve"
    BET_SETTLE = "bet_settle"
    REVERSAL = "reversal"


@dataclass(frozen=True, slots=True)
class Posting:
    account_id: str
    amount: int


@dataclass(frozen=True, slots=True)
class LedgerTransaction:
    id: str
    kind: TxnKind
    reference: str
    postings: tuple[Posting, ...]
    created_at: datetime
    reverses: str | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if len(self.postings) < 2:
            raise UnbalancedTransaction("a transaction needs at least two postings", txn=self.id)
        if any(p.amount == 0 for p in self.postings):
            raise UnbalancedTransaction("zero-amount posting", txn=self.id)
        total = sum(p.amount for p in self.postings)
        if total != 0:
            raise UnbalancedTransaction(
                f"postings sum to {total}, expected 0", txn=self.id, total=total
            )

    def reversal(self, new_id: str, created_at: datetime) -> LedgerTransaction:
        """Mirror transaction that cancels this one; history stays untouched."""
        return LedgerTransaction(
            id=new_id,
            kind=TxnKind.REVERSAL,
            reference=self.reference,
            postings=tuple(Posting(p.account_id, -p.amount) for p in self.postings),
            created_at=created_at,
            reverses=self.id,
            metadata={"reversed_kind": str(self.kind)},
        )

    def amount_for(self, account_id: str) -> int:
        return sum(p.amount for p in self.postings if p.account_id == account_id)


def _postings(pairs: Iterable[tuple[str, int]]) -> tuple[Posting, ...]:
    """Merge postings per account and drop zeros, keeping first-seen order."""
    merged: dict[str, int] = {}
    for account, amount in pairs:
        merged[account] = merged.get(account, 0) + amount
    return tuple(Posting(a, v) for a, v in merged.items() if v != 0)


def deposit(txn_id: str, player_id: str, amount: int, at: datetime) -> LedgerTransaction:
    if amount <= 0:
        raise ValueError("deposit must be positive")
    return LedgerTransaction(
        txn_id,
        TxnKind.DEPOSIT,
        f"deposit:{player_id}",
        _postings([(player_cash(player_id), amount), (EXTERNAL_PAYMENTS, -amount)]),
        at,
    )


def bonus_grant(txn_id: str, player_id: str, amount: int, at: datetime) -> LedgerTransaction:
    """Bonus money is a marketing cost: it is funded from house revenue."""
    if amount <= 0:
        raise ValueError("bonus must be positive")
    return LedgerTransaction(
        txn_id,
        TxnKind.BONUS_GRANT,
        f"bonus:{player_id}",
        _postings([(player_bonus(player_id), amount), (HOUSE_REVENUE, -amount)]),
        at,
    )


def split_stake(stake: int, cash_balance: int, bonus_balance: int) -> tuple[int, int]:
    """Fund a stake from cash first, then bonus. Returns (cash_part, bonus_part)."""
    if stake <= 0:
        raise ValueError("stake must be positive")
    available = max(cash_balance, 0) + max(bonus_balance, 0)
    if available < stake:
        raise InsufficientFunds("insufficient funds", stake=stake, available=available)
    cash_part = min(stake, max(cash_balance, 0))
    return cash_part, stake - cash_part


def bet_reserve(
    txn_id: str, player_id: str, bet_id: str, cash_part: int, bonus_part: int, at: datetime
) -> LedgerTransaction:
    """Move the stake from the player's wallets into escrow."""
    stake = cash_part + bonus_part
    return LedgerTransaction(
        txn_id,
        TxnKind.BET_RESERVE,
        f"bet:{bet_id}",
        _postings(
            [
                (player_cash(player_id), -cash_part),
                (player_bonus(player_id), -bonus_part),
                (HOUSE_ESCROW, stake),
            ]
        ),
        at,
    )


def bet_settle(
    txn_id: str,
    player_id: str,
    bet_id: str,
    stake: int,
    payout: int,
    refund_cash: int,
    refund_bonus: int,
    at: datetime,
) -> LedgerTransaction:
    """Release the stake from escrow.

    ``payout`` (winnings including the winning part of the stake) goes to player cash,
    refunded stake goes back to the wallet it came from, and the rest is house revenue
    (negative when the player wins more than they staked).
    """
    returned = payout + refund_cash + refund_bonus
    return LedgerTransaction(
        txn_id,
        TxnKind.BET_SETTLE,
        f"bet:{bet_id}",
        _postings(
            [
                (HOUSE_ESCROW, -stake),
                (player_cash(player_id), payout + refund_cash),
                (player_bonus(player_id), refund_bonus),
                (HOUSE_REVENUE, stake - returned),
            ]
        ),
        at,
    )


class Ledger:
    """In-memory reference ledger. Used by the domain property tests and as the model
    the SQL implementation is checked against."""

    def __init__(self) -> None:
        self._txns: dict[str, LedgerTransaction] = {}
        self._balances: defaultdict[str, int] = defaultdict(int)

    def post(self, txn: LedgerTransaction) -> None:
        if txn.id in self._txns:
            raise ValueError(f"duplicate transaction id {txn.id}")
        if txn.reverses is not None and txn.reverses not in self._txns:
            raise ValueError(f"reversal of unknown transaction {txn.reverses}")
        self._txns[txn.id] = txn
        for p in txn.postings:
            self._balances[p.account_id] += p.amount

    def balance(self, account_id: str) -> int:
        return self._balances.get(account_id, 0)

    def balances(self) -> dict[str, int]:
        return {k: v for k, v in self._balances.items() if v != 0}

    def transactions(self) -> list[LedgerTransaction]:
        return list(self._txns.values())

    def total(self) -> int:
        return sum(self._balances.values())
