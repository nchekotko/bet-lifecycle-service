from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from bet_lifecycle.domain.errors import InsufficientFunds, UnbalancedTransaction
from bet_lifecycle.domain.ledger import (
    EXTERNAL_PAYMENTS,
    HOUSE_ESCROW,
    HOUSE_REVENUE,
    Ledger,
    LedgerTransaction,
    Posting,
    TxnKind,
    bet_reserve,
    bet_settle,
    bonus_grant,
    deposit,
    player_bonus,
    player_cash,
    split_stake,
)
from bet_lifecycle.domain.settlement import Outcome, compute_amounts

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def test_transaction_must_balance() -> None:
    with pytest.raises(UnbalancedTransaction):
        LedgerTransaction("t", TxnKind.DEPOSIT, "x", (Posting("a", 10), Posting("b", -9)), NOW)


def test_transaction_rejects_single_and_zero_postings() -> None:
    with pytest.raises(UnbalancedTransaction):
        LedgerTransaction("t", TxnKind.DEPOSIT, "x", (Posting("a", 0),), NOW)
    with pytest.raises(UnbalancedTransaction):
        LedgerTransaction(
            "t", TxnKind.DEPOSIT, "x", (Posting("a", 5), Posting("b", -5), Posting("c", 0)), NOW
        )


def test_deposit_and_bonus_move_money_between_accounts() -> None:
    ledger = Ledger()
    ledger.post(deposit("d1", "p1", 1000, NOW))
    ledger.post(bonus_grant("b1", "p1", 300, NOW))
    assert ledger.balance(player_cash("p1")) == 1000
    assert ledger.balance(player_bonus("p1")) == 300
    assert ledger.balance(EXTERNAL_PAYMENTS) == -1000
    assert ledger.balance(HOUSE_REVENUE) == -300
    assert ledger.total() == 0


def test_split_stake_uses_cash_first_then_bonus() -> None:
    assert split_stake(500, 1000, 200) == (500, 0)
    assert split_stake(1100, 1000, 200) == (1000, 100)
    with pytest.raises(InsufficientFunds):
        split_stake(1300, 1000, 200)


def test_reserve_and_settle_winning_bet() -> None:
    ledger = Ledger()
    ledger.post(deposit("d1", "p1", 1000, NOW))
    ledger.post(bet_reserve("r1", "p1", "b1", 1000, 0, NOW))
    assert ledger.balance(HOUSE_ESCROW) == 1000
    amounts = compute_amounts(Outcome.WON, 1000, Decimal("2.5"))
    ledger.post(bet_settle("s1", "p1", "b1", 1000, amounts.payout, 0, 0, NOW))
    assert ledger.balance(player_cash("p1")) == 2500
    assert ledger.balance(HOUSE_ESCROW) == 0
    assert ledger.balance(HOUSE_REVENUE) == -1500


def test_reversal_negates_every_posting_and_references_original() -> None:
    original = bet_settle("s1", "p1", "b1", 1000, 2500, 0, 0, NOW)
    rev = original.reversal("s1r", NOW)
    assert rev.reverses == "s1"
    assert rev.kind is TxnKind.REVERSAL
    assert {p.account_id: p.amount for p in rev.postings} == {
        p.account_id: -p.amount for p in original.postings
    }


def test_reversal_of_unknown_txn_rejected() -> None:
    ledger = Ledger()
    with pytest.raises(ValueError, match="unknown"):
        ledger.post(deposit("d1", "p1", 10, NOW).reversal("r", NOW))


# ------------------------------------------------------------------ properties
outcomes = st.sampled_from(list(Outcome))
bets_strategy = st.lists(
    st.tuples(
        st.integers(min_value=1, max_value=50_000),  # stake
        st.decimals(min_value=Decimal("1.01"), max_value=Decimal("50"), places=2),
        st.lists(outcomes, min_size=0, max_size=4),  # settlement history (resettlements)
    ),
    min_size=1,
    max_size=25,
)


@settings(max_examples=200, deadline=None)
@given(
    cash=st.integers(min_value=0, max_value=2_000_000),
    bonus=st.integers(min_value=0, max_value=200_000),
    bets=bets_strategy,
)
def test_ledger_invariants_hold_for_any_bet_history(
    cash: int, bonus: int, bets: list[tuple[int, Decimal, list[Outcome]]]
) -> None:
    """Every transaction balances; global sum is zero; escrow == open stakes;
    the final state depends only on the *last* settlement (resettlement via reversal)."""
    ledger = Ledger()
    if cash:
        ledger.post(deposit("dep", "p", cash, NOW))
    if bonus:
        ledger.post(bonus_grant("bon", "p", bonus, NOW))
    open_stakes = 0
    expected_cash_delta = 0
    for i, (stake, odds, history) in enumerate(bets):
        try:
            c_part, b_part = split_stake(
                stake, ledger.balance(player_cash("p")), ledger.balance(player_bonus("p"))
            )
        except InsufficientFunds:
            continue
        ledger.post(bet_reserve(f"r{i}", "p", f"b{i}", c_part, b_part, NOW))
        last_txn: LedgerTransaction | None = None
        for j, outcome in enumerate(history):
            if last_txn is not None:
                ledger.post(last_txn.reversal(f"s{i}.{j}.rev", NOW))
            a = compute_amounts(outcome, stake, odds, b_part)
            last_txn = bet_settle(
                f"s{i}.{j}", "p", f"b{i}", stake, a.payout, a.refund_cash, a.refund_bonus, NOW
            )
            ledger.post(last_txn)
        if last_txn is None:
            open_stakes += stake
        else:
            expected_cash_delta += last_txn.amount_for(player_cash("p"))

    for txn in ledger.transactions():
        assert sum(p.amount for p in txn.postings) == 0
    assert ledger.total() == 0
    assert ledger.balance(HOUSE_ESCROW) == open_stakes
    assert ledger.balance(player_cash("p")) >= 0
    assert ledger.balance(player_bonus("p")) >= 0


@settings(max_examples=200, deadline=None)
@given(
    stake=st.integers(min_value=1, max_value=10_000_000),
    odds=st.decimals(min_value=Decimal("1.001"), max_value=Decimal("1000"), places=3),
    path=st.lists(outcomes, min_size=1, max_size=6),
)
def test_resettlement_is_path_independent(stake: int, odds: Decimal, path: list[Outcome]) -> None:
    """Settling through any chain of corrections ends in the same balances as settling
    directly with the final outcome."""
    direct, corrected = Ledger(), Ledger()
    for ledger in (direct, corrected):
        ledger.post(deposit("d", "p", stake, NOW))
        ledger.post(bet_reserve("r", "p", "b", stake, 0, NOW))
    final = compute_amounts(path[-1], stake, odds)
    direct.post(bet_settle("s", "p", "b", stake, final.payout, final.refund_cash, 0, NOW))
    prev: LedgerTransaction | None = None
    for k, outcome in enumerate(path):
        if prev is not None:
            corrected.post(prev.reversal(f"rev{k}", NOW))
        a = compute_amounts(outcome, stake, odds)
        prev = bet_settle(f"s{k}", "p", "b", stake, a.payout, a.refund_cash, 0, NOW)
        corrected.post(prev)
    assert direct.balances() == corrected.balances()
