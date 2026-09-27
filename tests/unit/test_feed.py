from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from hypothesis import given
from hypothesis import strategies as st

from bet_lifecycle.domain.feed import EventStatus, FeedKind, FeedMessage, SequenceGate
from bet_lifecycle.domain.markets import MarketType
from bet_lifecycle.domain.settlement import ResultStatus
from bet_lifecycle.feed.matching import Candidate, best_match, normalize_team
from bet_lifecycle.feed.providers import FeedSchemaError, normalize
from bet_lifecycle.feed.simulator import AlphaFixture, BetaEvent

T0 = datetime(2026, 10, 3, 19, 0, tzinfo=UTC)


def msg(seq: int, stream: str = "e1") -> FeedMessage:
    return FeedMessage("alpha", stream, seq, FeedKind.HEARTBEAT, T0)


# ---------------------------------------------------------------- sequence gate
def test_gate_in_order_and_duplicates() -> None:
    g = SequenceGate()
    assert [m.seq for m in g.offer(msg(1), 0).ready] == [1]
    assert g.offer(msg(1), 1).duplicate
    assert g.offer(msg(0), 1).duplicate


def test_gate_buffers_until_gap_filled() -> None:
    g = SequenceGate()
    assert g.offer(msg(3), 1).buffered
    assert g.offer(msg(4), 1).buffered
    assert g.offer(msg(4), 1).duplicate  # already buffered
    d = g.offer(msg(2), 1)
    assert [m.seq for m in d.ready] == [2, 3, 4]
    assert d.new_cursor == 4
    assert g.buffered(("alpha", "e1")) == 0


def test_gate_skips_gap_when_buffer_overflows() -> None:
    g = SequenceGate(max_buffer=2)
    g.offer(msg(5), 1)
    g.offer(msg(6), 1)
    d = g.offer(msg(7), 1)
    assert d.skipped == (2, 4)
    assert [m.seq for m in d.ready] == [5, 6, 7]


def test_gate_streams_are_independent_and_restore_works() -> None:
    g = SequenceGate()
    g.offer(msg(2, "a"), 0)
    assert [m.seq for m in g.offer(msg(1, "b"), 0).ready] == [1]
    d = g.offer(msg(1, "a"), 0)
    g.restore(d.ready[1:])  # pretend the txn rolled back
    assert [m.seq for m in g.offer(msg(1, "a"), 0).ready] == [1, 2]


@given(st.permutations(list(range(1, 21))), st.lists(st.integers(1, 20), max_size=20))
def test_gate_any_order_with_duplicates_yields_exact_sequence(
    order: list[int], dups: list[int]
) -> None:
    g = SequenceGate(max_buffer=64)
    cursor, applied = 0, []
    for seq in order + dups:
        d = g.offer(msg(seq), cursor)
        applied += [m.seq for m in d.ready]
        cursor = d.new_cursor or cursor
    assert applied == list(range(1, 21))


# ---------------------------------------------------------------- providers
def test_alpha_and_beta_normalise_to_same_canonical_prices() -> None:
    a = AlphaFixture("A-1", "Arsenal FC", "Chelsea", T0)
    b = BetaEvent(42, "ARSENAL", "CHELSEA FC", T0)
    alpha = normalize("alpha", a.odds(("2.10", "3.40", "3.60"), ("2.25", "1.90", "1.95")))
    beta = normalize("beta", b.prices(("2.10", "3.40", "3.60"), ("2.25", "1.90", "1.95")))
    assert alpha.kind is beta.kind is FeedKind.ODDS
    assert (
        alpha.quotes[0].prices
        == beta.quotes[0].prices
        == {"home": Decimal("2.1"), "draw": Decimal("3.4"), "away": Decimal("3.6")}
    )
    assert alpha.quotes[1].market_type is MarketType.TOTAL_GOALS
    assert alpha.quotes[1].line == beta.quotes[1].line == Decimal("2.25")
    assert alpha.fixture is not None and beta.fixture is None  # beta: only on EVENT_CREATED
    assert beta.provider_event_id == "42"


def test_beta_result_status_and_incident() -> None:
    b = BetaEvent(7, "A", "B", T0)
    b.created()
    assert normalize("beta", b.final(2, 1)).score.home == 2  # type: ignore[union-attr]
    ab = normalize("beta", b.final(0, 0, abandoned=True))
    assert ab.score is not None and ab.score.status is ResultStatus.ABANDONED
    assert normalize("beta", b.goal()).incident == "goal"
    assert normalize("beta", b.state("FT")).status is EventStatus.ENDED


@pytest.mark.parametrize(
    ("provider", "raw"),
    [
        ("alpha", {"type": "odds"}),
        ("alpha", {"type": "nope", "seq": 1}),
        (
            "beta",
            {
                "kind": "FINAL",
                "msg_seq": 1,
                "sent": 0,
                "event": {"id": 1},
                "payload": {"score": "two-one"},
            },
        ),
        ("gamma", {}),
        (
            "alpha",
            json.loads(
                '{"type":"status","seq":1,"ts":"2026-01-01T00:00:00Z",'
                '"fixture_id":"x","status":"WEIRD"}'
            ),
        ),
    ],
)
def test_schema_errors(provider: str, raw: dict[str, object]) -> None:
    with pytest.raises(FeedSchemaError):
        normalize(provider, raw)


# ---------------------------------------------------------------- matching
def test_team_normalisation() -> None:
    assert normalize_team("Manchester United FC") == "manchester united"
    assert normalize_team("MAN UTD") == "manchester united"
    assert normalize_team("Bayern München") == normalize_team("Bayern Munich")


def test_best_match_requires_both_teams_and_close_kickoff() -> None:
    cands = [
        Candidate("e1", "manchester united", "tottenham hotspur", T0),
        Candidate("e2", "manchester city", "chelsea", T0),
    ]
    assert best_match("Man Utd", "Spurs", T0 + timedelta(minutes=10), cands) == "e1"
    assert best_match("Man Utd", "Spurs", T0 + timedelta(hours=3), cands) is None
    assert best_match("Manchester United", "Chelsea", T0, cands) is None
    assert best_match("Manchester Citty", "Chelsea FC", T0, cands) == "e2"  # typo tolerated
