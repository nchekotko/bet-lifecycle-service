from __future__ import annotations

import pytest

from bet_lifecycle.domain.settlement import ResultStatus
from bet_lifecycle.messaging.envelopes import (
    REGISTRY,
    Envelope,
    PoisonMessage,
    ResultPublishedV1,
    ResultPublishedV2,
)


def test_v1_result_is_upcast_to_v2() -> None:
    env = REGISTRY.envelope(
        ResultPublishedV1(event_id="e1", score="2-1", result_version=3), key="e1"
    )
    assert (env.type, env.version) == ("result.published", 1)
    decoded = REGISTRY.decode(Envelope.from_bytes(env.to_bytes()))
    assert decoded == ResultPublishedV2(
        event_id="e1", home_score=2, away_score=1, status=ResultStatus.FINAL, result_version=3
    )


def test_v1_abandoned_flag_becomes_status() -> None:
    env = REGISTRY.envelope(
        ResultPublishedV1(event_id="e1", score="0-0", abandoned=True, result_version=1), key="e1"
    )
    decoded = REGISTRY.decode(env)
    assert isinstance(decoded, ResultPublishedV2)
    assert decoded.status is ResultStatus.ABANDONED


def test_v2_passes_through() -> None:
    p = ResultPublishedV2(
        event_id="e", home_score=0, away_score=0, status=ResultStatus.FINAL, result_version=1
    )
    assert REGISTRY.decode(REGISTRY.envelope(p, key="e")) == p


@pytest.mark.parametrize(
    "env",
    [
        Envelope(type="result.published", version=9, key="k", payload={}),
        Envelope(type="unknown.type", version=1, key="k", payload={}),
        Envelope(type="result.published", version=1, key="k", payload={"event_id": "e"}),
        Envelope(
            type="result.published",
            version=1,
            key="k",
            payload={"event_id": "e", "score": "x", "result_version": 1},
        ),
    ],
)
def test_poison_messages(env: Envelope) -> None:
    with pytest.raises(PoisonMessage):
        REGISTRY.decode(env)


def test_garbage_bytes_are_poison() -> None:
    with pytest.raises(PoisonMessage):
        Envelope.from_bytes(b"not json")
