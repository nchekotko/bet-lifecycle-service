from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta

import pytest

from bet_lifecycle.application.ports import FixedClock
from bet_lifecycle.bootstrap import Container
from tests.factories import KICKOFF, make_container


@pytest.fixture
def clock() -> FixedClock:
    return FixedClock(KICKOFF - timedelta(hours=1))


@pytest.fixture
async def container(clock: FixedClock) -> AsyncIterator[Container]:
    c = make_container(clock)
    await c.start()
    try:
        yield c
    finally:
        await c.stop()
