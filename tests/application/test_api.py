from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest

from bet_lifecycle.api.app import create_app
from bet_lifecycle.bootstrap import Container
from bet_lifecycle.feed.simulator import AlphaFixture
from tests.factories import KICKOFF, MATCH, TOTAL


@pytest.fixture
async def client(container: Container) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(container.settings, container=container)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


async def _event(client: httpx.AsyncClient) -> tuple[str, AlphaFixture]:
    alpha = AlphaFixture("A-1", "Arsenal", "Chelsea", KICKOFF)
    r = await client.post("/v1/feed/alpha", json=alpha.fixture())
    assert r.status_code == 202
    await client.post("/v1/feed/alpha", json=alpha.odds(MATCH, TOTAL))
    return r.json()["event_id"], alpha


async def test_bet_flow_over_http(client: httpx.AsyncClient) -> None:
    event_id, alpha = await _event(client)
    r = await client.post(
        "/v1/wallets/p1/deposits",
        json={"amount": 100_00},
        headers={"Idempotency-Key": "dep-000001"},
    )
    assert r.status_code == 201 and r.json()["cash"] == 100_00

    market = (await client.get(f"/v1/markets/{event_id}.1x2")).json()
    bet = {
        "player_id": "p1",
        "market_id": market["market_id"],
        "selection": "home",
        "stake": 10_00,
        "odds": market["prices"]["home"],
        "price_version": market["price_version"],
    }
    r = await client.post("/v1/bets", json=bet, headers={"Idempotency-Key": "bet-000001"})
    assert r.status_code == 201, r.text
    bet_id = r.json()["bet_id"]
    replay = await client.post("/v1/bets", json=bet, headers={"Idempotency-Key": "bet-000001"})
    assert replay.status_code == 201 and replay.headers["Idempotent-Replayed"] == "true"
    assert replay.json()["bet_id"] == bet_id

    await client.post("/v1/feed/alpha", json=alpha.odds(("2.00", "3.50", "3.90")))
    requote = await client.post("/v1/bets", json=bet, headers={"Idempotency-Key": "bet-000002"})
    assert requote.status_code == 409
    assert requote.json()["error"] == "requote"
    assert requote.json()["details"]["current_price_version"] == 2

    await client.post("/v1/feed/alpha", json=alpha.goal())
    suspended = await client.post(
        "/v1/bets",
        json=bet | {"price_version": 2, "odds": "2"},
        headers={"Idempotency-Key": "bet-000003"},
    )
    assert suspended.status_code == 409 and suspended.json()["error"] == "market_not_open"

    assert (await client.get(f"/v1/bets/{bet_id}")).json()["status"] == "open"
    assert (await client.get("/v1/wallets/p1")).json()["cash"] == 90_00
    assert (await client.get("/v1/admin/reconciliation")).json()["ok"] is True
    metrics = (await client.get("/metrics")).text
    assert "bet_placement_seconds_bucket" in metrics
    assert 'bets_total{result="requote"}' in metrics


async def test_validation_errors(client: httpx.AsyncClient) -> None:
    event_id, _ = await _event(client)
    bet = {
        "player_id": "p1",
        "market_id": f"{event_id}.1x2",
        "selection": "home",
        "stake": 10_00,
        "odds": "2.1",
        "price_version": 1,
    }
    assert (await client.post("/v1/bets", json=bet)).status_code == 422  # no key
    r = await client.post("/v1/bets", json=bet, headers={"Idempotency-Key": "bet-000001"})
    assert r.status_code == 422 and r.json()["error"] == "insufficient_funds"
    r = await client.post(
        "/v1/bets", json=bet | {"odds": "0.5"}, headers={"Idempotency-Key": "bet-000002"}
    )
    assert r.status_code == 422
    assert (await client.get("/v1/bets/nope")).status_code == 404
    r = await client.post("/v1/feed/alpha", json={"type": "odds"})
    assert r.status_code == 400 and r.json()["error"] == "feed_schema"
    assert (await client.get("/healthz")).json() == {"status": "ok"}
    assert "X-Request-ID" in (await client.get("/healthz")).headers


async def test_trader_endpoints(client: httpx.AsyncClient) -> None:
    event_id, _ = await _event(client)
    r = await client.post(f"/v1/admin/markets/{event_id}.1x2/suspend")
    assert r.json()["suspend_reason"] == "trader"
    assert (await client.get(f"/v1/markets/{event_id}.1x2")).json()["status"] == "suspended"
    r = await client.post(f"/v1/admin/markets/{event_id}.1x2/open")
    assert r.json()["status"] == "open"
    prices = (await client.get(f"/v1/markets/{event_id}.1x2/prices")).json()
    assert [p["version"] for p in prices] == [1]
    assert len((await client.get(f"/v1/events/{event_id}/markets")).json()) == 2
