"""Bet placement load test (asyncio + httpx).

By default starts the API in-process with uvicorn on a random port, SQLite file DB and
the in-memory broker (embedded outbox relay / settlement consumer running), so it
works without Docker:

    uv run python scripts/load_test.py --bets 2000 --concurrency 32

Against a running stack (e.g. docker compose, PostgreSQL + Redpanda):

    uv run python scripts/load_test.py --base-url http://localhost:8000

While bets are placed, a background task reprices the markets through the feed
endpoint, so some bets hit a genuine 409 requote; clients re-read the market and
retry once, like a real bet slip.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import random
import re
import socket
import statistics
import tempfile
import time
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from bet_lifecycle.feed.simulator import AlphaFixture

MATCHES = [
    ("Arsenal", "Chelsea"),
    ("Inter", "Milan"),
    ("Porto", "Benfica"),
    ("Ajax", "PSV"),
    ("Celtic", "Rangers"),
]


def pct(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, round(q * (len(ordered) - 1)))]


async def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@contextlib.asynccontextmanager
async def local_server(workdir: Path) -> Any:
    import uvicorn

    from bet_lifecycle.api.app import create_app
    from bet_lifecycle.config import Settings

    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{(workdir / 'load.db').as_posix()}",
        log_level="WARNING",
        embedded_workers=True,
    )
    port = await free_port()
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(settings),
            host="127.0.0.1",
            port=port,
            log_level="warning",
            log_config=None,
            access_log=False,
        )
    )
    task = asyncio.create_task(server.serve())
    while not server.started:  # noqa: ASYNC110 - uvicorn exposes a flag, not an event
        await asyncio.sleep(0.05)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await task


async def seed(
    client: httpx.AsyncClient, players: int
) -> tuple[list[AlphaFixture], list[str], list[str]]:
    kickoff = datetime.now(UTC) + timedelta(hours=2)
    fixtures, market_ids = [], []
    run = uuid.uuid4().hex[:6]
    for i, (home, away) in enumerate(MATCHES):
        fx = AlphaFixture(f"LT-{run}-{i}", home, away, kickoff)
        r = await client.post("/v1/feed/alpha", json=fx.fixture())
        r.raise_for_status()
        event_id = r.json()["event_id"]
        await client.post(
            "/v1/feed/alpha",
            json=fx.odds(
                ("2.10", "3.40", "3.60"), ("2.5", "1.90", "1.95"), ("-0.25", "1.95", "1.90")
            ),
        )
        fixtures.append(fx)
        market_ids += [f"{event_id}.1x2", f"{event_id}.ou2.5", f"{event_id}.ah-0.25"]
    for p in range(players):
        r = await client.post(
            f"/v1/wallets/lt-{run}-{p}/deposits",
            json={"amount": 1_000_000},
            headers={"Idempotency-Key": f"lt-dep-{run}-{p}"},
        )
        r.raise_for_status()
    return fixtures, market_ids, [f"lt-{run}-{p}" for p in range(players)]


async def repricer(
    client: httpx.AsyncClient, fixtures: list[AlphaFixture], stop: asyncio.Event, every: float
) -> int:
    n = 0
    rng = random.Random(1)
    while not stop.is_set():
        fx = rng.choice(fixtures)
        home = f"{rng.uniform(1.8, 2.4):.2f}"
        await client.post("/v1/feed/alpha", json=fx.odds((home, "3.40", "3.60")))
        n += 1
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), every)
    return n


async def run(
    base_url: str, bets: int, concurrency: int, players: int, reprice_every: float
) -> dict[str, Any]:
    limits = httpx.Limits(max_connections=concurrency * 2, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(base_url=base_url, timeout=30, limits=limits) as client:
        fixtures, market_ids, player_ids = await seed(client, players)

        latencies: list[float] = []
        statuses: Counter[str] = Counter()
        sem = asyncio.Semaphore(concurrency)
        rng = random.Random(42)

        async def one(i: int) -> None:
            market_id = rng.choice(market_ids)
            player = rng.choice(player_ids)
            async with sem:
                for _attempt in range(2):
                    m = (await client.get(f"/v1/markets/{market_id}")).json()
                    if m["status"] != "open":
                        statuses["market_not_open(skip)"] += 1
                        return
                    selection = rng.choice(list(m["prices"]))
                    body = {
                        "player_id": player,
                        "market_id": market_id,
                        "selection": selection,
                        "stake": rng.randint(100, 5_000),
                        "odds": m["prices"][selection],
                        "price_version": m["price_version"],
                    }
                    started = time.perf_counter()
                    r = await client.post(
                        "/v1/bets",
                        json=body,
                        headers={"Idempotency-Key": f"{player}-{i}-{_attempt}"},
                    )
                    latencies.append(time.perf_counter() - started)
                    label = (
                        r.json().get("error", str(r.status_code))
                        if r.status_code >= 400
                        else str(r.status_code)
                    )
                    statuses[label] += 1
                    if label != "requote":
                        return

        stop = asyncio.Event()
        rep = asyncio.create_task(repricer(client, fixtures, stop, reprice_every))
        t0 = time.perf_counter()
        await asyncio.gather(*(one(i) for i in range(bets)))
        elapsed = time.perf_counter() - t0
        stop.set()
        reprices = await rep

        recon = (await client.get("/v1/admin/reconciliation")).json()
        metrics = (await client.get("/metrics")).text

    return {
        "requests": len(latencies),
        "elapsed_s": round(elapsed, 2),
        "throughput_rps": round(len(latencies) / elapsed, 1),
        "p50_ms": round(statistics.median(latencies) * 1000, 1),
        "p95_ms": round(pct(latencies, 0.95) * 1000, 1),
        "p99_ms": round(pct(latencies, 0.99) * 1000, 1),
        "max_ms": round(max(latencies) * 1000, 1),
        "statuses": dict(statuses),
        "reprices": reprices,
        "server_p95_bucket_ms": server_quantile(metrics, 0.95),
        "server_p99_bucket_ms": server_quantile(metrics, 0.99),
        "reconciliation_ok": recon["ok"],
        "bets_checked": recon["bets_checked"],
    }


def server_quantile(metrics: str, q: float) -> float | None:
    """Upper bucket bound containing the q-quantile of bet_placement_seconds."""
    buckets = [
        (float(le), float(v))
        for le, v in re.findall(
            r'bet_placement_seconds_bucket\{le="([^"]+)"\} ([0-9.e+]+)', metrics
        )
        if le != "+Inf"
    ]
    total = re.search(r"bet_placement_seconds_count ([0-9.e+]+)", metrics)
    if not buckets or not total:
        return None
    target = q * float(total.group(1))
    for le, count in buckets:
        if count >= target:
            return le * 1000
    return None


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url")
    ap.add_argument("--bets", type=int, default=2000)
    ap.add_argument("--concurrency", type=int, default=32)
    ap.add_argument("--players", type=int, default=200)
    ap.add_argument("--reprice-every", type=float, default=0.5)
    args = ap.parse_args()
    if args.base_url:
        result = await run(
            args.base_url, args.bets, args.concurrency, args.players, args.reprice_every
        )
    else:
        with tempfile.TemporaryDirectory() as tmp:
            async with local_server(Path(tmp)) as url:
                result = await run(
                    url, args.bets, args.concurrency, args.players, args.reprice_every
                )
    width = max(map(len, result))
    for k, v in result.items():
        print(f"{k:<{width}}  {v}")


if __name__ == "__main__":
    asyncio.run(main())
