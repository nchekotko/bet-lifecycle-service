"""Publish a simulated match from both mock providers to Redpanda/Kafka.

    uv run python scripts/feed_producer.py --bootstrap localhost:19092

Messages are keyed by provider event id (same stream -> same partition -> one
ingestor), a few are duplicated and neighbours are swapped to exercise the
sequence gate. The final result is followed by a correction.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
from datetime import UTC, datetime, timedelta
from typing import Any

from bet_lifecycle.feed.simulator import AlphaFixture, BetaEvent
from bet_lifecycle.messaging.envelopes import FEED_TOPIC_PREFIX
from bet_lifecycle.messaging.kafka import KafkaPublisher


def script(seed: int) -> list[tuple[str, str, dict[str, Any]]]:
    rng = random.Random(seed)
    kickoff = datetime.now(UTC) + timedelta(minutes=30)
    a = AlphaFixture(f"A-{seed}", "Manchester United FC", "Tottenham Hotspur", kickoff)
    b = BetaEvent(90_000 + seed, "MAN UTD", "SPURS", kickoff + timedelta(minutes=5))
    alpha = [
        a.fixture(),
        a.odds(("2.10", "3.40", "3.60"), ("2.5", "1.90", "1.95"), ("-0.25", "1.95", "1.90")),
        a.odds(("2.05", "3.45", "3.70")),
        a.status("LIVE"),
        a.goal("home"),
        a.odds(("1.45", "4.50", "7.00"), ("2.5", "1.60", "2.30"), ("-0.25", "1.40", "2.90")),
        a.status("ENDED"),
        a.result(1, 0),
        a.result(2, 0),  # correction
    ]
    beta = [b.created(), b.prices(("2.2", "3.3", "3.4")), b.goal("H"), b.heartbeat()]
    out = [("alpha", a.fixture_id, m) for m in alpha] + [("beta", str(b.event_id), m) for m in beta]
    # swap a few neighbours and duplicate a few messages
    for i in range(1, len(out) - 1, 3):
        if rng.random() < 0.5:
            out[i], out[i + 1] = out[i + 1], out[i]
    return out + rng.sample(out, 3)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:19092")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--delay", type=float, default=0.2)
    args = ap.parse_args()
    pub = KafkaPublisher(args.bootstrap, client_id="feed-simulator")
    await pub.start()
    try:
        for provider, key, msg in script(args.seed):
            await pub.publish(FEED_TOPIC_PREFIX + provider, key, json.dumps(msg).encode())
            print(provider, key, msg.get("type") or msg.get("kind"))
            await asyncio.sleep(args.delay)
    finally:
        await pub.stop()


if __name__ == "__main__":
    asyncio.run(main())
