"""Prometheus metrics.

p95/p99 of bet placement are derived in Prometheus from the histogram, e.g.
``histogram_quantile(0.99, sum by (le) (rate(bet_placement_seconds_bucket[5m])))``.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

BET_PLACEMENT_SECONDS = Histogram(
    "bet_placement_seconds",
    "End-to-end bet placement latency inside the service",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)
BETS_TOTAL = Counter("bets_total", "Bet placement attempts by result", ["result"])
SETTLEMENTS_TOTAL = Counter("settlements_total", "Bet settlements by outcome", ["outcome"])
RESETTLEMENTS_TOTAL = Counter("resettlements_total", "Settlements reversed due to corrections")
FEED_MESSAGES_TOTAL = Counter(
    "feed_messages_total", "Feed messages by provider and handling result", ["provider", "result"]
)
FEED_GAPS_TOTAL = Counter("feed_gaps_total", "Sequence gaps skipped", ["provider"])
MARKET_SUSPENSIONS_TOTAL = Counter(
    "market_suspensions_total", "Market suspensions by reason", ["reason"]
)
CONSUMER_LAG = Gauge(
    "consumer_lag_messages",
    "Committed-offset lag per partition",
    ["consumer", "topic", "partition"],
)
CONSUMER_MESSAGES_TOTAL = Counter(
    "consumer_messages_total", "Consumed messages by result", ["consumer", "result"]
)
INBOX_DUPLICATES_TOTAL = Counter(
    "inbox_duplicates_total", "Redelivered messages dropped by the inbox", ["consumer"]
)
OUTBOX_PENDING = Gauge("outbox_pending_messages", "Outbox rows not yet published")
OUTBOX_PUBLISHED_TOTAL = Counter("outbox_published_total", "Outbox rows published", ["topic"])
OUTBOX_FAILURES_TOTAL = Counter("outbox_publish_failures_total", "Failed outbox publishes")
RECONCILIATION_MISMATCHES = Gauge(
    "reconciliation_mismatches", "Mismatches found by the last reconciliation run", ["kind"]
)
