"""In-process broker with Kafka-like semantics (single partition per topic).

Offsets, consumer groups and *commit after processing* are modelled so that
at-least-once redelivery can be exercised in tests: :meth:`MemorySource.rewind`
simulates a consumer crash / rebalance by going back to the last committed offset.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict

from bet_lifecycle.application.ports import Record


class BrokerUnavailable(ConnectionError):
    pass


class InMemoryBroker:
    def __init__(self) -> None:
        self.topics: defaultdict[str, list[Record]] = defaultdict(list)
        self.committed: dict[tuple[str, str], int] = {}
        self.fail_next_publishes = 0
        self._new_data = asyncio.Event()

    async def publish(
        self, topic: str, key: str | None, value: bytes, headers: dict[str, str] | None = None
    ) -> None:
        if self.fail_next_publishes > 0:
            self.fail_next_publishes -= 1
            raise BrokerUnavailable("simulated broker outage")
        log = self.topics[topic]
        log.append(Record(topic, 0, len(log), key, value, dict(headers or {})))
        self._new_data.set()

    def messages(self, topic: str) -> list[Record]:
        return list(self.topics.get(topic, []))

    def source(self, group: str, topics: list[str]) -> MemorySource:
        return MemorySource(self, group, topics)

    async def wait_for_data(self, timeout: float) -> None:
        try:
            await asyncio.wait_for(self._new_data.wait(), timeout)
        except TimeoutError:
            return
        self._new_data.clear()


class MemorySource:
    def __init__(self, broker: InMemoryBroker, group: str, topics: list[str]) -> None:
        self.broker = broker
        self.group = group
        self.topic_names = list(topics)
        self._position = {t: broker.committed.get((group, t), 0) for t in topics}

    async def poll(self, max_records: int, timeout: float) -> list[Record]:
        out = self._take(max_records)
        if not out and timeout > 0:
            await self.broker.wait_for_data(timeout)
            out = self._take(max_records)
        return out

    def _take(self, max_records: int) -> list[Record]:
        out: list[Record] = []
        for t in self.topic_names:
            log = self.broker.topics.get(t, [])
            pos = self._position[t]
            while pos < len(log) and len(out) < max_records:
                out.append(log[pos])
                pos += 1
            self._position[t] = pos
        return out

    async def commit(self, record: Record) -> None:
        key = (self.group, record.topic)
        self.broker.committed[key] = max(self.broker.committed.get(key, 0), record.offset + 1)

    async def lag(self) -> dict[tuple[str, int], int]:
        return {
            (t, 0): len(self.broker.topics.get(t, []))
            - self.broker.committed.get((self.group, t), 0)
            for t in self.topic_names
        }

    def rewind(self) -> None:
        """Forget uncommitted progress, as after a crash or a partition rebalance."""
        for t in self.topic_names:
            self._position[t] = self.broker.committed.get((self.group, t), 0)
