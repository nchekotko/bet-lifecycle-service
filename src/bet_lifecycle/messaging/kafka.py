"""aiokafka adapters (Kafka / Redpanda).

Producer: idempotent, ``acks=all`` - a publish is confirmed only after all in-sync
replicas have the record. Partition key = aggregate id (event id / bet id), which keeps
per-aggregate ordering.

Consumer: auto-commit disabled, offsets are committed *after* the handler's database
transaction commits. A crash between the two causes a redelivery, which the inbox
table turns into a no-op (at-least-once delivery, exactly-once effect).
"""

from __future__ import annotations

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition

from bet_lifecycle.application.ports import Record


class KafkaPublisher:
    def __init__(self, bootstrap_servers: str, client_id: str = "bet-lifecycle") -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            enable_idempotence=True,
            acks="all",
            linger_ms=5,
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def publish(
        self, topic: str, key: str | None, value: bytes, headers: dict[str, str] | None = None
    ) -> None:
        await self._producer.send_and_wait(
            topic,
            value=value,
            key=key.encode() if key is not None else None,
            headers=[(k, v.encode()) for k, v in (headers or {}).items()],
        )


class KafkaSource:
    def __init__(self, bootstrap_servers: str, group: str, topics: list[str]) -> None:
        self._consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            isolation_level="read_committed",
        )

    async def start(self) -> None:
        await self._consumer.start()

    async def stop(self) -> None:
        await self._consumer.stop()

    async def poll(self, max_records: int, timeout: float) -> list[Record]:
        batches = await self._consumer.getmany(
            timeout_ms=int(timeout * 1000), max_records=max_records
        )
        out: list[Record] = []
        for tp, msgs in batches.items():
            for m in msgs:
                out.append(
                    Record(
                        topic=tp.topic,
                        partition=tp.partition,
                        offset=m.offset,
                        key=m.key.decode() if m.key else None,
                        value=m.value,
                        headers={k: v.decode() for k, v in (m.headers or ())},
                    )
                )
        return out

    async def commit(self, record: Record) -> None:
        await self._consumer.commit(
            {TopicPartition(record.topic, record.partition): record.offset + 1}
        )

    async def lag(self) -> dict[tuple[str, int], int]:
        out: dict[tuple[str, int], int] = {}
        for tp in self._consumer.assignment():
            high = self._consumer.highwater(tp)
            if high is None:
                continue
            committed = await self._consumer.committed(tp) or 0
            out[(tp.topic, tp.partition)] = max(high - committed, 0)
        return out
