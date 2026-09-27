"""Engine + unit of work.

``Database.transaction()`` yields a :class:`Tx`: one database transaction with all
repositories bound to it. Everything a use case changes (business rows, ledger,
outbox, inbox, idempotency record, audit) commits or rolls back together.

SQLite (tests / local mode) has a single writer; transactions are serialised with an
asyncio lock so concurrent requests queue instead of failing with "database is
locked". PostgreSQL relies on row locks (``SELECT ... FOR UPDATE``).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable

from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.pool import StaticPool

from bet_lifecycle.infrastructure import repositories as r
from bet_lifecycle.infrastructure.tables import metadata

AfterCommit = Callable[[], Awaitable[None]]


class Tx:
    def __init__(self, conn: AsyncConnection) -> None:
        self.conn = conn
        self.events = r.EventRepository(conn)
        self.feed = r.FeedStreamRepository(conn)
        self.markets = r.MarketRepository(conn)
        self.results = r.ResultRepository(conn)
        self.bets = r.BetRepository(conn)
        self.settlements = r.SettlementRepository(conn)
        self.ledger = r.LedgerRepository(conn)
        self.idempotency = r.IdempotencyRepository(conn)
        self.inbox = r.InboxRepository(conn)
        self.outbox = r.OutboxRepository(conn)
        self.audit = r.AuditRepository(conn)
        self._after_commit: list[AfterCommit] = []

    def after_commit(self, hook: AfterCommit) -> None:
        """Run ``hook`` only if the transaction commits (cache invalidation etc.)."""
        self._after_commit.append(hook)


class Database:
    def __init__(self, url: str, *, echo: bool = False) -> None:
        self.url = url
        is_sqlite = url.startswith("sqlite")
        kwargs: dict[str, object] = {"echo": echo}
        if is_sqlite and ":memory:" in url:
            kwargs["poolclass"] = StaticPool
            kwargs["connect_args"] = {"check_same_thread": False}
        elif not is_sqlite:
            kwargs.update(pool_size=20, max_overflow=10, pool_pre_ping=True)
        self.engine: AsyncEngine = create_async_engine(url, **kwargs)
        self.dialect = self.engine.dialect.name
        self._lock = asyncio.Lock() if is_sqlite else None
        if is_sqlite:
            event.listen(self.engine.sync_engine, "connect", _sqlite_pragmas)

    async def create_schema(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(metadata.create_all)

    async def drop_schema(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)

    async def dispose(self) -> None:
        await self.engine.dispose()

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncIterator[Tx]:
        lock = self._lock or contextlib.nullcontext()
        async with lock, self.engine.begin() as conn:
            tx = Tx(conn)
            yield tx
        for hook in tx._after_commit:
            await hook()


def _sqlite_pragmas(dbapi_conn: object, _record: object) -> None:
    cur = dbapi_conn.cursor()  # type: ignore[attr-defined]
    cur.execute("PRAGMA foreign_keys=ON")
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA busy_timeout=5000")
    cur.close()
