"""Command line entry point: ``bet-lifecycle <command>``."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import signal
import sys

from bet_lifecycle.config import Settings
from bet_lifecycle.observability.logging import configure_logging


async def _worker(name: str, settings: Settings) -> None:
    from bet_lifecycle.bootstrap import Container

    c = Container(settings)
    await c.start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    try:
        if name == "outbox":
            await c.outbox.run_forever(stop)
        elif name == "settlement":
            await (await c.settlement_consumer()).run_forever(stop)
        elif name == "feed":
            await (await c.feed_consumer()).run_forever(stop)
        elif name == "staleness":
            await c.staleness_loop(stop)
        else:
            raise SystemExit(f"unknown worker {name}")
    finally:
        await c.stop()


async def _reconcile(settings: Settings) -> int:
    from bet_lifecycle.bootstrap import Container

    c = Container(settings)
    await c.start()
    try:
        report = await c.reconciliation.run()
    finally:
        await c.stop()
    print(json.dumps(report.to_dict(), indent=2, default=str))
    return 0 if report.ok else 1


async def _init_db(settings: Settings) -> None:
    from bet_lifecycle.infrastructure.database import Database

    db = Database(settings.database_url)
    await db.create_schema()
    await db.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bet-lifecycle")
    sub = parser.add_subparsers(dest="cmd", required=True)
    api = sub.add_parser("api", help="run the HTTP API")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8000)
    w = sub.add_parser("worker", help="run a background worker")
    w.add_argument("name", choices=["outbox", "settlement", "feed", "staleness"])
    sub.add_parser("reconcile", help="run reconciliation, exit 1 on mismatches")
    sub.add_parser("init-db", help="create tables")
    sub.add_parser("demo", help="run the full lifecycle in-process")
    args = parser.parse_args(argv)

    settings = Settings()
    if args.cmd == "demo":
        from bet_lifecycle.demo import run_demo

        asyncio.run(run_demo())
        return 0
    configure_logging(settings.log_level)
    if args.cmd == "api":
        import uvicorn

        from bet_lifecycle.api.app import create_app

        uvicorn.run(create_app(settings), host=args.host, port=args.port, log_config=None)
        return 0
    if args.cmd == "worker":
        asyncio.run(_worker(args.name, settings))
        return 0
    if args.cmd == "reconcile":
        return asyncio.run(_reconcile(settings))
    if args.cmd == "init-db":
        asyncio.run(_init_db(settings))
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
