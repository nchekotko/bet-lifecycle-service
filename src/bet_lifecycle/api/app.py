"""FastAPI application."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import Body, FastAPI, Header, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from bet_lifecycle.api.schemas import AmountRequest, PlaceBetRequest
from bet_lifecycle.application.bets import PlaceBetCommand
from bet_lifecycle.bootstrap import Container
from bet_lifecycle.config import Settings
from bet_lifecycle.domain import errors as e
from bet_lifecycle.feed.providers import FeedSchemaError
from bet_lifecycle.observability.logging import configure_logging, get_logger, request_id_var

log = get_logger("bet_lifecycle.api")

STATUS: dict[type[e.DomainError], int] = {
    e.NotFound: 404,
    e.MarketNotOpen: 409,
    e.Requote: 409,
    e.InvalidStateTransition: 409,
    e.StakeOutOfLimits: 422,
    e.InsufficientFunds: 422,
    e.UnknownSelection: 422,
    e.IdempotencyConflict: 422,
    e.UnknownEvent: 422,
}

IdempotencyKey = Annotated[str, Header(alias="Idempotency-Key", min_length=8, max_length=128)]


def create_app(settings: Settings | None = None, container: Container | None = None) -> FastAPI:
    settings = settings or Settings()
    c = container or Container(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if container is None:
            configure_logging(settings.log_level)
        await c.start()
        stop = asyncio.Event()
        tasks = await c.run_embedded_workers(stop) if settings.embedded_workers else []
        try:
            yield
        finally:
            stop.set()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await c.stop()

    app = FastAPI(title="bet-lifecycle-service", version="0.1.0", lifespan=lifespan)
    app.state.container = c

    @app.middleware("http")
    async def request_context(request: Request, call_next: Any) -> Response:
        rid = request.headers.get("X-Request-ID") or uuid.uuid4().hex
        token = request_id_var.set(rid)
        started = time.perf_counter()
        try:
            response: Response = await call_next(request)
        finally:
            request_id_var.reset(token)
        response.headers["X-Request-ID"] = rid
        log.debug(
            "http request",
            extra={
                "method": request.method,
                "path": request.url.path,
                "status": response.status_code,
                "ms": round((time.perf_counter() - started) * 1000, 2),
                "request_id": rid,
            },
        )
        return response

    @app.exception_handler(e.DomainError)
    async def domain_error(_: Request, exc: e.DomainError) -> JSONResponse:
        status = next((s for cls, s in STATUS.items() if isinstance(exc, cls)), 400)
        return JSONResponse(
            {"error": exc.code, "message": exc.message, "details": exc.details}, status
        )

    @app.exception_handler(FeedSchemaError)
    async def feed_error(_: Request, exc: FeedSchemaError) -> JSONResponse:
        return JSONResponse({"error": "feed_schema", "message": str(exc), "details": {}}, 400)

    # ---------------------------------------------------------------- bets
    @app.post("/v1/bets", status_code=201)
    async def place_bet(
        body: PlaceBetRequest, idempotency_key: IdempotencyKey, response: Response
    ) -> dict[str, Any]:
        res = await c.bets.place(
            PlaceBetCommand(
                body.player_id,
                body.market_id,
                body.selection,
                body.stake,
                body.odds,
                body.price_version,
            ),
            idempotency_key,
        )
        response.status_code = res.status_code
        if res.replayed:
            response.headers["Idempotent-Replayed"] = "true"
        return res.body

    @app.get("/v1/bets/{bet_id}")
    async def get_bet(bet_id: str) -> dict[str, Any]:
        return await c.bets.get(bet_id)

    # ------------------------------------------------------------- markets
    @app.get("/v1/markets/{market_id}")
    async def get_market(market_id: str) -> dict[str, Any]:
        return await c.markets.get(market_id)

    @app.get("/v1/markets/{market_id}/prices")
    async def market_prices(market_id: str) -> list[dict[str, Any]]:
        return await c.markets.price_history(market_id)

    @app.get("/v1/events/{event_id}/markets")
    async def event_markets(event_id: str) -> list[dict[str, Any]]:
        return await c.markets.list_for_event(event_id)

    @app.post("/v1/admin/markets/{market_id}/suspend")
    async def suspend_market(market_id: str) -> dict[str, Any]:
        return await c.markets.trader_suspend(market_id)

    @app.post("/v1/admin/markets/{market_id}/open")
    async def open_market(market_id: str) -> dict[str, Any]:
        return await c.markets.trader_open(market_id)

    # -------------------------------------------------------------- wallet
    @app.get("/v1/wallets/{player_id}")
    async def wallet(player_id: str) -> dict[str, int]:
        return await c.wallets.balances(player_id)

    @app.post("/v1/wallets/{player_id}/deposits", status_code=201)
    async def deposit(
        player_id: str, body: AmountRequest, idempotency_key: IdempotencyKey
    ) -> dict[str, int]:
        return await c.wallets.deposit(player_id, body.amount, idempotency_key)

    @app.post("/v1/wallets/{player_id}/bonuses", status_code=201)
    async def bonus(
        player_id: str, body: AmountRequest, idempotency_key: IdempotencyKey
    ) -> dict[str, int]:
        return await c.wallets.grant_bonus(player_id, body.amount, idempotency_key)

    # ---------------------------------------------------------------- feed
    @app.post("/v1/feed/{provider}", status_code=202)
    async def feed(provider: str, raw: Annotated[dict[str, Any], Body()]) -> dict[str, Any]:
        """Local/demo entry point. In docker-compose the feed arrives via Kafka."""
        res = await c.ingestion.ingest(provider, raw)
        return {
            "event_id": res.event_id,
            "applied": res.applied,
            "rejected": res.rejected,
            "duplicate": res.duplicate,
            "buffered": res.buffered,
            "skipped": res.skipped,
        }

    # --------------------------------------------------------------- admin
    @app.get("/v1/admin/reconciliation")
    async def reconciliation() -> dict[str, Any]:
        return (await c.reconciliation.run()).to_dict()

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app
