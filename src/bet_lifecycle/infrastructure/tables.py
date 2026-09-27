"""Relational schema (SQLAlchemy Core). Portable between PostgreSQL and SQLite.

Append-only tables (never UPDATEd or DELETEd by application code):
``ledger_transactions``, ``ledger_postings``, ``bet_settlements``, ``market_prices``,
``event_results``, ``audit_log``, ``inbox``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.types import TypeDecorator

metadata = MetaData()

# BIGINT autoincrement on PostgreSQL, INTEGER PRIMARY KEY (rowid) on SQLite.
BigId = BigInteger().with_variant(Integer, "sqlite")


class UTCDateTime(TypeDecorator[datetime]):
    """Always returns tz-aware UTC datetimes (SQLite drops tzinfo)."""

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect: Any) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("naive datetime")
        value = value.astimezone(UTC)
        return value.replace(tzinfo=None) if dialect.name == "sqlite" else value

    def process_result_value(self, value: datetime | None, dialect: Any) -> datetime | None:
        if value is None:
            return None
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


events = Table(
    "events",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("home", String(200), nullable=False),
    Column("away", String(200), nullable=False),
    Column("home_norm", String(200), nullable=False),
    Column("away_norm", String(200), nullable=False),
    Column("kickoff", UTCDateTime, nullable=False, index=True),
    Column("status", String(16), nullable=False),
    Column("primary_provider", String(32), nullable=False),
    Column("last_feed_at", UTCDateTime, nullable=True),
    Column("created_at", UTCDateTime, nullable=False),
)

provider_events = Table(
    "provider_events",
    metadata,
    Column("provider", String(32), primary_key=True),
    Column("provider_event_id", String(64), primary_key=True),
    Column("event_id", String(64), ForeignKey("events.id"), nullable=True, index=True),
    Column("last_seq", BigInteger, nullable=False, default=0),
    Column("last_seen_at", UTCDateTime, nullable=True),
)

markets = Table(
    "markets",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("event_id", String(64), ForeignKey("events.id"), nullable=False, index=True),
    Column("type", String(8), nullable=False),
    Column("line", String(16), nullable=False),  # "" for markets without a line
    Column("status", String(16), nullable=False),
    Column("price_version", Integer, nullable=False),
    Column("prices", JSON, nullable=False),
    Column("suspend_reason", String(32), nullable=True),
    Column("settled_result_version", Integer, nullable=True),
    Column("updated_at", UTCDateTime, nullable=False),
    UniqueConstraint("event_id", "type", "line", name="uq_market_key"),
)

market_prices = Table(
    "market_prices",
    metadata,
    Column("market_id", String(64), ForeignKey("markets.id"), primary_key=True),
    Column("version", Integer, primary_key=True),
    Column("prices", JSON, nullable=False),
    Column("provider", String(32), nullable=False),
    Column("created_at", UTCDateTime, nullable=False),
)

event_results = Table(
    "event_results",
    metadata,
    Column("id", BigId, primary_key=True, autoincrement=True),
    Column("event_id", String(64), ForeignKey("events.id"), nullable=False),
    Column("result_version", Integer, nullable=False),
    Column("home_score", Integer, nullable=False),
    Column("away_score", Integer, nullable=False),
    Column("status", String(16), nullable=False),
    Column("provider", String(32), nullable=False),
    Column("created_at", UTCDateTime, nullable=False),
    UniqueConstraint("event_id", "result_version", name="uq_event_result_version"),
)

bets = Table(
    "bets",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("player_id", String(64), nullable=False, index=True),
    Column("market_id", String(64), ForeignKey("markets.id"), nullable=False, index=True),
    Column("selection", String(16), nullable=False),
    Column("stake", BigInteger, nullable=False),
    Column("odds", String(16), nullable=False),
    Column("price_version", Integer, nullable=False),
    Column("stake_cash", BigInteger, nullable=False),
    Column("stake_bonus", BigInteger, nullable=False),
    Column("status", String(16), nullable=False),
    Column("outcome", String(16), nullable=True),
    Column("payout", BigInteger, nullable=True),
    Column("placed_at", UTCDateTime, nullable=False),
    Column("reserve_txn_id", String(64), nullable=False),
)

bet_settlements = Table(
    "bet_settlements",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("bet_id", String(64), ForeignKey("bets.id"), nullable=False),
    Column("seq", Integer, nullable=False),
    Column("outcome", String(16), nullable=False),
    Column("payout", BigInteger, nullable=False),
    Column("refund_cash", BigInteger, nullable=False),
    Column("refund_bonus", BigInteger, nullable=False),
    Column("result_version", Integer, nullable=False),
    Column("txn_id", String(64), nullable=False),
    Column("reversal_txn_id", String(64), nullable=True),
    Column("created_at", UTCDateTime, nullable=False),
    UniqueConstraint("bet_id", "seq", name="uq_bet_settlement_seq"),
)

ledger_accounts = Table(
    "ledger_accounts",
    metadata,
    Column("id", String(128), primary_key=True),
    Column("kind", String(32), nullable=False),
    Column("created_at", UTCDateTime, nullable=False),
)

ledger_transactions = Table(
    "ledger_transactions",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("kind", String(32), nullable=False),
    Column("reference", String(128), nullable=False, index=True),
    # UNIQUE: a transaction can be reversed at most once.
    Column("reverses", String(64), ForeignKey("ledger_transactions.id"), unique=True),
    Column("metadata", JSON, nullable=False),
    Column("created_at", UTCDateTime, nullable=False),
)

ledger_postings = Table(
    "ledger_postings",
    metadata,
    Column("id", BigId, primary_key=True, autoincrement=True),
    Column("txn_id", String(64), ForeignKey("ledger_transactions.id"), nullable=False, index=True),
    Column("account_id", String(128), ForeignKey("ledger_accounts.id"), nullable=False),
    Column("amount", BigInteger, nullable=False),
    Index("ix_postings_account", "account_id"),
)

idempotency_keys = Table(
    "idempotency_keys",
    metadata,
    Column("scope", String(64), primary_key=True),
    Column("key", String(128), primary_key=True),
    Column("request_hash", String(64), nullable=False),
    Column("status_code", Integer, nullable=False),
    Column("response", JSON, nullable=False),
    Column("created_at", UTCDateTime, nullable=False),
)

inbox = Table(
    "inbox",
    metadata,
    Column("consumer", String(64), primary_key=True),
    Column("message_id", String(64), primary_key=True),
    Column("processed_at", UTCDateTime, nullable=False),
)

outbox = Table(
    "outbox",
    metadata,
    Column("id", BigId, primary_key=True, autoincrement=True),
    Column("message_id", String(64), nullable=False, unique=True),
    Column("topic", String(128), nullable=False),
    Column("key", String(128), nullable=True),
    Column("payload", Text, nullable=False),
    Column("headers", JSON, nullable=False),
    Column("created_at", UTCDateTime, nullable=False),
    Column("published_at", UTCDateTime, nullable=True),
    Column("attempts", Integer, nullable=False, default=0),
    Column("next_attempt_at", UTCDateTime, nullable=False),
    Column("last_error", Text, nullable=True),
    Index("ix_outbox_pending", "published_at", "next_attempt_at", "id"),
)

audit_log = Table(
    "audit_log",
    metadata,
    Column("id", BigId, primary_key=True, autoincrement=True),
    Column("entity_type", String(32), nullable=False),
    Column("entity_id", String(64), nullable=False),
    Column("action", String(64), nullable=False),
    Column("data", JSON, nullable=False),
    Column("created_at", UTCDateTime, nullable=False),
    Index("ix_audit_entity", "entity_type", "entity_id"),
)
