"""Domain errors. The API layer maps them to HTTP status codes."""

from __future__ import annotations

from typing import Any


class DomainError(Exception):
    code: str = "domain_error"

    def __init__(self, message: str, **details: Any) -> None:
        super().__init__(message)
        self.message = message
        self.details = details


class NotFound(DomainError):
    code = "not_found"


class MarketNotOpen(DomainError):
    code = "market_not_open"


class InvalidStateTransition(DomainError):
    code = "invalid_state_transition"


class StakeOutOfLimits(DomainError):
    code = "stake_out_of_limits"


class InsufficientFunds(DomainError):
    code = "insufficient_funds"


class UnknownSelection(DomainError):
    code = "unknown_selection"


class IdempotencyConflict(DomainError):
    """Same Idempotency-Key reused with a different request body."""

    code = "idempotency_conflict"


class Requote(DomainError):
    """Client priced the bet against an outdated price version."""

    code = "requote"

    def __init__(
        self,
        market_id: str,
        current_version: int,
        current_odds: str,
        requested_version: int,
    ) -> None:
        super().__init__(
            "price changed, requote required",
            market_id=market_id,
            current_price_version=current_version,
            current_odds=current_odds,
            requested_price_version=requested_version,
        )
        self.market_id = market_id
        self.current_version = current_version
        self.current_odds = current_odds


class UnbalancedTransaction(DomainError):
    code = "unbalanced_transaction"


class UnknownEvent(DomainError):
    """Feed message refers to a provider event we cannot map to a canonical one."""

    code = "unknown_event"
