"""HTTP request/response models."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, Field, field_validator

from bet_lifecycle.domain.money import parse_odds


class PlaceBetRequest(BaseModel):
    # In production the player id comes from the auth token, not from the body.
    player_id: str = Field(min_length=1, max_length=64)
    market_id: str = Field(min_length=1, max_length=64)
    selection: str = Field(min_length=1, max_length=16)
    stake: int = Field(gt=0, description="minor units (cents)")
    odds: Decimal = Field(description="decimal odds the client saw, e.g. '2.1'")
    price_version: int = Field(ge=1)

    @field_validator("odds", mode="before")
    @classmethod
    def _odds(cls, v: object) -> Decimal:
        return parse_odds(str(v))


class AmountRequest(BaseModel):
    amount: int = Field(gt=0, le=10_000_000_00)


class ErrorResponse(BaseModel):
    error: str
    message: str
    details: dict[str, object] = Field(default_factory=dict)
