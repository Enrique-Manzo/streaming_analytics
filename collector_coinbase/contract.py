# contract.py
# ---------------------------------------------------------------------------
# Data contract for a Coinbase 'match' trade event.
#
# This is a first-class, versioned artifact. Other services and pipelines
# may import TradeEvent directly to validate or deserialise trade messages.
#
# Contract metadata:
#   Source:  Coinbase Exchange WebSocket – matches channel
#   Version: 1.0.0
#   Owner:   data-platform-team
#   SLA:     message lag < 5 s, schema compliance > 99 %
# ---------------------------------------------------------------------------

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class TradeEvent(BaseModel):
    """Validated representation of a single Coinbase match/last_match event."""

    # ── Required fields ──────────────────────────────────────────────────────
    type: str = Field(..., pattern=r"^(match|last_match)$")
    trade_id: int = Field(..., gt=0)
    sequence: int = Field(..., ge=0)
    product_id: str = Field(..., pattern=r"^[A-Z]+-[A-Z]+$")
    price: str = Field(..., description="Positive decimal string, e.g. '65432.10'")
    size: str = Field(..., description="Positive decimal string, e.g. '0.001'")
    side: str = Field(..., pattern=r"^(buy|sell)$")
    time: str = Field(..., description="ISO-8601 timestamp from the exchange")

    # ── Optional exchange fields ──────────────────────────────────────────────
    maker_order_id: Optional[str] = None
    taker_order_id: Optional[str] = None

    # ── Collector-enriched fields (populated after construction) ─────────────
    ingest_timestamp: Optional[float] = Field(
        default=None,
        description="Unix epoch (seconds) when the collector received this message",
    )
    freshness_seconds: Optional[float] = Field(
        default=None,
        description="Lag in seconds between exchange timestamp and ingest_timestamp",
    )

    # ── Validators ───────────────────────────────────────────────────────────

    @field_validator("price", "size")
    @classmethod
    def must_be_positive_decimal(cls, v: str) -> str:
        try:
            val = float(v)
            if val <= 0:
                raise ValueError("must be > 0")
        except (TypeError, ValueError):
            raise ValueError(f"'{v}' is not a valid positive decimal string")
        return v

    @field_validator("time")
    @classmethod
    def must_be_valid_iso8601(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(f"'{v}' is not a valid ISO-8601 timestamp")
        return v

    # Tolerate undocumented fields from the exchange without raising errors
    model_config = {"extra": "ignore"}
