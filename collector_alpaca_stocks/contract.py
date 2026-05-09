# contract.py
# ---------------------------------------------------------------------------
# Data contracts for Alpaca IEX stream events.
#
# This is a first-class, versioned artifact. Other services and pipelines
# may import TradeEvent or QuoteEvent directly to validate or deserialise
# messages from the Alpaca WebSocket stream.
#
# Contract metadata:
#   Source:  Alpaca Markets WebSocket – wss://stream.data.alpaca.markets/v2/iex
#   Version: 1.0.0
#   Owner:   data-platform-team
#   SLA:     message lag < 5 s, schema compliance > 99 %
#
# Alpaca message type identifiers:
#   "t" → trade event
#   "q" → quote event
# ---------------------------------------------------------------------------

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Trade Event  (T == "t")
# ---------------------------------------------------------------------------
# Example raw message:
# {"T":"t","S":"AAPL","i":12345,"x":"V","p":195.10,"s":100,
#  "c":["@","I"],"z":"C","t":"2025-09-19T19:41:06.268341055Z"}
# ---------------------------------------------------------------------------

class TradeEvent(BaseModel):
    """Validated representation of a single Alpaca IEX trade event."""

    # ── Required exchange fields ──────────────────────────────────────────────
    T: str = Field(..., pattern=r"^t$", description="Message type — always 't' for trades")
    S: str = Field(..., description="Ticker symbol, e.g. 'AAPL'")
    i: int = Field(..., ge=0, description="Trade ID")
    x: str = Field(..., description="Exchange code where the trade occurred")
    p: float = Field(..., gt=0, description="Trade price")
    s: int = Field(..., gt=0, description="Trade size (shares)")
    t: str = Field(..., description="ISO-8601 exchange timestamp")

    # ── Optional exchange fields ──────────────────────────────────────────────
    c: Optional[list[str]] = Field(default=None, description="Trade condition codes")
    z: Optional[str] = Field(default=None, description="Tape identifier (A/B/C)")

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

    @field_validator("t")
    @classmethod
    def must_be_valid_iso8601(cls, v: str) -> str:
        try:
            # Alpaca timestamps may have nanosecond precision; truncate to microseconds
            normalized = v[:26] + "Z" if len(v) > 27 else v
            datetime.fromisoformat(normalized.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(f"'{v}' is not a valid ISO-8601 timestamp")
        return v

    @field_validator("S")
    @classmethod
    def must_be_valid_symbol(cls, v: str) -> str:
        if not v or not v.replace(".", "").replace("-", "").isalpha():
            raise ValueError(f"'{v}' is not a valid ticker symbol")
        return v.upper()

    # Tolerate undocumented fields from the exchange without raising errors
    model_config = {"extra": "ignore"}


# ---------------------------------------------------------------------------
# Quote Event  (T == "q")
# ---------------------------------------------------------------------------
# Example raw message:
# {"T":"q","S":"AAPL","bx":"V","bp":244.96,"bs":1,
#  "ax":"V","ap":245.55,"as":1,"c":["R"],"z":"C",
#  "t":"2025-09-19T19:41:06.268341055Z"}
# ---------------------------------------------------------------------------

class QuoteEvent(BaseModel):
    """Validated representation of a single Alpaca IEX quote (NBBO) event."""

    # ── Required exchange fields ──────────────────────────────────────────────
    T: str = Field(..., pattern=r"^q$", description="Message type — always 'q' for quotes")
    S: str = Field(..., description="Ticker symbol, e.g. 'AAPL'")
    bx: str = Field(..., description="Bid exchange code")
    bp: float = Field(..., ge=0, description="Bid price")
    bs: int = Field(..., ge=0, description="Bid size (shares)")
    ax: str = Field(..., description="Ask exchange code")
    ap: float = Field(..., ge=0, description="Ask price")
    as_: int = Field(..., ge=0, alias="as", description="Ask size (shares)")
    t: str = Field(..., description="ISO-8601 exchange timestamp")

    # ── Optional exchange fields ──────────────────────────────────────────────
    c: Optional[list[str]] = Field(default=None, description="Quote condition codes")
    z: Optional[str] = Field(default=None, description="Tape identifier (A/B/C)")

    # ── Collector-enriched fields (populated after construction) ─────────────
    ingest_timestamp: Optional[float] = Field(
        default=None,
        description="Unix epoch (seconds) when the collector received this message",
    )
    freshness_seconds: Optional[float] = Field(
        default=None,
        description="Lag in seconds between exchange timestamp and ingest_timestamp",
    )

    # ── Derived fields ────────────────────────────────────────────────────────
    spread: Optional[float] = Field(
        default=None,
        description="Ask price minus bid price",
    )

    # ── Validators ───────────────────────────────────────────────────────────

    @field_validator("t")
    @classmethod
    def must_be_valid_iso8601(cls, v: str) -> str:
        try:
            normalized = v[:26] + "Z" if len(v) > 27 else v
            datetime.fromisoformat(normalized.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError(f"'{v}' is not a valid ISO-8601 timestamp")
        return v

    @field_validator("S")
    @classmethod
    def must_be_valid_symbol(cls, v: str) -> str:
        if not v or not v.replace(".", "").replace("-", "").isalpha():
            raise ValueError(f"'{v}' is not a valid ticker symbol")
        return v.upper()

    @field_validator("ap")
    @classmethod
    def ask_must_be_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("ask price must be >= 0")
        return v

    @field_validator("bp")
    @classmethod
    def bid_must_be_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("bid price must be >= 0")
        return v

    # Allow Pydantic to use the alias "as" for the as_ field
    model_config = {"extra": "ignore", "populate_by_name": True}
