# contract.py
# ---------------------------------------------------------------------------
# Data contract for an Alpha Vantage NEWS_SENTIMENT feed article.
#
# Contract metadata:
#   Source:  Alpha Vantage – NEWS_SENTIMENT endpoint
#   Version: 1.0.0
#   Owner:   data-platform-team
#   SLA:     schema compliance > 99 %
#
# Only the fields relevant to downstream consumers are retained.
# All other fields returned by the API are silently ignored via
# model_config extra="ignore".
# ---------------------------------------------------------------------------

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


# Mapping from Alpha Vantage label strings to canonical values
_VALID_SENTIMENT_LABELS = {
    "Bearish",
    "Somewhat-Bearish",
    "Neutral",
    "Somewhat_Bullish",   # Alpha Vantage uses underscore here
    "Bullish",
}


class NewsArticle(BaseModel):
    """Validated, filtered representation of one Alpha Vantage news feed item."""

    # ── Required fields ───────────────────────────────────────────────────────
    title: str = Field(..., min_length=1, description="Article headline")
    url: str = Field(..., description="Canonical URL of the article")
    time_published: str = Field(
        ...,
        description="Publication timestamp in Alpha Vantage format: YYYYMMDDTHHmmss",
    )
    summary: str = Field(..., min_length=1, description="Article summary / lede")
    source: str = Field(..., min_length=1, description="Publisher name")

    # ── Optional fields ───────────────────────────────────────────────────────
    category_within_source: Optional[str] = Field(
        default=None, description="Section/category label from the publisher"
    )
    overall_sentiment_score: Optional[float] = Field(
        default=None,
        description=(
            "Continuous sentiment score: "
            "<= -0.35 Bearish | (-0.35, -0.15] Somewhat-Bearish | "
            "(-0.15, 0.15) Neutral | [0.15, 0.35) Somewhat_Bullish | >= 0.35 Bullish"
        ),
    )
    overall_sentiment_label: Optional[str] = Field(
        default=None,
        description="Categorical sentiment label derived from overall_sentiment_score",
    )

    # ── Collector-enriched fields ─────────────────────────────────────────────
    ingest_timestamp: Optional[float] = Field(
        default=None,
        description="Unix epoch (seconds) when the collector received this article",
    )
    freshness_seconds: Optional[float] = Field(
        default=None,
        description="Lag in seconds between time_published and ingest_timestamp",
    )

    # ── Validators ────────────────────────────────────────────────────────────

    @field_validator("time_published")
    @classmethod
    def must_be_valid_av_timestamp(cls, v: str) -> str:
        """Alpha Vantage uses the compact format YYYYMMDDTHHmmss."""
        try:
            datetime.strptime(v, "%Y%m%dT%H%M%S")
        except ValueError:
            raise ValueError(
                f"'{v}' is not a valid Alpha Vantage timestamp (expected YYYYMMDDTHHmmss)"
            )
        return v

    @field_validator("overall_sentiment_label")
    @classmethod
    def must_be_valid_sentiment_label(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in _VALID_SENTIMENT_LABELS:
            raise ValueError(
                f"'{v}' is not a recognised sentiment label. "
                f"Expected one of: {sorted(_VALID_SENTIMENT_LABELS)}"
            )
        return v

    @field_validator("url")
    @classmethod
    def must_be_non_empty_url(cls, v: str) -> str:
        if not v.startswith("http"):
            raise ValueError(f"'{v}' does not look like a valid URL")
        return v

    model_config = {"extra": "ignore"}

    # ── Helpers ───────────────────────────────────────────────────────────────

    def published_datetime(self) -> datetime:
        """Return time_published as a timezone-naive datetime object."""
        return datetime.strptime(self.time_published, "%Y%m%dT%H%M%S")
