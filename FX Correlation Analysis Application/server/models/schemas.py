"""
Pydantic request / response schemas for the REST API.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from .enums import Domain, JobStatus, Timeframe


# ── Data Fetch ────────────────────────────────────────────────────

class FetchRequest(BaseModel):
    """Request to fetch tick/trade data from a source."""
    symbol: str = Field(..., examples=["XAUUSDm", "BTCUSDT"])
    start: datetime = Field(..., description="UTC start datetime")
    end: datetime = Field(..., description="UTC end datetime")
    use_cache: bool = Field(True, description="Check local cache first")


class FetchResponse(BaseModel):
    """Result of a data fetch operation."""
    status: str
    symbol: str
    ticks_fetched: int
    cached_ranges: list[list[str]] = []
    fresh_ranges: list[list[str]] = []
    total_time_ms: float


# ── Analysis ──────────────────────────────────────────────────────

class AnalysisRequest(BaseModel):
    """Request to run correlation analysis on two assets."""
    domain: Domain
    symbol_1: str
    symbol_2: str
    timeframe: Timeframe
    start: Optional[datetime] = None
    end: Optional[datetime] = None


class AnalysisResponse(BaseModel):
    """Result of a standard correlation analysis."""
    status: str
    total_bars: int
    metrics: dict
    data: list[dict]  # list of row dicts for the frontend table


class CompareRequest(BaseModel):
    """Request to run Set A vs Set B comparison."""
    set_a: AnalysisRequest
    set_b: AnalysisRequest


class CompareResponse(BaseModel):
    """Result combining two analysis runs."""
    status: str
    set_a: AnalysisResponse
    set_b: AnalysisResponse


# ── Super Test Mode ───────────────────────────────────────────────

class SuperTestRequest(BaseModel):
    """Request to start a Super Test run."""
    domain: Domain
    symbol_1: str
    symbol_2: str
    timeframe: Timeframe
    date: str = Field(..., description="Date in YYYY-MM-DD format")
    start_time: str = Field(..., examples=["00:00"], description="HH:MM UTC")
    end_time: str = Field(..., examples=["08:00"], description="HH:MM UTC")
    interval_minutes: int = Field(5, ge=1, le=1440)


class IntervalResult(BaseModel):
    """Metrics for a single interval in Super Test."""
    interval_start: str
    interval_end: str
    total_bars: int
    total_flips: int
    total_flip_loss: float
    max_spread: float
    avg_spread: float
    max_single_flip_loss: float


class SuperTestJobResponse(BaseModel):
    """Returned when a Super Test job is submitted."""
    job_id: str
    total_intervals: int
    estimated_time_seconds: float
    ws_url: str


class SuperTestResult(BaseModel):
    """Complete result of a Super Test run."""
    job_id: str
    status: JobStatus
    total_intervals: int
    completed_intervals: int
    intervals: list[IntervalResult] = []
    rankings: list[dict] = []


# ── Configuration ─────────────────────────────────────────────────

class ConfigResponse(BaseModel):
    """Current application configuration (safe to expose)."""
    mt5_configured: bool
    mt5_server: str
    mt5_login: int
    binance_has_key: bool
    data_cache_dir: str
    available_timeframes: dict[str, str]


class SymbolsResponse(BaseModel):
    """Available symbols from a data source."""
    domain: Domain
    symbols: list[str]
