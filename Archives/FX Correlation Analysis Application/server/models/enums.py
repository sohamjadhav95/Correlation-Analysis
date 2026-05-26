"""
Enumerations used across the application.
"""

from enum import Enum


class Domain(str, Enum):
    """Data source domain."""
    FOREX = "forex"
    CRYPTO = "crypto"


class JobStatus(str, Enum):
    """Background job lifecycle states."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Timeframe(str, Enum):
    """Supported resampling timeframes."""
    TEN_SECONDS = "10s"
    THIRTY_SECONDS = "30s"
    ONE_MINUTE = "1min"
    FIVE_MINUTES = "5min"
    FIFTEEN_MINUTES = "15min"
    THIRTY_MINUTES = "30min"
    ONE_HOUR = "1h"
    FOUR_HOURS = "4h"
    ONE_DAY = "1D"


class FetchMode(str, Enum):
    """Tick fetch strategy."""
    TICKS_ONLY = "ticks"           # copy_ticks_range (MT5) / aggTrades (Binance)
    OHLC_FALLBACK = "ohlc"        # copy_rates_range (MT5) / klines (Binance)
    AUTO = "auto"                  # try ticks first, fall back to OHLC
