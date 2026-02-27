"""
Application configuration — loads .env and exposes typed settings.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ── Load .env from project root ──────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")


class MT5Config:
    """MetaTrader 5 connection settings."""
    login: int = int(os.getenv("MT5_LOGIN") or "0")
    password: str = os.getenv("MT5_PASSWORD", "")
    server: str = os.getenv("MT5_SERVER", "")
    path: str = os.getenv("MT5_PATH", "")  # e.g. C:\Program Files\MetaTrader 5\terminal64.exe

    @classmethod
    def is_configured(cls) -> bool:
        return cls.login != 0 and bool(cls.password) and bool(cls.server)


class BinanceConfig:
    """Binance Futures API settings."""
    api_key: str = os.getenv("BINANCE_API_KEY", "")
    api_secret: str = os.getenv("BINANCE_API_SECRET", "")
    base_url: str = "https://fapi.binance.com"

    @classmethod
    def has_api_key(cls) -> bool:
        return bool(cls.api_key) and bool(cls.api_secret)


class AppConfig:
    """Application-level settings."""
    data_cache_dir: Path = _PROJECT_ROOT / os.getenv("DATA_CACHE_DIR", "data_cache")
    host: str = os.getenv("HOST", "127.0.0.1")
    port: int = int(os.getenv("PORT", "8000"))
    frontend_dir: Path = _PROJECT_ROOT / "frontend"

    # MT5 fetch settings
    mt5_chunk_hours: int = 24          # fetch ticks in 24-hour windows
    mt5_max_retries: int = 3
    mt5_retry_delays: list = [1, 2, 5]  # seconds

    # Binance fetch settings
    binance_agg_trades_limit: int = 1000    # max per REST call
    binance_rate_limit_weight: int = 2400   # per minute
    binance_rate_limit_buffer: float = 0.8  # use 80% of limit

    # Super Test
    super_test_max_workers: int = min(os.cpu_count() or 4, 8)

    @classmethod
    def ensure_dirs(cls):
        """Create required directories."""
        cls.data_cache_dir.mkdir(parents=True, exist_ok=True)
        (cls.data_cache_dir / "forex").mkdir(exist_ok=True)
        (cls.data_cache_dir / "crypto").mkdir(exist_ok=True)


# ── Timeframe mappings (shared between backend and frontend) ─────
TIMEFRAME_MAP = {
    "10 Seconds":  "10s",
    "30 Seconds":  "30s",
    "1 Minute":    "1min",
    "5 Minutes":   "5min",
    "15 Minutes":  "15min",
    "30 Minutes":  "30min",
    "1 Hour":      "1h",
    "4 Hours":     "4h",
    "1 Day":       "1D",
}
