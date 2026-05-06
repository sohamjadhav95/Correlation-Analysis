"""
Application configuration — loads .env and exposes typed settings.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ── Load .env from project root ──────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")


import json

class MT5Config:
    """MetaTrader 5 connection settings."""
    # Default initialize
    login: int = 0
    password: str = ""
    server: str = ""
    path: str = ""
    
    # Attempt to read from config/mt5_credentials.json first
    _creds_file = _PROJECT_ROOT / "config" / "mt5_credentials.json"
    if _creds_file.exists():
        try:
            with open(_creds_file, 'r', encoding='utf-8') as _f:
                _creds = json.load(_f)
                login = int(_creds.get("MT5_LOGIN", 0))
                password = str(_creds.get("MT5_PASSWORD", ""))
                server = str(_creds.get("MT5_SERVER", ""))
                path = str(_creds.get("MT5_PATH", ""))
        except Exception:
            pass

    # Fallback to .env / os.getenv if missing
    if not login:
        login = int(os.getenv("MT5_LOGIN") or "0")
    if not password:
        password = os.getenv("MT5_PASSWORD", "")
    if not server:
        server = os.getenv("MT5_SERVER", "")
    if not path:
        path = os.getenv("MT5_PATH", "")

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
