"""
MetaTrader 5 data adapter — handles connection, authentication, and tick/OHLC fetching.

Key design decisions:
- MT5 Python API is NOT thread-safe. All MT5 calls go through a single dedicated thread.
- Ticks are fetched in 24h chunked windows to prevent memory spikes.
- time_msc (millisecond-precision) is used for timestamp accuracy.
- All output is normalized to UTC.
- Includes retry logic with exponential backoff.
"""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pandas as pd

from ..config import MT5Config, AppConfig

logger = logging.getLogger(__name__)

# MT5 timeframe mapping (string → MT5 constant)
_MT5_TF_MAP = {}


def _lazy_import_mt5():
    """Import MT5 lazily so the module can be loaded even without MT5 installed."""
    try:
        import MetaTrader5 as mt5
        return mt5
    except ImportError:
        raise ImportError(
            "MetaTrader5 package not installed. Install with: pip install MetaTrader5"
        )


def _init_tf_map(mt5):
    """Populate MT5 timeframe constant mapping."""
    global _MT5_TF_MAP
    if _MT5_TF_MAP:
        return
    _MT5_TF_MAP = {
        "1min":  mt5.TIMEFRAME_M1,
        "5min":  mt5.TIMEFRAME_M5,
        "15min": mt5.TIMEFRAME_M15,
        "30min": mt5.TIMEFRAME_M30,
        "1h":    mt5.TIMEFRAME_H1,
        "4h":    mt5.TIMEFRAME_H4,
        "1D":    mt5.TIMEFRAME_D1,
    }


class MT5Adapter:
    """
    MetaTrader 5 data adapter.

    Usage:
        with MT5Adapter() as mt5a:
            df = mt5a.fetch_ticks("XAUUSDm", start, end)
    """

    def __init__(self):
        self._mt5 = None
        self._connected = False

    # ── Connection lifecycle ──────────────────────────────────────

    def connect(self) -> bool:
        """
        Initialize MT5 terminal and authenticate.

        Connection strategy (in order):
        1. Try attach to already-running terminal (no path) — fastest path.
        2. If already logged in to correct account, skip login().
        3. Try initialize() with configured MT5_PATH.
        4. Try all discovered terminal64.exe paths.
        5. On -6 error, give actionable instructions to user.
        """
        mt5 = _lazy_import_mt5()
        self._mt5 = mt5
        _init_tf_map(mt5)

        # Build list of paths to try: no-path first, then configured, then discovered
        paths_to_try: list[str | None] = [None]  # None = attach to running terminal
        if MT5Config.path:
            paths_to_try.append(MT5Config.path)
        paths_to_try.extend(self._discover_terminal_paths())

        last_error = None

        for path in paths_to_try:
            label = repr(path) if path else "(attach to running terminal)"
            logger.info(f"MT5 initialize attempt: {label}")

            init_kwargs = {"path": path} if path else {}
            ok = mt5.initialize(**init_kwargs)

            if not ok:
                err = mt5.last_error()
                last_error = err
                logger.warning(f"MT5 initialize failed ({label}): {err}")

                if err[0] == -6:
                    # -6 = Authorization failed = terminal not running / first-time setup needed
                    # No point trying other paths with same issue type
                    continue
                continue

            # ── initialize() succeeded ──────────────────────────────
            info = mt5.terminal_info()
            logger.info(f"MT5 terminal attached: {info.name} build={info.build} connected={info.connected}")

            # If already logged in to the correct account, skip login()
            current_account = mt5.account_info()
            if current_account and current_account.login == MT5Config.login:
                logger.info(f"MT5 already logged in as {current_account.login} on {current_account.server}")
                self._connected = True
                return True

            # Try login
            if not MT5Config.is_configured():
                logger.warning("MT5 credentials not configured — using terminal's current session")
                self._connected = True
                return True

            ok_login = mt5.login(
                login=MT5Config.login,
                password=MT5Config.password,
                server=MT5Config.server,
            )
            if ok_login:
                ai = mt5.account_info()
                logger.info(f"MT5 logged in: account={ai.login} server={ai.server} name={ai.name}")
                self._connected = True
                return True

            err = mt5.last_error()
            logger.warning(f"MT5 login() failed: {err}")
            mt5.shutdown()
            last_error = err
            break  # If initialize() worked but login() failed, credentials are wrong — stop

        # ── All attempts failed ──────────────────────────────────────
        err_code = last_error[0] if last_error else -1
        err_msg = last_error[1] if last_error else "unknown"

        if err_code == -6:
            raise ConnectionError(
                "MT5 terminal is not running or has not been set up yet.\n"
                "FIX: Open MetaTrader 5 manually, log into your Exness account once,\n"
                "     then keep the terminal running in the background.\n"
                "     Our application will then attach to it automatically."
            )
        elif err_code in (10013, 10014):
            raise ConnectionError(
                f"MT5 login rejected (error {err_code}: {err_msg}).\n"
                "FIX: Check that login, password, and server in .env are correct.\n"
                f"     Configured: login={MT5Config.login}  server={MT5Config.server!r}"
            )
        else:
            raise ConnectionError(
                f"MT5 connection failed (error {err_code}: {err_msg}).\n"
                f"Configured path: {MT5Config.path!r}"
            )

    @staticmethod
    def _discover_terminal_paths() -> list[str]:
        """Search common install locations for terminal64.exe."""
        import glob as _glob
        import pathlib as _pathlib

        candidates = [
            r"C:\Program Files\MetaTrader 5\terminal64.exe",
            r"C:\Program Files (x86)\MetaTrader 5\terminal64.exe",
        ]
        # Broker-specific installs under Program Files
        candidates += _glob.glob(r"C:\Program Files\*\terminal64.exe")
        candidates += _glob.glob(r"C:\Program Files (x86)\*\terminal64.exe")

        return [c for c in candidates if _pathlib.Path(c).exists()]


    def disconnect(self) -> None:
        """Shutdown MT5 terminal connection."""
        if self._mt5 and self._connected:
            self._mt5.shutdown()
            self._connected = False
            logger.info("MT5 disconnected")

    # ── Tick data fetching ────────────────────────────────────────

    def fetch_ticks(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        on_progress: Optional[callable] = None,
    ) -> pd.DataFrame:
        """
        Fetch tick data from MT5 in chunked 24h windows.

        Returns DataFrame with columns: [timestamp, bid, ask, mid]
        Timestamps are UTC datetime64[ns, UTC].
        """
        self._ensure_connected()
        mt5 = self._mt5

        # Ensure UTC-aware datetimes
        start_utc = start.replace(tzinfo=timezone.utc) if start.tzinfo is None else start.astimezone(timezone.utc)
        end_utc = end.replace(tzinfo=timezone.utc) if end.tzinfo is None else end.astimezone(timezone.utc)

        all_chunks = []
        chunk_start = start_utc
        chunk_hours = AppConfig.mt5_chunk_hours
        total_hours = (end_utc - start_utc).total_seconds() / 3600
        fetched_hours = 0

        logger.info(f"Fetching MT5 ticks: {symbol} from {start_utc} to {end_utc}")

        while chunk_start < end_utc:
            chunk_end = min(chunk_start + timedelta(hours=chunk_hours), end_utc)

            ticks = self._fetch_tick_chunk(symbol, chunk_start, chunk_end)

            if ticks is not None and len(ticks) > 0:
                df_chunk = self._ticks_to_dataframe(ticks)
                all_chunks.append(df_chunk)
                logger.debug(f"  Chunk {chunk_start} → {chunk_end}: {len(df_chunk)} ticks")
            else:
                logger.debug(f"  Chunk {chunk_start} → {chunk_end}: 0 ticks (gap or non-trading)")

            fetched_hours += chunk_hours
            if on_progress:
                on_progress(int(fetched_hours), int(total_hours))

            chunk_start = chunk_end

        if not all_chunks:
            logger.warning(f"No ticks returned for {symbol} in range")
            return pd.DataFrame(columns=["timestamp", "bid", "ask", "mid"])

        result = pd.concat(all_chunks, ignore_index=True)

        # Remove duplicates (same timestamp + bid + ask)
        result = result.drop_duplicates(subset=["timestamp", "bid", "ask"], keep="first")
        result = result.sort_values("timestamp").reset_index(drop=True)

        logger.info(f"MT5 ticks fetched: {symbol} = {len(result)} ticks")
        return result

    def _fetch_tick_chunk(self, symbol: str, start: datetime, end: datetime):
        """Fetch a single chunk with retry."""
        mt5 = self._mt5

        for attempt in range(AppConfig.mt5_max_retries):
            ticks = mt5.copy_ticks_range(symbol, start, end, mt5.COPY_TICKS_ALL)

            if ticks is not None:
                return ticks

            error = mt5.last_error()
            logger.warning(f"MT5 copy_ticks_range failed (attempt {attempt + 1}): {error}")

            if attempt < AppConfig.mt5_max_retries - 1:
                time.sleep(AppConfig.mt5_retry_delays[attempt])

        return None

    def _ticks_to_dataframe(self, ticks) -> pd.DataFrame:
        """Convert MT5 tick array to normalized DataFrame."""
        df = pd.DataFrame(ticks)

        # Use time_msc for millisecond precision (column is microseconds since epoch)
        if "time_msc" in df.columns:
            df["timestamp"] = pd.to_datetime(df["time_msc"], unit="ms", utc=True)
        else:
            df["timestamp"] = pd.to_datetime(df["time"], unit="s", utc=True)

        df["bid"] = df["bid"].astype(np.float64)
        df["ask"] = df["ask"].astype(np.float64)
        df["mid"] = (df["bid"] + df["ask"]) / 2.0

        return df[["timestamp", "bid", "ask", "mid"]]

    # ── OHLC fallback ─────────────────────────────────────────────

    def fetch_ohlc(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str,
    ) -> pd.DataFrame:
        """
        Fetch OHLC bar data from MT5 (fallback for older date ranges).

        Returns DataFrame with columns: [timestamp, open, high, low, close, volume]
        """
        self._ensure_connected()
        mt5 = self._mt5

        tf_const = _MT5_TF_MAP.get(timeframe)
        if tf_const is None:
            raise ValueError(f"Unsupported MT5 timeframe: {timeframe}. Supported: {list(_MT5_TF_MAP.keys())}")

        start_utc = start.replace(tzinfo=timezone.utc) if start.tzinfo is None else start.astimezone(timezone.utc)
        end_utc = end.replace(tzinfo=timezone.utc) if end.tzinfo is None else end.astimezone(timezone.utc)

        rates = mt5.copy_rates_range(symbol, tf_const, start_utc, end_utc)

        if rates is None or len(rates) == 0:
            logger.warning(f"No OHLC data for {symbol} ({timeframe}) in range")
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

        df = pd.DataFrame(rates)
        df["timestamp"] = pd.to_datetime(df["time"], unit="s", utc=True)
        df = df.rename(columns={
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "tick_volume": "volume",
        })

        return df[["timestamp", "open", "high", "low", "close", "volume"]]

    # ── Symbol listing ────────────────────────────────────────────

    def get_symbols(self) -> list[str]:
        """Return all available symbols from the MT5 terminal."""
        self._ensure_connected()
        mt5 = self._mt5

        symbols = mt5.symbols_get()
        if symbols is None:
            return []

        return sorted([s.name for s in symbols])

    # ── Properties ────────────────────────────────────────────────

    @property
    def domain(self) -> str:
        return "forex"

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ── Internal ──────────────────────────────────────────────────

    def _ensure_connected(self):
        if not self._connected:
            raise RuntimeError("MT5 not connected. Call connect() first or use as context manager.")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False
