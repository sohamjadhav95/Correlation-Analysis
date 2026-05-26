"""
Binance Futures data adapter — fetches aggregated trades via REST API.

Design decisions:
- Uses /fapi/v1/aggTrades for tick-like data (most granular available).
- Paginates using fromId for deterministic, gap-free retrieval.
- Supports both public (no key) and authenticated endpoints.
- Rate-limit aware with token-bucket tracking.
- All timestamps normalized to UTC.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Optional

import httpx
import numpy as np
import pandas as pd

from ..config import BinanceConfig, AppConfig

logger = logging.getLogger(__name__)

# Binance Futures OHLC interval mapping
_BINANCE_INTERVAL_MAP = {
    "1min":  "1m",
    "5min":  "5m",
    "15min": "15m",
    "30min": "30m",
    "1h":    "1h",
    "4h":    "4h",
    "1D":    "1d",
}


class BinanceAdapter:
    """
    Binance Futures data adapter.

    Usage:
        with BinanceAdapter() as ba:
            df = ba.fetch_ticks("BTCUSDT", start, end)
    """

    def __init__(self):
        self._client: Optional[httpx.Client] = None
        self._connected = False
        self._request_weight_used = 0
        self._weight_reset_time = 0.0

    # ── Connection lifecycle ──────────────────────────────────────

    def connect(self) -> bool:
        """Create HTTP client session."""
        headers = {"Content-Type": "application/json"}

        if BinanceConfig.has_api_key():
            headers["X-MBX-APIKEY"] = BinanceConfig.api_key

        self._client = httpx.Client(
            base_url=BinanceConfig.base_url,
            headers=headers,
            timeout=30.0,
        )
        self._connected = True
        logger.info(f"Binance adapter connected (API key: {'yes' if BinanceConfig.has_api_key() else 'public'})")
        return True

    def disconnect(self) -> None:
        """Close HTTP client."""
        if self._client:
            self._client.close()
            self._connected = False
            logger.info("Binance adapter disconnected")

    # ── Tick (aggTrades) fetching ─────────────────────────────────

    def fetch_ticks(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        on_progress: Optional[callable] = None,
    ) -> pd.DataFrame:
        """
        Fetch aggregated trades from Binance Futures.

        Uses time-based initial query, then switches to fromId pagination
        for gap-free retrieval.

        Returns DataFrame with columns: [timestamp, bid, ask, mid]
        (For aggTrades, bid=ask=mid=price since aggTrades only have a single price)
        """
        self._ensure_connected()

        start_ms = int(start.replace(tzinfo=timezone.utc).timestamp() * 1000) if start.tzinfo is None \
            else int(start.astimezone(timezone.utc).timestamp() * 1000)
        end_ms = int(end.replace(tzinfo=timezone.utc).timestamp() * 1000) if end.tzinfo is None \
            else int(end.astimezone(timezone.utc).timestamp() * 1000)

        all_trades = []
        current_start_ms = start_ms
        last_agg_id = None
        total_ms = end_ms - start_ms
        page = 0

        logger.info(f"Fetching Binance aggTrades: {symbol} from {start} to {end}")

        while current_start_ms < end_ms:
            self._rate_limit_check()

            params = {
                "symbol": symbol,
                "limit": AppConfig.binance_agg_trades_limit,
            }

            if last_agg_id is not None:
                # Subsequent pages: use fromId for deterministic pagination
                params["fromId"] = last_agg_id + 1
            else:
                # First page: use time range
                params["startTime"] = current_start_ms
                params["endTime"] = end_ms

            trades = self._request_with_retry("/fapi/v1/aggTrades", params)

            if not trades:
                # No more trades in range
                break

            # Filter out trades beyond our end time
            trades = [t for t in trades if t["T"] <= end_ms]

            if not trades:
                break

            all_trades.extend(trades)
            last_agg_id = trades[-1]["a"]  # last aggregate trade ID
            current_start_ms = trades[-1]["T"] + 1  # move past last trade time

            page += 1
            if on_progress:
                progress = min((current_start_ms - start_ms) / max(total_ms, 1), 1.0)
                on_progress(int(progress * 100), 100)

            logger.debug(f"  Page {page}: {len(trades)} trades, last_id={last_agg_id}")

            # If we got fewer than the limit, we've reached the end
            if len(trades) < AppConfig.binance_agg_trades_limit:
                break

        if not all_trades:
            logger.warning(f"No aggTrades for {symbol} in range")
            return pd.DataFrame(columns=["timestamp", "bid", "ask", "mid"])

        result = self._trades_to_dataframe(all_trades)

        # Dedup on agg_trade_id
        result = result.drop_duplicates(subset=["timestamp"], keep="first")
        result = result.sort_values("timestamp").reset_index(drop=True)

        logger.info(f"Binance aggTrades fetched: {symbol} = {len(result)} trades over {page} pages")
        return result

    def _trades_to_dataframe(self, trades: list[dict]) -> pd.DataFrame:
        """Convert Binance aggTrade dicts to normalized DataFrame."""
        df = pd.DataFrame(trades)

        # T = timestamp in ms, p = price, q = quantity, a = agg trade id
        df["timestamp"] = pd.to_datetime(df["T"], unit="ms", utc=True)
        df["price"] = df["p"].astype(np.float64)

        # aggTrades don't have separate bid/ask — use price for all three
        df["bid"] = df["price"]
        df["ask"] = df["price"]
        df["mid"] = df["price"]

        return df[["timestamp", "bid", "ask", "mid"]]

    # ── OHLC (klines) fallback ────────────────────────────────────

    def fetch_ohlc(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str,
    ) -> pd.DataFrame:
        """
        Fetch OHLC klines from Binance Futures.

        Returns DataFrame with columns: [timestamp, open, high, low, close, volume]
        """
        self._ensure_connected()

        interval = _BINANCE_INTERVAL_MAP.get(timeframe)
        if interval is None:
            raise ValueError(f"Unsupported Binance interval: {timeframe}. Supported: {list(_BINANCE_INTERVAL_MAP.keys())}")

        start_ms = int(start.replace(tzinfo=timezone.utc).timestamp() * 1000) if start.tzinfo is None \
            else int(start.astimezone(timezone.utc).timestamp() * 1000)
        end_ms = int(end.replace(tzinfo=timezone.utc).timestamp() * 1000) if end.tzinfo is None \
            else int(end.astimezone(timezone.utc).timestamp() * 1000)

        all_klines = []
        current_start = start_ms

        while current_start < end_ms:
            self._rate_limit_check()

            params = {
                "symbol": symbol,
                "interval": interval,
                "startTime": current_start,
                "endTime": end_ms,
                "limit": 1500,
            }

            klines = self._request_with_retry("/fapi/v1/klines", params)

            if not klines:
                break

            all_klines.extend(klines)
            # Move to next window (kline close time + 1ms)
            current_start = klines[-1][6] + 1

            if len(klines) < 1500:
                break

        if not all_klines:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

        df = pd.DataFrame(all_klines, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore",
        ])

        df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(np.float64)

        return df[["timestamp", "open", "high", "low", "close", "volume"]]

    # ── Symbol listing ────────────────────────────────────────────

    def get_symbols(self) -> list[str]:
        """Return available Binance Futures USDT-M symbols."""
        self._ensure_connected()

        data = self._request_with_retry("/fapi/v1/exchangeInfo", {})
        if not data or "symbols" not in data:
            return []

        return sorted([
            s["symbol"] for s in data["symbols"]
            if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING"
        ])

    # ── Properties ────────────────────────────────────────────────

    @property
    def domain(self) -> str:
        return "crypto"

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ── Rate limiting ─────────────────────────────────────────────

    def _rate_limit_check(self):
        """Sleep if approaching rate limit ceiling."""
        now = time.time()

        # Reset weight counter every 60 seconds
        if now - self._weight_reset_time > 60:
            self._request_weight_used = 0
            self._weight_reset_time = now

        max_weight = int(AppConfig.binance_rate_limit_weight * AppConfig.binance_rate_limit_buffer)

        if self._request_weight_used >= max_weight:
            sleep_time = 60 - (now - self._weight_reset_time)
            if sleep_time > 0:
                logger.info(f"Binance rate limit: sleeping {sleep_time:.1f}s")
                time.sleep(sleep_time)
            self._request_weight_used = 0
            self._weight_reset_time = time.time()

    # ── HTTP request with retry ───────────────────────────────────

    def _request_with_retry(self, endpoint: str, params: dict, max_retries: int = 3):
        """Make GET request with exponential backoff retry."""
        delays = [1, 2, 5]

        for attempt in range(max_retries):
            try:
                resp = self._client.get(endpoint, params=params)

                # Track rate limit weight from response headers
                weight = int(resp.headers.get("X-MBX-USED-WEIGHT-1m", "0"))
                if weight:
                    self._request_weight_used = weight

                if resp.status_code == 200:
                    return resp.json()

                if resp.status_code == 429:
                    # Rate limited — wait and retry
                    retry_after = int(resp.headers.get("Retry-After", "60"))
                    logger.warning(f"Binance 429 rate limited, sleeping {retry_after}s")
                    time.sleep(retry_after)
                    continue

                if resp.status_code >= 500:
                    logger.warning(f"Binance server error {resp.status_code}, retry {attempt + 1}")
                    if attempt < max_retries - 1:
                        time.sleep(delays[attempt])
                    continue

                # Client error (4xx other than 429) — don't retry
                logger.error(f"Binance API error {resp.status_code}: {resp.text}")
                return None

            except httpx.TimeoutException:
                logger.warning(f"Binance request timeout, retry {attempt + 1}")
                if attempt < max_retries - 1:
                    time.sleep(delays[attempt])
            except httpx.HTTPError as e:
                logger.warning(f"Binance HTTP error: {e}, retry {attempt + 1}")
                if attempt < max_retries - 1:
                    time.sleep(delays[attempt])

        logger.error(f"Binance request failed after {max_retries} attempts: {endpoint}")
        return None

    # ── Internal ──────────────────────────────────────────────────

    def _ensure_connected(self):
        if not self._connected:
            raise RuntimeError("Binance adapter not connected. Call connect() first or use as context manager.")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False
