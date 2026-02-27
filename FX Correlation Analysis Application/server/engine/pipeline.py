"""
Pipeline — orchestrates the full data-to-analysis flow.

Connects: data adapter → cache → normalize → resample → correlate → metrics
"""

import logging
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd


def _to_utc_ts(dt: datetime) -> pd.Timestamp:
    """Safely convert a datetime (tz-aware or naive) to a UTC pd.Timestamp."""
    ts = pd.Timestamp(dt)
    if ts.tzinfo is not None:
        return ts.tz_convert("UTC")
    return ts.tz_localize("UTC")

from ..data.cache_manager import CacheManager
from ..data.mt5_adapter import MT5Adapter
from ..data.binance_adapter import BinanceAdapter
from ..data.normalizer import normalize_tick_dataframe
from ..data.validators import validate_tick_data
from .resampler import resample_ticks_to_ohlc
from .correlation import compute_correlation
from .metrics import compute_summary_metrics, compute_raw_metrics

logger = logging.getLogger(__name__)

# Singleton cache manager
_cache = CacheManager()
_cache.initialize()


def fetch_and_cache(
    domain: str,
    symbol: str,
    start: datetime,
    end: datetime,
    on_progress: Optional[callable] = None,
) -> pd.DataFrame:
    """
    Smart fetch: check cache first, fetch only gaps, store new data.

    Returns unified tick DataFrame [timestamp, bid, ask, mid].
    """
    t0 = time.time()

    # Ensure UTC
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    # Check cache
    cached_df = _cache.read(domain, symbol, start, end)
    gaps = _cache.find_gaps(domain, symbol, start, end)

    if not gaps:
        logger.info(f"Full cache hit for {domain}/{symbol}")
        return cached_df if cached_df is not None else pd.DataFrame(columns=["timestamp", "bid", "ask", "mid"])

    # Fetch missing ranges
    all_new = []

    adapter = _get_adapter(domain)
    with adapter:
        for gap_start, gap_end in gaps:
            logger.info(f"Fetching gap: {gap_start} → {gap_end}")
            df = adapter.fetch_ticks(symbol, gap_start, gap_end, on_progress=on_progress)

            if not df.empty:
                # Normalize and validate
                df = normalize_tick_dataframe(df, source=f"{domain}/{symbol}")
                validation = validate_tick_data(df, symbol, gap_start, gap_end)
                if validation.warnings:
                    logger.warning(f"Validation warnings for {symbol}: {validation.warnings}")

                # Cache the fresh data
                _cache.store(domain, symbol, df, gap_start, gap_end)
                all_new.append(df)

    # Combine cached + new
    parts = []
    if cached_df is not None and not cached_df.empty:
        parts.append(cached_df)
    parts.extend(all_new)

    if not parts:
        return pd.DataFrame(columns=["timestamp", "bid", "ask", "mid"])

    result = pd.concat(parts, ignore_index=True)
    result = result.drop_duplicates(subset=["timestamp"], keep="first")
    result = result.sort_values("timestamp").reset_index(drop=True)

    elapsed = time.time() - t0
    logger.info(f"fetch_and_cache {domain}/{symbol}: {len(result)} ticks in {elapsed:.1f}s")

    return result


def run_analysis(
    domain: str,
    symbol_1: str,
    symbol_2: str,
    timeframe: str,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    on_progress: Optional[callable] = None,
) -> dict:
    """
    Full analysis pipeline: fetch → resample → correlate → metrics.

    Returns dict with keys: result (DataFrame), metrics (dict), data (list[dict])
    """
    # Fetch tick data for both symbols
    df1 = fetch_and_cache(domain, symbol_1, start, end, on_progress)
    df2 = fetch_and_cache(domain, symbol_2, start, end, on_progress)

    if df1.empty or df2.empty:
        return {
            "status": "error",
            "message": "No tick data available for one or both symbols",
            "result": pd.DataFrame(),
            "metrics": {},
            "data": [],
        }

    # Resample to OHLC
    ohlc1 = resample_ticks_to_ohlc(df1, timeframe)
    ohlc2 = resample_ticks_to_ohlc(df2, timeframe)

    if ohlc1.empty or ohlc2.empty:
        return {
            "status": "error",
            "message": "No OHLC bars generated. Try a larger timeframe or wider date range.",
            "result": pd.DataFrame(),
            "metrics": {},
            "data": [],
        }

    # Apply time filtering if provided (on OHLC level)
    if start:
        start_ts = _to_utc_ts(start)
        ohlc1 = ohlc1[ohlc1.index >= start_ts]
        ohlc2 = ohlc2[ohlc2.index >= start_ts]
    if end:
        end_ts = _to_utc_ts(end)
        ohlc1 = ohlc1[ohlc1.index <= end_ts]
        ohlc2 = ohlc2[ohlc2.index <= end_ts]

    # Compute correlation
    result = compute_correlation(ohlc1, ohlc2, symbol_1, symbol_2)

    if result.empty:
        return {
            "status": "error",
            "message": "No overlapping timestamps between the two assets.",
            "result": pd.DataFrame(),
            "metrics": {},
            "data": [],
        }

    metrics = compute_summary_metrics(result)

    # Convert timestamps to ISO strings for JSON serialization
    data = result.copy()
    data["timestamp"] = data["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    data_list = data.to_dict(orient="records")

    return {
        "status": "success",
        "total_bars": len(result),
        "result": result,
        "metrics": metrics,
        "data": data_list,
    }


def _get_adapter(domain: str):
    """Factory: return the correct adapter for a domain."""
    if domain == "forex":
        return MT5Adapter()
    elif domain == "crypto":
        return BinanceAdapter()
    else:
        raise ValueError(f"Unknown domain: {domain}")
