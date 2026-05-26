"""
Data normalizer — converts raw tick data from different sources into a unified format.

All adapters already return [timestamp, bid, ask, mid] DataFrames with UTC timestamps.
This module provides additional normalization utilities for edge cases.
"""

import logging
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def normalize_tick_dataframe(
    df: pd.DataFrame,
    source: str = "unknown",
) -> pd.DataFrame:
    """
    Ensure a tick DataFrame conforms to the unified format.

    Expected input columns: [timestamp, bid, ask, mid]
    Output: same columns, with:
    - timestamp: UTC-aware datetime64[ns, UTC], sorted ascending
    - bid, ask, mid: float64, no NaN
    - No duplicate rows (same timestamp + bid + ask)
    """
    if df.empty:
        return df

    required_cols = {"timestamp", "bid", "ask", "mid"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing} (source: {source})")

    result = df[["timestamp", "bid", "ask", "mid"]].copy()

    # Ensure UTC-aware timestamps
    if result["timestamp"].dt.tz is None:
        result["timestamp"] = result["timestamp"].dt.tz_localize("UTC")
    elif str(result["timestamp"].dt.tz) != "UTC":
        result["timestamp"] = result["timestamp"].dt.tz_convert("UTC")

    # Ensure float64
    for col in ["bid", "ask", "mid"]:
        result[col] = pd.to_numeric(result[col], errors="coerce").astype(np.float64)

    # Drop rows with NaN prices
    before = len(result)
    result = result.dropna(subset=["bid", "ask", "mid"])
    dropped = before - len(result)
    if dropped > 0:
        logger.warning(f"Normalizer ({source}): dropped {dropped} rows with NaN prices")

    # Drop negative/zero prices
    price_mask = (result["bid"] > 0) & (result["ask"] > 0)
    invalid = (~price_mask).sum()
    if invalid > 0:
        logger.warning(f"Normalizer ({source}): dropped {invalid} rows with non-positive prices")
        result = result[price_mask]

    # Dedup and sort
    result = result.drop_duplicates(subset=["timestamp", "bid", "ask"], keep="first")
    result = result.sort_values("timestamp").reset_index(drop=True)

    logger.debug(f"Normalizer ({source}): {len(result)} ticks after normalization")
    return result


def compute_mid_if_missing(df: pd.DataFrame) -> pd.DataFrame:
    """Compute mid column if only bid and ask are present."""
    if "mid" not in df.columns and "bid" in df.columns and "ask" in df.columns:
        df = df.copy()
        df["mid"] = (df["bid"] + df["ask"]) / 2.0
    return df
