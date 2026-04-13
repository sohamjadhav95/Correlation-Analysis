"""
Resampler — converts tick-level data to OHLC bars at configurable timeframes.

Extracted from the original Streamlit app's resample_to_ohlc() function.
"""

import pandas as pd


def resample_ticks_to_ohlc(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """
    Resample tick MID prices to OHLC bars.

    Args:
        df: DataFrame with 'timestamp' and 'mid' columns (or DatetimeIndex + 'mid')
        rule: Pandas resample rule string (e.g. '10s', '1min', '5min', '1h', '1D')

    Returns:
        DataFrame with DatetimeIndex and columns: [open, high, low, close]
    """
    if df.empty:
        return pd.DataFrame(columns=["open", "high", "low", "close"])

    # If timestamp is a column, set it as index
    if "timestamp" in df.columns:
        work = df.set_index("timestamp")
    else:
        work = df

    # Ensure we have a MID column
    if "mid" not in work.columns:
        if "bid" in work.columns and "ask" in work.columns:
            work = work.copy()
            work["mid"] = (work["bid"] + work["ask"]) / 2.0
        else:
            raise ValueError("DataFrame must have 'mid' column or both 'bid' and 'ask'")

    ohlc = work["mid"].resample(rule).ohlc()
    ohlc = ohlc.dropna()
    return ohlc


def resample_ohlc_to_ohlc(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """
    Re-resample OHLC bars to a larger timeframe.

    Uses proper OHLC aggregation: open=first, high=max, low=min, close=last.
    """
    if df.empty:
        return df

    if "timestamp" in df.columns:
        work = df.set_index("timestamp")
    else:
        work = df

    resampled = work.resample(rule).agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
    })
    resampled = resampled.dropna()
    return resampled
