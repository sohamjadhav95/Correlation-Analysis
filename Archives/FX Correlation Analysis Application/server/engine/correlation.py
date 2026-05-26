"""
Correlation engine — computes correlation indices, spread, position, flips, and flip loss.

Extracted and enhanced from the original compute_correlation_output() function.
"""

import numpy as np
import pandas as pd


def compute_correlation(
    df1_ohlc: pd.DataFrame,
    df2_ohlc: pd.DataFrame,
    sym1: str,
    sym2: str,
) -> pd.DataFrame:
    """
    Align two OHLC series on common timestamps and compute:
    - pct_change, cumulative index, spread, position, flip, flip_loss

    Args:
        df1_ohlc: OHLC DataFrame for asset 1 (DatetimeIndex + open/high/low/close)
        df2_ohlc: OHLC DataFrame for asset 2
        sym1: Symbol name for asset 1
        sym2: Symbol name for asset 2

    Returns:
        DataFrame with correlation metrics, or empty DataFrame if no overlap.
    """
    common = df1_ohlc.index.intersection(df2_ohlc.index)
    if len(common) == 0:
        return pd.DataFrame()

    c1 = df1_ohlc.loc[common, "close"].values.astype(np.float64)
    c2 = df2_ohlc.loc[common, "close"].values.astype(np.float64)

    n = len(common)

    # Percent changes
    pct1 = np.zeros(n)
    pct2 = np.zeros(n)
    pct1[1:] = (c1[1:] - c1[:-1]) / c1[:-1] * 100
    pct2[1:] = (c2[1:] - c2[:-1]) / c2[:-1] * 100

    # Cumulative index (base 1000)
    idx1 = 1000 * np.cumprod(1 + pct1 / 100)
    idx2 = 1000 * np.cumprod(1 + pct2 / 100)

    # Spread
    spread = idx1 - idx2

    # Position
    positions = np.where(
        spread >= 0,
        f"LONG {sym1} / SHORT {sym2}",
        f"SHORT {sym1} / LONG {sym2}",
    )

    # Flips
    flips = np.zeros(n, dtype=bool)
    flips[1:] = positions[1:] != positions[:-1]

    # Flip loss
    flip_loss = np.zeros(n, dtype=np.float64)
    flip_mask = np.where(flips)[0]
    if len(flip_mask) > 0:
        flip_loss[flip_mask] = np.abs(spread[flip_mask] - spread[flip_mask - 1])

    # Build output DataFrame
    out = pd.DataFrame({
        "timestamp": common,
        f"{sym1}_price": np.round(c1, 4),
        f"{sym2}_price": np.round(c2, 4),
        f"{sym1}_pct_change": np.round(pct1, 4),
        f"{sym2}_pct_change": np.round(pct2, 4),
        f"{sym1}_index": np.round(idx1, 4),
        f"{sym2}_index": np.round(idx2, 4),
        "index_spread": np.round(spread, 4),
        "current_position": positions,
        "flip_occurred": flips,
        "flip_loss": np.round(flip_loss, 4),
    })

    return out
