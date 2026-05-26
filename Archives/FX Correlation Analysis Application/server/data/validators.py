"""
Data validators — integrity checks for tick and OHLC data.
"""

import logging
from datetime import timedelta

import pandas as pd

logger = logging.getLogger(__name__)


class ValidationResult:
    """Container for validation outcomes."""

    def __init__(self):
        self.passed = True
        self.warnings: list[str] = []
        self.errors: list[str] = []

    def warn(self, msg: str):
        self.warnings.append(msg)
        logger.warning(f"Validation warning: {msg}")

    def fail(self, msg: str):
        self.passed = False
        self.errors.append(msg)
        logger.error(f"Validation error: {msg}")

    def to_dict(self) -> dict:
        return {
            "passed": self.passed,
            "warnings": self.warnings,
            "errors": self.errors,
        }


def validate_tick_data(
    df: pd.DataFrame,
    symbol: str,
    expected_start: pd.Timestamp = None,
    expected_end: pd.Timestamp = None,
    min_tick_density: float = 0.1,  # minimum ticks per second expected
) -> ValidationResult:
    """
    Validate tick data integrity.

    Checks:
    - Non-empty
    - Monotonically increasing timestamps
    - No extreme price outliers (>50% jump in single tick)
    - Reasonable tick density
    - Coverage of expected time range
    """
    result = ValidationResult()

    if df.empty:
        result.fail(f"{symbol}: empty DataFrame")
        return result

    # Check required columns
    required = {"timestamp", "bid", "ask", "mid"}
    if not required.issubset(df.columns):
        result.fail(f"{symbol}: missing columns {required - set(df.columns)}")
        return result

    # Check monotonic timestamps
    if not df["timestamp"].is_monotonic_increasing:
        result.warn(f"{symbol}: timestamps not strictly monotonic (has duplicates or out-of-order)")

    # Check for extreme price jumps (> 50% in single tick)
    mid = df["mid"].values
    if len(mid) > 1:
        pct_change = abs((mid[1:] - mid[:-1]) / mid[:-1])
        extreme_jumps = (pct_change > 0.5).sum()
        if extreme_jumps > 0:
            result.warn(f"{symbol}: {extreme_jumps} extreme price jumps (>50% in single tick)")

    # Check bid/ask spread sanity (ask should be >= bid)
    bad_spread = (df["ask"] < df["bid"]).sum()
    if bad_spread > 0:
        result.warn(f"{symbol}: {bad_spread} ticks where ask < bid (crossed spread)")

    # Check tick density
    if expected_start and expected_end:
        total_seconds = (expected_end - expected_start).total_seconds()
        if total_seconds > 0:
            density = len(df) / total_seconds
            if density < min_tick_density:
                result.warn(
                    f"{symbol}: low tick density {density:.3f} ticks/sec "
                    f"(expected >= {min_tick_density})"
                )

    # Check time range coverage
    actual_start = df["timestamp"].iloc[0]
    actual_end = df["timestamp"].iloc[-1]

    if expected_start:
        gap = (actual_start - expected_start).total_seconds()
        if gap > 3600:  # > 1 hour gap at start
            result.warn(f"{symbol}: data starts {gap/3600:.1f}h after expected start")

    if expected_end:
        gap = (expected_end - actual_end).total_seconds()
        if gap > 3600:  # > 1 hour gap at end
            result.warn(f"{symbol}: data ends {gap/3600:.1f}h before expected end")

    # Detect large gaps within the data (> 30 min gap for forex, excluding weekends)
    if len(df) > 1:
        time_diffs = df["timestamp"].diff().dropna()
        large_gaps = time_diffs[time_diffs > timedelta(minutes=30)]

        # Filter out weekend gaps (Friday 22:00 UTC → Sunday 22:00 UTC is normal)
        non_weekend_gaps = []
        for idx, gap in large_gaps.items():
            gap_start = df["timestamp"].iloc[df.index.get_loc(idx) - 1]
            # Skip if Friday evening → Sunday evening
            if gap_start.weekday() == 4 and gap.total_seconds() < 54 * 3600:
                continue
            non_weekend_gaps.append((str(gap_start), str(gap)))

        if non_weekend_gaps and len(non_weekend_gaps) <= 5:
            result.warn(f"{symbol}: {len(non_weekend_gaps)} large gaps detected (>30min)")
        elif len(non_weekend_gaps) > 5:
            result.warn(f"{symbol}: {len(non_weekend_gaps)} large gaps detected (showing first 5)")

    return result


def validate_ohlc_data(
    df: pd.DataFrame,
    symbol: str,
) -> ValidationResult:
    """Validate OHLC bar data integrity."""
    result = ValidationResult()

    if df.empty:
        result.fail(f"{symbol}: empty OHLC DataFrame")
        return result

    required = {"timestamp", "open", "high", "low", "close"}
    if not required.issubset(df.columns):
        result.fail(f"{symbol}: missing columns {required - set(df.columns)}")
        return result

    # Check high >= low
    bad_hl = (df["high"] < df["low"]).sum()
    if bad_hl > 0:
        result.warn(f"{symbol}: {bad_hl} bars where high < low")

    # Check high >= open, close and low <= open, close
    bad_ho = (df["high"] < df["open"]).sum() + (df["high"] < df["close"]).sum()
    bad_lo = (df["low"] > df["open"]).sum() + (df["low"] > df["close"]).sum()
    if bad_ho > 0:
        result.warn(f"{symbol}: {bad_ho} bars where high < open or close")
    if bad_lo > 0:
        result.warn(f"{symbol}: {bad_lo} bars where low > open or close")

    return result
