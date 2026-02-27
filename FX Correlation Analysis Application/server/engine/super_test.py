"""
Super Test Engine — Comparative Interval Analysis.

Divides a time range into discrete equal intervals and runs
the correlation analysis independently for each interval.
Results are aggregated into a comparative ranking table.

Design:
- Uses ProcessPoolExecutor for true CPU parallelism (bypasses GIL).
- Each interval gets its own data slice — no shared mutable state.
- Interval boundaries are half-open [start, end) to prevent overlap.
- Index base resets to 1000 per interval — no look-ahead bias.
- Progress is tracked per-interval and can be streamed via WebSocket.
"""

import logging
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pandas as pd

from .resampler import resample_ticks_to_ohlc
from .correlation import compute_correlation
from .metrics import compute_raw_metrics
from ..config import AppConfig

logger = logging.getLogger(__name__)


# ── Interval Slicer ──────────────────────────────────────────────

def generate_intervals(
    date: str,
    start_time: str,
    end_time: str,
    interval_minutes: int,
) -> list[tuple[datetime, datetime]]:
    """
    Generate discrete time intervals for Super Test.

    Args:
        date: "YYYY-MM-DD"
        start_time: "HH:MM" (UTC)
        end_time: "HH:MM" (UTC)
        interval_minutes: width of each interval in minutes

    Returns:
        List of (start, end) UTC datetime tuples, half-open intervals [start, end)
    """
    start = datetime.strptime(f"{date} {start_time}", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    end = datetime.strptime(f"{date} {end_time}", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)

    # Handle overnight ranges (e.g., 22:00 → 06:00)
    if end <= start:
        end += timedelta(days=1)

    delta = timedelta(minutes=interval_minutes)
    intervals = []
    current = start

    while current + delta <= end:
        intervals.append((current, current + delta))
        current += delta

    return intervals


# ── Single Interval Worker ────────────────────────────────────────

def _run_single_interval(args: tuple) -> dict:
    """
    Process a single interval — designed to run in a worker process.

    Args (as tuple for ProcessPool compatibility):
        (interval_start, interval_end, df1_slice_values, df2_slice_values,
         df1_slice_timestamps, df2_slice_timestamps, timeframe, sym1, sym2)

    Returns:
        dict with interval metrics or error info.
    """
    (
        interval_start, interval_end,
        df1_timestamps, df1_mid,
        df2_timestamps, df2_mid,
        timeframe, sym1, sym2,
    ) = args

    try:
        # Reconstruct lightweight DataFrames from numpy arrays
        df1 = pd.DataFrame({
            "timestamp": pd.to_datetime(df1_timestamps, utc=True),
            "mid": df1_mid.astype(np.float64),
        })
        df2 = pd.DataFrame({
            "timestamp": pd.to_datetime(df2_timestamps, utc=True),
            "mid": df2_mid.astype(np.float64),
        })

        if df1.empty or df2.empty:
            return {
                "interval_start": interval_start.isoformat(),
                "interval_end": interval_end.isoformat(),
                "status": "no_data",
                "total_bars": 0,
                "total_flips": 0,
                "total_flip_loss": 0.0,
                "max_spread": 0.0,
                "avg_spread": 0.0,
                "max_single_flip_loss": 0.0,
            }

        # Resample
        ohlc1 = resample_ticks_to_ohlc(df1, timeframe)
        ohlc2 = resample_ticks_to_ohlc(df2, timeframe)

        if ohlc1.empty or ohlc2.empty:
            return {
                "interval_start": interval_start.isoformat(),
                "interval_end": interval_end.isoformat(),
                "status": "insufficient_bars",
                "total_bars": 0,
                "total_flips": 0,
                "total_flip_loss": 0.0,
                "max_spread": 0.0,
                "avg_spread": 0.0,
                "max_single_flip_loss": 0.0,
            }

        # Correlate
        result = compute_correlation(ohlc1, ohlc2, sym1, sym2)

        if result.empty:
            return {
                "interval_start": interval_start.isoformat(),
                "interval_end": interval_end.isoformat(),
                "status": "no_overlap",
                "total_bars": 0,
                "total_flips": 0,
                "total_flip_loss": 0.0,
                "max_spread": 0.0,
                "avg_spread": 0.0,
                "max_single_flip_loss": 0.0,
            }

        # Metrics
        metrics = compute_raw_metrics(result)

        return {
            "interval_start": interval_start.isoformat(),
            "interval_end": interval_end.isoformat(),
            "status": "success",
            **metrics,
        }

    except Exception as e:
        return {
            "interval_start": interval_start.isoformat(),
            "interval_end": interval_end.isoformat(),
            "status": "error",
            "error": str(e),
            "total_bars": 0,
            "total_flips": 0,
            "total_flip_loss": 0.0,
            "max_spread": 0.0,
            "avg_spread": 0.0,
            "max_single_flip_loss": 0.0,
        }


# ── Super Test Executor ──────────────────────────────────────────

def run_super_test(
    df1_ticks: pd.DataFrame,
    df2_ticks: pd.DataFrame,
    sym1: str,
    sym2: str,
    timeframe: str,
    date: str,
    start_time: str,
    end_time: str,
    interval_minutes: int,
    on_interval_complete: Optional[callable] = None,
) -> dict:
    """
    Run the Super Test: divide time range into intervals, analyze each, rank results.

    Args:
        df1_ticks: Full tick DataFrame for asset 1 [timestamp, bid, ask, mid]
        df2_ticks: Full tick DataFrame for asset 2
        sym1, sym2: Symbol names
        timeframe: Resample rule (e.g. '10s', '1min')
        date: "YYYY-MM-DD"
        start_time: "HH:MM" UTC
        end_time: "HH:MM" UTC
        interval_minutes: Minutes per interval
        on_interval_complete: callback(completed, total, interval_result)

    Returns:
        dict with intervals list and rankings
    """
    t0 = time.time()

    intervals = generate_intervals(date, start_time, end_time, interval_minutes)
    total = len(intervals)

    if total == 0:
        return {
            "status": "error",
            "message": "No intervals generated. Check time range and interval size.",
            "total_intervals": 0,
            "completed_intervals": 0,
            "intervals": [],
            "rankings": [],
        }

    logger.info(f"Super Test: {total} intervals of {interval_minutes}min each")

    # Ensure timestamp is the correct type for slicing
    if "timestamp" in df1_ticks.columns:
        df1 = df1_ticks.set_index("timestamp").sort_index()
    else:
        df1 = df1_ticks.sort_index()

    if "timestamp" in df2_ticks.columns:
        df2 = df2_ticks.set_index("timestamp").sort_index()
    else:
        df2 = df2_ticks.sort_index()

    # Pre-slice data for each interval and serialize as numpy arrays
    tasks = []
    for iv_start, iv_end in intervals:
        s1 = df1.loc[iv_start:iv_end - timedelta(microseconds=1)]  # half-open [start, end)
        s2 = df2.loc[iv_start:iv_end - timedelta(microseconds=1)]

        tasks.append((
            iv_start, iv_end,
            s1.index.values if not s1.empty else np.array([], dtype="datetime64[ns]"),
            s1["mid"].values if not s1.empty else np.array([], dtype=np.float64),
            s2.index.values if not s2.empty else np.array([], dtype="datetime64[ns]"),
            s2["mid"].values if not s2.empty else np.array([], dtype=np.float64),
            timeframe, sym1, sym2,
        ))

    # Execute in parallel
    results = []
    max_workers = min(AppConfig.super_test_max_workers, total)

    # For small counts, run serially to avoid process spawn overhead
    if total <= 4:
        for i, task in enumerate(tasks):
            result = _run_single_interval(task)
            results.append(result)
            if on_interval_complete:
                on_interval_complete(i + 1, total, result)
    else:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_idx = {
                executor.submit(_run_single_interval, task): i
                for i, task in enumerate(tasks)
            }

            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    result = future.result()
                except Exception as e:
                    result = {
                        "interval_start": intervals[idx][0].isoformat(),
                        "interval_end": intervals[idx][1].isoformat(),
                        "status": "error",
                        "error": str(e),
                        "total_bars": 0,
                        "total_flips": 0,
                        "total_flip_loss": 0.0,
                        "max_spread": 0.0,
                        "avg_spread": 0.0,
                        "max_single_flip_loss": 0.0,
                    }
                results.append((idx, result))
                if on_interval_complete:
                    on_interval_complete(len(results), total, result)

        # Sort by original interval order
        results.sort(key=lambda x: x[0])
        results = [r for _, r in results]

    # Generate rankings
    rankings = _compute_rankings(results)

    elapsed = time.time() - t0
    logger.info(f"Super Test complete: {total} intervals in {elapsed:.1f}s")

    return {
        "status": "success",
        "total_intervals": total,
        "completed_intervals": len([r for r in results if r.get("status") == "success"]),
        "elapsed_seconds": round(elapsed, 2),
        "intervals": results,
        "rankings": rankings,
    }


def _compute_rankings(results: list[dict]) -> list[dict]:
    """
    Rank intervals by composite score.

    Scoring:
    - Lower total_flip_loss is better (stability)
    - Lower total_flips is better (fewer reversals)
    - Higher total_bars is better (more data)

    Returns list of interval results with added 'rank' and 'score' fields.
    """
    # Filter to successful intervals only
    scored = []
    for r in results:
        if r.get("status") != "success" or r.get("total_bars", 0) == 0:
            continue

        # Composite score: lower is better
        # Normalize by bars to make intervals comparable
        bars = max(r["total_bars"], 1)
        flip_rate = r["total_flips"] / bars
        loss_per_bar = r["total_flip_loss"] / bars

        score = round(flip_rate * 50 + loss_per_bar * 50, 6)

        scored.append({
            **r,
            "score": score,
            "flip_rate": round(flip_rate, 6),
            "loss_per_bar": round(loss_per_bar, 6),
        })

    # Sort by score ascending (lower = more stable)
    scored.sort(key=lambda x: x["score"])

    # Assign ranks
    for i, item in enumerate(scored):
        item["rank"] = i + 1

    return scored
