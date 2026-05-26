"""
Super Test Engine — Rolling-Start Interval Analysis.

Each "window" shares the same fixed end time but has a progressively
later start time, shifted by interval_minutes each step.

Example: start=00:00, end=08:00, interval=5min generates:
  Window 1:  00:00 → 08:00  (full 8 hours)
  Window 2:  00:05 → 08:00
  Window 3:  00:10 → 08:00
  ...
  Window 96: 07:55 → 08:00  (5 minutes only)

This reveals which START TIME produces the most stable correlation
over the rest of the session — a "best entry time" ranking.

Design:
- Rolling start windows, fixed end time.
- Each window independently resets index to 1000 (no look-ahead bias).
- ProcessPoolExecutor for CPU parallelism.
- Progress streamed via WebSocket callback.
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


# ── Interval Generator ───────────────────────────────────────────

def generate_intervals(
    date: str,
    start_time: str,
    end_time: str,
    interval_minutes: int,
) -> list[tuple[datetime, datetime]]:
    """
    Generate rolling-start windows for Super Test.

    Each window starts interval_minutes later than the previous,
    but ALL windows share the same fixed end time.

    Args:
        date: "YYYY-MM-DD"
        start_time: "HH:MM" (UTC) — earliest possible start
        end_time: "HH:MM" (UTC) — fixed end for every window
        interval_minutes: how many minutes to shift the start each step

    Returns:
        List of (window_start, fixed_end) UTC datetime tuples.
        The last window has at least interval_minutes of data.
    """
    fixed_start = datetime.strptime(f"{date} {start_time}", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    fixed_end   = datetime.strptime(f"{date} {end_time}",   "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)

    # Handle overnight ranges (e.g. 22:00 → 06:00)
    if fixed_end <= fixed_start:
        fixed_end += timedelta(days=1)

    delta = timedelta(minutes=interval_minutes)
    intervals = []
    current_start = fixed_start

    # Generate until we'd have less than one interval of data left
    while current_start + delta <= fixed_end:
        intervals.append((current_start, fixed_end))
        current_start += delta

    return intervals


# ── Single Window Worker ─────────────────────────────────────────

def _run_single_interval(args: tuple) -> dict:
    """
    Process a single rolling window — runs in a worker process.

    Args (tuple):
        (window_start, fixed_end, df1_timestamps, df1_mid,
         df2_timestamps, df2_mid, timeframe, sym1, sym2, window_index)

    Returns:
        dict with window metrics or error info.
    """
    (
        window_start, fixed_end,
        df1_timestamps, df1_mid,
        df2_timestamps, df2_mid,
        timeframe, sym1, sym2,
        window_index,
    ) = args

    def _empty(status, error=None):
        r = {
            "window_index":    window_index,
            "interval_start":  window_start.isoformat(),
            "interval_end":    fixed_end.isoformat(),
            "window_minutes":  int((fixed_end - window_start).total_seconds() / 60),
            "status":          status,
            "total_bars":      0,
            "total_flips":     0,
            "total_flip_loss": 0.0,
            "max_spread":      0.0,
            "avg_spread":      0.0,
            "max_single_flip_loss": 0.0,
        }
        if error:
            r["error"] = error
        return r

    try:
        df1 = pd.DataFrame({
            "timestamp": pd.to_datetime(df1_timestamps, utc=True),
            "mid":       df1_mid.astype(np.float64),
        })
        df2 = pd.DataFrame({
            "timestamp": pd.to_datetime(df2_timestamps, utc=True),
            "mid":       df2_mid.astype(np.float64),
        })

        if df1.empty or df2.empty:
            return _empty("no_data")

        ohlc1 = resample_ticks_to_ohlc(df1, timeframe)
        ohlc2 = resample_ticks_to_ohlc(df2, timeframe)

        if ohlc1.empty or ohlc2.empty:
            return _empty("insufficient_bars")

        result = compute_correlation(ohlc1, ohlc2, sym1, sym2)

        if result.empty:
            return _empty("no_overlap")

        metrics = compute_raw_metrics(result)

        return {
            "window_index":    window_index,
            "interval_start":  window_start.isoformat(),
            "interval_end":    fixed_end.isoformat(),
            "window_minutes":  int((fixed_end - window_start).total_seconds() / 60),
            "status":          "success",
            **metrics,
        }

    except Exception as e:
        return _empty("error", str(e))


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
    Run the Super Test: rolling-start windows, all sharing the same end time.

    Returns:
        dict with:
          - intervals: all window results (ordered by start time)
          - rankings:  sorted by composite stability score (best first)
          - summary:   aggregate stats (best_start, worst_start, etc.)
    """
    t0 = time.time()

    intervals = generate_intervals(date, start_time, end_time, interval_minutes)
    total = len(intervals)

    if total == 0:
        return {
            "status": "error",
            "message": "No windows generated. Check time range and interval size.",
            "total_intervals": 0,
            "completed_intervals": 0,
            "intervals": [],
            "rankings": [],
            "summary": {},
        }

    logger.info(f"Super Test: {total} rolling windows of {interval_minutes}min step, ending {end_time}")

    # Index tick data for fast slicing
    def _prep(df):
        if "timestamp" in df.columns:
            return df.set_index("timestamp").sort_index()
        return df.sort_index()

    df1 = _prep(df1_ticks)
    df2 = _prep(df2_ticks)

    # Build task list — slice data per window
    tasks = []
    for idx, (win_start, win_end) in enumerate(intervals):
        s1 = df1.loc[win_start:win_end]
        s2 = df2.loc[win_start:win_end]

        tasks.append((
            win_start, win_end,
            s1.index.values if not s1.empty else np.array([], dtype="datetime64[ns]"),
            s1["mid"].values if not s1.empty else np.array([], dtype=np.float64),
            s2.index.values if not s2.empty else np.array([], dtype="datetime64[ns]"),
            s2["mid"].values if not s2.empty else np.array([], dtype=np.float64),
            timeframe, sym1, sym2,
            idx,  # window_index for ordering
        ))

    # Execute
    results_map: dict[int, dict] = {}
    max_workers = min(AppConfig.super_test_max_workers, total)

    if total <= 4:
        for task in tasks:
            r = _run_single_interval(task)
            results_map[r["window_index"]] = r
            if on_interval_complete:
                on_interval_complete(len(results_map), total, r)
    else:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(_run_single_interval, task): task[-1]
                for task in tasks
            }
            for future in as_completed(future_map):
                try:
                    r = future.result()
                except Exception as e:
                    idx = future_map[future]
                    win_start, win_end = intervals[idx]
                    r = {
                        "window_index":    idx,
                        "interval_start":  win_start.isoformat(),
                        "interval_end":    win_end.isoformat(),
                        "window_minutes":  int((win_end - win_start).total_seconds() / 60),
                        "status":          "error",
                        "error":           str(e),
                        "total_bars":      0,
                        "total_flips":     0,
                        "total_flip_loss": 0.0,
                        "max_spread":      0.0,
                        "avg_spread":      0.0,
                        "max_single_flip_loss": 0.0,
                    }
                results_map[r["window_index"]] = r
                if on_interval_complete:
                    on_interval_complete(len(results_map), total, r)

    # Re-order by original window index (chronological start time)
    results = [results_map[i] for i in range(total)]

    # Compute rankings and summary
    rankings = _compute_rankings(results)
    summary  = _compute_summary(results, rankings, start_time, end_time, interval_minutes)

    elapsed = time.time() - t0
    logger.info(f"Super Test complete: {total} windows in {elapsed:.1f}s")

    return {
        "status":               "success",
        "total_intervals":      total,
        "completed_intervals":  len([r for r in results if r.get("status") == "success"]),
        "elapsed_seconds":      round(elapsed, 2),
        "intervals":            results,
        "rankings":             rankings,
        "summary":              summary,
    }


def _compute_rankings(results: list[dict]) -> list[dict]:
    """
    Rank windows by composite stability score.

    Scoring (lower = more stable = better):
      - flip_rate     = total_flips / total_bars       (weight: 40)
      - loss_per_bar  = total_flip_loss / total_bars   (weight: 40)
      - avg_spread    = avg |spread| per bar            (weight: 20)

    Returns list sorted best → worst, with rank + score fields.
    """
    scored = []
    for r in results:
        if r.get("status") != "success" or r.get("total_bars", 0) == 0:
            continue

        bars       = max(r["total_bars"], 1)
        flip_rate  = r["total_flips"]     / bars
        loss_pb    = r["total_flip_loss"] / bars
        avg_sp     = r.get("avg_spread", 0)

        # Composite (all normalized, lower is better)
        score = round(flip_rate * 40 + loss_pb * 40 + avg_sp * 20, 6)

        # Start-time label for display (e.g. "00:05")
        start_label = r["interval_start"][11:16]  # "HH:MM" from ISO

        scored.append({
            **r,
            "start_time_label":  start_label,
            "score":             score,
            "flip_rate":         round(flip_rate, 6),
            "loss_per_bar":      round(loss_pb,   6),
        })

    scored.sort(key=lambda x: x["score"])

    for i, item in enumerate(scored):
        item["rank"] = i + 1

    return scored


def _compute_summary(
    results: list[dict],
    rankings: list[dict],
    start_time: str,
    end_time: str,
    interval_minutes: int,
) -> dict:
    """Aggregate statistics across all windows for the header summary card."""
    successful = [r for r in results if r.get("status") == "success"]
    if not successful:
        return {}

    flip_losses = [r["total_flip_loss"] for r in successful]
    flip_counts = [r["total_flips"]     for r in successful]
    avg_spreads = [r.get("avg_spread", 0) for r in successful]

    best  = rankings[0]  if rankings else None
    worst = rankings[-1] if rankings else None

    return {
        "total_windows":     len(results),
        "successful":        len(successful),
        "range_start":       start_time,
        "range_end":         end_time,
        "step_minutes":      interval_minutes,
        "avg_flip_loss":     round(float(np.mean(flip_losses)), 4),
        "min_flip_loss":     round(float(np.min(flip_losses)),  4),
        "max_flip_loss":     round(float(np.max(flip_losses)),  4),
        "avg_flips":         round(float(np.mean(flip_counts)), 2),
        "avg_spread":        round(float(np.mean(avg_spreads)), 4),
        "best_start_time":   best["interval_start"][11:16]  if best  else "—",
        "best_score":        best["score"]                   if best  else 0,
        "best_flip_loss":    best["total_flip_loss"]         if best  else 0,
        "best_flips":        best["total_flips"]             if best  else 0,
        "worst_start_time":  worst["interval_start"][11:16] if worst else "—",
        "worst_score":       worst["score"]                  if worst else 0,
    }
