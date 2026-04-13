"""
Absolute Flip Engine — Distance-Based Flip Detection at Tick Level.

Unlike time-based flips (confirmed at bar close), a flip here is only
confirmed when the spread travels >= N index points past zero in the
new direction after a zero-crossing.

This eliminates phantom flips caused by spread oscillating near zero —
only genuine directional moves of size >= N are counted.

Flow:
  raw ticks → unified tick timeline → cumulative index (base 1000)
  → state machine (zero-cross detection + distance confirmation)
  → resample for display at chosen timeframe

Loss = |spread at confirmation tick|  (Option A — distance IS the minimum loss).
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ── Step 1: Build unified tick-level index ───────────────────────

def build_tick_index(
    df1_ticks: pd.DataFrame,
    df2_ticks: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge two tick series on the union of their timestamps via forward-fill,
    then compute cumulative % change index (base 1000) for both and their spread.

    The outer join ensures every tick from either symbol is represented.
    Forward-fill assigns the last known price to whichever symbol didn't
    tick at a given moment — correct behaviour for a continuous mid price.

    Args:
        df1_ticks: DataFrame with [timestamp, mid] for asset 1
        df2_ticks: DataFrame with [timestamp, mid] for asset 2

    Returns:
        DataFrame with columns [timestamp, mid1, mid2, idx1, idx2, spread]
        Index is integer (not timestamp).
    """
    s1 = df1_ticks.set_index("timestamp")["mid"].rename("mid1")
    s2 = df2_ticks.set_index("timestamp")["mid"].rename("mid2")

    # Outer join on the union of timestamps, then forward-fill gaps
    merged = pd.concat([s1, s2], axis=1).sort_index()
    merged = merged.ffill().dropna()          # drop leading rows where either symbol has no prior tick

    if merged.empty:
        return pd.DataFrame(columns=["timestamp", "mid1", "mid2", "idx1", "idx2", "spread"])

    # Cumulative index — each starts at 1000 regardless of raw price level
    pct1 = merged["mid1"].pct_change().fillna(0.0)
    pct2 = merged["mid2"].pct_change().fillna(0.0)

    idx1 = 1000.0 * (1.0 + pct1).cumprod()
    idx2 = 1000.0 * (1.0 + pct2).cumprod()
    spread = idx1 - idx2

    return pd.DataFrame({
        "timestamp": merged.index,
        "mid1":   merged["mid1"].values,
        "mid2":   merged["mid2"].values,
        "idx1":   idx1.values,
        "idx2":   idx2.values,
        "spread": spread.values,
    }).reset_index(drop=True)


# ── Step 2: State machine — distance-confirmed flip detection ────

def detect_distance_flips(
    tick_df:    pd.DataFrame,
    sym1:       str,
    sym2:       str,
    distance_n: float,
) -> tuple[pd.DataFrame, list[dict]]:
    """
    Walk tick-level spread and detect distance-confirmed flips.

    State machine:
      STABLE
        → on zero-crossing: enter PENDING (record pending new position)
      PENDING
        → if |spread| >= distance_n in new direction: CONFIRMED FLIP → STABLE
        → if spread re-crosses back before reaching N: cancel, back to STABLE
           (we were wrong about the direction; just wait for next real crossing)

    Loss (Option A):
        flip_loss = |spread at confirmation tick|
        Because we only confirm after the spread has already travelled N
        units past zero, N is the *minimum* loss — the actual confirmation
        tick captures the exact spread at the moment of confirmation.

    Args:
        tick_df:    output of build_tick_index()
        sym1/sym2:  symbol names for position labels
        distance_n: minimum index-point distance past zero to confirm a flip (float)

    Returns:
        tick_df augmented with:
            confirmed_flip  (bool)  — True at the exact tick of confirmation
            flip_loss       (float) — |spread| at confirmation, 0 otherwise
            current_position (str)  — label at each tick
        confirmed_flips: list of dicts for each confirmed event
    """
    POS_LONG1  = f"LONG {sym1} / SHORT {sym2}"
    POS_SHORT1 = f"SHORT {sym1} / LONG {sym2}"

    n = len(tick_df)
    if n == 0:
        out = tick_df.copy()
        out["confirmed_flip"]  = False
        out["flip_loss"]       = 0.0
        out["current_position"] = ""
        return out, []

    spread_arr    = tick_df["spread"].values.astype(np.float64)
    flip_arr      = np.zeros(n, dtype=bool)
    loss_arr      = np.zeros(n, dtype=np.float64)
    position_arr  = np.empty(n, dtype=object)

    # Initial position based on first tick
    cur_pos     = POS_LONG1 if spread_arr[0] >= 0 else POS_SHORT1
    position_arr[0] = cur_pos

    pending        = False   # are we inside a potential flip?
    pending_to_long = False  # True → waiting to confirm LONG1; False → waiting SHORT1

    confirmed_flips: list[dict] = []
    timestamps = tick_df["timestamp"].values

    for i in range(1, n):
        s = float(spread_arr[i])

        if not pending:
            # Check for a zero-crossing
            implied = POS_LONG1 if s >= 0 else POS_SHORT1
            if implied != cur_pos:
                # Crossing detected — enter PENDING
                pending = True
                pending_to_long = (implied == POS_LONG1)
        else:
            # Inside a potential flip — wait for distance confirmation or re-cross
            if pending_to_long:
                # Waiting to confirm LONG1 → need spread >= +distance_n
                if s >= distance_n:
                    # ── CONFIRMED FLIP TO LONG1 ──
                    flip_arr[i]  = True
                    loss_arr[i]  = abs(s)
                    cur_pos      = POS_LONG1
                    pending      = False
                    confirmed_flips.append({
                        "timestamp":               str(timestamps[i]),
                        "spread_at_confirmation":  round(s, 4),
                        "flip_loss":               round(abs(s), 4),
                        "new_position":            cur_pos,
                    })
                elif s < 0:
                    # Re-crossed back to negative before reaching +N → cancel
                    # (Spread is now in SHORT1 territory = current position territory
                    #  since cur_pos is still SHORT1 — no new crossing to detect here)
                    pending = False
                # else: 0 <= s < distance_n → still in transition zone, keep waiting

            else:
                # Waiting to confirm SHORT1 → need spread <= -distance_n
                if s <= -distance_n:
                    # ── CONFIRMED FLIP TO SHORT1 ──
                    flip_arr[i]  = True
                    loss_arr[i]  = abs(s)
                    cur_pos      = POS_SHORT1
                    pending      = False
                    confirmed_flips.append({
                        "timestamp":               str(timestamps[i]),
                        "spread_at_confirmation":  round(s, 4),
                        "flip_loss":               round(abs(s), 4),
                        "new_position":            cur_pos,
                    })
                elif s >= 0:
                    # Re-crossed back to positive before reaching -N → cancel
                    pending = False
                # else: -distance_n < s < 0 → still waiting

        position_arr[i] = cur_pos

    out = tick_df.copy()
    out["confirmed_flip"]   = flip_arr
    out["flip_loss"]        = loss_arr
    out["current_position"] = position_arr

    return out, confirmed_flips


# ── Step 3: Resample tick-level output for display ───────────────

def resample_for_display(
    tick_df:  pd.DataFrame,
    sym1:     str,
    sym2:     str,
    timeframe: str,
) -> list[dict]:
    """
    Resample the tick-augmented DataFrame to the chosen timeframe.

    For each bar the output captures the state *at bar close*:
        - price:    last mid tick in the bar
        - index:    cumulative index value at bar close tick
        - spread:   spread at bar close tick
        - position: position label at bar close
        - flip_occurred: True if >= 1 confirmed flip happened inside this bar
        - flip_loss: total confirmed flip loss inside this bar

    X-axis is bar timestamps (clean, readable regardless of tick density).
    Flip markers on the chart will appear on the bar in which the
    confirmation tick fell.
    """
    if tick_df.empty:
        return []

    work = tick_df.set_index("timestamp").sort_index()

    # ── Bar-close snapshots (last tick value per bar) ──
    idx1_close     = work["idx1"].resample(timeframe).last()
    idx2_close     = work["idx2"].resample(timeframe).last()
    spread_close   = work["spread"].resample(timeframe).last()
    position_close = work["current_position"].resample(timeframe).last()
    mid1_close     = work["mid1"].resample(timeframe).last()
    mid2_close     = work["mid2"].resample(timeframe).last()

    # ── Flip aggregation per bar ──
    # Sum of all confirmed flip losses within this bar's time range
    flip_loss_bar  = work["flip_loss"].resample(timeframe).sum()
    flip_count_bar = work["confirmed_flip"].resample(timeframe).sum()

    common = idx1_close.dropna().index.intersection(idx2_close.dropna().index)

    POS_LONG1  = f"LONG {sym1} / SHORT {sym2}"

    rows = []
    for ts in common:
        s    = float(spread_close.get(ts, 0.0) or 0.0)
        pos  = position_close.get(ts) or (POS_LONG1 if s >= 0 else f"SHORT {sym1} / LONG {sym2}")
        loss = float(flip_loss_bar.get(ts, 0.0) or 0.0)
        flips_in_bar = int(flip_count_bar.get(ts, 0) or 0)

        rows.append({
            "timestamp":             ts.strftime("%Y-%m-%d %H:%M:%S"),
            f"{sym1}_price":         round(float(mid1_close.get(ts, 0.0) or 0.0), 5),
            f"{sym2}_price":         round(float(mid2_close.get(ts, 0.0) or 0.0), 5),
            f"{sym1}_index":         round(float(idx1_close.get(ts, 1000.0) or 1000.0), 4),
            f"{sym2}_index":         round(float(idx2_close.get(ts, 1000.0) or 1000.0), 4),
            "index_spread":          round(s, 4),
            "current_position":      str(pos),
            "flip_occurred":         flips_in_bar > 0,
            "flip_loss":             round(loss, 4),
        })

    return rows


# ── Step 4: Metrics ──────────────────────────────────────────────

def compute_absolute_flip_metrics(
    confirmed_flips: list[dict],
    display_data:    list[dict],
    distance_n:      float,
) -> dict:
    """
    Summary metrics matching the existing analysis format.
    Adds a 'Distance N' row to make the threshold visible in the UI.
    """
    total_bars  = len(display_data)
    total_flips = len(confirmed_flips)
    spreads     = [abs(d["index_spread"]) for d in display_data]

    max_spread = max(spreads, default=0.0)
    avg_spread = (sum(spreads) / len(spreads)) if spreads else 0.0

    if not confirmed_flips:
        return {
            "Total Bars":                  f"{total_bars:,}",
            "Distance N (index pts)":      f"{distance_n:.4f}",
            "Total Flips (Confirmed)":     "0",
            "Total Flip Loss":             "0.0000",
            "Max |Spread|":                f"{max_spread:.4f}",
            "Avg |Spread|":                f"{avg_spread:.4f}",
            "Max Single Flip Loss":        "0.0000",
        }

    flip_losses = [f["flip_loss"] for f in confirmed_flips]

    return {
        "Total Bars":              f"{total_bars:,}",
        "Distance N (index pts)": f"{distance_n:.4f}",
        "Total Flips (Confirmed)": f"{total_flips:,}",
        "Total Flip Loss":         f"{sum(flip_losses):.4f}",
        "Max |Spread|":            f"{max_spread:.4f}",
        "Avg |Spread|":            f"{avg_spread:.4f}",
        "Max Single Flip Loss":    f"{max(flip_losses):.4f}",
    }


# ── Public Entry Point ───────────────────────────────────────────

def run_absolute_flip_analysis(
    domain:     str,
    symbol_1:   str,
    symbol_2:   str,
    timeframe:  str,
    start:      datetime,
    end:        datetime,
    distance_n: float,
) -> dict:
    """
    Full Absolute Flip pipeline.

    1. Fetch raw ticks for both symbols (uses existing cache/fetch infra)
    2. Build unified tick-level cumulative index
    3. Run distance-based flip state machine
    4. Resample to chosen timeframe for display
    5. Compute metrics

    Returns:
        status, total_ticks, total_bars, distance_n, metrics, data, confirmed_flips
    """
    # Import here to avoid circular imports
    from ..engine.pipeline import fetch_and_cache

    # Ensure UTC
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    logger.info(f"Absolute Flip: {symbol_1}/{symbol_2}, N={distance_n}, tf={timeframe}")

    # ── 1. Fetch ticks ──
    df1_ticks = fetch_and_cache(domain, symbol_1, start, end)
    df2_ticks = fetch_and_cache(domain, symbol_2, start, end)

    if df1_ticks.empty or df2_ticks.empty:
        return {
            "status":  "error",
            "message": "No tick data available for one or both symbols",
        }

    logger.info(f"  Ticks: {len(df1_ticks)} ({symbol_1}), {len(df2_ticks)} ({symbol_2})")

    # ── 2. Build tick-level index ──
    tick_df = build_tick_index(df1_ticks, df2_ticks)

    if tick_df.empty:
        return {
            "status":  "error",
            "message": "No overlapping tick data — the two symbols have no common time range.",
        }

    logger.info(f"  Merged tick timeline: {len(tick_df)} rows")

    # ── 3. Distance-confirmed flip detection ──
    tick_df, confirmed_flips = detect_distance_flips(tick_df, symbol_1, symbol_2, distance_n)

    logger.info(f"  Confirmed flips (N={distance_n}): {len(confirmed_flips)}")

    # ── 4. Resample for display ──
    display_data = resample_for_display(tick_df, symbol_1, symbol_2, timeframe)

    if not display_data:
        return {
            "status":  "error",
            "message": "No display bars generated. Try a smaller timeframe or wider date range.",
        }

    # ── 5. Metrics ──
    metrics = compute_absolute_flip_metrics(confirmed_flips, display_data, distance_n)

    return {
        "status":           "success",
        "total_ticks":      len(tick_df),
        "total_bars":       len(display_data),
        "distance_n":       distance_n,
        "metrics":          metrics,
        "data":             display_data,
        "confirmed_flips":  confirmed_flips,
    }
