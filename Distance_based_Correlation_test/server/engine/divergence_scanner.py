"""
Divergence Scanner Engine.

Sliding fixed-length window analysis across all pair combinations.
Finds pairs with persistent spread growth and minimal zero-crossings.

Key design:
- Operates on pre-resampled OHLC DataFrames (not raw ticks).
- Each window resets the index to 1000 — no look-ahead bias.
- ProcessPoolExecutor for CPU parallelism (one worker per pair).
"""

import itertools
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import numpy as np
import pandas as pd

from .correlation import compute_correlation
from .metrics import compute_raw_metrics
from ..config import AppConfig

logger = logging.getLogger(__name__)


# ── Pair Generation ──────────────────────────────────────────────

def generate_pair_combinations(symbols: list[str]) -> list[tuple[str, str]]:
    """
    Return all unique pairs from symbol list using itertools.combinations.
    10 symbols → 45 pairs. Order is alphabetical within each pair.
    """
    sorted_syms = sorted(symbols)
    return list(itertools.combinations(sorted_syms, 2))


# ── Slope Computation ────────────────────────────────────────────

def compute_spread_slope(spread: np.ndarray) -> float:
    """
    Linear regression slope of spread array.
    Uses numpy.polyfit(range(n), spread, 1)[0].
    Returns 0.0 if n < 2.
    """
    n = len(spread)
    if n < 2:
        return 0.0
    try:
        slope = float(np.polyfit(range(n), spread, 1)[0])
        # Guard against inf/nan from degenerate data
        if not np.isfinite(slope):
            return 0.0
        return slope
    except Exception:
        return 0.0


def make_baseline_ohlc(reference_ohlc: pd.DataFrame) -> pd.DataFrame:
    """
    Generate a synthetic flat OHLC DataFrame that always stays at price 1.0.
    Uses the same DatetimeIndex as the reference asset.
    compute_correlation() will produce idx2 = 1000 throughout,
    making spread = idx1 - 1000 = pure asset movement.
    """
    flat = pd.DataFrame(
        {
            "open":  1.0,
            "high":  1.0,
            "low":   1.0,
            "close": 1.0,
        },
        index=reference_ohlc.index,
    )
    return flat


def compute_phase_metrics(corr: pd.DataFrame) -> dict:
    """
    Compute Phase 1 / Phase 2 metrics from a single window correlation DataFrame.

    Phase 1 = bars from start until last flip occurs
    Phase 2 = bars from last flip until window end

    Args:
        corr: output of compute_correlation() for one window

    Returns dict with:
        last_flip_bar      - index of last flip (0 if no flips)
        phase1_length      - number of bars in Phase 1 (= last_flip_bar + 1)
        phase2_length      - number of bars in Phase 2
        post_flip_spread_growth - spread[-1] - spread[last_flip_bar]
        has_clean_phase2   - True if phase2_length >= 10 and no flips in Phase 2
    """
    if corr.empty:
        return {
            "last_flip_bar": 0,
            "phase1_length": 0,
            "phase2_length": 0,
            "post_flip_spread_growth": 0.0,
            "has_clean_phase2": False,
        }

    flip_indices = corr.index[corr["flip_occurred"] == True].tolist()
    spread_vals = corr["index_spread"].values.astype(float)
    n = len(spread_vals)

    if len(flip_indices) == 0:
        # No flips at all — entire window is clean Phase 2
        return {
            "last_flip_bar": 0,
            "phase1_length": 0,
            "phase2_length": n,
            "post_flip_spread_growth": float(spread_vals[-1] - spread_vals[0]),
            "has_clean_phase2": True,
        }

    # corr uses integer positional index internally after reset
    # flip_occurred is a boolean column — get positional indices
    flip_positions = [i for i, v in enumerate(corr["flip_occurred"].values) if v]
    last_flip_pos = flip_positions[-1]

    phase1_length = last_flip_pos + 1
    phase2_length = n - phase1_length

    # Spread growth purely in Phase 2 (from last flip bar to end)
    post_flip_growth = float(spread_vals[-1] - spread_vals[last_flip_pos])

    # Clean Phase 2 = at least 10 bars AND no flips after last_flip_pos
    has_clean_phase2 = phase2_length >= 10

    return {
        "last_flip_bar": last_flip_pos,
        "phase1_length": phase1_length,
        "phase2_length": phase2_length,
        "post_flip_spread_growth": round(post_flip_growth, 4),
        "has_clean_phase2": has_clean_phase2,
    }


# ── Single Pair Sliding Window ───────────────────────────────────

def run_sliding_windows(
    ohlc1: pd.DataFrame,
    ohlc2: pd.DataFrame,
    sym1: str,
    sym2: str,
    window_bars: int,
) -> dict:
    """
    Run sliding window analysis for one pair.

    Steps:
    1. Align on common timestamps (same as compute_correlation does).
    2. If total aligned bars < window_bars * 2: return insufficient_data.
    3. Slide window one bar at a time across the full aligned series.
    4. For each window: slice ohlc1/ohlc2, call compute_correlation(),
       compute_raw_metrics(), then compute spread_growth and spread_slope.
    5. Aggregate across all windows into per-pair metrics.
    6. Compute divergence score = avg_spread_slope * (pct_zero_crossing / 100)
       * avg_max_spread.

    Returns dict with all per-pair aggregated metrics, or
    {"status": "insufficient_data", "sym1": ..., "sym2": ...} if not enough bars.
    """
    pair_label = f"{sym1}/{sym2}"

    try:
        # Align on common timestamps
        common = ohlc1.index.intersection(ohlc2.index)
        total_bars = len(common)

        if total_bars < window_bars * 2:
            return {
                "status": "insufficient_data",
                "pair": pair_label,
                "sym1": sym1,
                "sym2": sym2,
                "total_aligned_bars": total_bars,
                "required_bars": window_bars * 2,
            }

        aligned1 = ohlc1.loc[common]
        aligned2 = ohlc2.loc[common]

        num_windows = total_bars - window_bars + 1
        logger.debug(f"{pair_label}: {total_bars} bars → {num_windows} windows of {window_bars}")

        # Per-window accumulation lists
        w_flips = []
        w_flip_loss = []
        w_max_spread = []
        w_avg_spread = []
        w_spread_growth = []
        w_spread_slope = []
        w_scores = []
        windows_data = []

        # NEW accumulation lists for phase metrics
        w_phase1_length = []
        w_phase2_length = []
        w_post_flip_growth = []
        w_clean_phase2 = []
        w_max_single_flip_loss = []

        best_window_score = -float("inf")
        best_window_start_ts = None

        for start_idx in range(num_windows):
            end_idx = start_idx + window_bars
            w1 = aligned1.iloc[start_idx:end_idx]
            w2 = aligned2.iloc[start_idx:end_idx]

            try:
                corr = compute_correlation(w1, w2, sym1, sym2)
                if corr.empty:
                    continue

                m = compute_raw_metrics(corr)

                # Phase metrics
                pm = compute_phase_metrics(corr)
                w_phase1_length.append(pm["phase1_length"])
                w_phase2_length.append(pm["phase2_length"])
                w_post_flip_growth.append(pm["post_flip_spread_growth"])
                w_clean_phase2.append(pm["has_clean_phase2"])
                w_max_single_flip_loss.append(m["max_single_flip_loss"])

                spread_vals = corr["index_spread"].values.astype(np.float64)
                spread_growth = float(spread_vals[-1] - spread_vals[0])
                spread_slope = compute_spread_slope(spread_vals)

                total_flips = m["total_flips"]
                max_spread = m["max_spread"]
                avg_spread = m["avg_spread"]
                total_flip_loss = m["total_flip_loss"]

                # Per-window divergence score
                w_score = spread_slope * max_spread  # simplified per-window score
                w_scores.append(w_score)

                if w_score > best_window_score:
                    best_window_score = w_score
                    # Timestamp of the first bar in this window
                    ts = common[start_idx]
                    best_window_start_ts = str(ts) if ts is not None else None

                w_flips.append(total_flips)
                w_flip_loss.append(total_flip_loss)
                w_max_spread.append(max_spread)
                w_avg_spread.append(avg_spread)
                w_spread_growth.append(spread_growth)
                w_spread_slope.append(spread_slope)

                windows_data.append({
                    "window_index":   start_idx,
                    "window_start":   str(common[start_idx]),
                    "window_end":     str(common[end_idx - 1]),
                    "total_bars":     window_bars,
                    "total_flips":    m["total_flips"],
                    "total_flip_loss": round(m["total_flip_loss"], 4),
                    "max_spread":     round(m["max_spread"], 4),
                    "avg_spread":     round(m["avg_spread"], 4),
                    "max_single_flip_loss": round(m["max_single_flip_loss"], 4),
                    "spread_growth":  round(spread_growth, 4),
                    "spread_slope":   round(spread_slope, 6),
                    "window_score":   round(w_score, 6),
                    "phase1_length":  pm["phase1_length"],
                    "post_flip_growth": round(pm["post_flip_spread_growth"], 4),
                })

            except Exception as e:
                logger.debug(f"{pair_label} window {start_idx} error: {e}")
                continue

        windows_tested = len(w_flips)
        if windows_tested == 0:
            return {
                "status": "no_successful_windows",
                "pair": pair_label,
                "sym1": sym1,
                "sym2": sym2,
            }

        # Aggregate
        windows_zero_crossings = int(sum(1 for f in w_flips if f == 0))
        pct_zero_crossing_windows = windows_zero_crossings / windows_tested * 100

        avg_spread_growth = float(np.mean(w_spread_growth))
        avg_spread_slope  = float(np.mean(w_spread_slope))
        avg_max_spread    = float(np.mean(w_max_spread))
        avg_avg_spread    = float(np.mean(w_avg_spread))
        avg_flips         = float(np.mean(w_flips))
        avg_flip_loss     = float(np.mean(w_flip_loss))
        avg_window_score  = float(np.mean(w_scores)) if w_scores else 0.0

        # ── Phase 1 metrics ──────────────────────────────────────────
        avg_phase1_length    = float(np.mean(w_phase1_length)) if w_phase1_length else 0.0
        avg_phase2_length    = float(np.mean(w_phase2_length)) if w_phase2_length else 0.0
        avg_post_flip_growth = float(np.mean(w_post_flip_growth)) if w_post_flip_growth else 0.0
        pct_clean_phase2     = float(sum(w_clean_phase2) / windows_tested * 100) if windows_tested else 0.0

        # ── Flip distribution buckets ────────────────────────────────
        flip_counts = w_flips
        dist_zero   = int(sum(1 for f in flip_counts if f == 0))
        dist_low    = int(sum(1 for f in flip_counts if 1 <= f <= 3))
        dist_mid    = int(sum(1 for f in flip_counts if 4 <= f <= 7))
        dist_high   = int(sum(1 for f in flip_counts if f >= 8))
        max_flips_any_window = int(max(flip_counts)) if flip_counts else 0

        # Stop-scaling threshold = avg + 1 std (round up)
        flips_std = float(np.std(flip_counts)) if len(flip_counts) > 1 else 0.0
        stop_scaling_threshold = int(np.ceil(avg_flips + flips_std))

        # ── Cost vs Spread ratios ────────────────────────────────────
        # Use per-window values for ratio computation (more accurate than using aggregated)
        # Compute per-window ratios then average
        ratio_max_flip_vs_max_spread_list = []
        ratio_total_flip_vs_max_spread_list = []
        ratio_total_flip_vs_avg_spread_list = []

        for i in range(windows_tested):
            ms  = w_max_spread[i]
            avs = w_avg_spread[i]
            mfl = w_max_single_flip_loss[i]
            tfl = w_flip_loss[i]

            if ms > 0:
                ratio_max_flip_vs_max_spread_list.append(mfl / ms)
                ratio_total_flip_vs_max_spread_list.append(tfl / ms)
            if avs > 0:
                ratio_total_flip_vs_avg_spread_list.append(tfl / avs)

        avg_ratio_maxflip_maxspread   = round(float(np.mean(ratio_max_flip_vs_max_spread_list)), 4)   if ratio_max_flip_vs_max_spread_list   else 0.0
        avg_ratio_totalflip_maxspread = round(float(np.mean(ratio_total_flip_vs_max_spread_list)), 4) if ratio_total_flip_vs_max_spread_list else 0.0
        avg_ratio_totalflip_avgspread = round(float(np.mean(ratio_total_flip_vs_avg_spread_list)), 4) if ratio_total_flip_vs_avg_spread_list else 0.0

        # Viability verdict based on primary ratio (total flip loss / avg spread)
        # < 0.30 = Strong, 0.30-0.60 = Moderate, 0.60-1.0 = Tight, > 1.0 = Not viable
        if avg_ratio_totalflip_avgspread < 0.30:
            viability = "strong"
        elif avg_ratio_totalflip_avgspread < 0.60:
            viability = "moderate"
        elif avg_ratio_totalflip_avgspread < 1.0:
            viability = "tight"
        else:
            viability = "not_viable"

        # Pair-level divergence score (higher = better)
        score = avg_spread_slope * (pct_zero_crossing_windows / 100) * avg_max_spread

        return {
            "status": "success",
            "pair": pair_label,
            "sym1": sym1,
            "sym2": sym2,
            "windows_tested": windows_tested,
            "windows_zero_crossings": windows_zero_crossings,
            "pct_zero_crossing_windows": round(pct_zero_crossing_windows, 2),
            "avg_spread_growth": round(avg_spread_growth, 6),
            "avg_spread_slope": round(avg_spread_slope, 8),
            "avg_max_spread": round(avg_max_spread, 4),
            "avg_avg_spread": round(avg_avg_spread, 4),
            "avg_flips": round(avg_flips, 3),
            "avg_flip_loss": round(avg_flip_loss, 6),
            "best_window_score": round(best_window_score, 6),
            "best_window_start": best_window_start_ts,
            "avg_window_score": round(avg_window_score, 6),
            "score": round(score, 6),

            # Phase 1 / Phase 2
            "avg_phase1_length":    round(avg_phase1_length, 1),
            "avg_phase2_length":    round(avg_phase2_length, 1),
            "avg_post_flip_growth": round(avg_post_flip_growth, 4),
            "pct_clean_phase2":     round(pct_clean_phase2, 2),

            # Flip capacity planning
            "max_flips_any_window":     max_flips_any_window,
            "stop_scaling_threshold":   stop_scaling_threshold,
            "flip_dist_zero":           dist_zero,
            "flip_dist_low":            dist_low,    # 1-3 flips
            "flip_dist_mid":            dist_mid,    # 4-7 flips
            "flip_dist_high":           dist_high,   # 8+ flips

            # Cost vs spread ratios
            "ratio_maxflip_maxspread":   avg_ratio_maxflip_maxspread,
            "ratio_totalflip_maxspread": avg_ratio_totalflip_maxspread,
            "ratio_totalflip_avgspread": avg_ratio_totalflip_avgspread,
            "viability":                 viability,

            "windows_data": windows_data,
        }

    except Exception as e:
        logger.error(f"run_sliding_windows failed for {pair_label}: {e}")
        return {
            "status": "error",
            "pair": pair_label,
            "sym1": sym1,
            "sym2": sym2,
            "error": str(e),
        }


# ── Full Scan Orchestrator ───────────────────────────────────────

def run_divergence_scan(
    df1_map: dict,           # {symbol: ohlc_dataframe}
    pairs: list[tuple],      # [(sym1, sym2), ...]
    window_bars: int,
    on_pair_complete: Optional[callable] = None,
    baseline_key: str = "__BASELINE__",
) -> dict:
    """
    Run full divergence scan across all pairs.

    Args:
        df1_map: pre-fetched and resampled OHLC data per symbol.
        pairs: list of (sym1, sym2) tuples.
        window_bars: fixed window size in bars.
        on_pair_complete: callback(completed_count, total_count, pair_result).

    Returns dict with status, pairs list (ordered), rankings (sorted by score),
    and aggregate summary stats.
    """
    t0 = time.time()
    total_pairs = len(pairs)

    if total_pairs == 0:
        return {
            "status": "error",
            "message": "No pairs to scan.",
            "total_pairs": 0,
            "completed_pairs": 0,
            "pairs": [],
            "rankings": [],
            "summary": {},
        }

    logger.info(f"Divergence Scan: {total_pairs} pairs, window={window_bars} bars")

    results_map: dict[int, dict] = {}
    lock_obj = None  # not needed — ThreadPoolExecutor results handled via futures

    def _run_pair(args):
        idx, sym1, sym2 = args

        # Resolve baseline — generate flat OHLC from the real asset's index
        if sym1 == baseline_key:
            ohlc2 = df1_map.get(sym2)
            if ohlc2 is None or ohlc2.empty:
                return idx, {"status": "no_data", "pair": f"{sym1}/{sym2}",
                             "sym1": sym1, "sym2": sym2, "pair_index": idx}
            ohlc1 = make_baseline_ohlc(ohlc2)
        elif sym2 == baseline_key:
            ohlc1 = df1_map.get(sym1)
            if ohlc1 is None or ohlc1.empty:
                return idx, {"status": "no_data", "pair": f"{sym1}/{sym2}",
                             "sym1": sym1, "sym2": sym2, "pair_index": idx}
            ohlc2 = make_baseline_ohlc(ohlc1)
        else:
            ohlc1 = df1_map.get(sym1)
            ohlc2 = df1_map.get(sym2)
            if ohlc1 is None or ohlc2 is None or ohlc1.empty or ohlc2.empty:
                return idx, {"status": "no_data", "pair": f"{sym1}/{sym2}",
                             "sym1": sym1, "sym2": sym2, "pair_index": idx}

        result = run_sliding_windows(ohlc1, ohlc2, sym1, sym2, window_bars)
        result["pair_index"] = idx
        return idx, result

    tasks = [(idx, sym1, sym2) for idx, (sym1, sym2) in enumerate(pairs)]
    max_workers = min(AppConfig.super_test_max_workers, max(total_pairs, 1))

    if total_pairs <= 2:
        for task in tasks:
            idx, r = _run_pair(task)
            results_map[idx] = r
            if on_pair_complete:
                on_pair_complete(len(results_map), total_pairs, r)
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {executor.submit(_run_pair, task): task[0] for task in tasks}
            for future in as_completed(future_map):
                try:
                    idx, r = future.result()
                except Exception as e:
                    idx = future_map[future]
                    sym1, sym2 = pairs[idx]
                    r = {
                        "status": "error",
                        "pair": f"{sym1}/{sym2}",
                        "sym1": sym1,
                        "sym2": sym2,
                        "error": str(e),
                        "pair_index": idx,
                    }
                results_map[idx] = r
                if on_pair_complete:
                    on_pair_complete(len(results_map), total_pairs, r)

    # Reconstruct ordered list
    results = [results_map.get(i, {"status": "missing", "pair_index": i}) for i in range(total_pairs)]

    # Build rankings from successful results only
    successful = [r for r in results if r.get("status") == "success"]
    rankings = sorted(successful, key=lambda x: x.get("score", -float("inf")), reverse=True)
    for rank_i, item in enumerate(rankings):
        item["rank"] = rank_i + 1

    # Summary
    summary = _compute_summary(results, rankings, window_bars)

    elapsed = time.time() - t0
    logger.info(f"Divergence Scan complete: {len(successful)}/{total_pairs} pairs in {elapsed:.1f}s")

    return {
        "status": "success",
        "total_pairs": total_pairs,
        "completed_pairs": len(successful),
        "elapsed_seconds": round(elapsed, 2),
        "pairs": results,
        "rankings": rankings,
        "summary": summary,
    }


def _compute_summary(results: list[dict], rankings: list[dict], window_bars: int) -> dict:
    """Aggregate statistics for the summary card."""
    successful = [r for r in results if r.get("status") == "success"]
    if not successful:
        return {}

    scores = [r["score"] for r in successful]
    pct_cleans = [r["pct_zero_crossing_windows"] for r in successful]

    best = rankings[0] if rankings else None

    return {
        "total_pairs":          len(results),
        "pairs_with_data":      len(successful),
        "window_bars":          window_bars,
        "best_pair":            best["pair"] if best else "—",
        "best_score":           round(best["score"], 6) if best else 0,
        "avg_pct_clean_windows": round(float(np.mean(pct_cleans)), 2),
        "max_score":            round(float(np.max(scores)), 6),
        "min_score":            round(float(np.min(scores)), 6),
    }
