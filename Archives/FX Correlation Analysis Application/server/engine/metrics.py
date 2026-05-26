"""
Metrics computation — summary statistics from correlation results.

Extracted from the original get_summary_metrics() function.
"""

import pandas as pd


def compute_summary_metrics(result: pd.DataFrame) -> dict:
    """
    Compute summary metrics from a correlation result DataFrame.

    Returns dict with string-formatted values for display.
    """
    if result.empty:
        return {
            "Total Bars": "0",
            "Total Flips": "0",
            "Total Flip Loss": "0.0000",
            "Max |Spread|": "0.0000",
            "Avg |Spread|": "0.0000",
            "Max Single Flip Loss": "0.0000",
        }

    return {
        "Total Bars": f"{len(result):,}",
        "Total Flips": f"{int(result['flip_occurred'].sum()):,}",
        "Total Flip Loss": f"{float(result['flip_loss'].sum()):.4f}",
        "Max |Spread|": f"{float(result['index_spread'].abs().max()):.4f}",
        "Avg |Spread|": f"{float(result['index_spread'].abs().mean()):.4f}",
        "Max Single Flip Loss": f"{float(result['flip_loss'].max()):.4f}",
    }


def compute_raw_metrics(result: pd.DataFrame) -> dict:
    """
    Compute raw (numeric) metrics for programmatic use (e.g., Super Test ranking).

    Returns dict with float values.
    """
    if result.empty:
        return {
            "total_bars": 0,
            "total_flips": 0,
            "total_flip_loss": 0.0,
            "max_spread": 0.0,
            "avg_spread": 0.0,
            "max_single_flip_loss": 0.0,
        }

    return {
        "total_bars": len(result),
        "total_flips": int(result["flip_occurred"].sum()),
        "total_flip_loss": float(result["flip_loss"].sum()),
        "max_spread": float(result["index_spread"].abs().max()),
        "avg_spread": float(result["index_spread"].abs().mean()),
        "max_single_flip_loss": float(result["flip_loss"].max()),
    }
