"""
Divergence Scanner API routes — submit, track, and retrieve Divergence Scan jobs.
"""

import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException

from ..models.schemas import DivergenceScanRequest, DivergenceScanJobResponse
from ..models.enums import JobStatus
from ..engine.pipeline import fetch_and_cache
from ..engine.resampler import resample_ticks_to_ohlc
from ..engine.divergence_scanner import generate_pair_combinations, run_divergence_scan
from ..jobs.job_manager import create_job, submit_job, get_job, update_job_progress

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/divergence", tags=["divergence"])


def _execute_divergence_scan(job_id: str, req_dict: dict) -> dict:
    """Background worker function for Divergence Scan execution."""
    from ..models.schemas import DivergenceScanRequest
    req = DivergenceScanRequest(**req_dict)

    # Parse full datetime range for data fetching
    date_str = req.date
    start_dt = datetime.strptime(f"{date_str} {req.start_time}", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(f"{date_str} {req.end_time}", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)

    if end_dt <= start_dt:
        end_dt += timedelta(days=1)

    # Fetch and resample OHLC for every symbol up front (shared across all pairs)
    df_map = {}  # {symbol: ohlc_dataframe}
    for symbol in req.symbols:
        try:
            ticks = fetch_and_cache(req.domain.value, symbol, start_dt, end_dt)
            if not ticks.empty:
                ohlc = resample_ticks_to_ohlc(ticks, req.timeframe.value)
                if not ohlc.empty:
                    df_map[symbol] = ohlc
                else:
                    logger.warning(f"No OHLC bars for {symbol} after resampling")
            else:
                logger.warning(f"No tick data for {symbol}")
        except Exception as e:
            logger.error(f"Failed to fetch {symbol}: {e}")

    if len(df_map) < 2:
        return {
            "status": "error",
            "message": "Fewer than 2 symbols have data. Cannot scan pairs.",
        }

    # Only scan pairs where both symbols have data
    all_pairs = generate_pair_combinations(req.symbols)
    valid_pairs = [(s1, s2) for s1, s2 in all_pairs if s1 in df_map and s2 in df_map]

    if not valid_pairs:
        return {
            "status": "error",
            "message": "No valid pairs found with overlapping data.",
        }

    def on_progress(completed, total, pair_result):
        update_job_progress(job_id, completed, total)

    result = run_divergence_scan(
        df1_map=df_map,
        pairs=valid_pairs,
        window_bars=req.window_bars,
        on_pair_complete=on_progress,
    )

    # Attach request context for frontend use
    result["scan_date"] = req.date
    result["scan_start_time"] = req.start_time
    result["scan_end_time"] = req.end_time
    result["timeframe"] = req.timeframe.value
    result["window_bars"] = req.window_bars
    result["symbols"] = req.symbols

    return result


@router.post("/run", response_model=DivergenceScanJobResponse)
async def start_divergence_scan(req: DivergenceScanRequest):
    """Submit a Divergence Scan job for background execution."""
    from itertools import combinations
    total_pairs = len(list(combinations(req.symbols, 2)))

    if total_pairs == 0:
        raise HTTPException(status_code=400, detail="Need at least 2 symbols to form pairs.")

    # Rough estimate: ~1s per pair (IO + windowing)
    estimated_time = total_pairs * 1.2

    job_id = create_job("divergence_scan", req.model_dump())
    submit_job(job_id, _execute_divergence_scan, job_id, req.model_dump())

    return DivergenceScanJobResponse(
        job_id=job_id,
        total_pairs=total_pairs,
        estimated_time_seconds=round(estimated_time, 1),
        ws_url=f"/ws/progress?job_id={job_id}",
    )


@router.get("/status/{job_id}")
async def get_divergence_status(job_id: str):
    """Get the status and progress of a Divergence Scan job."""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "job_id": job["id"],
        "status": job["status"],
        "progress": job["progress"],
        "total": job["total"],
        "error": job.get("error"),
    }


@router.get("/result/{job_id}")
async def get_divergence_result(job_id: str):
    """Get the completed results of a Divergence Scan job."""
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job["status"] == JobStatus.RUNNING:
        return {"status": "running", "progress": job["progress"], "total": job["total"]}
    elif job["status"] == JobStatus.FAILED:
        raise HTTPException(status_code=500, detail=job.get("error", "Job failed"))
    elif job["status"] == JobStatus.PENDING:
        return {"status": "pending"}
    elif job["status"] == JobStatus.CANCELLED:
        return {"status": "cancelled"}

    return job.get("result", {})
