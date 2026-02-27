"""
Super Test API routes — submit, track, and retrieve Super Test jobs.
"""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from ..models.schemas import SuperTestRequest, SuperTestJobResponse, SuperTestResult
from ..models.enums import JobStatus
from ..engine.pipeline import fetch_and_cache
from ..engine.super_test import run_super_test, generate_intervals
from ..jobs.job_manager import create_job, submit_job, get_job, update_job_progress

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/super-test", tags=["super-test"])


def _execute_super_test(job_id: str, req_dict: dict) -> dict:
    """Background worker function for Super Test execution."""
    from ..models.schemas import SuperTestRequest
    req = SuperTestRequest(**req_dict)

    # Parse full datetime range for data fetching
    date_str = req.date
    start_dt = datetime.strptime(f"{date_str} {req.start_time}", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(f"{date_str} {req.end_time}", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)

    from datetime import timedelta
    if end_dt <= start_dt:
        end_dt += timedelta(days=1)

    # Fetch data for both symbols
    df1 = fetch_and_cache(req.domain.value, req.symbol_1, start_dt, end_dt)
    df2 = fetch_and_cache(req.domain.value, req.symbol_2, start_dt, end_dt)

    if df1.empty or df2.empty:
        return {"status": "error", "message": "No data available for one or both symbols"}

    # Run super test with progress tracking
    def on_progress(completed, total, interval_result):
        update_job_progress(job_id, completed, total)

    result = run_super_test(
        df1_ticks=df1,
        df2_ticks=df2,
        sym1=req.symbol_1,
        sym2=req.symbol_2,
        timeframe=req.timeframe.value,
        date=req.date,
        start_time=req.start_time,
        end_time=req.end_time,
        interval_minutes=req.interval_minutes,
        on_interval_complete=on_progress,
    )

    return result


@router.post("/run", response_model=SuperTestJobResponse)
async def start_super_test(req: SuperTestRequest):
    """Submit a Super Test job for background execution."""
    # Validate intervals
    intervals = generate_intervals(req.date, req.start_time, req.end_time, req.interval_minutes)
    total_intervals = len(intervals)

    if total_intervals == 0:
        raise HTTPException(status_code=400, detail="No intervals generated. Check time range and interval size.")

    # Estimate execution time (~0.15s per interval, rough)
    estimated_time = total_intervals * 0.15

    # Create and submit job
    job_id = create_job("super_test", req.model_dump())
    submit_job(job_id, _execute_super_test, job_id, req.model_dump())

    return SuperTestJobResponse(
        job_id=job_id,
        total_intervals=total_intervals,
        estimated_time_seconds=round(estimated_time, 1),
        ws_url=f"/ws/progress?job_id={job_id}",
    )


@router.get("/status/{job_id}")
async def get_super_test_status(job_id: str):
    """Get the status and progress of a Super Test job."""
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
async def get_super_test_result(job_id: str):
    """Get the completed results of a Super Test job."""
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
