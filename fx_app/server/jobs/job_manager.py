"""
Background job manager — manages async job lifecycle for long-running operations.
"""

import logging
import uuid
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional, Callable

from ..models.enums import JobStatus

logger = logging.getLogger(__name__)

# In-memory job store (single-user, local app — no persistence needed)
_jobs: dict[str, dict] = {}
_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="job")


def create_job(job_type: str, params: dict) -> str:
    """Create a new pending job and return its ID."""
    job_id = f"{job_type}_{uuid.uuid4().hex[:8]}"
    _jobs[job_id] = {
        "id": job_id,
        "type": job_type,
        "params": params,
        "status": JobStatus.PENDING,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "started_at": None,
        "completed_at": None,
        "progress": 0,
        "total": 0,
        "result": None,
        "error": None,
    }
    logger.info(f"Job created: {job_id} ({job_type})")
    return job_id


def submit_job(job_id: str, fn: Callable, *args, **kwargs):
    """Submit a job for background execution."""
    if job_id not in _jobs:
        raise ValueError(f"Job not found: {job_id}")

    def wrapper():
        _jobs[job_id]["status"] = JobStatus.RUNNING
        _jobs[job_id]["started_at"] = datetime.now(timezone.utc).isoformat()

        try:
            result = fn(*args, **kwargs)
            _jobs[job_id]["status"] = JobStatus.COMPLETED
            _jobs[job_id]["result"] = result
        except Exception as e:
            _jobs[job_id]["status"] = JobStatus.FAILED
            _jobs[job_id]["error"] = str(e)
            logger.error(f"Job {job_id} failed: {e}")
        finally:
            _jobs[job_id]["completed_at"] = datetime.now(timezone.utc).isoformat()

    _executor.submit(wrapper)
    logger.info(f"Job submitted: {job_id}")


def update_job_progress(job_id: str, progress: int, total: int):
    """Update progress counters for a running job."""
    if job_id in _jobs:
        _jobs[job_id]["progress"] = progress
        _jobs[job_id]["total"] = total


def get_job(job_id: str) -> Optional[dict]:
    """Get job status and result."""
    return _jobs.get(job_id)


def get_all_jobs() -> list[dict]:
    """Return list of all jobs (summary only)."""
    return [
        {
            "id": j["id"],
            "type": j["type"],
            "status": j["status"],
            "progress": j["progress"],
            "total": j["total"],
            "created_at": j["created_at"],
        }
        for j in _jobs.values()
    ]


def cancel_job(job_id: str) -> bool:
    """Mark a job as cancelled (doesn't kill running threads)."""
    if job_id in _jobs and _jobs[job_id]["status"] in (JobStatus.PENDING, JobStatus.RUNNING):
        _jobs[job_id]["status"] = JobStatus.CANCELLED
        _jobs[job_id]["completed_at"] = datetime.now(timezone.utc).isoformat()
        return True
    return False


def cleanup_old_jobs(max_age_seconds: int = 3600):
    """Remove completed/failed jobs older than max_age_seconds."""
    now = time.time()
    to_remove = []
    for job_id, job in _jobs.items():
        if job["completed_at"]:
            completed = datetime.fromisoformat(job["completed_at"]).timestamp()
            if now - completed > max_age_seconds:
                to_remove.append(job_id)

    for job_id in to_remove:
        del _jobs[job_id]

    if to_remove:
        logger.info(f"Cleaned up {len(to_remove)} old jobs")
