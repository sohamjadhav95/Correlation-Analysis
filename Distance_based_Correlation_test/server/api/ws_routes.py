"""
WebSocket routes — real-time progress updates for long-running jobs.
"""

import asyncio
import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ..jobs.job_manager import get_job

logger = logging.getLogger(__name__)
router = APIRouter()


@router.websocket("/ws/progress")
async def ws_progress(websocket: WebSocket, job_id: str = ""):
    """
    WebSocket endpoint for streaming job progress.

    Client connects with ?job_id=xxx
    Server sends JSON updates: {progress, total, status}
    Polls every 500ms until job completes.
    """
    await websocket.accept()

    if not job_id:
        await websocket.send_json({"error": "job_id parameter required"})
        await websocket.close()
        return

    logger.info(f"WS connected for job: {job_id}")

    try:
        last_progress = -1

        while True:
            job = get_job(job_id)

            if job is None:
                await websocket.send_json({"error": "Job not found", "status": "not_found"})
                break

            current_progress = job.get("progress", 0)
            status = job["status"].value if hasattr(job["status"], "value") else str(job["status"])

            # Only send update if progress changed
            if current_progress != last_progress:
                await websocket.send_json({
                    "job_id": job_id,
                    "status": status,
                    "progress": current_progress,
                    "total": job.get("total", 0),
                })
                last_progress = current_progress

            # Check if job is done
            if status in ("completed", "failed", "cancelled"):
                if status == "completed" and job.get("result"):
                    result = job["result"]
                    await websocket.send_json({
                        "job_id": job_id,
                        "status": "completed",
                        "total_intervals": result.get("total_intervals", 0),
                        "completed_intervals": result.get("completed_intervals", 0),
                        "elapsed_seconds": result.get("elapsed_seconds", 0),
                    })
                break

            await asyncio.sleep(0.5)

    except WebSocketDisconnect:
        logger.info(f"WS disconnected: {job_id}")
    except Exception as e:
        logger.error(f"WS error for {job_id}: {e}")
    finally:
        try:
            await websocket.close()
        except Exception:
            pass
