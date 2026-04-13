"""
Absolute Flip API routes — distance-confirmed flip analysis on raw ticks.
"""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from ..models.schemas import AbsoluteFlipRequest, AbsoluteFlipResponse
from ..engine.absolute_flip import run_absolute_flip_analysis

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/absolute-flip", tags=["absolute-flip"])


@router.post("/run", response_model=AbsoluteFlipResponse)
async def run_absolute_flip(req: AbsoluteFlipRequest):
    """
    Run Absolute Flip analysis: distance-confirmed flips on raw tick data.

    Unlike standard correlation (time-based, confirmed at bar close),
    a flip here is only counted when the spread travels >= distance_n
    index points past zero in the new direction after a zero-crossing.
    """
    try:
        result = run_absolute_flip_analysis(
            domain=req.domain.value,
            symbol_1=req.symbol_1,
            symbol_2=req.symbol_2,
            timeframe=req.timeframe.value,
            start=req.start,
            end=req.end,
            distance_n=req.distance_n,
        )
    except Exception as e:
        logger.error(f"Absolute Flip error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result.get("message", "Analysis failed"))

    return AbsoluteFlipResponse(
        status="success",
        total_ticks=result["total_ticks"],
        total_bars=result["total_bars"],
        distance_n=result["distance_n"],
        metrics=result["metrics"],
        data=result["data"],
        confirmed_flips=result["confirmed_flips"],
    )
