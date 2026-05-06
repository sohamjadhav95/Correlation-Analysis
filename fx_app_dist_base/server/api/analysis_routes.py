"""
Analysis API routes — correlation analysis and comparison endpoints.
"""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from ..models.schemas import AnalysisRequest, AnalysisResponse, CompareRequest, CompareResponse
from ..engine.pipeline import run_analysis
from ..config import TIMEFRAME_MAP

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/analysis", tags=["analysis"])


@router.post("/run", response_model=AnalysisResponse)
async def run_correlation_analysis(req: AnalysisRequest):
    """Run standard correlation analysis on two assets."""
    try:
        result = run_analysis(
            domain=req.domain.value,
            symbol_1=req.symbol_1,
            symbol_2=req.symbol_2,
            timeframe=req.timeframe.value,
            start=req.start,
            end=req.end,
        )
    except Exception as e:
        logger.error(f"Analysis error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result.get("message", "Analysis failed"))

    return AnalysisResponse(
        status="success",
        total_bars=result["total_bars"],
        metrics=result["metrics"],
        data=result["data"],
    )


@router.post("/compare", response_model=CompareResponse)
async def run_comparison(req: CompareRequest):
    """Run Set A vs Set B comparison."""
    try:
        result_a = run_analysis(
            domain=req.set_a.domain.value,
            symbol_1=req.set_a.symbol_1,
            symbol_2=req.set_a.symbol_2,
            timeframe=req.set_a.timeframe.value,
            start=req.set_a.start,
            end=req.set_a.end,
        )
        result_b = run_analysis(
            domain=req.set_b.domain.value,
            symbol_1=req.set_b.symbol_1,
            symbol_2=req.set_b.symbol_2,
            timeframe=req.set_b.timeframe.value,
            start=req.set_b.start,
            end=req.set_b.end,
        )
    except Exception as e:
        logger.error(f"Comparison error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    return CompareResponse(
        status="success",
        set_a=AnalysisResponse(
            status=result_a["status"],
            total_bars=result_a.get("total_bars", 0),
            metrics=result_a.get("metrics", {}),
            data=result_a.get("data", []),
        ),
        set_b=AnalysisResponse(
            status=result_b["status"],
            total_bars=result_b.get("total_bars", 0),
            metrics=result_b.get("metrics", {}),
            data=result_b.get("data", []),
        ),
    )


@router.get("/timeframes")
async def get_timeframes():
    """Return available timeframes."""
    return {"timeframes": TIMEFRAME_MAP}
