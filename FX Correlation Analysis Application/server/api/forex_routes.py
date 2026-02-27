"""
Forex API routes — endpoints for MT5 data operations.
"""

import logging
import time
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from ..models.schemas import FetchRequest, FetchResponse, SymbolsResponse
from ..models.enums import Domain
from ..data.mt5_adapter import MT5Adapter
from ..engine.pipeline import fetch_and_cache
from ..config import MT5Config

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/forex", tags=["forex"])


@router.post("/fetch", response_model=FetchResponse)
async def fetch_forex_data(req: FetchRequest):
    """Fetch tick data from MT5 for a forex symbol."""
    if not MT5Config.is_configured():
        raise HTTPException(status_code=400, detail="MT5 credentials not configured. Update .env file.")

    t0 = time.time()

    try:
        df = fetch_and_cache("forex", req.symbol, req.start, req.end)
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=f"MT5 connection failed: {e}")
    except Exception as e:
        logger.error(f"Forex fetch error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    elapsed = (time.time() - t0) * 1000

    return FetchResponse(
        status="success",
        symbol=req.symbol,
        ticks_fetched=len(df),
        total_time_ms=round(elapsed, 1),
    )


@router.get("/symbols", response_model=SymbolsResponse)
async def get_forex_symbols():
    """List available MT5 symbols."""
    if not MT5Config.is_configured():
        raise HTTPException(status_code=400, detail="MT5 credentials not configured.")

    try:
        with MT5Adapter() as adapter:
            symbols = adapter.get_symbols()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"MT5 error: {e}")

    return SymbolsResponse(domain=Domain.FOREX, symbols=symbols)
