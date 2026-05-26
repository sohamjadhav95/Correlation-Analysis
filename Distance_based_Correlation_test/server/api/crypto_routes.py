"""
Crypto API routes — endpoints for Binance Futures data operations.
"""

import logging
import time
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from ..models.schemas import FetchRequest, FetchResponse, SymbolsResponse
from ..models.enums import Domain
from ..data.binance_adapter import BinanceAdapter
from ..engine.pipeline import fetch_and_cache

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/crypto", tags=["crypto"])


@router.post("/fetch", response_model=FetchResponse)
async def fetch_crypto_data(req: FetchRequest):
    """Fetch aggregated trade data from Binance Futures."""
    t0 = time.time()

    try:
        df = fetch_and_cache("crypto", req.symbol, req.start, req.end)
    except Exception as e:
        logger.error(f"Crypto fetch error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    elapsed = (time.time() - t0) * 1000

    return FetchResponse(
        status="success",
        symbol=req.symbol,
        ticks_fetched=len(df),
        total_time_ms=round(elapsed, 1),
    )


@router.get("/symbols", response_model=SymbolsResponse)
async def get_crypto_symbols():
    """List available Binance Futures perpetual symbols."""
    try:
        with BinanceAdapter() as adapter:
            symbols = adapter.get_symbols()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Binance error: {e}")

    return SymbolsResponse(domain=Domain.CRYPTO, symbols=symbols)
