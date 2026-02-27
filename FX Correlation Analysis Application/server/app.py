"""
FastAPI application — entry point for the backend server.

Serves:
- REST API routes under /api/*
- WebSocket routes under /ws/*
- Static frontend files from /frontend/
"""

import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from .config import AppConfig, MT5Config, BinanceConfig, TIMEFRAME_MAP
from .api.forex_routes import router as forex_router
from .api.crypto_routes import router as crypto_router
from .api.analysis_routes import router as analysis_router
from .api.super_test_routes import router as super_test_router
from .api.ws_routes import router as ws_router

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Create app
app = FastAPI(
    title="FX Correlation Analysis Platform",
    description="Forex & crypto correlation analysis with Super Test mode",
    version="2.0.0",
)

# CORS — allow local frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── API routes ────────────────────────────────────────────────────
app.include_router(forex_router)
app.include_router(crypto_router)
app.include_router(analysis_router)
app.include_router(super_test_router)
app.include_router(ws_router)


# ── Config endpoint ──────────────────────────────────────────────
@app.get("/api/config")
async def get_config():
    """Return current application configuration (safe to expose)."""
    return {
        "mt5_configured": MT5Config.is_configured(),
        "mt5_server": MT5Config.server,
        "mt5_login": MT5Config.login,
        "binance_has_key": BinanceConfig.has_api_key(),
        "data_cache_dir": str(AppConfig.data_cache_dir),
        "available_timeframes": TIMEFRAME_MAP,
    }


@app.get("/api/data/status")
async def get_data_status():
    """Return cache status."""
    from .data.cache_manager import CacheManager
    cache = CacheManager()
    return cache.get_status()


# ── Startup ──────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    """Initialize directories on startup."""
    AppConfig.ensure_dirs()
    logger.info("=" * 60)
    logger.info("FX Correlation Analysis Platform v2.0.0")
    logger.info(f"MT5 configured: {MT5Config.is_configured()}")
    logger.info(f"Binance API key: {BinanceConfig.has_api_key()}")
    logger.info(f"Cache dir: {AppConfig.data_cache_dir}")
    logger.info("=" * 60)


# ── Static files (frontend) ──────────────────────────────────────
frontend_dir = AppConfig.frontend_dir

if frontend_dir.exists():
    app.mount("/assets", StaticFiles(directory=str(frontend_dir / "assets")), name="assets")
    app.mount("/css", StaticFiles(directory=str(frontend_dir / "css")), name="css")
    app.mount("/js", StaticFiles(directory=str(frontend_dir / "js")), name="js")

    @app.get("/")
    async def serve_frontend():
        return FileResponse(str(frontend_dir / "index.html"))

    @app.get("/{path:path}")
    async def catch_all(path: str):
        """Catch-all for SPA routing — serve index.html for non-API paths."""
        file_path = frontend_dir / path
        if file_path.exists() and file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(frontend_dir / "index.html"))
