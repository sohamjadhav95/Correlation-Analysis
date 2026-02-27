"""
Entry point — starts the FastAPI server with Uvicorn.
"""

import uvicorn
from server.config import AppConfig


def main():
    AppConfig.ensure_dirs()
    uvicorn.run(
        "server.app:app",
        host=AppConfig.host,
        port=AppConfig.port,
        reload=True,
        log_level="info",
    )


if __name__ == "__main__":
    main()
