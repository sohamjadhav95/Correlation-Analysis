"""
Abstract base adapter — defines the interface all data source adapters must implement.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

import pandas as pd


class BaseDataAdapter(ABC):
    """
    Abstract interface for data source adapters.

    Every adapter (MT5, Binance, future exchanges) must implement:
    - connect / disconnect lifecycle
    - tick-level data fetching
    - OHLC fallback fetching
    - symbol listing
    """

    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to the data source.
        Returns True on success, raises on unrecoverable failure.
        """
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """Cleanly close the connection."""
        ...

    @abstractmethod
    def fetch_ticks(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        on_progress: Optional[callable] = None,
    ) -> pd.DataFrame:
        """
        Fetch tick-level data for a symbol in a UTC time range.

        Returns DataFrame with columns: [timestamp, bid, ask, mid]
        - timestamp: UTC datetime64[ns, UTC]
        - bid, ask, mid: float64

        on_progress(fetched: int, estimated_total: int) is called periodically.
        """
        ...

    @abstractmethod
    def fetch_ohlc(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        timeframe: str,
    ) -> pd.DataFrame:
        """
        Fetch OHLC bar data (fallback when ticks are unavailable).

        Returns DataFrame with columns: [timestamp, open, high, low, close, volume]
        - timestamp: UTC datetime64[ns, UTC]
        """
        ...

    @abstractmethod
    def get_symbols(self) -> list[str]:
        """Return list of available symbols from this source."""
        ...

    @property
    @abstractmethod
    def domain(self) -> str:
        """Return the domain name ('forex' or 'crypto')."""
        ...

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False
