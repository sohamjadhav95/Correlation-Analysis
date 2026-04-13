"""
Cache manager — Parquet-based tick data cache with SQLite metadata tracking.

Design:
- Tick data stored as Parquet files partitioned by symbol and date range.
- SQLite metadata DB tracks what's cached: symbol, start, end, row_count, fetched_at.
- Supports partial cache hits (return cached ranges + identify gaps).
- File-level locking via filelock for write safety.
"""

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from filelock import FileLock

from ..config import AppConfig

logger = logging.getLogger(__name__)

_METADATA_DB = "metadata.db"
_LOCK_SUFFIX = ".lock"


class CacheManager:
    """
    Manages local Parquet cache for tick and OHLC data.

    Usage:
        cache = CacheManager()
        cache.initialize()

        # Check what's cached
        ranges = cache.get_cached_ranges("forex", "XAUUSDm")

        # Read from cache
        df = cache.read("forex", "XAUUSDm", start, end)

        # Write to cache
        cache.store("forex", "XAUUSDm", df, start, end)
    """

    def __init__(self, cache_dir: Path = None):
        self._cache_dir = cache_dir or AppConfig.data_cache_dir
        self._db_path = self._cache_dir / _METADATA_DB
        self._initialized = False

    def initialize(self):
        """Create cache directories and metadata database."""
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        (self._cache_dir / "forex").mkdir(exist_ok=True)
        (self._cache_dir / "crypto").mkdir(exist_ok=True)

        conn = sqlite3.connect(str(self._db_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cache_index (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain TEXT NOT NULL,
                symbol TEXT NOT NULL,
                start_utc TEXT NOT NULL,
                end_utc TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                data_type TEXT DEFAULT 'ticks'
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_domain_symbol
            ON cache_index (domain, symbol)
        """)
        conn.commit()
        conn.close()
        self._initialized = True
        logger.info(f"Cache manager initialized at {self._cache_dir}")

    # ── Query cache ───────────────────────────────────────────────

    def get_cached_ranges(
        self, domain: str, symbol: str, data_type: str = "ticks"
    ) -> list[tuple[datetime, datetime]]:
        """Return list of (start, end) UTC datetime tuples that are cached."""
        self._ensure_initialized()

        conn = sqlite3.connect(str(self._db_path))
        cursor = conn.execute(
            "SELECT start_utc, end_utc FROM cache_index "
            "WHERE domain = ? AND symbol = ? AND data_type = ? "
            "ORDER BY start_utc",
            (domain, symbol, data_type),
        )
        ranges = []
        for row in cursor:
            s = datetime.fromisoformat(row[0]).replace(tzinfo=timezone.utc)
            e = datetime.fromisoformat(row[1]).replace(tzinfo=timezone.utc)
            ranges.append((s, e))
        conn.close()
        return ranges

    def find_gaps(
        self,
        domain: str,
        symbol: str,
        start: datetime,
        end: datetime,
        data_type: str = "ticks",
    ) -> list[tuple[datetime, datetime]]:
        """
        Given a requested [start, end], return uncached sub-ranges.
        Used to determine what needs to be freshly fetched.
        """
        cached = self.get_cached_ranges(domain, symbol, data_type)

        if not cached:
            return [(start, end)]

        gaps = []
        current = start

        for cs, ce in cached:
            if cs > current:
                gap_end = min(cs, end)
                if gap_end > current:
                    gaps.append((current, gap_end))
            current = max(current, ce)

        if current < end:
            gaps.append((current, end))

        return gaps

    # ── Read from cache ───────────────────────────────────────────

    def read(
        self,
        domain: str,
        symbol: str,
        start: datetime,
        end: datetime,
        data_type: str = "ticks",
    ) -> Optional[pd.DataFrame]:
        """
        Read cached data for a symbol in a time range.
        Returns None if nothing is cached, or a DataFrame with available data.
        """
        self._ensure_initialized()

        conn = sqlite3.connect(str(self._db_path))
        cursor = conn.execute(
            "SELECT file_path FROM cache_index "
            "WHERE domain = ? AND symbol = ? AND data_type = ? "
            "AND end_utc > ? AND start_utc < ? "
            "ORDER BY start_utc",
            (domain, symbol, data_type,
             start.isoformat(), end.isoformat()),
        )
        file_paths = [row[0] for row in cursor]
        conn.close()

        if not file_paths:
            return None

        chunks = []
        for fp in file_paths:
            path = Path(fp)
            if path.exists():
                df = pd.read_parquet(path)
                chunks.append(df)
            else:
                logger.warning(f"Cache file missing: {fp}")

        if not chunks:
            return None

        combined = pd.concat(chunks, ignore_index=True)

        # Filter to exact requested range
        if "timestamp" in combined.columns:
            start_ts = pd.Timestamp(start)
            end_ts = pd.Timestamp(end)
            # Ensure UTC-aware for comparison
            if start_ts.tzinfo is not None:
                start_ts = start_ts.tz_convert("UTC")
            else:
                start_ts = start_ts.tz_localize("UTC")
            if end_ts.tzinfo is not None:
                end_ts = end_ts.tz_convert("UTC")
            else:
                end_ts = end_ts.tz_localize("UTC")
            combined = combined[
                (combined["timestamp"] >= start_ts) &
                (combined["timestamp"] <= end_ts)
            ]

        combined = combined.drop_duplicates(subset=["timestamp"], keep="first")
        combined = combined.sort_values("timestamp").reset_index(drop=True)

        return combined if not combined.empty else None

    # ── Write to cache ────────────────────────────────────────────

    def store(
        self,
        domain: str,
        symbol: str,
        df: pd.DataFrame,
        start: datetime,
        end: datetime,
        data_type: str = "ticks",
    ) -> str:
        """
        Store a DataFrame to the Parquet cache.
        Returns the file path written.
        """
        self._ensure_initialized()

        if df.empty:
            logger.warning(f"Skipping cache store for empty DataFrame: {domain}/{symbol}")
            return ""

        # Build file path
        symbol_dir = self._cache_dir / domain / symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)

        start_str = start.strftime("%Y%m%d_%H%M%S")
        end_str = end.strftime("%Y%m%d_%H%M%S")
        filename = f"{start_str}_{end_str}_{data_type}.parquet"
        file_path = symbol_dir / filename

        # Write with file lock
        lock_path = str(file_path) + _LOCK_SUFFIX
        lock = FileLock(lock_path, timeout=30)

        with lock:
            df.to_parquet(file_path, engine="pyarrow", index=False)

        # Update metadata
        conn = sqlite3.connect(str(self._db_path))
        conn.execute(
            "INSERT INTO cache_index (domain, symbol, start_utc, end_utc, row_count, file_path, fetched_at, data_type) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                domain,
                symbol,
                start.isoformat(),
                end.isoformat(),
                len(df),
                str(file_path),
                datetime.now(timezone.utc).isoformat(),
                data_type,
            ),
        )
        conn.commit()
        conn.close()

        logger.info(f"Cached {len(df)} rows → {file_path}")
        return str(file_path)

    # ── Cache info ────────────────────────────────────────────────

    def get_status(self) -> dict:
        """Return cache statistics."""
        self._ensure_initialized()

        conn = sqlite3.connect(str(self._db_path))
        cursor = conn.execute(
            "SELECT domain, symbol, COUNT(*), SUM(row_count), "
            "MIN(start_utc), MAX(end_utc) "
            "FROM cache_index GROUP BY domain, symbol"
        )
        entries = []
        for row in cursor:
            entries.append({
                "domain": row[0],
                "symbol": row[1],
                "files": row[2],
                "total_rows": row[3],
                "earliest": row[4],
                "latest": row[5],
            })
        conn.close()

        return {
            "cache_dir": str(self._cache_dir),
            "entries": entries,
        }

    def clear(self, domain: str = None, symbol: str = None):
        """Clear cached data. Optionally filter by domain/symbol."""
        self._ensure_initialized()

        conn = sqlite3.connect(str(self._db_path))

        if domain and symbol:
            cursor = conn.execute(
                "SELECT file_path FROM cache_index WHERE domain = ? AND symbol = ?",
                (domain, symbol),
            )
        elif domain:
            cursor = conn.execute(
                "SELECT file_path FROM cache_index WHERE domain = ?",
                (domain,),
            )
        else:
            cursor = conn.execute("SELECT file_path FROM cache_index")

        for row in cursor:
            path = Path(row[0])
            if path.exists():
                path.unlink()

        if domain and symbol:
            conn.execute(
                "DELETE FROM cache_index WHERE domain = ? AND symbol = ?",
                (domain, symbol),
            )
        elif domain:
            conn.execute(
                "DELETE FROM cache_index WHERE domain = ?", (domain,),
            )
        else:
            conn.execute("DELETE FROM cache_index")

        conn.commit()
        conn.close()
        logger.info(f"Cache cleared: domain={domain}, symbol={symbol}")

    # ── Internal ──────────────────────────────────────────────────

    def _ensure_initialized(self):
        if not self._initialized:
            self.initialize()
