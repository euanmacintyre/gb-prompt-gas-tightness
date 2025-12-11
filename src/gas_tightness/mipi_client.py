"""
Simple client for pulling National Gas publication data.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from io import StringIO
from pathlib import Path
from typing import Union
from zoneinfo import ZoneInfo

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Project / data locations
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"

DateLike = Union[str, "pd.Timestamp"]

# ---------------------------------------------------------------------------
# Original helper functions (unchanged)
# ---------------------------------------------------------------------------


def fetch_series(series_code, start_date, end_date):
    """
    Fetch one National Gas data series for a date range.

    Returns a cleaned DataFrame with:
    - Applicable For (gas day)
    - Applicable At (timestamp)
    - Value (the data point)
    - gas_day_start_utc (aligned to 05:00 UK time)
    """

    # API endpoint for publication data
    url = "https://data.nationalgas.com/api/find-gas-data-download"

    # Query parameters for the API call
    params = {
        "ids": series_code,
        "dateFrom": f"{start_date}T00:00:00",
        "dateTo": f"{end_date}T23:59:59",
        "dateType": "GASDAY",      # Interpret dates as gas days
        "applicableFor": "Y",      # Use gas day field
        "latestFlag": "Y",         # Only return latest publication per gas day
        "type": "CSV",             # Response format
    }

    # --- 1. Download CSV text ---
    response = requests.get(url, params=params)
    response.raise_for_status()
    csv_text = response.text

    # --- 2. Convert CSV to DataFrame ---
    df = pd.read_csv(StringIO(csv_text))

    # --- 3. Clean timestamps (day/month/year in the CSV) ---
    df["Applicable For"] = pd.to_datetime(
        df["Applicable For"], dayfirst=True
    ).dt.date
    df["Applicable At"] = pd.to_datetime(
        df["Applicable At"], dayfirst=True
    )

    # --- 4. Add gas day start timestamp in UTC ---
    local = ZoneInfo("Europe/London")

    def gas_day_start_utc(date_obj):
        # Gas day starts at 05:00 UK local time
        dt_local = datetime.combine(date_obj, time(5, 0)).replace(tzinfo=local)
        return dt_local.astimezone(ZoneInfo("UTC"))

    df["gas_day_start_utc"] = df["Applicable For"].apply(gas_day_start_utc)

    return df


def latest_per_gas_day(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reduce a raw series to one value per gas day (latest timestamp).

    Returns a DataFrame with:
    - gas_day_start_utc
    - Value
    """

    # Sort so the last publication for each day is the most recent
    df_sorted = df.sort_values(["gas_day_start_utc", "Applicable At"])

    latest = df_sorted.groupby("gas_day_start_utc").tail(1)

    # Keep just the timestamp + value
    return latest[["gas_day_start_utc", "Value"]]


# ---------------------------------------------------------------------------
# Caching client
# ---------------------------------------------------------------------------


@dataclass
class NationalGasClient:
    """
    Thin wrapper around the National Gas API + existing helpers,
    with simple parquet caching in data/raw.
    """

    use_cache: bool = True

    def _cache_path(
        self,
        series_code: str,
        start_date: DateLike,
        end_date: DateLike,
        level: str = "raw",  # "raw" or "daily"
    ) -> Path:
        start_str = pd.to_datetime(start_date).date().isoformat()
        end_str = pd.to_datetime(end_date).date().isoformat()
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        fname = f"{series_code}_{level}_{start_str}_{end_str}.parquet"
        return RAW_DIR / fname

    def get_raw_series(
        self,
        series_code: str,
        start_date: DateLike,
        end_date: DateLike,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """
        Use the existing fetch_series() but cache the result.
        """
        cache_path = self._cache_path(series_code, start_date, end_date, level="raw")

        if self.use_cache and not force_refresh and cache_path.exists():
            return pd.read_parquet(cache_path)

        df = fetch_series(series_code, start_date, end_date)

        if self.use_cache:
            df.to_parquet(cache_path)

        return df

    def get_daily_series(
        self,
        series_code: str,
        start_date: DateLike,
        end_date: DateLike,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """
        One value per gas day (using latest_per_gas_day), also cached.
        """
        cache_path = self._cache_path(series_code, start_date, end_date, level="daily")

        if self.use_cache and not force_refresh and cache_path.exists():
            return pd.read_parquet(cache_path)

        raw = self.get_raw_series(
            series_code,
            start_date,
            end_date,
            force_refresh=force_refresh,
        )
        daily = latest_per_gas_day(raw)

        if self.use_cache:
            daily.to_parquet(cache_path)

        return daily
