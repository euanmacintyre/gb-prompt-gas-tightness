"""
Helpers for building daily datasets from National Gas series.
"""

from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

# Relative import: we are inside gas_tightness/features/
from ..mipi_client import (
    fetch_series,
    latest_per_gas_day,
    NationalGasClient,
)


def fetch_multiple_series(
    series_codes: Dict[str, str],
    start_date: str,
    end_date: str,
    client: Optional[NationalGasClient] = None,
) -> pd.DataFrame:
    
    frames = []

    for col_name, pub_id in series_codes.items():
        if client is None:
            # Old behaviour (no caching): use raw helpers directly
            df = fetch_series(pub_id, start_date, end_date)
            daily = latest_per_gas_day(df)
        else:
            # New behaviour (with caching): go via the client
            # daily has columns ["gas_day_start_utc", "Value"]
            daily = client.get_daily_series(
                series_code=pub_id,
                start_date=start_date,
                end_date=end_date,
            )

        # Rename 'Value' -> desired column name
        daily = daily.rename(columns={"Value": col_name})
        frames.append(daily)

    if not frames:
        raise ValueError("No series provided to fetch_multiple_series")

    # Merge all series on gas_day_start_utc
    combined = frames[0]
    for other in frames[1:]:
        combined = combined.merge(other, on="gas_day_start_utc", how="outer")

    # Sort by time and set index
    combined = combined.sort_values("gas_day_start_utc")
    combined = combined.set_index("gas_day_start_utc")

    return combined
