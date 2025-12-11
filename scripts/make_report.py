#!/usr/bin/env python
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

# Make src/ importable when running this script
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT / "src"))

from gas_tightness.mipi_client import NationalGasClient
from gas_tightness.features.build_dataset import fetch_multiple_series
from gas_tightness.features.tightness import add_tightness_metrics
from gas_tightness.report.charts import make_basic_charts
from gas_tightness.report.render_md import write_markdown_report


def main() -> None:
    # ---- 1. Choose date range ----
    today = date.today()
    end = today - timedelta(days=1)      # last *completed* gas day (D)
    start = end - timedelta(days=20)     # last 20 gas days

    # ---- 2. Create API client with caching ----
    client = NationalGasClient(use_cache=True)

    # ---- 3. Build daily dataset with the series we want ----
    series_map = {
        "forecast_39": "PUBOB39",
        "actual_demand": "PUBOB637",  # my demand series
        "linepack": "PUBOBJ486",  # Linepack, Hourly Actual, Aggregate, D+1
    }

    daily_panel = fetch_multiple_series(
        series_codes=series_map,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        client=client,
    )

    # ---- 4. Add tightness metrics (new method) ----
    scored = add_tightness_metrics(daily_panel)

    # ---- 5. Save processed data ----
    processed_dir = PROJECT_ROOT / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    processed_path = processed_dir / "daily_panel.parquet"
    scored.to_parquet(processed_path)

    # ---- 6. Make charts ----
    reports_dir = PROJECT_ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    chart_paths = make_basic_charts(
        scored,
        end,
        reports_dir=reports_dir,
    )

    # ---- 7. Write markdown report ----
    report_path = reports_dir / f"{end}.md"

    write_markdown_report(
        scored_df=scored,
        report_date=end,
        chart_paths=chart_paths,
        out_path=report_path,
    )

    print(f"Report written to {report_path}")


if __name__ == "__main__":
    main()
