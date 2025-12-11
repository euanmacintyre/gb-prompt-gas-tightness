from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Dict

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd


def _format_date_axis(ax) -> None:
    """
    Make the x-axis dates readable:
    - few ticks
    - short date format
    - rotated labels
    """
    locator = mdates.AutoDateLocator(minticks=4, maxticks=7)
    formatter = mdates.DateFormatter("%d-%b")  # e.g. 21-Nov

    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)

    for label in ax.get_xticklabels():
        label.set_rotation(45)
        label.set_horizontalalignment("right")


def make_basic_charts(
    scored_df: pd.DataFrame,
    report_date: date,
    reports_dir: Path,
) -> Dict[str, Path]:
    """
    Create core charts and save them to the reports directory.

    Parameters
    ----------
    scored_df : DataFrame
        Daily panel with tightness metrics already added.
    report_date : date
        Report as-of gas day (used to tag filenames).
    reports_dir : Path
        Folder to write PNG files into.

    Returns
    -------
    dict
        Mapping from logical chart name to file path.
    """
    reports_dir.mkdir(parents=True, exist_ok=True)
    chart_paths: Dict[str, Path] = {}

    # Work on a copy, sorted by date
    df = scored_df.copy().sort_index()

    # ------------------------------------------------------------------
    # 1) Demand forecast vs actual
    # ------------------------------------------------------------------
    fig, ax = plt.subplots()

    demand_df = df[["forecast_39", "actual_demand"]].dropna(how="all")
    demand_df.plot(ax=ax)

    ax.set_title(f"Demand forecast vs outturn (up to {report_date:%Y-%m-%d})")
    ax.set_xlabel("gas_day_start_utc")
    ax.set_ylabel("Demand (mscm/d)")  # tweak units if needed
    ax.grid(True)
    ax.legend(title="Series")
    _format_date_axis(ax)

    path_supply = reports_dir / f"supply_vs_demand_{report_date:%Y%m%d}.png"
    fig.savefig(path_supply, bbox_inches="tight")
    plt.close(fig)
    chart_paths["supply_vs_demand"] = path_supply

    # ------------------------------------------------------------------
    # 2) Imbalance and tightness score
    # ------------------------------------------------------------------
    fig, ax1 = plt.subplots()

    # Only plot days where both series are present
    mask = df["imbalance_raw"].notna() & df["tightness_score"].notna()
    df_imb = df.loc[mask]

    line1, = ax1.plot(
        df_imb.index,
        df_imb["imbalance_raw"],
        label="Imbalance (forecast - actual)",
    )
    ax1.set_title("Imbalance and tightness score")
    ax1.set_xlabel("gas_day_start_utc")
    ax1.set_ylabel("Imbalance (mscm/d)")
    ax1.grid(True)
    _format_date_axis(ax1)

    ax2 = ax1.twinx()
    line2, = ax2.plot(
        df_imb.index,
        df_imb["tightness_score"],
        linestyle="--",
        label="Tightness score",
    )
    ax2.set_ylabel("Tightness score (dimensionless)")

    # Combined legend
    lines = [line1, line2]
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper left", title="Series")

    path_imb = reports_dir / f"imbalance_and_score_{report_date:%Y%m%d}.png"
    fig.savefig(path_imb, bbox_inches="tight")
    plt.close(fig)
    chart_paths["imbalance_and_score"] = path_imb

    # ------------------------------------------------------------------
    # 3) Linepack vs recent normal
    # ------------------------------------------------------------------
    fig, ax = plt.subplots()

    # Drop NaNs so we don't get gaps
    lp = df["linepack"].dropna()
    lp_roll = lp.rolling(14, min_periods=5).mean()

    line_lp, = ax.plot(lp.index, lp.values, label="Linepack")
    line_roll, = ax.plot(
        lp_roll.index,
        lp_roll.values,
        linestyle="--",
        label="14-day mean",
    )

    ax.set_title("System linepack vs recent normal")
    ax.set_xlabel("gas_day_start_utc")
    ax.set_ylabel("Linepack (mscm)")
    ax.grid(True)
    ax.legend(title="Series")
    _format_date_axis(ax)

    path_lp = reports_dir / f"linepack_{report_date:%Y%m%d}.png"
    fig.savefig(path_lp, bbox_inches="tight")
    plt.close(fig)
    chart_paths["linepack"] = path_lp

    return chart_paths