from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Dict

import pandas as pd


def write_markdown_report(
    scored_df: pd.DataFrame,
    report_date: date,
    chart_paths: Dict[str, Path],
    out_path: Path,
) -> None:
    """
    Write a markdown report summarising gas tightness.

    Parameters
    ----------
    scored_df : DataFrame
        Daily panel including tightness metrics.
    report_date : date
        As-of gas day.
    chart_paths : dict
        Mapping from chart name to image Path.
    out_path : Path
        Where to write the .md file.
    """
    # Use the data as-is; no extra date filtering here either
    df = scored_df.copy()

    if df.empty:
        raise ValueError("No data available for report.")

    latest = df.iloc[-1]

    headline_label = str(latest.get("tightness_label", "neutral"))
    latest_score = float(latest.get("tightness_score", float("nan")))
    latest_lp = float(latest.get("linepack", float("nan")))
    latest_lp_dev = float(latest.get("linepack_dev", float("nan")))

    lines: list[str] = []

    # ---------- Heading and headline ----------
    lines.append(f"# GB Gas Tightness – {report_date:%Y-%m-%d}")
    lines.append("")
    lines.append("## Headline")
    lines.append("")
    lines.append(f"- System assessed as **{headline_label}** on the day.")
    lines.append(f"- Latest tightness score: **{latest_score:+.2f}**")
    lines.append(
        f"- Latest linepack: **{latest_lp:,.0f}** "
        f"({latest_lp_dev:+.2f}σ vs recent normal)."
    )
    lines.append("")

    # ---------- Charts section ----------
    lines.append("## Charts")
    lines.append("")

    def rel(path: Path) -> str:
        return path.name

    if "supply_vs_demand" in chart_paths:
        lines.append(f"![Supply vs demand]({rel(chart_paths['supply_vs_demand'])})")
        lines.append("")
    if "imbalance_and_score" in chart_paths:
        lines.append(
            f"![Imbalance and tightness score]({rel(chart_paths['imbalance_and_score'])})"
        )
        lines.append("")
    if "linepack" in chart_paths:
        lines.append(f"![Linepack vs recent normal]({rel(chart_paths['linepack'])})")
        lines.append("")

    # ---------- Recent 7-day summary ----------
    recent = df.last("7D").copy()
    daily = (
        recent.reset_index()
        .groupby(recent.index.date)
        .agg(
            tightness_score=("tightness_score", "mean"),
            tightness_label=("tightness_label", lambda x: x.tail(1).iloc[0]),
            lp_dev_min=("linepack_dev", "min"),
            lp_dev_max=("linepack_dev", "max"),
        )
        .reset_index(names="gas_day")
    )

    lines.append("## Recent 7-day summary")
    lines.append("")

    if not daily.empty:
        header = "| Gas day | Label | Score | Min lp dev (σ) | Max lp dev (σ) |"
        sep = "|---------|-------|-------|----------------|----------------|"
        lines.append(header)
        lines.append(sep)
        for _, row in daily.iterrows():
            lines.append(
                f"| {row['gas_day']} | {row['tightness_label']} | "
                f"{row['tightness_score']:+.2f} | "
                f"{row['lp_dev_min']:+.2f} | {row['lp_dev_max']:+.2f} |"
            )
    else:
        lines.append("_Not enough history yet to show a 7-day summary._")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8")
