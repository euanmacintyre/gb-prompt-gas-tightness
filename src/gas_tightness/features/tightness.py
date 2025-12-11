from __future__ import annotations

import numpy as np
import pandas as pd


def add_tightness_metrics(
    df: pd.DataFrame,
    window_days: int = 14,
    use_linepack_if_available: bool = True,
) -> pd.DataFrame:
    """
    Add imbalance, normalised imbalance, (optional) linepack deviation,
    tightness score and tightness label.

    Works in two modes:
    - If 'linepack' column is present and use_linepack_if_available is True:
        uses both imbalance and linepack in the score.
    - Otherwise:
        uses imbalance only, and sets linepack_dev = 0.

    Assumptions
    -----------
    - df has daily frequency (one row per gas day).
    - Columns always include:
        * 'forecast_39'
        * 'actual_demand'
      and may optionally include:
        * 'linepack'

    Parameters
    ----------
    df : DataFrame
        Daily panel from build_dataset.fetch_multiple_series.
    window_days : int
        Rolling window length in days used for normalisation.
    use_linepack_if_available : bool
        If True and a 'linepack' column exists (with some non-NaN data),
        it is used in the tightness score. Otherwise it is ignored.

    Returns
    -------
    DataFrame
        Original df with new columns:
        - imbalance_raw
        - imbalance_norm
        - linepack_dev
        - tightness_score
        - tightness_label
    """
    df = df.copy()

    # ---------- 1) Forecast imbalance ----------
    df["imbalance_raw"] = df["forecast_39"] - df["actual_demand"]

    # ---------- 2) Normalise imbalance by rolling mean demand ----------
    demand_roll = df["actual_demand"].rolling(
        window_days,
        min_periods=window_days // 2,
    ).mean()

    df["imbalance_norm"] = df["imbalance_raw"] / demand_roll.replace(0, np.nan)
    df["imbalance_norm"] = df["imbalance_norm"].clip(-5, 5)

    # ---------- 3) Linepack deviation (if available) ----------
    has_linepack = (
        use_linepack_if_available
        and "linepack" in df.columns
        and df["linepack"].notna().any()
    )

    if has_linepack:
        lp_mean = df["linepack"].rolling(
            window_days,
            min_periods=window_days // 2,
        ).mean()
        lp_std = df["linepack"].rolling(
            window_days,
            min_periods=window_days // 2,
        ).std()

        df["linepack_dev"] = (df["linepack"] - lp_mean) / lp_std.replace(0, np.nan)
        df["linepack_dev"] = df["linepack_dev"].clip(-5, 5)

        # Weight imbalance more than linepack
        w_imb = 0.7
        w_lp = 0.3
    else:
        # No linepack available: set dev to 0 and ignore in scoring
        df["linepack_dev"] = 0.0
        w_imb = 1.0
        w_lp = 0.0

    # ---------- 4) Combine into a single score ----------
    df["tightness_score"] = (
        w_imb * df["imbalance_norm"].fillna(0.0)
        + w_lp * df["linepack_dev"].fillna(0.0)
    )

    # ---------- 5) Score -> label ----------
    def label_from_score(x: float) -> str:
        if np.isnan(x):
            return "neutral"
        if x >= 0.75:
            return "long"
        if x <= -0.75:
            return "short"
        return "neutral"

    df["tightness_label"] = df["tightness_score"].apply(label_from_score)

    return df
