# GB Gas Tightness Monitor

An end-to-end Python project that pulls UK National Gas data (MIPI), builds a daily fundamentals panel, scores system tightness, and produces a simple daily report with charts.

The goal is to replicate the kind of internal “morning note” a gas trading or analytics desk might use to track GB NTS tightness.

---

## Features

- **MIPI API integration**
  - Uses publication IDs for:
    - Day-ahead firm demand forecast (`PUBOB39`)
    - Actual demand (`PUBOB637`)
    - NTS linepack (hourly actual, aggregate, D+1 – `PUBO…`)
  - Caching via a `NationalGasClient` wrapper.

- **Daily dataset construction**
  - Pulls raw series for the last N gas days.
  - Aggregates hourly data to a single value per gas day.
  - Combines multiple series into one panel indexed by `gas_day_start_utc`.

- **Tightness scoring model**
  - Raw imbalance: forecast − actual.
  - Normalised imbalance (% of rolling average demand).
  - Linepack deviation (z-score vs rolling mean and std).
  - Composite tightness score = 0.7 × imbalance_norm + 0.3 × linepack_dev.
  - Discrete label per day: `long`, `neutral`, or `short`.

- **Reporting**
  - Charts (Matplotlib):
    - Demand forecast vs outturn.
    - Imbalance and tightness score.
    - Linepack vs recent normal (14-day mean).
  - Markdown report:
    - Headline tightness summary for the latest day.
    - Embedded charts.
    - 7-day table of tightness metrics.

---

## Project structure

```text
gb-prompt-gas-tightness/
├── scripts/
│   └── make_report.py           # Orchestrates the daily run
├── src/
│   └── gas_tightness/
│       ├── __init__.py
│       ├── mipi_client.py       # MIPI API wrapper + caching
│       ├── features/
│       │   ├── build_dataset.py # Fetch/combine series into daily panel
│       │   └── tightness.py     # Tightness metrics / scoring
│       └── report/
│           ├── charts.py        # Matplotlib charts
│           └── render_md.py     # Markdown report writer
├── data/
│   ├── raw/                     # (git-ignored) API responses, if stored
│   └── processed/               # (git-ignored) daily_panel.parquet, etc.
├── reports/                     
└── 01_connect_and_pull_one_series.ipynb  # Dev notebook
