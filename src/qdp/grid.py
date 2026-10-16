"""Own-grid time-series operations.

Each instrument lives on its *own* bar grid (its own listing date, its own outages). Ops here
never forward-fill across a common calendar: joins are exact-timestamp joins on the
instrument's grid, gaps are detected and logged, and resampling only aggregates bars that
exist. Timestamps are int64 milliseconds since epoch (bar open time, UTC).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

INTERVAL_MS = {
    "1m": 60_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "8h": 28_800_000,
    "12h": 43_200_000, "1d": 86_400_000,
}


def interval_ms(interval: str) -> int:
    try:
        return INTERVAL_MS[interval]
    except KeyError as exc:
        raise ValueError(f"unsupported interval {interval!r}") from exc


def expected_grid(start_ms: int, end_ms: int, interval: str) -> np.ndarray:
    """Inclusive grid of bar open times from start to end."""
    step = interval_ms(interval)
    if start_ms % step or end_ms % step:
        raise ValueError("grid endpoints must be aligned to the interval")
    return np.arange(start_ms, end_ms + step, step, dtype=np.int64)


def find_gaps(ts: np.ndarray, interval: str) -> pd.DataFrame:
    """Missing-bar runs on the instrument's own grid (between its first and last bar)."""
    ts = np.asarray(ts, dtype=np.int64)
    step = interval_ms(interval)
    if len(ts) < 2:
        return pd.DataFrame(columns=["gap_start", "gap_end", "missing_bars"])
    d = np.diff(ts)
    if (d <= 0).any():
        raise ValueError("timestamps must be strictly increasing")
    idx = np.nonzero(d != step)[0]
    rows = []
    for i in idx:
        if d[i] % step:
            raise ValueError(f"off-grid timestamp after {ts[i]}")
        rows.append({"gap_start": int(ts[i] + step), "gap_end": int(ts[i + 1] - step),
                     "missing_bars": int(d[i] // step - 1)})
    return pd.DataFrame(rows, columns=["gap_start", "gap_end", "missing_bars"])


def check_on_grid(ts: np.ndarray, interval: str) -> None:
    ts = np.asarray(ts, dtype=np.int64)
    if (ts % interval_ms(interval)).any():
        raise ValueError("timestamps not aligned to interval grid")


def align_exact(left: pd.DataFrame, right: pd.DataFrame, on: str = "open_time",
                suffix: str = "_r") -> tuple[pd.DataFrame, pd.DataFrame]:
    """Left-join ``right`` onto ``left``'s own grid by exact timestamp (no fill).

    Returns (joined, missing) where ``missing`` lists left timestamps absent from right.
    """
    r = right.set_index(on)
    r.columns = [f"{c}{suffix}" for c in r.columns]
    joined = left.join(r, on=on, how="left")
    probe = r.columns[0]
    missing = joined.loc[joined[probe].isna(), [on]].reset_index(drop=True)
    return joined, missing


def resample_ohlc(df: pd.DataFrame, interval_from: str, interval_to: str,
                  require_complete: bool = True) -> pd.DataFrame:
    """Aggregate OHLCV bars on the instrument's own grid.

    With ``require_complete`` a target bar is emitted only if every source bar exists; a
    partial bucket is dropped rather than fabricated.
    """
    step_from, step_to = interval_ms(interval_from), interval_ms(interval_to)
    if step_to % step_from:
        raise ValueError("target interval must be a multiple of source interval")
    n = step_to // step_from
    g = df["open_time"].to_numpy(np.int64) // step_to * step_to
    agg = df.assign(_bucket=g).groupby("_bucket", sort=True).agg(
        open=("open", "first"), high=("high", "max"), low=("low", "min"),
        close=("close", "last"), volume=("volume", "sum"), _n=("open", "size"))
    if require_complete:
        agg = agg[agg["_n"] == n]
    return agg.drop(columns="_n").rename_axis("open_time").reset_index()


def to_datetime(ms) -> pd.DatetimeIndex:
    return pd.to_datetime(np.asarray(ms, dtype=np.int64), unit="ms", utc=True)


def map_events_to_bars(bar_open_ms: np.ndarray, event_ms: np.ndarray) -> np.ndarray:
    """Index of the first bar whose open time is >= each event time.

    An event at T inside (open[j-1], open[j]] maps to bar j: a position that is open at the
    start of bar j and was entered before bar j held the position at T. Events after the last
    bar map to len(bar_open_ms) (i.e. outside the sample).
    """
    return np.searchsorted(np.asarray(bar_open_ms, np.int64), np.asarray(event_ms, np.int64),
                           side="left")
