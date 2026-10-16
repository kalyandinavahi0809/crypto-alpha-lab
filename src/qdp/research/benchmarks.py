"""Passive benchmarks: buy-and-hold perp (with funding), buy-and-hold spot, equal-weight basket."""
from __future__ import annotations

import numpy as np
import pandas as pd

DAY_MS = 86_400_000


def perp_daily_returns(bars: pd.DataFrame, funding: pd.DataFrame, interval_ms: int) -> pd.Series:
    """Daily returns of a 1x long perp position: price return of the last close of each UTC day
    minus the realized funding paid by longs during that day (funding ~ rate x notional)."""
    close_t = pd.to_datetime(bars["open_time"].to_numpy(np.int64) + interval_ms - 1, unit="ms", utc=True)
    px = pd.Series(bars["close"].to_numpy(float), index=close_t)
    day_px = px.groupby(px.index.floor("D")).last()
    fr = pd.Series(funding["funding_rate"].to_numpy(float),
                   index=pd.to_datetime(funding["funding_time"].to_numpy(np.int64), unit="ms", utc=True))
    # funding at T is paid by positions held at T: attribute it to the day containing T
    day_f = fr.groupby(fr.index.floor("D")).sum().reindex(day_px.index, fill_value=0.0)
    ret = day_px.pct_change() - day_f
    return ret.iloc[1:]


def spot_daily_returns(spot: pd.DataFrame) -> pd.Series:
    t = pd.to_datetime(spot["open_time"].to_numpy(np.int64), unit="ms", utc=True)
    px = pd.Series(spot["close"].to_numpy(float), index=t.floor("D"))
    return px.pct_change().iloc[1:]


def window_stats(daily_ret: pd.Series, start, end, entry_cost: float = 0.0) -> dict:
    r = daily_ret[(daily_ret.index >= start) & (daily_ret.index < end)].dropna()
    if len(r) < 3:
        return {"total_return": np.nan, "cagr": np.nan, "sharpe": np.nan, "max_dd": np.nan, "days": len(r)}
    eq = (1 - entry_cost) * (1 + r).cumprod()
    years = len(r) / 365.0
    dd = (eq / eq.cummax() - 1).min()
    sd = r.std(ddof=1)
    return {"total_return": eq.iloc[-1] - 1, "cagr": eq.iloc[-1] ** (1 / years) - 1,
            "sharpe": np.sqrt(365) * r.mean() / sd if sd > 0 else np.nan, "max_dd": dd,
            "days": len(r), "start": r.index[0], "end": r.index[-1]}


def equal_weight_basket(rets: dict[str, pd.Series]) -> pd.Series:
    """Daily-rebalanced equal weight across the members trading that day (own grids)."""
    df = pd.DataFrame(rets)
    return df.mean(axis=1, skipna=True)
