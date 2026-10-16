"""Trade- and equity-level performance metrics."""
from __future__ import annotations

import numpy as np
import pandas as pd

DAY_MS = 86_400_000


def equity_daily(equity: np.ndarray, open_time_ms: np.ndarray, interval_ms: int) -> pd.Series:
    """Mark-to-market equity sampled at each UTC day's last bar close."""
    close_time = pd.to_datetime(np.asarray(open_time_ms, np.int64) + interval_ms - 1, unit="ms", utc=True)
    s = pd.Series(np.asarray(equity, float), index=close_time)
    return s.groupby(s.index.floor("D")).last()


def equity_stats(daily_equity: pd.Series) -> dict:
    eq = daily_equity.dropna()
    if len(eq) < 3:
        return {"sharpe": np.nan, "sortino": np.nan, "max_dd": np.nan, "total_return": np.nan,
                "cagr": np.nan, "days": len(eq)}
    r = eq.pct_change().dropna()
    sd = r.std(ddof=1)
    sharpe = np.sqrt(365) * r.mean() / sd if sd > 0 else np.nan
    downside = np.sqrt(np.mean(np.minimum(r, 0.0) ** 2))
    sortino = np.sqrt(365) * r.mean() / downside if downside > 0 else np.nan
    dd = (eq / eq.cummax() - 1.0).min()
    years = max((eq.index[-1] - eq.index[0]).days / 365.25, 1e-9)
    total = eq.iloc[-1] / eq.iloc[0] - 1.0
    cagr = (eq.iloc[-1] / eq.iloc[0]) ** (1 / years) - 1.0 if eq.iloc[-1] > 0 else -1.0
    return {"sharpe": sharpe, "sortino": sortino, "max_dd": dd, "total_return": total,
            "cagr": cagr, "days": len(eq)}


def trade_stats(trades: pd.DataFrame, months: float, interval_ms: int,
                target_r: float | None = None) -> dict:
    n = len(trades)
    base = {"trades": n, "trades_per_month": n / months if months > 0 else np.nan}
    if n == 0:
        return {**base, "win_rate": np.nan, "breakeven_win_rate": np.nan,
                "nominal_breakeven_win_rate": (1 / (1 + target_r)) if target_r and target_r > 0 else np.nan,
                "avg_win_r": np.nan, "avg_loss_r": np.nan, "expectancy_r": np.nan,
                "expectancy_r_ex_funding": np.nan, "profit_factor": np.nan, "avg_hold_h": np.nan,
                "gross_pnl": 0.0, "fees": 0.0, "slippage": 0.0, "funding": 0.0, "net_pnl": 0.0,
                "liquidations": 0, "r_t_stat": np.nan}
    r = trades["r_multiple"].to_numpy()
    win = r > 0
    avg_w = r[win].mean() if win.any() else np.nan
    avg_l = r[~win].mean() if (~win).any() else np.nan
    if win.any() and (~win).any():
        be = abs(avg_l) / (avg_w + abs(avg_l))
    else:
        be = np.nan
    gains = trades.loc[trades["net_pnl"] > 0, "net_pnl"].sum()
    losses = -trades.loc[trades["net_pnl"] <= 0, "net_pnl"].sum()
    hold_h = (trades["exit_bar"] - trades["entry_bar"]).to_numpy() * interval_ms / 3_600_000
    t_stat = r.mean() / (r.std(ddof=1) / np.sqrt(n)) if n > 1 and r.std(ddof=1) > 0 else np.nan
    return {**base,
            "win_rate": win.mean(), "breakeven_win_rate": be,
            "nominal_breakeven_win_rate": (1 / (1 + target_r)) if target_r and target_r > 0 else np.nan,
            "avg_win_r": avg_w, "avg_loss_r": avg_l, "expectancy_r": r.mean(),
            "expectancy_r_ex_funding": trades["r_ex_funding"].mean(),
            "profit_factor": gains / losses if losses > 0 else np.inf,
            "avg_hold_h": hold_h.mean(),
            "gross_pnl": trades["gross_pnl"].sum(), "fees": trades["fees"].sum(),
            "slippage": trades["slippage"].sum(), "funding": trades["funding"].sum(),
            "net_pnl": trades["net_pnl"].sum(),
            "liquidations": int((trades["exit_reason"] == 4).sum()), "r_t_stat": t_stat}
