"""Data-coverage and integrity audits over RAW / CLEANED (per symbol, dataset, interval).

* coverage      first/last timestamp, rows, missing bars, and how many monthly partitions came
                from the verified archive vs the venue's REST API
* resample      every CLEANED 4h bar is compared with the 4h bar rebuilt from the CLEANED 1h
                bars on the instrument's own grid (last and mark price)
* funding       reported funding interval vs the observed spacing of events
* mark history  what the venue returns for mark-price klines in months before the first mark
                bar (proves absence instead of assuming it)
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .grid import find_gaps, resample_ohlc
from .layers import Layer, Store


def _raw_sources(store: Store, dataset: str, venue: str, symbol: str, interval: str | None) -> dict:
    part = {"venue": venue, "symbol": symbol, "interval": interval or "na"}
    n_arch = n_rest = 0
    for p in store.list_partitions(Layer.RAW, dataset, part):
        meta = store.read_meta(Layer.RAW, dataset, p) or {}
        if meta.get("verified"):
            n_arch += 1
        else:
            n_rest += 1
    return {"months_archive_verified": n_arch, "months_rest": n_rest}


def coverage(store: Store, venue: str, symbols: list[str], intervals: list[str]) -> pd.DataFrame:
    rows = []
    ts = lambda v: pd.Timestamp(int(v), unit="ms", tz="UTC")
    for sym in symbols:
        for iv in intervals:
            for ds in ("klines", "mark_klines"):
                df = store.read_partition(Layer.CLEANED, ds, {"venue": venue, "symbol": sym, "interval": iv})
                gaps = find_gaps(df["open_time"].to_numpy(), iv)
                rows.append({"symbol": sym, "dataset": ds, "interval": iv, "first": ts(df["open_time"].iloc[0]),
                             "last": ts(df["open_time"].iloc[-1]), "rows": len(df),
                             "missing_bars": int(gaps["missing_bars"].sum()) if len(gaps) else 0,
                             "gaps": len(gaps), **_raw_sources(store, ds, venue, sym, iv)})
        f = store.read_partition(Layer.CLEANED, "funding", {"venue": venue, "symbol": sym, "interval": "na"})
        rows.append({"symbol": sym, "dataset": "funding", "interval": "events",
                     "first": ts(f["funding_time"].iloc[0]), "last": ts(f["funding_time"].iloc[-1]),
                     "rows": len(f), "missing_bars": np.nan, "gaps": np.nan,
                     **_raw_sources(store, "funding", venue, sym, None)})
    return pd.DataFrame(rows)


def resample_consistency(store: Store, venue: str, symbol: str, fine: str = "1h",
                         coarse: str = "4h", rtol: float = 1e-9) -> list[dict]:
    """Compare stored ``coarse`` bars with bars rebuilt from stored ``fine`` bars."""
    out = []
    for ds in ("klines", "mark_klines"):
        f = store.read_partition(Layer.CLEANED, ds, {"venue": venue, "symbol": symbol, "interval": fine})
        c = store.read_partition(Layer.CLEANED, ds, {"venue": venue, "symbol": symbol, "interval": coarse})
        if "volume" not in f:
            f = f.assign(volume=0.0)
        rebuilt = resample_ohlc(f, fine, coarse, require_complete=True)
        m = c.merge(rebuilt, on="open_time", suffixes=("", "_rb"))
        bad = np.zeros(len(m), bool)
        for col in ("open", "high", "low", "close"):
            bad |= ~np.isclose(m[col].to_numpy(), m[f"{col}_rb"].to_numpy(), rtol=rtol, atol=0)
        out.append({"symbol": symbol, "dataset": ds, "coarse_bars": len(c), "compared": len(m),
                    "not_comparable": len(c) - len(m), "mismatches": int(bad.sum()),
                    "first_mismatch": pd.Timestamp(int(m.loc[bad, "open_time"].iloc[0]), unit="ms", tz="UTC")
                    if bad.any() else pd.NaT})
    return out


def funding_consistency(store: Store, venue: str, symbol: str) -> dict:
    f = store.read_partition(Layer.CLEANED, "funding", {"venue": venue, "symbol": symbol, "interval": "na"})
    rep = f["funding_interval_hours"]
    obs = f["observed_spacing_h"]
    both = rep.notna() & obs.notna()
    # an event may follow an interval change, so compare the reported interval with the
    # spacing to the NEXT event as well as the previous one
    nxt = f["observed_spacing_h"].shift(-1)
    agree = both & (np.isclose(rep, obs) | np.isclose(rep, nxt))
    return {"symbol": symbol, "events": len(f), "with_reported_interval": int(rep.notna().sum()),
            "without_reported_interval": int(rep.isna().sum()),
            "reported_vs_observed_checked": int(both.sum()),
            "reported_vs_observed_disagree": int((both & ~agree).sum()),
            "intervals_seen_h": ", ".join(f"{x:g}" for x in sorted(set(rep.dropna()) | set(obs.dropna().round(3)))
                                          if x <= 24)}


def mark_history_probe(adapter, symbol: str, months: list[str], interval: str = "1h") -> list[dict]:
    """Ask the venue for mark-price klines in each month; record what comes back."""
    out = []
    for m in months:
        res = adapter.fetch_month("mark_klines", symbol, m, interval)
        first = (pd.Timestamp(int(res.frame["open_time"].iloc[0]), unit="ms", tz="UTC")
                 if len(res.frame) else pd.NaT)
        out.append({"symbol": symbol, "month": m, "interval": interval, "rows_returned": len(res.frame),
                    "first_bar": first, "source": res.source})
    return out
