"""RAW -> CLEANED.

* klines: concatenate monthly partitions, de-duplicate on open_time (last write wins),
  verify the own grid, flag OHLC violations, log gaps (never filled).
* funding: de-duplicate, normalize the venue's millisecond jitter to the funding minute,
  keep the venue-reported interval per event and the observed spacing.
* perp_bars: exact own-grid join of last-price and mark-price bars (missing mark bars logged).
* instrument master: tick size inferred from observed price precision per month and the
  funding interval read from the funding history are applied as effective-dated versions;
  every change is logged (contract-change log).
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from .grid import align_exact, check_on_grid, find_gaps, interval_ms
from .instruments import InstrumentMaster, make_instrument_id
from .layers import Layer, Store

log = logging.getLogger(__name__)

PRICE_COLS = ["open", "high", "low", "close"]


def _part(venue: str, symbol: str, interval: str | None) -> dict[str, str]:
    return {"venue": venue, "symbol": symbol, "interval": interval or "na"}


def clean_klines_frame(raw: pd.DataFrame, interval: str) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """Returns (clean bars, gap log, n_ohlc_violations)."""
    df = raw.drop(columns=[c for c in ("venue", "symbol", "interval", "month") if c in raw])
    df = df.sort_values("open_time", kind="stable")
    df = df.drop_duplicates("open_time", keep="last").reset_index(drop=True)
    df = df.dropna(subset=PRICE_COLS)
    check_on_grid(df["open_time"].to_numpy(), interval)
    bad = ((df["high"] < df["low"]) | (df["high"] < df[["open", "close"]].max(axis=1)) |
           (df["low"] > df[["open", "close"]].min(axis=1)))
    df["ohlc_flag"] = bad.to_numpy()
    gaps = find_gaps(df["open_time"].to_numpy(), interval)
    return df, gaps, int(bad.sum())


def normalize_funding_frame(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.drop(columns=[c for c in ("venue", "symbol", "interval", "month") if c in raw])
    df = df.sort_values("funding_time", kind="stable")
    # Binance stamps events a few ms after the funding minute (e.g. ...00:00:00.001).
    df["funding_time_raw"] = df["funding_time"].astype("int64")
    df["funding_time"] = (df["funding_time_raw"] + 30_000) // 60_000 * 60_000
    df = df.drop_duplicates("funding_time", keep="last").reset_index(drop=True)
    spacing_h = np.diff(df["funding_time"].to_numpy(), prepend=np.nan) / 3_600_000
    df["observed_spacing_h"] = spacing_h
    return df[["funding_time", "funding_rate", "funding_interval_hours", "observed_spacing_h",
               "funding_time_raw"]]


def infer_monthly_tick(raw: pd.DataFrame) -> pd.DataFrame:
    """Observed tick per month = 10**-(max decimals used by any OHLC price that month)."""
    g = raw.groupby("month")["px_decimals"].max().sort_index()
    dec = g.to_numpy().astype(int)
    return pd.DataFrame({"month": g.index, "decimals": dec,
                         "tick_observed": [round(10.0 ** -d, int(d)) for d in dec]})


def apply_observed_ticks(master: InstrumentMaster, instrument_id: str, ticks: pd.DataFrame) -> None:
    prev = master.history(instrument_id)[0].tick_size
    for r in ticks.itertuples():
        t = float(r.tick_observed)
        if not np.isclose(t, prev, rtol=1e-9, atol=0):
            master.add_version(instrument_id, f"{r.month}-01",
                               reason=f"tick size observed from price precision ({prev:g} -> {t:g})",
                               tick_size=t)
            prev = t


def apply_funding_intervals(master: InstrumentMaster, instrument_id: str, funding: pd.DataFrame) -> None:
    """Funding interval is read from the history (venue-reported, else observed spacing)."""
    f = funding.copy()
    rep = f["funding_interval_hours"]
    f["interval_h"] = rep.where(rep.notna(), f["observed_spacing_h"].round(3))
    f = f.dropna(subset=["interval_h"])
    prev = None
    for t, ih in zip(f["funding_time"].to_numpy(), f["interval_h"].to_numpy()):
        if prev is None or ih != prev:
            ts = pd.Timestamp(int(t), unit="ms", tz="UTC")
            if prev is None:
                ts = master.history(instrument_id)[0].valid_from
            master.add_version(instrument_id, ts,
                               reason=f"funding interval read from funding history: {prev} -> {ih}h",
                               funding_interval_hours=float(ih))
            prev = ih


def clean_series(store: Store, dataset: str, venue: str, symbol: str,
                 interval: str | None) -> dict:
    raw = store.read_dataset(Layer.RAW, dataset, _part(venue, symbol, interval))
    if raw.empty:
        raise FileNotFoundError(f"no RAW data for {dataset} {venue} {symbol} {interval}")
    part = _part(venue, symbol, interval)
    if dataset == "funding":
        clean = normalize_funding_frame(raw)
        store.write_partition(Layer.CLEANED, dataset, part, clean)
        return {"dataset": dataset, "symbol": symbol, "interval": interval, "rows": len(clean),
                "start": clean["funding_time"].iloc[0], "end": clean["funding_time"].iloc[-1],
                "missing_bars": 0, "gaps": 0, "ohlc_flags": 0, "raw": raw, "clean": clean}
    clean, gaps, nbad = clean_klines_frame(raw, interval)
    store.write_partition(Layer.CLEANED, dataset, part, clean)
    gaps = gaps.assign(dataset=dataset, venue=venue, symbol=symbol, interval=interval)
    store.write_partition(Layer.CLEANED, "gaps", {**part, "dataset": dataset}, gaps)
    if len(gaps):
        log.warning("%s %s %s: %d gaps (%d missing bars)", dataset, symbol, interval,
                    len(gaps), gaps["missing_bars"].sum())
    return {"dataset": dataset, "symbol": symbol, "interval": interval, "rows": len(clean),
            "start": int(clean["open_time"].iloc[0]), "end": int(clean["open_time"].iloc[-1]),
            "missing_bars": int(gaps["missing_bars"].sum()) if len(gaps) else 0,
            "gaps": len(gaps), "ohlc_flags": nbad, "raw": raw, "clean": clean}


def build_perp_bars(store: Store, venue: str, symbol: str, interval: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    part = _part(venue, symbol, interval)
    last = store.read_partition(Layer.CLEANED, "klines", part)
    mark = store.read_partition(Layer.CLEANED, "mark_klines", part)
    last = last[["open_time", "open", "high", "low", "close", "volume", "quote_volume", "ohlc_flag"]]
    mark = mark[["open_time", "open", "high", "low", "close"]].rename(
        columns={c: f"mark_{c}" for c in PRICE_COLS})
    joined, missing = align_exact(last, mark, on="open_time", suffix="")
    joined["mark_missing"] = joined["mark_close"].isna()
    store.write_partition(Layer.CLEANED, "perp_bars", part, joined)
    return joined, missing


def run_clean(store: Store, master: InstrumentMaster, venue: str, spot_venue: str,
              symbols: list[str], intervals: list[str], spot_interval: str = "1d",
              venue_info: list[dict] | None = None,
              venue_brackets: dict[str, dict] | None = None) -> pd.DataFrame:
    summary = []
    for sym in symbols:
        iid = make_instrument_id(venue, sym, "perp")
        for iv in intervals:
            for ds in ("klines", "mark_klines"):
                r = clean_series(store, ds, venue, sym, iv)
                if ds == "klines" and iv == intervals[0]:
                    apply_observed_ticks(master, iid, infer_monthly_tick(r["raw"]))
                summary.append({k: v for k, v in r.items() if k not in ("raw", "clean")})
            _, missing = build_perp_bars(store, venue, sym, iv)
            summary.append({"dataset": "perp_bars.mark_missing", "symbol": sym, "interval": iv,
                            "rows": len(missing)})
        r = clean_series(store, "funding", venue, sym, None)
        apply_funding_intervals(master, iid, r["clean"])
        summary.append({k: v for k, v in r.items() if k not in ("raw", "clean")})
        r = clean_series(store, "spot_klines", spot_venue, sym, spot_interval)
        summary.append({k: v for k, v in r.items() if k not in ("raw", "clean")})
    if venue_brackets:
        store.write_table(Layer.CLEANED, "venue_margin_brackets",
                          apply_venue_brackets(master, venue, venue_brackets, symbols))
    if venue_info:
        ver = verify_against_venue(master, venue, venue_info, symbols)
        store.write_table(Layer.CLEANED, "venue_reference_check", ver)
        for r in ver.itertuples():
            if getattr(r, "status", "") == "ok" and not (r.listing_match and r.tick_match):
                log.warning("instrument master differs from venue for %s: %s", r.symbol, r._asdict())
    store.write_table(Layer.CLEANED, "instrument_master", master.to_frame())
    chg = pd.DataFrame(master.change_log)
    if len(chg):
        chg["old"] = chg["old"].astype(str)
        chg["new"] = chg["new"].astype(str)
    store.write_table(Layer.CLEANED, "contract_changes", chg)
    out = pd.DataFrame(summary)
    store.write_table(Layer.CLEANED, "clean_summary", out)
    return out


def verify_against_venue(master: InstrumentMaster, venue: str, info: list[dict],
                         symbols: list[str]) -> pd.DataFrame:
    """Cross-check the master's listing date and current tick size against the venue's
    published exchangeInfo; mismatches are reported, the venue value is applied for ticks."""
    rows = []
    by_sym = {r["symbol"]: r for r in info}
    for sym in symbols:
        iid = make_instrument_id(venue, sym, "perp")
        cur = master.get(iid)
        v = by_sym.get(sym)
        if v is None:
            rows.append({"symbol": sym, "status": "not in venue exchangeInfo"})
            continue
        listing = pd.Timestamp(v["onboard_date"], unit="ms", tz="UTC")
        rows.append({"symbol": sym, "venue_listing": listing, "master_listing": cur.listing_date,
                     "listing_match": listing.normalize() == cur.listing_date.normalize(),
                     "venue_tick_now": v["tick_size"], "master_tick_now": cur.tick_size,
                     "tick_match": bool(np.isclose(v["tick_size"], cur.tick_size)),
                     "status": "ok"})
    return pd.DataFrame(rows)


def apply_venue_brackets(master: InstrumentMaster, venue: str, brackets: dict[str, dict],
                         symbols: list[str]) -> pd.DataFrame:
    """Replace the configured maintenance-margin tiers with the venue-published ones.

    The venue publishes only its current tiers (no history), so they apply to the whole
    sample; this is logged as a contract version. The venue's own maintenance amounts are
    checked against the continuity formula used by the simulator."""
    rows = []
    for sym in symbols:
        iid = make_instrument_id(venue, sym, "perp")
        b = brackets.get(sym)
        if not b:
            rows.append({"symbol": sym, "source": "config snapshot (venue tiers unavailable)"})
            continue
        tiers = tuple((f, r) for f, r, _ in b["brackets"])
        first = master.history(iid)[0].valid_from
        old = master.get(iid).mmr_brackets
        master.add_version(iid, first, reason="maintenance-margin tiers from venue (current, "
                           f"updated {pd.Timestamp(b['update_time'], unit='ms'):%Y-%m-%d})",
                           mmr_brackets=tiers)
        inst = master.get(iid)
        cum_ok = all(np.isclose(inst.maintenance(f)[1], cum, atol=1e-6) for f, _, cum in b["brackets"])
        rows.append({"symbol": sym, "source": "venue", "update_time": pd.Timestamp(b["update_time"], unit="ms", tz="UTC"),
                     "tier1_mmr": tiers[0][1], "tiers": len(tiers), "config_tier1_mmr": old[0][1],
                     "cum_matches_formula": cum_ok})
    return pd.DataFrame(rows)


def load_master(store: Store) -> InstrumentMaster:
    return InstrumentMaster.from_frame(store.read_table(Layer.CLEANED, "instrument_master"))


def bar_interval_ms(interval: str) -> int:
    return interval_ms(interval)
