"""CLEANED -> FEATURES for the market-structure study, plus causality audits on real data."""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..causal import CausalityError, assert_causal
from ..instruments import InstrumentMaster, make_instrument_id
from ..layers import Layer, Store
from .atr import wilder_atr
from .structure import StructureParams, structure_features

ID_COLS = ("zone_id", "arm_zid", "cf_zid")


def load_perp_bars(store: Store, venue: str, symbol: str, interval: str) -> pd.DataFrame:
    part = {"venue": venue, "symbol": symbol, "interval": interval}
    return store.read_partition(Layer.CLEANED, "perp_bars", part)


def tick_series(master: InstrumentMaster, venue: str, symbol: str, open_time_ms) -> np.ndarray:
    iid = make_instrument_id(venue, symbol, "perp")
    ts = pd.to_datetime(np.asarray(open_time_ms, np.int64), unit="ms", utc=True)
    return master.field_series(iid, "tick_size", ts)


def structure_frame(bars: pd.DataFrame, tick: np.ndarray, params: StructureParams,
                    atr_n: int = 14) -> pd.DataFrame:
    atr = wilder_atr(bars["high"].to_numpy(), bars["low"].to_numpy(), bars["close"].to_numpy(), atr_n)
    args = (bars["open"], bars["high"], bars["low"], bars["close"], atr, tick)
    lf = structure_features(*args, params, side=1).add_prefix("L_")
    sf = structure_features(*args, params, side=-1).add_prefix("S_")
    return pd.concat([pd.DataFrame({"open_time": bars["open_time"].to_numpy(), "atr": atr}),
                      lf, sf], axis=1)


def build_structure_features(store: Store, master: InstrumentMaster, venue: str, symbol: str,
                             interval: str, params: StructureParams, atr_n: int = 14) -> pd.DataFrame:
    bars = load_perp_bars(store, venue, symbol, interval)
    tick = tick_series(master, venue, symbol, bars["open_time"])
    feats = structure_frame(bars, tick, params, atr_n)
    part = {"venue": venue, "symbol": symbol, "interval": interval, "k": params.k,
            "bos": "close" if params.bos_close else "wick"}
    store.write_partition(Layer.FEATURES, "structure", part, feats)
    return feats


def causality_audit(bars: pd.DataFrame, tick: np.ndarray, params: StructureParams,
                    n_bars: int = 6000, n_cuts: int = 8, atr_n: int = 14, seed: int = 0) -> dict:
    """assert_causal on the zone / sweep / trend features over a real-data window. Returns a
    report row; ``passed`` is False (with the error) if look-ahead is detected."""
    sub = bars.iloc[-n_bars:].reset_index(drop=True)
    tk = np.asarray(tick)[-n_bars:]

    def fn(df):
        f = structure_frame(df, tk[:len(df)], params, atr_n)
        return f.drop(columns=["open_time"] + [f"{p}{c}" for p in ("L_", "S_") for c in ID_COLS])

    row = {"k": params.k, "bos_close": params.bos_close, "leaky": params.leaky,
           "bars": len(sub), "cuts": n_cuts}
    try:
        rep = assert_causal(fn, sub, n_cuts=n_cuts, min_history=500, seed=seed)
        row.update(passed=True, error="", columns=len(rep["columns"]))
    except CausalityError as exc:
        row.update(passed=False, error=str(exc)[:300], columns=np.nan)
    return row
