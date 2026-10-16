"""Effective-dated instrument master.

Each instrument has a stable ``instrument_id`` ("<venue>.<symbol>.<asset_class>") and one
or more *versions* with ``[valid_from, valid_to)`` validity. Contract changes (tick size,
funding interval, relisting) are new versions; lookups are always ``as of`` a timestamp.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from .config import CONFIG_DIR, load_yaml

UTC = "UTC"
OPEN_END = pd.Timestamp("2262-01-01", tz=UTC)


def make_instrument_id(venue: str, symbol: str, asset_class: str) -> str:
    return f"{venue}.{symbol}.{asset_class}"


def _ts(x) -> pd.Timestamp:
    if x is None:
        return OPEN_END
    t = pd.Timestamp(x)
    return t.tz_localize(UTC) if t.tzinfo is None else t.tz_convert(UTC)


@dataclass(frozen=True)
class Instrument:
    instrument_id: str
    venue: str
    symbol: str
    asset_class: str  # "perp" | "spot"
    base: str
    quote: str
    multiplier: float
    tick_size: float
    lot_size: float
    maker_fee: float
    taker_fee: float
    listing_date: pd.Timestamp
    valid_from: pd.Timestamp
    valid_to: pd.Timestamp = OPEN_END
    funding_interval_hours: float | None = None
    mmr_brackets: tuple[tuple[float, float], ...] = field(default_factory=tuple)
    source: str = "config"
    note: str = ""

    @property
    def is_perp(self) -> bool:
        return self.asset_class == "perp"

    def valid_at(self, ts: pd.Timestamp) -> bool:
        return self.valid_from <= ts < self.valid_to

    def maintenance(self, notional: float) -> tuple[float, float]:
        """(maintenance margin rate, maintenance amount 'cum') for a position notional.

        Binance defines cum_i = cum_{i-1} + floor_i * (mmr_i - mmr_{i-1}) so that the
        maintenance margin is continuous across brackets.
        """
        if not self.mmr_brackets:
            raise ValueError(f"{self.instrument_id} has no maintenance-margin brackets")
        cum, prev_rate, rate = 0.0, None, None
        for floor, r in self.mmr_brackets:
            if prev_rate is not None:
                cum += floor * (r - prev_rate)
            if notional >= floor:
                rate, cum_at = r, cum
            prev_rate = r
        if rate is None:  # below the first floor
            rate, cum_at = self.mmr_brackets[0][1], 0.0
        return rate, cum_at


class InstrumentMaster:
    def __init__(self, versions: Iterable[Instrument] = ()):
        self._versions: dict[str, list[Instrument]] = {}
        self.change_log: list[dict[str, Any]] = []
        for v in versions:
            self._versions.setdefault(v.instrument_id, []).append(v)
        for vs in self._versions.values():
            vs.sort(key=lambda r: r.valid_from)

    # --------------------------------------------------------------- loading
    @classmethod
    def from_yaml(cls, path: str | Path | None = None) -> "InstrumentMaster":
        cfg = load_yaml(path or CONFIG_DIR / "instruments.yaml")
        defaults = cfg.get("defaults", {})
        versions = []
        for venue, vcfg in cfg["venues"].items():
            ac = vcfg["asset_class"]
            for sym, spec in vcfg["instruments"].items():
                d = {**defaults.get(ac, {}), **spec}
                listing = _ts(d["listing_date"])
                brackets = tuple((float(f), float(r)) for f, r in d.get("mmr_brackets", []))
                versions.append(Instrument(
                    instrument_id=make_instrument_id(venue, sym, ac), venue=venue, symbol=sym,
                    asset_class=ac, base=d["base"], quote=d.get("quote", "USDT"),
                    multiplier=float(d.get("multiplier", 1.0)), tick_size=float(d["tick_size"]),
                    lot_size=float(d.get("lot_size", 0.0)), maker_fee=float(d["maker_fee"]),
                    taker_fee=float(d["taker_fee"]), listing_date=listing,
                    valid_from=_ts(d.get("valid_from", "2000-01-01")),
                    valid_to=_ts(d.get("valid_to")),
                    funding_interval_hours=d.get("funding_interval_hours"),
                    mmr_brackets=brackets, source="config"))
        return cls(versions)

    # --------------------------------------------------------------- queries
    @property
    def instrument_ids(self) -> list[str]:
        return sorted(self._versions)

    def history(self, instrument_id: str) -> list[Instrument]:
        return list(self._versions[instrument_id])

    def get(self, instrument_id: str, asof=None) -> Instrument:
        vs = self._versions.get(instrument_id)
        if not vs:
            raise KeyError(instrument_id)
        if asof is None:
            return vs[-1]
        ts = _ts(asof)
        for v in vs:
            if v.valid_at(ts):
                return v
        raise KeyError(f"{instrument_id} has no version valid at {ts}")

    def find(self, venue: str, symbol: str, asset_class: str | None = None, asof=None) -> Instrument:
        if asset_class is None:
            matches = [i for i in self._versions if i.startswith(f"{venue}.{symbol}.")]
            if len(matches) != 1:
                raise KeyError(f"ambiguous/missing instrument for {venue} {symbol}: {matches}")
            return self.get(matches[0], asof)
        return self.get(make_instrument_id(venue, symbol, asset_class), asof)

    def field_series(self, instrument_id: str, field_name: str, ts) -> np.ndarray:
        """Vector of a field's effective value at each timestamp in ``ts``."""
        idx = pd.DatetimeIndex(pd.to_datetime(ts, utc=True))
        out = np.full(len(idx), np.nan)
        for v in self._versions[instrument_id]:
            m = (idx >= v.valid_from) & (idx < v.valid_to)
            val = getattr(v, field_name)
            out[m] = np.nan if val is None else float(val)
        return out

    # --------------------------------------------------------------- updates
    def add_version(self, instrument_id: str, valid_from, reason: str, **changes) -> Instrument:
        """Set ``changes`` from ``valid_from`` onward (contract change).

        The version active at ``valid_from`` is split at ``valid_from``; the changed fields
        are applied to the new segment and to every later version (later change points are
        applied by later calls, in chronological order). Every change is logged.
        """
        start = _ts(valid_from)
        vs = self._versions[instrument_id]
        cur = self.get(instrument_id, start)
        old = {k: getattr(cur, k) for k in changes}
        if all(old[k] == v for k, v in changes.items()):
            return cur
        i = vs.index(cur)
        if cur.valid_from == start:
            vs[i] = replace(cur, **changes, source="observed", note=reason)
        else:
            vs[i:i + 1] = [replace(cur, valid_to=start),
                           replace(cur, valid_from=start, **changes, source="observed", note=reason)]
            i += 1
        for j in range(i + 1, len(vs)):
            vs[j] = replace(vs[j], **changes)
        self.change_log.append({"instrument_id": instrument_id, "valid_from": start,
                                "reason": reason, "old": old, "new": dict(changes)})
        return vs[i]

    # --------------------------------------------------------------- export
    def to_frame(self) -> pd.DataFrame:
        rows = []
        for vs in self._versions.values():
            for v in vs:
                d = asdict(v)
                d["mmr_brackets"] = repr(list(v.mmr_brackets))
                rows.append(d)
        return pd.DataFrame(rows).sort_values(["instrument_id", "valid_from"]).reset_index(drop=True)

    @classmethod
    def from_frame(cls, df: pd.DataFrame) -> "InstrumentMaster":
        import ast
        versions = []
        for r in df.to_dict("records"):
            r = dict(r)
            r["mmr_brackets"] = tuple(tuple(x) for x in ast.literal_eval(r["mmr_brackets"]))
            for k in ("listing_date", "valid_from", "valid_to"):
                r[k] = _ts(r[k])
            fih = r.get("funding_interval_hours")
            r["funding_interval_hours"] = None if fih is None or pd.isna(fih) else float(fih)
            versions.append(Instrument(**r))
        return cls(versions)
