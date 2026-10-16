"""Causality (no look-ahead) checks.

``assert_causal`` runs a feature function on the full history and on histories truncated at
several cut points. A causal feature's value at bar t may depend only on bars <= t, so every
row before the cut must be identical between the truncated and the full run. Any difference
means the feature used data from the future and raises ``CausalityError``.
"""
from __future__ import annotations

from typing import Callable, Iterable, Sequence

import numpy as np
import pandas as pd


class CausalityError(AssertionError):
    pass


def _equal(a: np.ndarray, b: np.ndarray, rtol: float, atol: float) -> np.ndarray:
    if a.dtype.kind in "fc" or b.dtype.kind in "fc":
        a, b = a.astype(float), b.astype(float)
        both_nan = np.isnan(a) & np.isnan(b)
        with np.errstate(invalid="ignore"):
            close = np.isclose(a, b, rtol=rtol, atol=atol)
        return both_nan | close
    return a == b


def assert_causal(fn: Callable[[pd.DataFrame], pd.DataFrame], data: pd.DataFrame,
                  cutpoints: Iterable[int] | None = None, n_cuts: int = 12,
                  columns: Sequence[str] | None = None, min_history: int = 50,
                  rtol: float = 1e-12, atol: float = 0.0, seed: int = 0) -> dict:
    """Assert ``fn`` is causal on ``data``.

    fn maps a frame of bars (rows in time order) to a frame of features with the same number
    of rows. Returns a small report dict on success.
    """
    full = fn(data.reset_index(drop=True))
    if len(full) != len(data):
        raise ValueError("feature function must return one row per input bar")
    cols = list(columns) if columns is not None else list(full.columns)
    n = len(data)
    if cutpoints is None:
        rng = np.random.default_rng(seed)
        lo = min(min_history, max(1, n - 1))
        cutpoints = sorted(set(rng.integers(lo, n, size=n_cuts).tolist()) | {n - 1})
    checked = 0
    for cut in cutpoints:
        part = fn(data.iloc[:cut].reset_index(drop=True))
        for col in cols:
            a = full[col].to_numpy()[:cut]
            b = part[col].to_numpy()
            ok = _equal(a, b, rtol, atol)
            if not ok.all():
                i = int(np.argmin(ok))
                raise CausalityError(
                    f"column {col!r} changes at row {i} when history is truncated at {cut} "
                    f"(full={a[i]!r}, truncated={b[i]!r}): feature uses future data")
        checked += 1
    return {"cutpoints": checked, "columns": cols, "rows": n}
