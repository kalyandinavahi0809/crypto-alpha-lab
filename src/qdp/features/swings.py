"""Fractal swing points with explicit confirmation time.

Bar s is a swing high if high[s] is strictly above the k bars before it and at least as high
as the k bars after it (the tie rule makes the first of equal highs the swing). The swing is
KNOWN only at the close of bar s+k: ``confirmed_swings`` stamps it there. The ``leaky``
variant stamps it at bar s (using future bars) and exists only to prove that the causality
checker catches look-ahead.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view


def _fractal_mask(x: np.ndarray, k: int, high: bool) -> np.ndarray:
    n = len(x)
    mask = np.zeros(n, dtype=bool)
    if n < 2 * k + 1:
        return mask
    win = sliding_window_view(x, 2 * k + 1)          # window centred on s = i + k
    centre = win[:, k]
    left, right = win[:, :k], win[:, k + 1:]
    if high:
        ok = (centre > left.max(axis=1)) & (centre >= right.max(axis=1))
    else:
        ok = (centre < left.min(axis=1)) & (centre <= right.min(axis=1))
    mask[k:n - k] = ok
    return mask


def confirmed_swings(high: np.ndarray, low: np.ndarray, k: int, leaky: bool = False) -> pd.DataFrame:
    """Per-bar columns: sh_level / sl_level = level of a swing *confirmed at this bar*
    (NaN otherwise) and sh_bar / sl_bar = index of the swing bar itself."""
    high, low = np.asarray(high, float), np.asarray(low, float)
    n = len(high)
    out = {c: np.full(n, np.nan) for c in ("sh_level", "sl_level", "sh_bar", "sl_bar")}
    lag = 0 if leaky else k
    for name, x, is_high in (("sh", high, True), ("sl", low, False)):
        s = np.nonzero(_fractal_mask(x, k, is_high))[0]
        t = s + lag
        keep = t < n
        out[f"{name}_level"][t[keep]] = x[s[keep]]
        out[f"{name}_bar"][t[keep]] = s[keep]
    return pd.DataFrame(out)
