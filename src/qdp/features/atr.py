"""Average True Range (Wilder). Value at bar t uses bars <= t only."""
from __future__ import annotations

import numpy as np


def true_range(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    prev = np.concatenate([[np.nan], close[:-1]])
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev), np.abs(low - prev)))
    tr[0] = high[0] - low[0]
    return tr


def wilder_atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, n: int = 14) -> np.ndarray:
    tr = true_range(np.asarray(high, float), np.asarray(low, float), np.asarray(close, float))
    out = np.full(len(tr), np.nan)
    if len(tr) < n:
        return out
    out[n - 1] = tr[:n].mean()
    for i in range(n, len(tr)):
        out[i] = out[i - 1] + (tr[i] - out[i - 1]) / n
    return out
