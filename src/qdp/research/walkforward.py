"""Anchored walk-forward evaluation over a parameter grid.

Folds: train = [data start, fold start), test = next ``test_months``. The first test window
starts ``train_months`` after the data start; the train window grows by ``test_months``
each fold (anchored). In each fold the candidate with the best TRAIN objective is selected
and its TEST result is recorded; the TEST results of every grid cell are kept as the
out-of-sample distribution (reported instead of the best cell).
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def anchored_folds(start: pd.Timestamp, end: pd.Timestamp, train_months: int = 12,
                   test_months: int = 3) -> list[dict]:
    start = pd.Timestamp(start).normalize().replace(day=1)
    folds = []
    test_start = start + pd.DateOffset(months=train_months)
    while test_start < end:
        test_end = min(test_start + pd.DateOffset(months=test_months), end)
        folds.append({"train_start": start, "train_end": test_start,
                      "test_start": test_start, "test_end": test_end})
        test_start = test_start + pd.DateOffset(months=test_months)
    return folds


def walk_forward(trades: pd.DataFrame, candidates: list[str], folds: list[dict],
                 min_train_trades: int = 30, metric: str = "r_multiple") -> tuple[pd.DataFrame, pd.DataFrame]:
    """``trades`` has columns config_id, entry_ts (Timestamp), and ``metric``.

    Returns (chain, cells): chain = per-fold selected candidate with its train/test stats;
    cells = per-fold test stats for every config in ``trades``.
    """
    ts = trades["entry_ts"]
    chain, cells = [], []
    for i, f in enumerate(folds):
        tr = trades[(ts >= f["train_start"]) & (ts < f["train_end"])]
        te = trades[(ts >= f["test_start"]) & (ts < f["test_end"])]
        g_tr = tr.groupby("config_id")[metric].agg(["mean", "size"])
        g_te = te.groupby("config_id")[metric].agg(["mean", "size", "sum"])
        for cid, row in g_te.iterrows():
            cells.append({"fold": i, "config_id": cid, "test_mean_r": row["mean"],
                          "test_trades": int(row["size"]), "test_sum_r": row["sum"]})
        elig = g_tr.loc[g_tr.index.isin(candidates) & (g_tr["size"] >= min_train_trades)]
        if elig.empty:
            continue
        best = elig.sort_values(["mean", "size"], ascending=[False, False]).index[0]
        sel = te[te["config_id"] == best]
        chain.append({"fold": i, **{k: v for k, v in f.items()}, "selected": best,
                      "train_mean_r": elig.loc[best, "mean"], "train_trades": int(elig.loc[best, "size"]),
                      "test_mean_r": sel[metric].mean() if len(sel) else np.nan,
                      "test_trades": len(sel), "test_sum_r": sel[metric].sum(),
                      "test_sum_r2": float((sel[metric] ** 2).sum())})
    return pd.DataFrame(chain), pd.DataFrame(cells)
