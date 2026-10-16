"""Overfitting-aware statistics: probabilistic and deflated Sharpe ratio, percentiles.

Bailey & Lopez de Prado (2014), "The Deflated Sharpe Ratio": the expected maximum Sharpe of
N independent trials with cross-trial Sharpe variance V is
    SR0 = sqrt(V) * ((1 - g) * Z^-1(1 - 1/N) + g * Z^-1(1 - 1/(N e))),  g = Euler-Mascheroni
and DSR = PSR(SR0) with non-normality (skew, kurtosis) adjustment. Sharpe ratios here are
per-observation (not annualized).
"""
from __future__ import annotations

import math
from statistics import NormalDist

import numpy as np

_N = NormalDist()
EULER_GAMMA = 0.5772156649015329


def probabilistic_sharpe(sr: float, sr_benchmark: float, n_obs: int, skew: float,
                         kurt: float) -> float:
    """P(true SR > benchmark) given an observed per-period SR over n_obs observations.
    ``kurt`` is the raw (non-excess) kurtosis."""
    if n_obs < 2 or not np.isfinite(sr):
        return float("nan")
    denom = 1.0 - skew * sr + (kurt - 1.0) / 4.0 * sr * sr
    if denom <= 0:
        return float("nan")
    z = (sr - sr_benchmark) * math.sqrt(n_obs - 1) / math.sqrt(denom)
    return _N.cdf(z)


def expected_max_sharpe(n_trials: int, var_sr: float) -> float:
    if n_trials <= 1 or var_sr <= 0:
        return 0.0
    a = _N.inv_cdf(1.0 - 1.0 / n_trials)
    b = _N.inv_cdf(1.0 - 1.0 / (n_trials * math.e))
    return math.sqrt(var_sr) * ((1 - EULER_GAMMA) * a + EULER_GAMMA * b)


def deflated_sharpe(returns: np.ndarray, trial_srs: np.ndarray, n_trials: int | None = None) -> dict:
    """DSR of a selected strategy's per-period ``returns`` given all trials' per-period SRs."""
    r = np.asarray(returns, float)
    r = r[np.isfinite(r)]
    trial_srs = np.asarray(trial_srs, float)
    trial_srs = trial_srs[np.isfinite(trial_srs)]
    n_trials = int(n_trials or len(trial_srs))
    if len(r) < 3 or r.std(ddof=1) == 0:
        return {"sr": np.nan, "sr0": np.nan, "dsr": np.nan, "psr0": np.nan, "n_trials": n_trials,
                "n_obs": len(r)}
    sr = r.mean() / r.std(ddof=1)
    c = r - r.mean()
    skew = (c ** 3).mean() / (c ** 2).mean() ** 1.5
    kurt = (c ** 4).mean() / (c ** 2).mean() ** 2
    sr0 = expected_max_sharpe(n_trials, trial_srs.var(ddof=1) if len(trial_srs) > 1 else 0.0)
    return {"sr": sr, "sr_annual": sr * math.sqrt(365), "sr0": sr0, "sr0_annual": sr0 * math.sqrt(365),
            "dsr": probabilistic_sharpe(sr, sr0, len(r), skew, kurt),
            "psr0": probabilistic_sharpe(sr, 0.0, len(r), skew, kurt),
            "skew": skew, "kurtosis": kurt, "n_trials": n_trials, "n_obs": len(r)}


def percentile_of(value: float, dist: np.ndarray) -> float:
    """Percentile rank (0-100) of ``value`` within ``dist`` (ties count half)."""
    d = np.asarray(dist, float)
    d = d[np.isfinite(d)]
    if len(d) == 0 or not np.isfinite(value):
        return float("nan")
    return 100.0 * ((d < value).sum() + 0.5 * (d == value).sum()) / len(d)
