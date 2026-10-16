import pandas as pd

from qdp.config import load_yaml
from qdp.experiments.report_md import verdict


def _tables(pct, exp, n=100, funding_bad=0, leaky_passes=False):
    rnd = pd.DataFrame([{"tf": "1h", "symbol": "ALL", "side": "long+short", "strategy_trades": n,
                         "strategy_expectancy_r": exp, "percentile": pct}])
    return {"random_baseline": rnd,
            "audit_funding": pd.DataFrame({"mismatches": [funding_bad]}),
            "audit_intrabar": pd.DataFrame({"violations": [0]}),
            "audit_causality": pd.DataFrame({"leaky": [False, True], "passed": [True, leaky_passes]})}


def test_verdict_rules_are_pre_registered():
    cfg = load_yaml("config/trend_pullback.yaml")
    assert verdict(_tables(97.0, 0.2), cfg)[0] == "EDGE"
    assert verdict(_tables(90.0, 0.2), cfg)[0] == "NO EDGE"       # positive but below 95th pct
    assert verdict(_tables(99.0, -0.1), cfg)[0] == "NO EDGE"      # high pct but losing
    assert verdict(_tables(99.0, 0.3, n=12), cfg)[0] == "INCONCLUSIVE"
    assert verdict(_tables(99.0, 0.3, funding_bad=1), cfg)[0] == "INCONCLUSIVE"
    assert verdict(_tables(99.0, 0.3, leaky_passes=True), cfg)[0] == "INCONCLUSIVE"
