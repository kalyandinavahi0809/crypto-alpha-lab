# crypto-alpha-lab
Crypto alpha research on **qdp**, a small layered quant data platform (`src/qdp`).

## qdp layers

```
venue adapter ──> RAW ──> CLEANED ──> FEATURES ──> SIGNALS ──> BACKTESTS
 (binance_usdm)   monthly   dedup,      causal      order        event-driven
                  partitions gap log,   swings,     intents      perp simulator,
                  checksums  mark join,  zones,                  metrics, reports
                             contract    sweeps
                             changes
```

- `qdp.layers.Store`: hive-partitioned parquet lake with partition-atomic upserts.
- `qdp.instruments`: effective-dated instrument master. Perps (`<venue>.<symbol>.perp`) are separate from spot and carry tick, multiplier, fees, maintenance-margin brackets and funding interval.
- `qdp.adapters`: venue-agnostic adapter interface. `binance_usdm` reads the Binance public archive with SHA-256 verification.
- `qdp.grid`: own-grid operations (gap detection, exact joins, no forward fill).
- `qdp.causal.assert_causal`: a truncation test for look-ahead.
- `qdp.sim`: event-driven perp simulator with isolated margin, mark-price liquidation, realized funding, maker/taker fees and slippage.

## Reproduce the Trend Pullback + Liquidity Sweep study

```bash
pip install -r requirements.txt
PYTHONPATH=src python -m qdp.cli ingest                      # RAW (idempotent)
PYTHONPATH=src python -m qdp.cli clean                       # CLEANED + instrument master
PYTHONPATH=src python -m qdp.experiments.trend_pullback      # FEATURES/SIGNALS/BACKTESTS + tables/figures
PYTHONPATH=src python -m qdp.experiments.report_md           # reports/trend_pullback/REPORT.md
pytest
```

All study parameters live in `config/trend_pullback.yaml`. Results, tables and figures are in
[`reports/trend_pullback/`](reports/trend_pullback/REPORT.md). The data lake (`data/`) is
rebuilt from source and is not committed.
