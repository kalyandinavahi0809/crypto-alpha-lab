from qdp.config import load_yaml
from qdp.experiments.trend_pullback import Cell, grid_cells, is_hypothesis


def test_grid_is_full_cartesian_product_with_unique_ids():
    cfg = load_yaml("config/trend_pullback.yaml")
    cells = grid_cells(cfg)
    assert len(cells) == 3 * 4 * 2 * 2 * 2 * 2          # k x exit x entry x sweep x bos x trigger
    assert len({c.cid for c in cells}) == len(cells)
    hyp = [c for c in cells if is_hypothesis(c, cfg)]
    # selectable on IS: swing k x exit x entry type, with the hypothesis rules fixed
    assert len(hyp) == 24
    assert all(c.sweep and c.bos_close and c.trigger == "last" for c in hyp)


def test_cell_target_r_and_trail():
    assert Cell(3, "2.0", "limit", True, True, "last").target_r == 2.0
    assert Cell(3, "trail", "limit", True, True, "last").target_r == 0.0


def test_is_oos_split_is_configured_as_specified():
    v = load_yaml("config/trend_pullback.yaml")["validation"]
    assert v["is_end"] == "2024-01-01" and v["oos_start"] == "2024-01-01"
    wf = v["walk_forward"]
    assert (wf["train_months"], wf["test_months"]) == (12, 3)
