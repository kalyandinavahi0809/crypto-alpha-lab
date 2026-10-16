import pytest

from qdp.layers import Store, ensure_layers


@pytest.fixture
def store(tmp_path):
    s = Store(tmp_path / "lake")
    ensure_layers(s)
    return s
