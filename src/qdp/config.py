"""Configuration loading for qdp."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "config"


def load_yaml(path: str | Path) -> dict[str, Any]:
    path = Path(path)
    if not path.is_absolute():
        path = REPO_ROOT / path
    with open(path) as fh:
        return yaml.safe_load(fh) or {}


def platform_config() -> dict[str, Any]:
    return load_yaml(CONFIG_DIR / "qdp.yaml")


def data_root() -> Path:
    root = os.environ.get("QDP_DATA_ROOT") or platform_config().get("data_root", "data")
    root = Path(root)
    return root if root.is_absolute() else REPO_ROOT / root
