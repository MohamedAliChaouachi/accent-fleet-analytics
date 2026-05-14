from __future__ import annotations

import pandas as pd
import pytest

from accent_fleet.ml import train_clustering


def test_required_modeling_tenant_missing_raises(monkeypatch):
    monkeypatch.setattr(
        train_clustering,
        "load_pipeline_config",
        lambda: {
            "modeling": {
                "expected_tenants": [235, 238, 264, 1787, 7486],
                "required_tenants": [7486],
            }
        },
    )
    df = pd.DataFrame({"tenant_id": [235, 238, 264, 1787]})

    with pytest.raises(ValueError, match="7486"):
        train_clustering._validate_tenant_coverage(df)


def test_required_modeling_tenant_present_passes(monkeypatch):
    monkeypatch.setattr(
        train_clustering,
        "load_pipeline_config",
        lambda: {
            "modeling": {
                "expected_tenants": [235, 238, 264, 1787, 7486],
                "required_tenants": [7486],
            }
        },
    )
    df = pd.DataFrame({"tenant_id": [235, 238, 264, 1787, 7486]})

    train_clustering._validate_tenant_coverage(df)
