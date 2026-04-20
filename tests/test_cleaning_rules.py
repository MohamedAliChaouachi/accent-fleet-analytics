"""
Unit tests for the cleaning rule engine.

Each rule C1..C7 has a dedicated test. These are the tests that guarantee
the SQL and Python implementations stay in lockstep with the YAML catalogue.
"""

from __future__ import annotations

from datetime import datetime

import polars as pl

from accent_fleet.cleaning import load_rule_engine


# ---------------------------------------------------------------------------
def test_engine_loads_from_yaml():
    """Smoke test: the catalogue is readable and has the expected rules."""
    engine = load_rule_engine()
    ids = {r.id for r in engine.rules}
    assert {"C1", "C2", "C3", "C4", "C5", "C6", "C7"}.issubset(ids)


def test_rules_for_path_table():
    """The path table should be targeted by C1, C2, C3, C4, C5."""
    engine = load_rule_engine()
    ids = {r.id for r in engine.rules_for("path")}
    assert {"C1", "C2", "C3", "C4", "C5"}.issubset(ids)
    # C6 is stop-only; C7 is device-only
    assert "C6" not in ids
    assert "C7" not in ids


# ---------------------------------------------------------------------------
def test_c1_rejects_epoch_dates(path_sample_df):
    """C1: any begin_path_time before 2019-10-01 must be rejected."""
    engine = load_rule_engine()
    out, result = engine.apply(path_sample_df, table="path")
    # The 1970 row should be gone
    assert out.filter(pl.col("begin_path_time") < datetime(2019, 10, 1)).is_empty()
    assert result.rejected_by_rule.get("C1", 0) == 1


def test_c2_rejects_negative_duration(path_sample_df):
    """C2: path_duration <= 0 is a data error."""
    engine = load_rule_engine()
    out, result = engine.apply(path_sample_df, table="path")
    assert out.filter(pl.col("path_duration") <= 0).is_empty()
    assert result.rejected_by_rule.get("C2", 0) >= 1


def test_c3_rejects_zero_distance(path_sample_df):
    """C3: distance_driven <= 0 is GPS noise."""
    engine = load_rule_engine()
    out, result = engine.apply(path_sample_df, table="path")
    assert out.filter(pl.col("distance_driven") <= 0).is_empty()
    assert result.rejected_by_rule.get("C3", 0) >= 1


def test_c4_nullifies_fuel_overflow(path_sample_df):
    """C4: fuel_used outside [0, 500] becomes NULL, the row survives."""
    engine = load_rule_engine()
    out, result = engine.apply(path_sample_df, table="path")
    # The overflow row (fuel_used=2.1B) should still be present, but fuel nullified
    assert result.nullified_by_rule.get("C4", 0) >= 1
    # No surviving row should still have an overflow fuel value
    assert out.filter(pl.col("fuel_used") > 500).is_empty()


def test_c5_clamps_speed(path_sample_df):
    """C5: max_speed is clamped at 200, never > 200 after rules apply."""
    engine = load_rule_engine()
    out, result = engine.apply(path_sample_df, table="path")
    assert out["max_speed"].max() <= 200
    assert result.clamped_by_rule.get("C5", 0) >= 1


def test_c6_rejects_stop_duration_out_of_bounds():
    """C6: stops must be 0 < duration < 1 year."""
    engine = load_rule_engine()
    stop_df = pl.DataFrame([
        {"tenant_id": 235, "device_id": 425239,
         "stop_start": datetime(2026, 4, 10),
         "stop_duration": 120},                 # valid
        {"tenant_id": 235, "device_id": 425239,
         "stop_start": datetime(2026, 4, 10),
         "stop_duration": 0},                   # rejected
        {"tenant_id": 235, "device_id": 425239,
         "stop_start": datetime(2026, 4, 10),
         "stop_duration": 99_999_999},          # > 1 year, rejected
    ])
    out, result = engine.apply(stop_df, table="stop")
    assert out.height == 1
    assert result.rejected_by_rule.get("C6", 0) == 2


def test_c7_rejects_devices_without_vehicle():
    """C7: device rows without vehicule_id are dropped at dim time."""
    engine = load_rule_engine()
    dev_df = pl.DataFrame([
        {"id_device": 1, "tenant_id": 235, "vehicule_id": 42},
        {"id_device": 2, "tenant_id": 235, "vehicule_id": None},
    ])
    out, result = engine.apply(dev_df, table="device")
    assert out.height == 1
    assert result.rejected_by_rule.get("C7", 0) == 1


# ---------------------------------------------------------------------------
def test_counts_accumulate_correctly(path_sample_df):
    """Sanity: total_in == total_out + rejected (for reject-type rules)."""
    engine = load_rule_engine()
    out, result = engine.apply(path_sample_df, table="path")
    assert result.total_in == path_sample_df.height
    assert result.total_out == out.height
    assert result.total_out + result.total_rejected == result.total_in
