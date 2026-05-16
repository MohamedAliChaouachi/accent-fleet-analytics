"""Chart-suggester heuristic tests."""

from __future__ import annotations

from app.ai.services.chart_suggester import suggest


def test_empty_rows_returns_table():
    assert suggest(["x"], []) == "table"


def test_single_row_returns_table():
    assert suggest(["total_trips"], [{"total_trips": 100}]) == "table"


def test_wide_result_returns_table():
    cols = [f"c{i}" for i in range(6)]
    rows = [dict.fromkeys(cols, i) for i in range(10)]
    assert suggest(cols, rows) == "table"


def test_time_series_with_year_month_returns_line():
    rows = [
        {"year_month": "2026-01", "total_trips": 100},
        {"year_month": "2026-02", "total_trips": 120},
        {"year_month": "2026-03", "total_trips": 150},
    ]
    assert suggest(["year_month", "total_trips"], rows) == "line"


def test_fleet_date_returns_line():
    rows = [
        {"fleet_date": "2026-04-01", "total_alerts": 10},
        {"fleet_date": "2026-04-02", "total_alerts": 12},
        {"fleet_date": "2026-04-03", "total_alerts": 14},
    ]
    assert suggest(["fleet_date", "total_alerts"], rows) == "line"


def test_risk_distribution_returns_pie():
    rows = [
        {"risk_category": "low", "device_count": 30},
        {"risk_category": "moderate", "device_count": 20},
        {"risk_category": "high", "device_count": 8},
        {"risk_category": "critical", "device_count": 2},
    ]
    assert suggest(["risk_category", "device_count"], rows) == "pie"


def test_top_vehicles_returns_bar():
    rows = [
        {"matricule": "TN-1", "total_cost": 5000},
        {"matricule": "TN-2", "total_cost": 4500},
        {"matricule": "TN-3", "total_cost": 4000},
    ]
    assert suggest(["matricule", "total_cost"], rows) == "bar"


def test_high_cardinality_category_no_pie():
    """Even with a pie-friendly column name, too many categories → bar."""
    rows = [{"risk_category": f"cat{i}", "device_count": i} for i in range(20)]
    assert suggest(["risk_category", "device_count"], rows) != "pie"


def test_leading_null_doesnt_break_type_detection():
    rows = [
        {"year_month": "2026-01", "total_trips": None},
        {"year_month": "2026-02", "total_trips": 120},
        {"year_month": "2026-03", "total_trips": 150},
    ]
    assert suggest(["year_month", "total_trips"], rows) == "line"
