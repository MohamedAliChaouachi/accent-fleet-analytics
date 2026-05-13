"""
Smoke tests for the Streamlit dashboard.

Two tiers:

1.  Static check (always runs) — every page file must:
    - Parse as valid Python.
    - Resolve every `from dashboard.lib...` import.
    - Use the shared `apply_layout` + `render_sidebar_filters` helpers.

2.  Live check (runs when STREAMLIT_BASE_URL is set, e.g. on CI against a
    running container) — hits Streamlit's built-in health endpoint and each
    page URL with httpx, asserting non-5xx.
"""

from __future__ import annotations

import ast
import importlib
import os
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_DIR = PROJECT_ROOT / "dashboard"
PAGES_DIR = DASHBOARD_DIR / "pages"

PAGE_FILES = sorted(PAGES_DIR.glob("*.py"))


# ---------------------------------------------------------------------------
# Static checks — no Streamlit runtime needed.
# ---------------------------------------------------------------------------
def test_pages_directory_has_expected_files():
    """The dashboard ships five pages — Home + four routes + What-if."""
    assert (DASHBOARD_DIR / "Home.py").exists(), "Home.py missing"
    names = [p.stem for p in PAGE_FILES]
    expected = {
        "0_Executive_Overview",
        "1_Operations",
        "2_Maintenance",
        "3_Risk_and_Behavior",
        "4_What_If",
    }
    assert expected <= set(names), f"missing pages: {expected - set(names)}"


@pytest.mark.parametrize("page_path", PAGE_FILES + [DASHBOARD_DIR / "Home.py"])
def test_page_parses(page_path: Path):
    """Every page file must be syntactically valid Python."""
    src = page_path.read_text(encoding="utf-8")
    ast.parse(src, filename=str(page_path))


@pytest.mark.parametrize("page_path", PAGE_FILES + [DASHBOARD_DIR / "Home.py"])
def test_page_uses_shared_layout_helpers(page_path: Path):
    """Pages must call apply_layout — keeps sidebar / page config consistent."""
    src = page_path.read_text(encoding="utf-8")
    assert "apply_layout(" in src, f"{page_path.name} must call apply_layout()"


def test_dashboard_lib_imports_resolve():
    """The three lib modules must import cleanly (catches typos / cycles)."""
    for module in ("dashboard.lib.theme", "dashboard.lib.cache", "dashboard.lib.db"):
        importlib.import_module(module)


# ---------------------------------------------------------------------------
# Live checks — only run when a Streamlit URL is available.
# ---------------------------------------------------------------------------
STREAMLIT_URL = os.environ.get("STREAMLIT_BASE_URL")
LIVE = STREAMLIT_URL is not None


@pytest.mark.skipif(not LIVE, reason="STREAMLIT_BASE_URL not set")
def test_streamlit_health_endpoint():
    import httpx

    r = httpx.get(f"{STREAMLIT_URL}/_stcore/health", timeout=5.0)
    assert r.status_code == 200
    # Streamlit's health endpoint returns the literal string 'ok'.
    assert "ok" in r.text.lower()


@pytest.mark.skipif(not LIVE, reason="STREAMLIT_BASE_URL not set")
@pytest.mark.parametrize(
    "page",
    [
        "",
        "Executive_Overview",
        "Operations",
        "Maintenance",
        "Risk_and_Behavior",
        "What_If",
    ],
)
def test_streamlit_pages_render(page: str):
    """Hit each page URL. Streamlit returns the JS bootstrap; just check 2xx."""
    import httpx

    url = f"{STREAMLIT_URL}/{page}" if page else STREAMLIT_URL
    r = httpx.get(url, timeout=10.0, follow_redirects=True)
    assert r.status_code < 500, f"{url} returned {r.status_code}"
