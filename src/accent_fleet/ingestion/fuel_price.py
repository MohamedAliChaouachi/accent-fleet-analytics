"""
Fuel-price fetcher.

Pulls the live diesel price from a configurable provider and stores it in
``warehouse.ref_fuel_price``. The executive (sql/33) and fleet-efficiency
(sql/36) dashboard views read the latest row from that table to value fuel
cost in DT/L.

Design goals:
  - Provider-agnostic. The endpoint, auth header, and the JSON field that
    holds the price are all driven by .env (see ``Settings`` in config.py).
    No Tunisia-specific free API exists for the regulated "gasoil 50" price,
    so the integration is generic and you wire in whichever provider/key you
    have access to.
  - Never break the dashboards. If the provider is unset or unreachable, the
    fetcher writes nothing and the views keep using the last good value (or
    the seeded STIR reference). The price is regulated/slow-moving, so the
    refresh is gated to run at most once per ``FUEL_PRICE_REFRESH_DAYS``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx
import structlog
from sqlalchemy import text
from sqlalchemy.engine import Connection

from accent_fleet.config import Settings, settings
from accent_fleet.db import transaction

log = structlog.get_logger()

# A sane band for a per-litre pump price in DT. Anything outside this is almost
# certainly a parsing error (wrong JSON field, price-per-gallon, cents, etc.)
# and we refuse it rather than poison the dashboards.
_MIN_PRICE = 0.1
_MAX_PRICE = 100.0

_FALLBACK_SOURCE = "STIR-reference"


@dataclass(frozen=True)
class RefreshResult:
    """Outcome of a refresh attempt."""

    status: str  # "inserted" | "skipped_fresh" | "disabled" | "failed"
    price_per_litre: float | None = None
    source: str | None = None
    detail: str | None = None


def _extract(payload: Any, dotted_path: str) -> Any:
    """Walk a dotted path into a parsed-JSON object.

    Each segment indexes a dict by key, or a list by integer position.
    Raises KeyError/IndexError/TypeError if the path doesn't resolve — the
    caller treats that as a fetch failure.
    """
    node = payload
    # Descend one segment at a time: integer index for lists, key for dicts.
    for segment in dotted_path.split("."):
        node = node[int(segment)] if isinstance(node, list) else node[segment]
    return node


def fetch_price_from_provider(s: Settings) -> tuple[float, str] | None:
    """Call the configured provider and return ``(price_per_litre, source)``.

    Returns None when the provider is disabled, unreachable, or returns an
    unparseable / out-of-band value. Every failure is logged, never raised,
    so a flaky API can't fail the ETL run.
    """
    # No URL configured → provider disabled.
    url = s.fuel_price_api_url.strip()
    if not url:
        return None

    # Optional bearer/API-key header.
    headers = {}
    if s.fuel_price_api_key:
        headers["Authorization"] = s.fuel_price_api_key

    # Fetch and parse the price; any error is swallowed (logged, not raised).
    try:
        resp = httpx.get(url, headers=headers, timeout=10.0)
        resp.raise_for_status()
        raw = _extract(resp.json(), s.fuel_price_json_path)
        price = float(raw)
    except (httpx.HTTPError, KeyError, IndexError, TypeError, ValueError) as exc:
        log.warning("fuel_price.fetch_failed", url=url, error=str(exc))
        return None

    # Sanity-band the value to reject parsing errors (wrong unit/field).
    if not (_MIN_PRICE < price < _MAX_PRICE):
        log.warning(
            "fuel_price.out_of_band",
            url=url,
            price=price,
            min=_MIN_PRICE,
            max=_MAX_PRICE,
        )
        return None

    # Source label = the provider host, for audit in the ref table.
    source = httpx.URL(url).host or url
    return price, source


def _live_price_age_days(conn: Connection, fuel_type: str) -> float | None:
    """Age (in days) of the latest *live* price row, or None if there is none.

    "Live" = a real fetch, i.e. source <> the STIR seed. Used by the
    staleness gate so we only call the provider once per refresh window.
    """
    row = conn.execute(
        text(
            """
            SELECT EXTRACT(EPOCH FROM (NOW() - MAX(effective_at))) / 86400.0
              FROM warehouse.ref_fuel_price
             WHERE fuel_type = :ft
               AND source <> :fallback
            """
        ),
        {"ft": fuel_type, "fallback": _FALLBACK_SOURCE},
    ).scalar()
    return float(row) if row is not None else None


def refresh_fuel_price(
    conn: Connection | None = None,
    *,
    force: bool = False,
) -> RefreshResult:
    """Fetch the live fuel price and append it to warehouse.ref_fuel_price.

    Gated to run at most once per ``FUEL_PRICE_REFRESH_DAYS`` unless ``force``
    is set. On any failure the table is left untouched, so the dashboards keep
    the last good value (or the seeded STIR reference).

    Pass ``conn`` to participate in an existing transaction; omit it to open a
    short-lived one.
    """
    # No connection passed → open our own short-lived transaction.
    if conn is None:
        with transaction() as own_conn:
            return refresh_fuel_price(own_conn, force=force)

    s = settings()
    fuel_type = s.fuel_price_fuel_type

    # Provider not configured → nothing to do, dashboards keep last value.
    if not s.fuel_price_api_url.strip():
        log.info("fuel_price.disabled", reason="FUEL_PRICE_API_URL unset")
        return RefreshResult(status="disabled")

    # Staleness gate: skip the fetch if a recent live price already exists.
    if not force:
        age = _live_price_age_days(conn, fuel_type)
        if age is not None and age < s.fuel_price_refresh_days:
            log.info(
                "fuel_price.skip_fresh",
                fuel_type=fuel_type,
                age_days=round(age, 1),
                refresh_days=s.fuel_price_refresh_days,
            )
            return RefreshResult(status="skipped_fresh", detail=f"{age:.1f}d old")

    # Call the provider; on failure leave the table untouched.
    fetched = fetch_price_from_provider(s)
    if fetched is None:
        return RefreshResult(status="failed", detail="provider returned no usable price")

    # Append the new price (idempotent on (fuel_type, effective_at)).
    price, source = fetched
    conn.execute(
        text(
            """
            INSERT INTO warehouse.ref_fuel_price
                (fuel_type, price_per_litre, currency, source, effective_at)
            VALUES (:ft, :price, :currency, :source, NOW())
            ON CONFLICT (fuel_type, effective_at) DO NOTHING
            """
        ),
        {
            "ft": fuel_type,
            "price": price,
            "currency": s.fuel_price_currency,
            "source": source,
        },
    )
    log.info(
        "fuel_price.updated",
        fuel_type=fuel_type,
        price_per_litre=price,
        currency=s.fuel_price_currency,
        source=source,
    )
    return RefreshResult(
        status="inserted",
        price_per_litre=price,
        source=source,
    )
