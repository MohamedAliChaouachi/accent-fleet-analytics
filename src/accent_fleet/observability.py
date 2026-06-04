"""
Cross-cutting observability — structured logging + Prometheus metrics.

Two concerns live here so every entrypoint (API, ETL, dashboard helpers)
configures them the same way:

  - **Structured logging** via structlog. JSON in non-dev environments,
    pretty console in dev. The stdlib `logging` module is also routed
    through structlog so libraries (sqlalchemy, prefect, uvicorn) emit
    structured records too.

  - **Prometheus metrics** as module-level singletons. The FastAPI
    middleware in `app/middleware.py` records into them; the
    `/metrics` route serves them in the standard exposition format.

Initialize from your entrypoint:

    from accent_fleet.observability import setup_logging
    setup_logging()  # call once at process start

Library code only needs `logger = structlog.get_logger(__name__)` —
no config needed.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any

import structlog
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

# ---------------------------------------------------------------------------
# Metrics — defined at module level so import order doesn't matter and tests
# can reset them via REGISTRY.unregister(...) if needed.
# ---------------------------------------------------------------------------
REGISTRY = CollectorRegistry()

http_requests_total = Counter(
    "accent_http_requests_total",
    "Count of HTTP requests handled, labelled by method, route template, status.",
    ["method", "route", "status"],
    registry=REGISTRY,
)

http_request_duration_seconds = Histogram(
    "accent_http_request_duration_seconds",
    "End-to-end request latency in seconds, labelled by method + route.",
    ["method", "route"],
    # Buckets tuned to our latency targets: 5ms..2s with extra resolution
    # under 100ms (where /score/risk should live) and 500ms (the
    # /devices/{id}/profile zone).
    buckets=(
        0.005, 0.01, 0.025, 0.05, 0.075, 0.1,
        0.15, 0.2, 0.3, 0.5, 0.75, 1.0, 2.0,
    ),
    registry=REGISTRY,
)

http_requests_in_flight = Gauge(
    "accent_http_requests_in_flight",
    "Number of HTTP requests currently being processed.",
    registry=REGISTRY,
)

# ETL-side metrics. The Prefect flow updates these on each run so we can
# alert on stale ETL or runaway error rates without scraping the etl_run_log
# table on every Prometheus poll.
etl_rows_processed_total = Counter(
    "accent_etl_rows_processed_total",
    "Rows processed by an ETL task, labelled by task name and outcome.",
    ["task", "outcome"],
    registry=REGISTRY,
)

etl_run_duration_seconds = Histogram(
    "accent_etl_run_duration_seconds",
    "Wall-clock duration of a complete ETL run, labelled by mode.",
    ["mode"],
    buckets=(1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0),
    registry=REGISTRY,
)

etl_last_success_timestamp = Gauge(
    "accent_etl_last_success_timestamp_seconds",
    "Unix timestamp of the last successful ETL run, labelled by mode.",
    ["mode"],
    registry=REGISTRY,
)

# ML drift metrics. Populated by the drift-detection Prefect task.
ml_feature_drift_score = Gauge(
    "accent_ml_feature_drift_score",
    "KL divergence between training and latest scoring batch, per feature.",
    ["feature"],
    registry=REGISTRY,
)

# Retraining metrics. Populated by the monthly retrain flow. Two gauges
# (candidate + production) instead of one labelled gauge so dashboards
# can plot them on the same axis without label tricks.
ml_candidate_silhouette = Gauge(
    "accent_ml_candidate_silhouette",
    "Silhouette score of the latest training candidate (gated, not yet promoted).",
    registry=REGISTRY,
)

ml_production_silhouette = Gauge(
    "accent_ml_production_silhouette",
    "Silhouette score of the current Production-stage clustering model.",
    registry=REGISTRY,
)

ml_last_retrain_promoted_timestamp = Gauge(
    "accent_ml_last_retrain_promoted_timestamp_seconds",
    "Unix timestamp of the most recent successful promotion to Production. "
    "Alerting on `time() - this > N` catches a stale model that's been "
    "training-gated out N retrains in a row.",
    registry=REGISTRY,
)

# Risk-model (Isolation Forest) retraining metrics. The risk gate is
# stability-based — silhouette doesn't apply — so we expose the two
# tail shares ('critical', 'high') for candidate and Production, plus
# the score PSI we measured at promotion time. Dashboards can plot
# candidate-vs-production on the same axis to spot a held promotion at
# a glance.
ml_candidate_risk_critical_share = Gauge(
    "accent_ml_candidate_risk_critical_share",
    "Share of devices in the 'critical' risk category in the latest "
    "candidate run (gated, may or may not have been promoted).",
    registry=REGISTRY,
)

ml_candidate_risk_high_share = Gauge(
    "accent_ml_candidate_risk_high_share",
    "Share of devices in the 'high' risk category in the latest "
    "candidate run (gated, may or may not have been promoted).",
    registry=REGISTRY,
)

ml_production_risk_critical_share = Gauge(
    "accent_ml_production_risk_critical_share",
    "Share of devices in the 'critical' risk category in the current "
    "Production-stage risk model. None when no Production version exists "
    "(cold-start) — gauge is simply not set in that case.",
    registry=REGISTRY,
)

ml_production_risk_high_share = Gauge(
    "accent_ml_production_risk_high_share",
    "Share of devices in the 'high' risk category in the current "
    "Production-stage risk model.",
    registry=REGISTRY,
)

ml_risk_score_psi = Gauge(
    "accent_ml_risk_score_psi",
    "PSI between the candidate's risk-score distribution and the current "
    "Production one, measured at retrain time. Promotion gate trips when "
    "this crosses the configured threshold (default 0.25).",
    registry=REGISTRY,
)

ml_last_risk_retrain_promoted_timestamp = Gauge(
    "accent_ml_last_risk_retrain_promoted_timestamp_seconds",
    "Unix timestamp of the most recent successful risk-model promotion to "
    "Production. Mirrors `ml_last_retrain_promoted_timestamp` but tracks "
    "the risk model separately so the two retrain cadences are observable "
    "without ambiguity.",
    registry=REGISTRY,
)


def render_metrics() -> tuple[bytes, str]:
    """Return (body, content_type) for the /metrics HTTP response."""
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST


# ---------------------------------------------------------------------------
# Structured logging — structlog routes through stdlib logging so library
# loggers (uvicorn, sqlalchemy, prefect) flow into the same JSON output.
# ---------------------------------------------------------------------------
_LOGGING_CONFIGURED = False


def _is_dev() -> bool:
    """Pretty-print mode when running locally; JSON in prod / CI / docker."""
    return os.environ.get("PIPELINE_ENV", "dev").lower() == "dev"


def setup_logging(level: str | None = None) -> None:
    """
    Configure structlog + stdlib logging once per process.

    Safe to call multiple times — subsequent calls are no-ops so unit tests
    that import the API module don't re-add handlers.
    """
    # Idempotency guard: only the first call configures logging.
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    # Resolve the effective level (arg > env > INFO default).
    log_level = (level or os.environ.get("PIPELINE_LOG_LEVEL", "INFO")).upper()
    numeric_level = getattr(logging, log_level, logging.INFO)

    # Stdlib root logger — single StreamHandler, structlog renders the format.
    root = logging.getLogger()
    # Strip any pre-existing handlers (pytest, uvicorn auto-config, etc.) so
    # we don't emit each line twice.
    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(numeric_level)
    root.addHandler(handler)
    root.setLevel(numeric_level)

    # Shared processor chain — these run for both structlog and stdlib loggers.
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    # Pretty console in dev, machine-readable JSON everywhere else.
    if _is_dev():
        renderer: Any = structlog.dev.ConsoleRenderer(colors=False)
    else:
        renderer = structlog.processors.JSONRenderer()

    # Route stdlib log records through the same processor chain.
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )
    handler.setFormatter(formatter)

    # Configure structlog itself.
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    _LOGGING_CONFIGURED = True
