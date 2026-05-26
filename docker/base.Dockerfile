# Base image shared by API and ETL containers.
# Builds the accent_fleet Python package once and caches dependencies.
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    # Make the workdir importable so scripts like `python scripts/run_batch.py`
    # resolve sibling packages without each entry point patching sys.path.
    PYTHONPATH=/app

# System packages:
#   - build-essential, libpq-dev: needed by psycopg + scikit-learn wheels on slim
#   - curl: used by container healthchecks
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first so they are layer-cached.
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Copy the package + SQL + config. App/ETL layers add their own code on top.
COPY pyproject.toml ./
COPY src ./src
COPY sql ./sql
COPY config ./config
COPY scripts ./scripts

# Install the package itself (editable to keep image small and rebuilds fast).
RUN pip install -e .

# Default to running as a non-root user for safety.
RUN useradd --create-home --shell /bin/bash fleet \
 && chown -R fleet:fleet /app
USER fleet
