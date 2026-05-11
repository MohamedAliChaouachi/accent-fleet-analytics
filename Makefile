# Accent Fleet Analytics — common dev workflow.
#
# Database hosting: by default we assume Postgres is hosted on Azure
# (Azure Database for PostgreSQL) and we run only the application stack
# in docker-compose. To run a fully local Postgres for dev, use the
# `localdb` targets.

PROJECT := accent-fleet-analytics
COMPOSE := docker compose

# ---------------------------------------------------------------------------
.PHONY: help
help:
	@echo "Common targets:"
	@echo "  build         build all docker images (base + api + dashboard + etl)"
	@echo "  up            start the application stack (api, dashboard, mlflow, etl)"
	@echo "  down          stop the stack"
	@echo "  logs          tail logs"
	@echo "  ps            show container status"
	@echo "  seed          run ETL bootstrap + small backfill against the configured DB"
	@echo "  train         train + register the clustering model in MLflow"
	@echo "  test          run pytest (skips integration unless PG is reachable)"
	@echo "  lint          run ruff"
	@echo ""
	@echo "  up-localdb    spin up a local Postgres alongside the stack (no Azure DB)"
	@echo "  down-localdb  stop the local Postgres"

# ---------------------------------------------------------------------------
.PHONY: build
build:
	$(COMPOSE) build base
	$(COMPOSE) build api dashboard etl

.PHONY: up
up:
	$(COMPOSE) up -d mlflow api dashboard etl

.PHONY: down
down:
	$(COMPOSE) down

.PHONY: logs
logs:
	$(COMPOSE) logs -f --tail=200

.PHONY: ps
ps:
	$(COMPOSE) ps

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
.PHONY: seed
seed:
	$(COMPOSE) run --rm etl python scripts/run_batch.py bootstrap
	$(COMPOSE) run --rm etl python scripts/run_batch.py backfill --from 2024-01-01

.PHONY: train
train:
	$(COMPOSE) run --rm etl python scripts/train_clustering.py

# ---------------------------------------------------------------------------
# Quality
# ---------------------------------------------------------------------------
.PHONY: test
test:
	pytest -ra -m "not slow"

.PHONY: lint
lint:
	ruff check src app dashboard tests scripts

# ---------------------------------------------------------------------------
# Optional local Postgres
# ---------------------------------------------------------------------------
.PHONY: up-localdb
up-localdb:
	$(COMPOSE) --profile localdb up -d postgres
	$(COMPOSE) up -d mlflow api dashboard etl

.PHONY: down-localdb
down-localdb:
	$(COMPOSE) --profile localdb down
