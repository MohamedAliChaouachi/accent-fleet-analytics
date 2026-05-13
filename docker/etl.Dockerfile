# ETL / Prefect worker container.
# Build from repo root:  docker build -f docker/etl.Dockerfile -t accent-fleet-etl .
FROM accent-fleet-base:latest

# Default command: run the incremental flow in a 5-minute loop. Keeping the
# container long-lived (rather than relying on restart-policy churn) makes
# `docker compose exec etl ...` viable for ad-hoc commands like training.
# Override at `docker compose run` to run bootstrap, backfill, or training:
#   docker compose run --rm etl python scripts/run_batch.py --mode bootstrap
#   docker compose run --rm etl python scripts/run_batch.py --mode backfill --from 2024-01-01
#   docker compose run --rm etl python scripts/train_clustering.py --month-from 2025-01
CMD ["sh", "-c", "while true; do python scripts/run_batch.py --mode incremental || echo 'incremental run failed; retrying in 5 min'; sleep 300; done"]
