# ETL / Prefect worker container.
# Build from repo root:  docker build -f docker/etl.Dockerfile -t accent-fleet-etl .
FROM accent-fleet-base:latest

# Default command is the incremental scheduler loop. Override at `docker compose run`
# to run bootstrap or backfill on demand:
#   docker compose run --rm etl python scripts/run_batch.py bootstrap
#   docker compose run --rm etl python scripts/run_batch.py backfill --from 2024-01-01
CMD ["python", "scripts/run_batch.py", "incremental"]
