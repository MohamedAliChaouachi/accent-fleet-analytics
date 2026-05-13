#!/usr/bin/env bash
# =============================================================================
# backup_postgres.sh — logical dump of the warehouse Postgres.
# =============================================================================
#
# What it captures:
#   A single `pg_dump --format=custom` of $PG_DATABASE. The custom format is
#   the right default — smaller than plain SQL, restorable with pg_restore,
#   and supports partial restores (--table, --schema). It's also the format
#   Azure Database for PostgreSQL's docs recommend.
#
# How it connects:
#   We don't shell into the postgres container directly because in production
#   the DB lives on Azure Database for PostgreSQL — there is no local
#   container. Instead, we spin up a one-shot `postgres:16-alpine` client
#   container with .env mounted, which works identically against:
#     • the `accent_postgres` local-dev container (via --profile localdb)
#     • an Azure-hosted server (.env has PG_HOST=<server>.postgres.database.azure.com)
#     • any other remote Postgres reachable from the host
#
#   pg_dump versions are forward-compatible with newer servers within the same
#   major line, so pinning the client to 16 is safe against Azure's 14/15/16.
#
# Output:
#   ./backups/postgres/postgres-${PG_DATABASE}-${STAMP}.dump
#   (one file per run, no tar wrap — pg_dump's custom format already compresses)
#
# Why this exists:
#   The warehouse is the system of record for every fact_*, mart_*, and
#   etl_run_log row. A corrupted Azure region, an accidental TRUNCATE, or a
#   bad migration can wipe months of incremental loads in seconds. The
#   incremental pipeline is idempotent FROM staging, but staging is also in
#   the same database — so without an external dump, total loss is total.
#   A daily logical dump moves the blast radius from "everything" to
#   "one day, restorable in <30 min on a fresh instance".
#
# Restore (manual, rare):
#   pg_restore --clean --if-exists --no-owner --no-privileges \
#              -h <host> -U <user> -d <database> <file.dump>
#
# Usage:
#   docker/scripts/backup_postgres.sh                    # write to ./backups/postgres
#   docker/scripts/backup_postgres.sh /mnt/nas/pg        # write somewhere else
#   docker/scripts/backup_postgres.sh --retain-days 30   # also prune old backups
#
# Cron suggestion (host crontab, 03:30 every night — 15 min after MLflow):
#   30 3 * * *  cd /opt/accent-fleet-analytics && \
#               docker/scripts/backup_postgres.sh --retain-days 30 \
#               >> /var/log/accent-postgres-backup.log 2>&1
# =============================================================================

set -euo pipefail

BACKUP_DIR="./backups/postgres"
RETAIN_DAYS=""
ENV_FILE="${ENV_FILE:-.env}"
NETWORK="${BACKUP_NETWORK:-fleet-net}"

# ----- argparse (tiny) ------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --retain-days)
      RETAIN_DAYS="$2"
      shift 2
      ;;
    -h|--help)
      sed -n '2,55p' "$0"
      exit 0
      ;;
    *)
      BACKUP_DIR="$1"
      shift
      ;;
  esac
done

# ----- preflight ------------------------------------------------------------
if [[ ! -f "$ENV_FILE" ]]; then
  echo "error: env file '$ENV_FILE' not found. Set ENV_FILE=… to override." >&2
  exit 1
fi

# Pull PG_* from .env without exec'ing it (the file may contain comments,
# spaces, or values that bash would mis-evaluate). We only need the names
# for the filename + sanity check; the actual values are passed to the
# pg_dump container via --env-file, never expanded by this shell.
PG_DATABASE=$(grep -E '^PG_DATABASE=' "$ENV_FILE" | tail -n 1 | cut -d= -f2- | tr -d '"' || true)
PG_HOST=$(grep -E '^PG_HOST=' "$ENV_FILE" | tail -n 1 | cut -d= -f2- | tr -d '"' || true)
PG_DATABASE="${PG_DATABASE:-accent_fleet}"
if [[ -z "$PG_HOST" ]]; then
  echo "error: PG_HOST not set in $ENV_FILE" >&2
  exit 1
fi

mkdir -p "$BACKUP_DIR"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUTFILE="${BACKUP_DIR}/postgres-${PG_DATABASE}-${STAMP}.dump"

echo "[$(date -u +%FT%TZ)] postgres backup → $OUTFILE"
echo "  • host=$PG_HOST database=$PG_DATABASE"

# If PG_HOST resolves to a docker-network alias (e.g. 'accent_postgres' for
# --profile localdb), the client container must join that network. For an
# Azure-hosted server PG_HOST is a public FQDN and the network attach is
# harmless — docker silently routes externally.
NETWORK_ARG=()
if docker network inspect "$NETWORK" >/dev/null 2>&1; then
  NETWORK_ARG=(--network "$NETWORK")
else
  echo "  • note: docker network '$NETWORK' not found; using host network"
fi

# ----- pg_dump --------------------------------------------------------------
# --format=custom    : compressed, restorable with pg_restore
# --no-owner         : restore on a fresh DB without recreating role grants
# --no-privileges    : same reason
# --jobs is NOT used because --format=custom is single-threaded by design;
# use --format=directory + --jobs if dumps grow past ~5 minutes.
#
# Password goes through PGPASSWORD (read from .env by --env-file). We could
# use a ~/.pgpass mount but env is one less moving part.
docker run --rm \
  "${NETWORK_ARG[@]}" \
  --env-file "$ENV_FILE" \
  -v "$(pwd)/${BACKUP_DIR#./}:/backup" \
  postgres:16-alpine \
  sh -c '
    set -eu
    : "${PG_HOST:?}" "${PG_USER:?}" "${PG_DATABASE:?}"
    export PGPASSWORD="${PG_PASSWORD:-}"
    export PGSSLMODE="${PG_SSLMODE:-prefer}"
    pg_dump \
      --host="$PG_HOST" \
      --port="${PG_PORT:-5432}" \
      --username="$PG_USER" \
      --dbname="$PG_DATABASE" \
      --format=custom \
      --no-owner \
      --no-privileges \
      --file="/backup/'"$(basename "$OUTFILE")"'"
  '

SIZE=$(du -h "$OUTFILE" | cut -f1)
echo "  • wrote $SIZE → $OUTFILE"

# ----- retention ------------------------------------------------------------
if [[ -n "$RETAIN_DAYS" ]]; then
  echo "  • pruning backups older than ${RETAIN_DAYS} days"
  find "$BACKUP_DIR" -name 'postgres-*.dump' -type f \
       -mtime "+${RETAIN_DAYS}" -print -delete || true
fi

echo "[$(date -u +%FT%TZ)] done"
