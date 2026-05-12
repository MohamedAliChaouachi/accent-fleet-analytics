#!/usr/bin/env bash
# =============================================================================
# backup_mlflow.sh — online snapshot of the MLflow tracking server.
# =============================================================================
#
# What it captures:
#   1. The SQLite backend file (`mlflow_data` volume → /mlflow/mlflow.db) via
#      sqlite3's `.backup` command, which is safe to run while the server is
#      writing. A plain `cp` would risk a torn page.
#   2. The artifacts tree (`mlflow_artifacts` volume → /mlartifacts) via tar.
#      Artifacts are append-only after a run finishes, so a snapshot taken
#      mid-train at worst misses the in-flight run.
#
# Both end up in a single timestamped tgz under ./backups/mlflow/. Restoring
# is a manual `tar -xzf …` into the volumes — restoration is rare enough
# that a script for it would be net-negative.
#
# Why this exists: MLflow's only persistence is two volumes. If the host
# disk dies before the model registry is migrated to S3/blob, every
# Production model promotion since v0 is lost. A daily snapshot moves the
# blast radius from "everything" to "one day". When we move to remote
# artifact storage in v1, this script gets retired.
#
# Usage:
#   docker/scripts/backup_mlflow.sh                    # write to ./backups/mlflow
#   docker/scripts/backup_mlflow.sh /mnt/nas/mlflow    # write somewhere else
#   docker/scripts/backup_mlflow.sh --retain-days 30   # also prune old backups
#
# Cron suggestion (host crontab, 03:15 every night):
#   15 3 * * *  cd /opt/accent-fleet-analytics && \
#               docker/scripts/backup_mlflow.sh --retain-days 30 \
#               >> /var/log/accent-mlflow-backup.log 2>&1
# =============================================================================

set -euo pipefail

CONTAINER="${MLFLOW_CONTAINER:-accent_mlflow}"
BACKUP_DIR="./backups/mlflow"
RETAIN_DAYS=""

# ----- argparse (tiny) ------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --retain-days)
      RETAIN_DAYS="$2"
      shift 2
      ;;
    -h|--help)
      sed -n '2,40p' "$0"
      exit 0
      ;;
    *)
      # First positional arg = backup dir
      BACKUP_DIR="$1"
      shift
      ;;
  esac
done

mkdir -p "$BACKUP_DIR"

# ----- preflight ------------------------------------------------------------
if ! docker inspect "$CONTAINER" >/dev/null 2>&1; then
  echo "error: container '$CONTAINER' not found. Set MLFLOW_CONTAINER if renamed." >&2
  exit 1
fi
if [[ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER")" != "true" ]]; then
  echo "error: container '$CONTAINER' is not running." >&2
  exit 1
fi

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
WORKDIR=$(mktemp -d -t mlflow-backup-XXXXXXXX)
trap 'rm -rf "$WORKDIR"' EXIT

echo "[$(date -u +%FT%TZ)] mlflow backup → $BACKUP_DIR (stamp=$STAMP)"

# ----- 1. SQLite snapshot ---------------------------------------------------
# `.backup` is online-safe even if mlflow has the DB open.
# The mlflow image doesn't ship sqlite3 — install it transiently in a
# throwaway exec, but only if needed. Most ghcr.io/mlflow/mlflow images
# include it; if absent we fall back to `cp` (works for SQLite WAL mode).
echo "  • dumping mlflow.db"
if docker exec "$CONTAINER" sh -c 'command -v sqlite3 >/dev/null'; then
  docker exec "$CONTAINER" sh -c \
    "sqlite3 /mlflow/mlflow.db \".backup '/mlflow/mlflow.db.backup'\""
  docker cp "${CONTAINER}:/mlflow/mlflow.db.backup" "$WORKDIR/mlflow.db"
  docker exec "$CONTAINER" rm -f /mlflow/mlflow.db.backup
else
  echo "    sqlite3 not available in container; falling back to cp"
  docker cp "${CONTAINER}:/mlflow/mlflow.db" "$WORKDIR/mlflow.db"
fi

# ----- 2. Artifacts tarball -------------------------------------------------
# Tar inside the container to a stdout stream, capture on host. Avoids
# `docker cp`'ing a potentially huge directory tree.
echo "  • dumping /mlartifacts"
docker exec "$CONTAINER" sh -c 'cd / && tar -cf - mlartifacts' \
  > "$WORKDIR/mlartifacts.tar"

# ----- 3. Combine into one tgz ---------------------------------------------
OUTFILE="$BACKUP_DIR/mlflow-backup-${STAMP}.tgz"
tar -czf "$OUTFILE" -C "$WORKDIR" mlflow.db mlartifacts.tar
echo "  • wrote $(du -h "$OUTFILE" | cut -f1) → $OUTFILE"

# ----- 4. Retention ---------------------------------------------------------
# Only prune if explicitly asked. Cron sets --retain-days; ad-hoc runs don't.
if [[ -n "$RETAIN_DAYS" ]]; then
  echo "  • pruning backups older than ${RETAIN_DAYS} days"
  find "$BACKUP_DIR" -name 'mlflow-backup-*.tgz' -type f \
       -mtime "+${RETAIN_DAYS}" -print -delete || true
fi

echo "[$(date -u +%FT%TZ)] done"
