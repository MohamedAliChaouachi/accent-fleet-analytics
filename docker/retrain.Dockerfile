# Monthly retraining scheduler container.
#
# Wraps the shared base image with supercronic so the silhouette-gated
# retrain flow runs unattended on the first Monday of each month.
#
# Why supercronic and not vanilla cron?
#   - Vanilla cron strips environment variables from the parent process,
#     so the container's MLFLOW_TRACKING_URI / PG_* config never reach
#     the job. Supercronic inherits the container env verbatim.
#   - Vanilla cron sinks job output to syslog or the local mail spool,
#     neither of which is visible via `docker logs`. Supercronic streams
#     each job's stdout/stderr to its own stdout, so logs flow through
#     the standard container plumbing.
#   - Supercronic is a ~7MB static Go binary with no runtime deps. It's
#     designed for exactly this container-cron-replacement use case.
#
# Build from repo root:
#   docker build -f docker/retrain.Dockerfile -t accent-fleet-retrain-scheduler .
FROM accent-fleet-base:latest

USER root

# Pin to a known release. The Aptible release process publishes signed
# artifacts; pinning to a version tag (rather than `latest`) is enough
# for our threat model. Bump deliberately rather than letting it drift.
ARG SUPERCRONIC_VERSION=v0.2.29
RUN curl -fsSL \
        -o /usr/local/bin/supercronic \
        "https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-amd64" \
 && chmod +x /usr/local/bin/supercronic

# Copy the crontab + wrapper. Owned by fleet so the (non-root) runtime
# user can read them under supercronic.
COPY docker/retrain.crontab /app/docker/retrain.crontab
COPY docker/scripts/run_monthly_retrain.sh /app/docker/scripts/run_monthly_retrain.sh
RUN chmod +x /app/docker/scripts/run_monthly_retrain.sh \
 && chown -R fleet:fleet /app/docker

USER fleet

# Supercronic runs in the foreground; it IS the container's PID 1, so a
# `docker compose down` sends SIGTERM directly to the scheduler and it
# stops cleanly. No need for a tini/dumb-init shim.
CMD ["supercronic", "/app/docker/retrain.crontab"]
