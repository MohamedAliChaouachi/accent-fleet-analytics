# Streamlit dashboard.
# Build from repo root:  docker build -f docker/dashboard.Dockerfile -t accent-fleet-dashboard .
FROM accent-fleet-base:latest

# v0.9.0 (post-B7 + FORCE RLS): the dashboard reuses the shared SQLAlchemy
# engine (dashboard/lib/db.py -> accent_fleet.db.engine), which has the RLS
# GUC setter wired on its `begin` event. The listener does
# `from app.auth.principal import current_principal` to fetch the request-
# scoped Principal. Without `app/` in the dashboard image, that import
# soft-fails and the listener no-ops; with `accent_app` (NOBYPASSRLS) +
# FORCE RLS that means every dashboard query is RLS-clamped to zero rows.
# Bundling `app/` lets dashboard/lib/theme.py stamp a service superadmin
# Principal at page init so the listener emits SET LOCAL ROLE
# accent_superadmin and the transaction sees all tenants.
#
# We ALSO overlay src/ on top of the base image (same pattern as
# api.Dockerfile) so the dashboard picks up v0.9.0 changes to
# accent_fleet.db.engine — specifically the superadmin role-swap branch
# added in M5. Without this overlay, the dashboard inherits whatever
# snapshot of src/ was baked into accent-fleet-base:latest when it was
# last built, which can be older than HEAD on a slow rebuild cycle and
# silently breaks the RLS listener even when the Principal IS stamped
# correctly.
COPY --chown=fleet:fleet src ./src
COPY --chown=fleet:fleet app ./app
COPY --chown=fleet:fleet dashboard ./dashboard

EXPOSE 8501

HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=5 \
    CMD curl -fsS http://localhost:8501/_stcore/health || exit 1

# --server.address=0.0.0.0 so the port is reachable from outside the container.
# --browser.gatherUsageStats=false to keep the container quiet.
CMD ["streamlit", "run", "dashboard/Home.py", \
     "--server.address=0.0.0.0", \
     "--server.port=8501", \
     "--browser.gatherUsageStats=false"]
