# Streamlit dashboard.
# Build from repo root:  docker build -f docker/dashboard.Dockerfile -t accent-fleet-dashboard .
FROM accent-fleet-base:latest

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
