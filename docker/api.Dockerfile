# FastAPI scoring service.
# Build from repo root:  docker build -f docker/api.Dockerfile -t accent-fleet-api .
FROM accent-fleet-base:latest

# API code lives in /app/app. Models live in /app/models (mounted as a volume in compose).
COPY --chown=fleet:fleet app ./app
COPY --chown=fleet:fleet models ./models

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=5 \
    CMD curl -fsS http://localhost:8000/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
