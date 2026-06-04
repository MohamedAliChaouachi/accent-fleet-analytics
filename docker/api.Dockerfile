# FastAPI scoring service.
# Build from repo root:  docker build -f docker/api.Dockerfile -t accent-fleet-api .
FROM accent-fleet-base:latest

# v0.9.0 auth packages — added on top of the base layer so existing base
# images don't need a full rebuild. When the base image is next rebuilt
# from requirements.txt these become no-ops (already satisfied).
USER root
RUN pip install --default-timeout=300 \
        "argon2-cffi>=23.1.0" \
        "PyJWT>=2.9.0" \
        "email-validator>=2.2.0" \
        "boto3>=1.34.0"
USER fleet

# API code lives in /app/app. Models live in /app/models (mounted as a volume in compose).
# We also re-COPY src/ on top of the base image so the editable-installed
# `accent_fleet` package picks up v0.9.0 changes (Settings.auth_enforcement,
# RLS listener in db/engine.py) without rebuilding the base from scratch.
COPY --chown=fleet:fleet src ./src
COPY --chown=fleet:fleet app ./app
COPY --chown=fleet:fleet models ./models

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=5 \
    CMD curl -fsS http://localhost:8000/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
