# React SPA — Vite build, served by nginx.
# Build from repo root: docker build -f docker/web.Dockerfile -t accent-fleet-web .
#
# Two-stage build:
#   1. node:20-alpine compiles the bundle into /app/dist.
#   2. nginx:alpine serves it with the SPA fallback config and reverse-proxies
#      /v1/* to the api service so the browser sees a single origin (matches
#      the Vite dev proxy in web/vite.config.ts).
#
# VITE_API_BASE_URL is hardcoded to "/v1" at build time — the SPA always talks
# to its own origin, and nginx forwards from there. Override with --build-arg
# only if you want to point the bundle at an external backend.

FROM node:20-alpine AS build
WORKDIR /app

ARG VITE_API_BASE_URL=/v1
ENV VITE_API_BASE_URL=$VITE_API_BASE_URL

# Cache npm install in its own layer.
COPY web/package.json web/package-lock.json* ./
RUN npm install --no-audit --no-fund

COPY web/ ./
RUN npm run build

FROM nginx:1.27-alpine AS runtime
RUN rm /etc/nginx/conf.d/default.conf
COPY web/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=5 \
    CMD wget -q -O - http://127.0.0.1/healthz || exit 1
