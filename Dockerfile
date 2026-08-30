# syntax=docker/dockerfile:1

FROM node:24-bookworm-slim AS frontend-build

WORKDIR /build
COPY frontend/package.json frontend/package-lock.json ./frontend/
RUN npm ci --prefix frontend

COPY frontend ./frontend
COPY docs/API.yaml ./docs/API.yaml
RUN npm --prefix frontend run generate:api \
    && npm --prefix frontend run build


FROM python:3.12-slim-bookworm AS server

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    OPENOCTOPUS_HOST=0.0.0.0

WORKDIR /app
COPY server ./server
COPY --from=frontend-build /build/frontend/dist ./server/src/openctopus_server/assets/web
RUN python -m pip install --no-cache-dir ./server \
    && groupadd --gid 10001 openoctopus \
    && useradd --uid 10001 --gid openoctopus --create-home --shell /usr/sbin/nologin openoctopus

USER openoctopus

EXPOSE 8080
CMD ["python", "-m", "openctopus_server.main"]
