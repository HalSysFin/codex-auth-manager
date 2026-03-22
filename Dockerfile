FROM node:20-alpine AS frontend-build

WORKDIR /frontend
COPY frontend/package.json frontend/tsconfig.json frontend/tsconfig.app.json frontend/vite.config.ts frontend/index.html ./
COPY frontend/src ./src
RUN npm install
RUN npm run build

FROM python:3.11-slim

ARG DEBIAN_FRONTEND=noninteractive
ARG CODEX_INSTALL_CMD="npm install -g @openai/codex"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl nodejs npm \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Assumes Codex CLI is available as an npm package.
RUN sh -c "$CODEX_INSTALL_CMD"

COPY . .
COPY --from=frontend-build /frontend/dist ./frontend/dist

ENV CODEX_CLI_BIN=codex \
    CODEX_AUTH_PATH=/root/.codex/auth.json \
    CALLBACK_STORE_DIR=/root/.codex-switch/callbacks \
    CODEX_PROFILES_DIR=/root/.codex-switch/profiles \
    USAGE_DB_PATH=/root/.codex-switch/auth-manager.sqlite3 \
    DATABASE_URL=postgresql://auth_manager:auth_manager@postgres:5432/auth_manager \
    LOGIN_SESSION_TTL_SECONDS=600

EXPOSE 8080

VOLUME ["/root/.codex", "/root/.codex-switch/profiles", "/root/.codex-switch/callbacks", "/root/.codex-switch"]

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
