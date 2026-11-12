# Recorder image. Build from the repository root:
#   docker compose -f recorder/docker-compose.yml build
# Multi-arch base image, so it builds on x86-64 and on ARM64 servers alike.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

# Dependencies first: this layer is rebuilt only when the lock file changes.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY python ./python
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH" \
    TFG_DATA_DIR=/data
VOLUME /data

# Exec form: Python is PID 1 and receives SIGTERM, so files are closed cleanly.
ENTRYPOINT ["python", "-m", "tfg.recorder"]
