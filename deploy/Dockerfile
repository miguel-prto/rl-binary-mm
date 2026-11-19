# Normaliser image: a one-shot container that turns finished raw files into
# Parquet next to the recorder. Build from the repository root:
#   docker build -f deploy/Dockerfile -t rl-normalize .
# Multi-arch base image, so it builds on x86-64 and on ARM64 servers alike.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# git: marketrec is a git dependency (pinned tag in pyproject.toml).
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

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
    RL_DATA_DIR=/data
VOLUME /data

ENTRYPOINT ["python", "-m", "rl_binary_mm.store.normalize"]
