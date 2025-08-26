FROM python:3.12-slim AS base

FROM base AS builder

COPY --from=ghcr.io/astral-sh/uv:0.8.3 /uv /bin/uv

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

WORKDIR /app

COPY uv.lock pyproject.toml README.md /app/

RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync --frozen --no-install-project --no-dev

COPY ./src /app/src

RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync --frozen --no-dev

FROM base

COPY --from=builder /app /app

ENV PATH="/app/.venv/bin:$PATH"

CMD ["pytest", "-v"]