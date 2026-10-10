FROM python:3.12-slim AS base

COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /uvx /bin/

WORKDIR /app

# Install deps first so this layer is cached across code-only changes.
# uv.lock is picked up automatically once you've run `uv lock` locally and
# committed it; until then this resolves fresh. --no-install-project skips
# installing the project as a package (its source isn't copied in yet) and
# installs only the dependencies.
COPY pyproject.toml uv.lock* ./


# The demo workloads (event consumer, end-to-end demo script). They are plain
# AWS-SDK clients of the emulator, so this image carries boto3 but none of
# the dev tooling, and deliberately not the emulator's own source.
FROM base AS demo

RUN uv sync --no-dev --group demo --no-install-project
# Run straight from the virtualenv. `uv run` would try to build and install
# the project itself, and app/ isn't in this image.
ENV PATH="/app/.venv/bin:$PATH"

COPY demo ./demo

CMD ["python", "-m", "demo.consumer"]


# The emulator. Kept as the last stage so a bare `docker build .` produces it.
FROM base AS emulator

RUN uv sync --no-dev --no-install-project

COPY app ./app

EXPOSE 4566

CMD ["uv", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "4566"]