FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /uvx /bin/

WORKDIR /app

# Install deps first so this layer is cached across code-only changes.
# uv.lock is picked up automatically once you've run `uv lock` locally and
# committed it; until then this resolves fresh. --no-install-project skips
# installing app/ as a package (it isn't copied in yet) and installs only
# the dependencies.
COPY pyproject.toml uv.lock* ./
RUN uv sync --no-dev --no-install-project

COPY app ./app

EXPOSE 4566

CMD ["uv", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "4566"]