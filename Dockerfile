# Osiris app image — one image, two roles (API surface, worker). The command picks
# the role (see deploy/docker-compose.full.yml). Browsers for the experimental
# co-browse path are NOT bundled (keyless collection needs none).
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /opt/osiris
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1

# install deps first (cached) from the lockfile, then the source
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY . .
RUN uv sync --frozen --no-dev

# default role is the API; the worker overrides `command` in compose.
EXPOSE 8011
CMD ["uv", "run", "uvicorn", "src.api.app:app", "--host", "0.0.0.0", "--port", "8011"]
