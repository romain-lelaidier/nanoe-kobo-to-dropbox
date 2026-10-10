FROM python:3.14-slim

# Pin the uv version for reproducibility
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencies first (better layer caching)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Then your code
COPY . .
RUN uv sync --frozen --no-dev

# Don't run as root
RUN useradd -m kobo-to-dropbox \
    && mkdir -p /tmp/kobo-to-dropbox \
    && chown -R kobo-to-dropbox:kobo-to-dropbox /app /tmp/kobo-to-dropbox
USER kobo-to-dropbox

CMD ["uv", "run", "--no-sync", "python", "kobo_to_dropbox.py"]