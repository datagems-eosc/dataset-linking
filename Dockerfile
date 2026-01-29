# Install uv
FROM python:3.12-slim

# Copy uv binary
COPY --from=ghcr.io/astral-sh/uv:0.7.4 /uv /bin/uv

# Accept version from Docker build argument
ARG APP_VERSION=dev
ENV APP_VERSION=${APP_VERSION}

# Change the working directory to the `app` directory
WORKDIR /app

# Copy the lockfile, `pyproject.toml` and the README into the image
COPY uv.lock /app/uv.lock
COPY pyproject.toml /app/pyproject.toml
COPY README.md /app/README.md

# Install dependencies
RUN uv sync --frozen --no-install-project

# Copy the dmm_api folder into the image
COPY dl /app/dl
COPY statistic /app/statistic
COPY templates /app/templates

# Sync the project
RUN uv sync --frozen

CMD ["uv", "run", "uvicorn", "dl.main:app", "--host", "0.0.0.0", "--port", "5000"]
