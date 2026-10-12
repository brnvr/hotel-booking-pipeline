FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.13.0 /uv /uvx /usr/local/bin/

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml uv.lock requirements.txt ./
COPY src/ src/
RUN uv sync --locked --no-cache
COPY tests/ tests/

ENV PATH="/app/.venv/bin:$PATH"
ENV PYSPARK_PYTHON=/app/.venv/bin/python
CMD ["python", "-m", "pipeline", "--input-dir", "/input", "--output-dir", "/output"]
