FROM python:3.13.11

# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Add virtual environment to PATH so we can use installed packages
ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# Copy dependency files first (better layer caching)
COPY "pyproject.toml" "uv.lock" ".python-version" ./

# Install dependencies from lock file (ensures reproducible builds)
RUN uv sync --locked

COPY ingest_data.py ingest_data.py 

ENTRYPOINT [ "python", "ingest_data.py --pg-user=root   --pg-pass=root   --pg-host=localhost   --pg-port=5432   --pg-db=ny_taxi   --target-table=yellow_taxi_trips   --year=2021   --month=01   --chunksize=10000" ]