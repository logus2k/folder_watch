# folder_watch — single-process app container (FastAPI + watcher poll loop).
#
# IMPORTANT: base image MUST be glibc (Debian slim), NOT alpine/MUSL.
# valkey-glide ships a Rust core with no MUSL wheels, so an Alpine base
# would fail to install/run the client.
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies first for layer caching.
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Shared bus SDK (canonical envelope + firing contract) from the sibling agent_bus repo,
# supplied as a named build context by docker-compose (busclient_sdk = ../agent_bus/sdk/python).
COPY --from=busclient_sdk . /opt/busclient_sdk
RUN pip install /opt/busclient_sdk

# Application source (package lives under src/folder_watch).
COPY src/ ./src/
ENV PYTHONPATH=/app/src

# Single-process entrypoint: uvicorn serving the admin API, which owns the watcher.
CMD ["python", "-m", "folder_watch.app"]
