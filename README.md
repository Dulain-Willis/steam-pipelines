# steam-pipelines

PySpark jobs and shared Python library for the Steam Data Platform.

## Contents

| Path | Description |
|------|-------------|
| `src/pipelines/` | Shared library: SteamSpy extraction, Spark session factory, MinIO client, ClickHouse client, replication state |
| `jobs/bronze/` | Bronze Spark job — raw JSON → Iceberg |
| `jobs/staging/` | Silver staging Spark job — parsed, typed, deduplicated |
| `jobs/replication/` | ClickHouse replication Spark job — snapshot-based CDC |
| `docker/Dockerfile` | Spark image built and pushed to ghcr.io by CI |
| `tests/` | pytest unit tests (mock-based, no live services) |

## Running Tests

```bash
python3 -m venv venv
venv/bin/pip install -e ".[dev]"
venv/bin/pytest tests/ -v
```

## Docker Image

CI builds and pushes `ghcr.io/Dulain-Willis/steam-pipelines:latest` on every merge to `main`. The image is consumed by `spark-master` and `spark-worker` in `steam-data-platform/compose.yml`.

## Architecture Decisions

ADRs live in [`steam-data-platform/docs/decisions/`](../steam-data-platform/docs/decisions/).
