from datetime import datetime, timezone

from clickhouse_driver import Client


STATE_TABLE = "analytics.replication_state"


def get_last_clickhouse_snapshot_id(client: Client, table_name: str) -> int | None:
    """Returns the last successfully processed Iceberg snapshot ID, or None for first run."""

    rows = client.execute(
        f"SELECT last_snapshot_id FROM {STATE_TABLE} FINAL WHERE table_name = %(table_name)s",
        {"table_name": table_name},
    )

    if rows and rows[0][0] is not None:
        return int(rows[0][0])

    return None


def update_snapshot_id(client: Client, table_name: str, snapshot_id: int) -> None:
    """Records the latest replicated snapshot ID. ReplacingMergeTree deduplicates by updated_at."""
    
    client.execute(
        f"INSERT INTO {STATE_TABLE} (table_name, last_snapshot_id, last_replicated_at) VALUES",
        [
            {
                "table_name": table_name,
                "last_snapshot_id": snapshot_id,
                "last_replicated_at": datetime.now(timezone.utc),
            }
        ],
    )
