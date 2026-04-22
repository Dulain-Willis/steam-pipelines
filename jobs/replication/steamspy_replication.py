"""Replicates an Iceberg silver table to a ClickHouse analytics table.

Table-agnostic: the Iceberg source and ClickHouse target are read from Spark
conf so a single script can replicate any table. Change detection uses Iceberg
snapshot IDs stored in analytics.replication_state.

Because silver jobs use .overwritePartitions() (OVERWRITE snapshots), the
standard Iceberg incremental scan API (APPEND-only) cannot be used. Instead,
this job queries the `entries` metadata table to find which dt partitions were
written in new snapshots, then re-reads those partitions from the current
snapshot and reloads them in ClickHouse.

Spark conf keys (set by Airflow DAG params):
    spark.replication.iceberg_table   — source Iceberg table (e.g. iceberg.steamspy.silver_stg_games)
    spark.replication.clickhouse_table — target ClickHouse table (e.g. analytics.steamspy_silver_stg_games)
    spark.replication.from_snapshot    — override start snapshot ID (exclusive)
    spark.replication.full_load        — "true" to ignore state and reload everything
"""
import os

from pyspark import AccumulatorParam
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pipelines.common.clickhouse.client import get_client
from pipelines.common.spark.session import build_spark_session
from pipelines.replication.state import get_last_clickhouse_snapshot_id, update_snapshot_id

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "clickhouse")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "analytics")


class _IntegerAccumulator(AccumulatorParam):
    """Spark AccumulatorParam for summing integer counts across executor partitions."""

    def zero(self, value):
        """Return the zero value for integer accumulation."""
        return 0

    def addInPlace(self, v1, v2):
        """Add two integer accumulator values in place."""
        return v1 + v2


def get_last_iceberg_snapshot_id(spark: SparkSession, iceberg_table: str) -> int:
    """Return the snapshot ID of the most recently committed Iceberg snapshot.

    Args:
        spark: A SparkSession configured with the Iceberg catalog.
        iceberg_table: Fully qualified Iceberg table name.

    Returns:
        The integer snapshot ID of the latest committed snapshot.

    Raises:
        RuntimeError: When no snapshots exist for the table.
    """
    snapshot_rows = spark.sql(f"""
        SELECT snapshot_id
        FROM {iceberg_table}.snapshots
        ORDER BY committed_at DESC
        LIMIT 1
    """
    ).collect()

    if not snapshot_rows:
        raise RuntimeError(f"No snapshots found for {iceberg_table} — has the silver job run yet?")

    return int(snapshot_rows[0][0])


def get_unreplicated_partition_dates(spark: SparkSession, iceberg_table: str, last_clickhouse_snapshot_id: int) -> list[str]:
    """Return dates for partitions written in snapshots committed after last_clickhouse_snapshot_id.

    Queries the Iceberg `entries` metadata table (status=1 means file was added in that
    snapshot). Works correctly for both APPEND and OVERWRITE operations.

    Args:
        spark: A SparkSession configured with the Iceberg catalog.
        iceberg_table: Fully qualified Iceberg table name.
        last_clickhouse_snapshot_id: The snapshot ID of the last successfully replicated
            snapshot. Only snapshots committed after this ID are inspected.

    Returns:
        A list of dt partition string values that were written in snapshots newer than
        last_clickhouse_snapshot_id.
    """
    partition_rows = spark.sql(f"""
        SELECT DISTINCT data_file.partition.dt
        FROM {iceberg_table}.entries
        WHERE snapshot_id IN (
            SELECT snapshot_id
            FROM {iceberg_table}.snapshots
            WHERE snapshot_id > {last_clickhouse_snapshot_id}
        )
        AND status = 1
    """).collect()
    return [row[0] for row in partition_rows if row[0] is not None]


def get_all_iceberg_partition_dates(spark: SparkSession, iceberg_table: str) -> list[str]:
    """Return all distinct dt partition values present in the Iceberg table.

    Args:
        spark: A SparkSession configured with the Iceberg catalog.
        iceberg_table: Fully qualified Iceberg table name.

    Returns:
        A list of all dt partition string values currently in the table.
    """
    partition_rows = spark.sql(f"""
        SELECT DISTINCT dt
        FROM {iceberg_table}
    """).collect()
    return [row[0] for row in partition_rows if row[0] is not None]


def delete_clickhouse_date_partitions(clickhouse_table: str, date_partitions: list[str]) -> None:
    """Clear the given date partitions from ClickHouse before reload for idempotency.

    Args:
        clickhouse_table: Fully qualified ClickHouse table name.
        date_partitions: List of dt string values whose rows should be deleted.
    """
    clickhouse_client = get_client()

    for date_partition in date_partitions:
        clickhouse_client.execute(
            f"ALTER TABLE {clickhouse_table} DELETE WHERE dt = %(dt)s",
            {"dt": date_partition},
        )

        print(f"  Cleared ClickHouse partition dt={date_partition}")


def write_dataframe_to_clickhouse(spark: SparkSession, dataframe, clickhouse_table: str) -> int:
    """Write a Spark DataFrame to ClickHouse using foreachPartition and clickhouse-driver.

    Args:
        spark: A SparkSession used to create the accumulator for row counting.
        dataframe: The Spark DataFrame to write.
        clickhouse_table: Fully qualified ClickHouse table name.

    Returns:
        The total number of rows written across all partitions.
    """
    host = CLICKHOUSE_HOST
    port = CLICKHOUSE_PORT
    user = CLICKHOUSE_USER
    password = CLICKHOUSE_PASSWORD
    database = CLICKHOUSE_DATABASE
    table = clickhouse_table

    rows_written_accumulator = spark.sparkContext.accumulator(0, _IntegerAccumulator())

    def write_partition(partition_rows):
        from clickhouse_driver import Client

        clickhouse_client = Client(host=host, port=port, user=user, password=password, database=database)
        rows_batch = [row.asDict() for row in partition_rows]
        if rows_batch:
            clickhouse_client.execute(f"INSERT INTO {table} VALUES", rows_batch)
            rows_written_accumulator.add(len(rows_batch))

    dataframe.foreachPartition(write_partition)
    return rows_written_accumulator.value


def main():
    """Detect changed Iceberg partitions and replicate them to ClickHouse.

    Reads table names from spark conf, then uses replication state from ClickHouse
    to determine which Iceberg snapshots have already been replicated, identifies dt
    partitions written in newer snapshots, clears those partitions in ClickHouse,
    reloads them from the current Iceberg snapshot, and updates the stored snapshot
    ID upon success.
    """
    spark = build_spark_session("steamspy-replication")
    clickhouse_client = get_client()

    try:
        iceberg_table = spark.conf.get("spark.replication.iceberg_table")
        clickhouse_table = spark.conf.get("spark.replication.clickhouse_table")
        state_key = iceberg_table

        print(f"Replicating {iceberg_table} → {clickhouse_table}")

        airflow_param_passed_snapshot_id = spark.conf.get("spark.replication.from_snapshot", "")
        airflow_param_passed_is_full_load = spark.conf.get("spark.replication.full_load", "false").lower() == "true"

        last_iceberg_snapshot_id = get_last_iceberg_snapshot_id(spark, iceberg_table)
        print(f"Latest Iceberg snapshot ID: {last_iceberg_snapshot_id}")

        if airflow_param_passed_is_full_load:
            print("full_load=true: reloading all partitions")
            affected_date_partitions = get_all_iceberg_partition_dates(spark, iceberg_table)

        else:
            override_snapshot_id = int(airflow_param_passed_snapshot_id) if airflow_param_passed_snapshot_id else None

            last_ch_snapshot_id = override_snapshot_id or get_last_clickhouse_snapshot_id(clickhouse_client, state_key)
            print(f"Last replicated snapshot ID: {last_ch_snapshot_id}")

            if last_ch_snapshot_id is not None and last_ch_snapshot_id == last_iceberg_snapshot_id:
                print("Already up to date — nothing to replicate")
                return

            if last_ch_snapshot_id is None:
                print("No previous state — performing full load")
                affected_date_partitions = get_all_iceberg_partition_dates(spark, iceberg_table)

            else:
                print(f"Detecting partitions changed since snapshot {last_ch_snapshot_id}")
                affected_date_partitions = get_unreplicated_partition_dates(spark, iceberg_table, last_ch_snapshot_id)

        if not affected_date_partitions:
            print("No affected partitions detected — nothing to replicate")
            return

        print(f"Affected dt partitions: {affected_date_partitions}")

        delete_clickhouse_date_partitions(clickhouse_table, affected_date_partitions)

        affected_partitions_df = spark.table(iceberg_table).filter(col("dt").isin(affected_date_partitions))

        rows_written = write_dataframe_to_clickhouse(spark, affected_partitions_df, clickhouse_table)
        print(f"Wrote {rows_written} rows to {clickhouse_table}")

        update_snapshot_id(clickhouse_client, state_key, get_last_iceberg_snapshot_id(spark, iceberg_table))
        print(f"State updated → snapshot_id={get_last_iceberg_snapshot_id(spark, iceberg_table)}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
