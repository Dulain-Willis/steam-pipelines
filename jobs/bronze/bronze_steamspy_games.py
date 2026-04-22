from pipelines.common.spark.session import build_spark_session
from pyspark.sql.functions import current_timestamp, to_date, lit, udf, input_file_name
from pyspark.sql.types import IntegerType
import re


def main():
    spark = build_spark_session("steamspy-bronze")

    ds = spark.conf.get("spark.steamspy.ds")
    run_id = spark.conf.get("spark.steamspy.run_id")

    landing_path = f"s3a://landing/steamspy/raw/request=all/dt={ds}/"

    df = spark.read.text(landing_path, wholetext=True)

    df = df.withColumnRenamed("value", "payload")

    def extract_page(path):
        if not path:
            raise ValueError("File path is None or empty")
        match = re.search(r'page=(\d+)\.json', path)
        if not match:
            raise ValueError(f"Could not extract page number from: {path}")
        return int(match.group(1))

    extract_page_udf = udf(extract_page, IntegerType())

    df = (
        df.withColumn("source_file", input_file_name())
          .withColumn("page", extract_page_udf("source_file"))
          .withColumn("ingestion_timestamp", current_timestamp())
          .withColumn("ingestion_date", to_date("ingestion_timestamp"))
          .withColumn("source", lit("steamspy"))
          .withColumn("run_id", lit(run_id))
          .withColumn("dt", lit(ds))
    )

    df_final = (
        df.select(
            "payload",
            "ingestion_timestamp",
            "ingestion_date",
            "source",
            "run_id",
            "dt",
            "page",
            "source_file"
        )
    )

    table_name = "iceberg.steamspy.bronze"

    if not spark.catalog.tableExists(table_name):
        print(f"Creating Iceberg table: {table_name}")
        df_final.writeTo(table_name).partitionedBy("dt").create()
    else:
        print(f"Overwriting Iceberg table partition dt={ds}: {table_name}")
        df_final.writeTo(table_name).overwritePartitions()

    count = spark.table(table_name).filter(f"dt = '{ds}'").count()
    print(f"Iceberg table {table_name} contains {count} rows for dt={ds}")

    spark.stop()


if __name__ == "__main__":
    main()
