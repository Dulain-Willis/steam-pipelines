from pipelines.common.spark.session import build_spark_session
from pyspark.sql.functions import from_json, explode, col, lit, row_number, desc, lower
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType
from pyspark.sql.window import Window

app_schema = StructType([
    StructField("appid", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("developer", StringType(), True),
    StructField("publisher", StringType(), True),
    StructField("score_rank", StringType(), True),
    StructField("positive", IntegerType(), True),
    StructField("negative", IntegerType(), True),
    StructField("userscore", IntegerType(), True),
    StructField("owners", StringType(), True),
    StructField("average_forever", IntegerType(), True),
    StructField("average_2weeks", IntegerType(), True),
    StructField("median_forever", IntegerType(), True),
    StructField("median_2weeks", IntegerType(), True),
    StructField("ccu", IntegerType(), True),
    StructField("price", StringType(), True),
    StructField("initialprice", StringType(), True),
    StructField("discount", StringType(), True),
])


def main():
    spark = build_spark_session("steamspy-silver-stg-games")

    ds = spark.conf.get("spark.steamspy.ds")

    df_bronze = spark.table("iceberg.steamspy.bronze").filter(col("dt") == ds)

    json_map = MapType(StringType(), StructType(app_schema))

    # Takes the payload column and makes the id a string and casts the value (json payload) to a struct from a string
    df_parsed = (
        df_bronze
        .withColumn(
            "struct_payload",
            from_json("payload", json_map)
        )
    )

    # Takes what was a huge string (now struct) of JSON and makes each row its own JSON entry for an appid
    df_exploded = (
        df_parsed
        .select(
            explode("struct_payload").alias("appid_key", "game"),
            "ingestion_timestamp",
            "run_id",
        )
    )

    # Goes to each row and unnests each JSON entry into actual columns
    df_flattened = (
        df_exploded
        .select(
            col("game.*"),
            "run_id",
            "ingestion_timestamp",
        )
    )

    window = Window.partitionBy("appid").orderBy(desc("ingestion_timestamp"))

    df_deduped = (
        df_flattened
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1).drop("rn")
        .withColumn("dt", lit(ds))
    )

    df_final = (
        df_deduped.select(
            "appid",
            lower(col("name")).alias("game_title"),
            lower(col("developer")).alias("developer"),
            lower(col("publisher")).alias("publisher"),
            "score_rank",
            "positive",
            "negative",
            "userscore",
            "owners",
            "average_forever",
            "average_2weeks",
            "median_forever",
            "median_2weeks",
            "ccu",
            "price",
            "initialprice",
            "discount",
            "run_id",
            "ingestion_timestamp",
            "dt",
        )
    )

    # Write to Iceberg table with atomic commits
    table_name = "iceberg.steamspy.silver_stg_games"

    # Check if table exists, create if not
    if not spark.catalog.tableExists(table_name):
        print(f"Creating Iceberg table: {table_name}")
        (df_final.writeTo(table_name)
            .partitionedBy("dt")
            .create())
    else:
        # Overwrite partitions for current dt (SCD Type 1)
        print(f"Overwriting Iceberg table: {table_name}")
        (df_final.writeTo(table_name)
            .overwritePartitions())

    # Verify write
    count = spark.table(table_name).filter(col("dt") == ds).count()
    print(f"Iceberg table {table_name} contains {count} rows for dt={ds}")

    spark.stop()


if __name__ == "__main__":
    main()
