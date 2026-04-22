"""Spark session factory for the Steam Data Platform pipelines.

Provides a single entry point for constructing a configured SparkSession with
S3A (MinIO) credentials and an Iceberg REST catalog wired in. All pipeline
jobs should obtain their session through this module rather than constructing
one directly.
"""

import os

from dotenv import load_dotenv
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from pipelines.common.spark.config import apply_s3a_conf, get_iceberg_catalog_conf


def build_spark_session(app_name: str) -> SparkSession:
    """Build and return a SparkSession configured for this platform.

    Loads environment variables from a .env file, then applies S3A credentials
    and Iceberg catalog settings before constructing the session. If
    ``SPARK_MASTER_URL`` is set in the environment the session connects to that
    master; otherwise Spark falls back to local mode.

    Args:
        app_name: The application name shown in the Spark UI and logs.

    Returns:
        A fully configured SparkSession with S3A and Iceberg support enabled.
    """
    load_dotenv()

    spark_master_url = os.getenv("SPARK_MASTER_URL")

    conf = SparkConf()
    apply_s3a_conf(conf)

    # Add Iceberg catalog configuration
    for key, value in get_iceberg_catalog_conf().items():
        conf.set(key, value)

    builder = SparkSession.builder.appName(app_name).config(conf=conf)
    if spark_master_url:
        builder = builder.master(spark_master_url)

    return builder.getOrCreate()
