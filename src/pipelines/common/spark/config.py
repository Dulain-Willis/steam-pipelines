"""Spark configuration builders for S3A (MinIO) and Iceberg REST catalog.

Module-level constants read MinIO credentials from the environment at import
time so they are shared across all config helpers without repeated env lookups.
All functions return plain dicts of Spark property key/value pairs that can be
applied to either a SparkConf object or passed as spark_conf to
SparkSubmitOperator.
"""

import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")


def get_s3a_conf() -> dict:
    """Return Hadoop S3A config keys for connecting Spark to MinIO.

    Uses path-style access and disables virtual-hosted-style URLs, which is
    required for MinIO compatibility.

    Returns:
        A dict of ``spark.hadoop.fs.s3a.*`` properties ready to pass to
        SparkConf or SparkSubmitOperator.
    """
    return {
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.path.style.access": "true",
    }


def apply_s3a_conf(spark_conf):
    """Apply S3A properties to a SparkConf object in place.

    Args:
        spark_conf (SparkConf): The Spark configuration object to mutate.

    Returns:
        The same SparkConf instance with S3A properties set.
    """
    for key, value in get_s3a_conf().items():
        spark_conf.set(key, value)
    return spark_conf


def get_spark_resource_conf() -> dict:
    """Return memory and shuffle settings tuned for the local Docker environment.

    Targets a ~1.5 GB total Spark footprint (512 MB + 256 MB driver overhead,
    512 MB + 256 MB executor overhead) to leave headroom for Airflow, MinIO, and
    other services sharing the Docker pool. Shuffle partitions are reduced from
    the Spark default of 200 to avoid excessive overhead on small datasets.

    Returns:
        A dict of Spark resource properties ready to pass to SparkConf or
        SparkSubmitOperator.
    """
    return {
        "spark.driver.memory": "512m",
        "spark.executor.memory": "512m",
        "spark.driver.memoryOverhead": "256m",
        "spark.executor.memoryOverhead": "256m",
        "spark.sql.shuffle.partitions": "4",  # Reduce from default 200; dataset is small
    }


def get_iceberg_catalog_conf() -> dict:
    """Return Spark properties for the Iceberg REST catalog backed by MinIO.

    Registers a catalog named ``iceberg`` that points at the REST catalog
    service and uses S3FileIO with the same MinIO credentials as the S3A
    layer. The ``us-east-1`` region is a required placeholder for the AWS SDK
    v2 client; MinIO itself ignores the value.

    Returns:
        A dict of ``spark.sql.catalog.iceberg.*`` properties ready to pass to
        SparkConf or SparkSubmitOperator.
    """
    iceberg_rest_uri = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")

    return {
        # Catalog registration
        "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg.type": "rest",
        "spark.sql.catalog.iceberg.uri": iceberg_rest_uri,

        # Storage configuration - dedicated warehouse bucket for all Iceberg tables
        "spark.sql.catalog.iceberg.warehouse": "s3a://warehouse/",

        # S3/MinIO integration
        "spark.sql.catalog.iceberg.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.iceberg.s3.endpoint": MINIO_ENDPOINT,
        "spark.sql.catalog.iceberg.s3.access-key-id": MINIO_ACCESS_KEY,
        "spark.sql.catalog.iceberg.s3.secret-access-key": MINIO_SECRET_KEY,
        "spark.sql.catalog.iceberg.s3.path-style-access": "true",
        "spark.sql.catalog.iceberg.s3.region": "us-east-1",  # Required by AWS SDK v2; MinIO ignores the value

        # Performance tuning
        "spark.sql.catalog.iceberg.cache-enabled": "false",  # Disable for dev simplicity
    }
