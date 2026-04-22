"""Test that all pipeline imports work correctly."""


def test_common_spark_config():
    from pipelines.common.spark.config import get_s3a_conf, get_spark_resource_conf, apply_s3a_conf

    s3a_conf = get_s3a_conf()
    assert "spark.hadoop.fs.s3a.endpoint" in s3a_conf

    resource_conf = get_spark_resource_conf()
    assert "spark.driver.memory" in resource_conf


def test_common_spark_session():
    from pipelines.common.spark.session import build_spark_session
    assert callable(build_spark_session)


def test_common_storage_minio_client():
    from pipelines.common.storage.minio_client import create_minio_client
    assert callable(create_minio_client)


def test_common_storage_minio_loader():
    from pipelines.common.storage.minio_loader import upload_to_minio
    assert callable(upload_to_minio)


def test_steamspy_extract():
    from pipelines.steamspy.extract import call_steamspy_api
    assert callable(call_steamspy_api)


if __name__ == "__main__":
    test_common_spark_config()
    test_common_spark_session()
    test_common_storage_minio_client()
    test_common_storage_minio_loader()
    test_steamspy_extract()
    print("All imports successful!")
